from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, MutableMapping

import polars as pl
import streamlit as st
import streamlit.components.v1 as components

import ynab_il_importer.map_updates as map_updates
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.validation as review_validation
import ynab_il_importer.review_app.working_schema as working_schema
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api


DEFAULT_SOURCE = Path("outputs/proposed_transactions.parquet")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.parquet")
DEFAULT_CATEGORIES = Path("outputs/ynab_categories.csv")
DEFAULT_RESUME_SENTINEL = "__DEFAULT_RESUME__"
QUIT_REQUEST_FILENAME = "quit_requested.json"


EDITOR_STATE_PREFIXES = (
    "payee_override_",
    "payee_select_",
    "source_payee_",
    "source_category_",
    "target_payee_override_",
    "target_payee_select_",
    "target_category_select_",
    "memo_append_",
    "decision_action_",
    "reviewed_",
    "propagate_action_source_",
    "propagate_action_target_",
    "update_maps_",
    "category_select_",
    "show_all_categories_",
    "group_payee_select_",
    "group_payee_override_",
    "group_category_",
    "group_memo_append_",
    "group_decision_",
    "group_show_all_categories_",
    "group_update_maps_",
    "group_row_page_",
    "group_page",
    "row_page",
    "_split_editor_",
)
EDITOR_STATE_KEYS = {
    "expanded_row_id",
    "expanded_group_fp",
    "expanded_group_row_id",
    "group_page",
    "row_page",
    "_split_editor",
}


def _default_reviewed_path(input_path: Path) -> Path:
    suffix = input_path.suffix or ".csv"
    base = input_path.with_suffix("") if input_path.suffix else input_path
    return Path(f"{base}_reviewed{suffix}")


def _cli_has_flag(flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in sys.argv[1:])


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="YNAB proposed transactions review app")
    parser.add_argument(
        "--in",
        dest="input_path",
        default=str(DEFAULT_SOURCE),
        help="Initial proposed-transactions artifact to load.",
    )
    parser.add_argument(
        "--out",
        dest="output_path",
        default=str(DEFAULT_SAVE),
        help="Save path for reviewed artifact. Defaults to <input>_reviewed with the same suffix.",
    )
    parser.add_argument(
        "--categories",
        dest="categories_path",
        default=str(DEFAULT_CATEGORIES),
        help="YNAB categories CSV (for category dropdowns).",
    )
    parser.add_argument(
        "--profile",
        default="",
        help="Workflow profile used to resolve default category file paths.",
    )
    parser.add_argument(
        "--resume",
        nargs="?",
        const=DEFAULT_RESUME_SENTINEL,
        help="Resume from a previously saved review CSV (optional path).",
    )
    parser.add_argument(
        "--control-dir",
        dest="control_dir",
        default="",
        help=argparse.SUPPRESS,
    )
    return parser


def _parse_cli_args() -> argparse.Namespace:
    parser = _build_arg_parser()
    return parser.parse_known_args(sys.argv[1:])[0]


def _effective_categories_path(
    *, categories_path: str, profile: str = "", categories_flag: bool | None = None
) -> Path:
    if categories_flag is None:
        categories_flag = _cli_has_flag("--categories")
    if categories_flag:
        return Path(categories_path)
    resolved_profile = workflow_profiles.resolve_profile(profile or None)
    return resolved_profile.categories_path


def _clear_editor_state() -> None:
    keys_to_remove: list[str] = []
    for key in st.session_state.keys():
        if key in EDITOR_STATE_KEYS:
            keys_to_remove.append(key)
            continue
        if any(key.startswith(prefix) for prefix in EDITOR_STATE_PREFIXES):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        del st.session_state[key]
    st.session_state["editor_version"] = int(st.session_state.get("editor_version", 0)) + 1


def _editor_key(base: str) -> str:
    version = int(st.session_state.get("editor_version", 0))
    return f"{base}__v{version}"


def _ensure_widget_state(key: str, value: Any) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def _consume_notice(key: str) -> str:
    if key not in st.session_state:
        return ""
    message = str(st.session_state[key] or "")
    del st.session_state[key]
    return message


def _clear_split_editor_state() -> None:
    st.session_state.pop("_split_editor", None)
    split_keys = [
        key
        for key in list(st.session_state.keys())
        if key.startswith("_split_editor_")
    ]
    for key in split_keys:
        del st.session_state[key]


def _next_row_cursor(
    indices: list[Any], current_idx: Any, page_size: int
) -> tuple[Any | None, int]:
    if page_size <= 0:
        return None, 1
    try:
        position = indices.index(current_idx)
    except ValueError:
        return None, 1

    current_page = (position // page_size) + 1
    next_position = position + 1
    if next_position >= len(indices):
        return None, current_page

    return indices[next_position], (next_position // page_size) + 1


def _focus_row_view(next_idx: Any | None, page: int) -> None:
    _clear_editor_state()
    st.session_state[_editor_key("row_page")] = max(1, int(page))
    if next_idx is not None:
        st.session_state["expanded_row_id"] = next_idx
    st.session_state["scroll_to_top_nonce"] = int(
        st.session_state.get("scroll_to_top_nonce", 0)
    ) + 1


def _preserve_expansion_context(*, idx: Any | None = None, group_fingerprint: str | None = None) -> None:
    if group_fingerprint:
        st.session_state["expanded_group_fp"] = group_fingerprint
        st.session_state["expanded_group_row_id"] = idx
        return
    if idx is not None:
        st.session_state["expanded_row_id"] = idx


def _consume_pending_scroll_to_top() -> None:
    nonce = int(st.session_state.get("scroll_to_top_nonce", 0))
    rendered_nonce = int(st.session_state.get("_scroll_to_top_rendered", 0))
    if nonce <= 0 or nonce == rendered_nonce:
        return
    components.html(
        """
<script>
const root = window.parent;
const doc = root.document;
const selectors = [
  '[data-testid="stAppViewContainer"]',
  'section.main',
  '.main',
];
function scrollTopNow() {
  root.scrollTo({ top: 0, left: 0, behavior: "auto" });
  doc.documentElement.scrollTop = 0;
  doc.body.scrollTop = 0;
  for (const selector of selectors) {
    const element = doc.querySelector(selector);
    if (element && typeof element.scrollTo === "function") {
      element.scrollTo({ top: 0, left: 0, behavior: "auto" });
    }
  }
}
scrollTopNow();
setTimeout(scrollTopNow, 0);
</script>
        """,
        height=0,
    )
    st.session_state["_scroll_to_top_rendered"] = nonce


def _accept_reviewed_components(
    df: pl.DataFrame,
    review_indices: list[Any],
    *,
    component_map: dict[Any, int],
) -> tuple[pl.DataFrame, list[str], list[Any]]:
    updated, errors, reviewed_indices = review_validation.apply_review_state_best_effort(
        df,
        review_indices,
        reviewed=True,
        component_map=component_map,
    )
    return updated, errors, reviewed_indices


def _call_apply_row_edit(
    df: pl.DataFrame,
    idx: Any,
    **kwargs: Any,
) -> pl.DataFrame:
    return review_state.apply_row_edit(df, idx, **kwargs)


def _call_apply_to_same_fingerprint(
    df: pl.DataFrame,
    fingerprint: str,
    *,
    payee: str | None = None,
    category: str | None = None,
    memo_append: str | None = None,
    update_maps: str | None = None,
    decision_action: str | None = None,
    reviewed: bool | None = None,
    eligible_mask: pl.Series | None = None,
) -> pl.DataFrame:
    return review_model.apply_to_same_fingerprint(
        df,
        fingerprint,
        payee=payee,
        category=category,
        memo_append=memo_append,
        update_maps=update_maps,
        decision_action=decision_action,
        reviewed=reviewed,
        eligible_mask=eligible_mask,
    )


def _call_apply_competing_row_resolution(
    df: pl.DataFrame,
    indices: list[Any],
) -> tuple[pl.DataFrame, list[Any]]:
    return review_model.apply_competing_row_resolution(df, indices)


def _call_apply_review_state(
    df: pl.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.DataFrame, list[str]]:
    updated, errors = review_validation.apply_review_state(
        df,
        indices,
        reviewed=reviewed,
        component_map=component_map,
    )
    return updated, errors


def _quit_request_path(control_dir: Path) -> Path:
    return control_dir / QUIT_REQUEST_FILENAME


def _request_quit(action: str) -> bool:
    control_dir_text = str(st.session_state.get("control_dir", "") or "").strip()
    if not control_dir_text:
        st.warning("No wrapper control directory was provided. Close the terminal process manually.")
        return False
    control_dir = Path(control_dir_text)
    control_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "action": action,
        "requested_at": datetime.now().isoformat(timespec="seconds"),
    }
    _quit_request_path(control_dir).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return True


def _bump_df_generation() -> None:
    st.session_state["_df_generation"] = int(st.session_state.get("_df_generation", 0)) + 1
    st.session_state["_series_generation"] = -1
    st.session_state.pop("_cached_series", None)
    st.session_state.pop("_cached_component_map", None)


def _refresh_validation_state(
    df: pl.DataFrame,
    *,
    changed_indices: list[Any] | None = None,
) -> None:
    prior_state = st.session_state.get("_validation_state")
    validation_state = review_validation.refresh_validation_state(
        df,
        validation_state=prior_state if isinstance(prior_state, dict) else None,
        changed_indices=changed_indices,
    )
    st.session_state["_validation_state"] = validation_state


def _canonical_review_bundle(df: pl.DataFrame | None) -> dict[str, Any]:
    if df is None:
        return {"table": None, "helpers": None, "helper_lookup": None}
    table = working_schema.build_working_dataframe(df)
    helper_columns = [
        "source_is_split",
        "target_is_split",
        "source_split_count",
        "target_split_count",
        "source_display_payee",
        "target_display_payee",
        "source_display_category",
        "target_display_category",
        "source_display_account",
        "target_display_account",
        "source_display_date",
        "target_display_date",
    ]
    helpers = review_state.canonical_review_helpers(table).with_row_index("_row_pos").select(
        "_row_pos", *helper_columns
    )
    helper_lookup = review_state.view_row_lookup(helpers, list(range(len(df))))
    return {"table": table, "helpers": helpers, "helper_lookup": helper_lookup}


def _set_review_frames(
    *,
    df: pl.DataFrame | None = None,
    original: pl.DataFrame | None = None,
    base: pl.DataFrame | None = None,
    changed_indices: list[Any] | None = None,
) -> None:
    changed = False
    if df is not None:
        df = _augment_with_account_budget_metadata(df)
        canonical = _canonical_review_bundle(df)
        st.session_state["df"] = df
        st.session_state["review_table"] = canonical["table"]
        st.session_state["review_helpers"] = canonical["helpers"]
        st.session_state["review_helper_lookup"] = canonical["helper_lookup"]
        changed = True
    if original is not None:
        original = _augment_with_account_budget_metadata(original)
        canonical = _canonical_review_bundle(original)
        st.session_state["df_original"] = original
        st.session_state["review_table_original"] = canonical["table"]
        st.session_state["review_helpers_original"] = canonical["helpers"]
        st.session_state["review_helper_lookup_original"] = canonical["helper_lookup"]
        changed = True
    if base is not None:
        base = _augment_with_account_budget_metadata(base)
        canonical = _canonical_review_bundle(base)
        st.session_state["df_base"] = base
        st.session_state["review_table_base"] = canonical["table"]
        st.session_state["review_helpers_base"] = canonical["helpers"]
        st.session_state["review_helper_lookup_base"] = canonical["helper_lookup"]
        changed = True
    if changed:
        _bump_df_generation()


def _require_groupable_review_rows(df: pl.DataFrame) -> None:
    if "fingerprint" not in df.columns:
        raise ValueError("Review dataframe missing required fingerprint column.")
    fingerprint = df.get_column("fingerprint").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    if fingerprint.eq("").any():
        raise ValueError(
            "Loaded review artifact contains blank fingerprint values; "
            "renormalize or manually correct the stale artifact before opening it in the app."
        )


def _load_df(path: Path, *, set_source_path: bool = False) -> None:
    df = review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(path)
    )
    df = _augment_with_account_budget_metadata(df)
    _require_groupable_review_rows(df)
    _set_review_frames(df=df, original=df)
    if set_source_path:
        st.session_state["source_path"] = str(path)
    _clear_editor_state()


def _load_base(path: Path) -> None:
    base = review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(path)
    )
    base = _augment_with_account_budget_metadata(base)
    _require_groupable_review_rows(base)
    _set_review_frames(base=base)


def _load_categories(path: Path) -> None:
    try:
        df = pl.from_pandas(review_io.load_category_list(path))
    except (FileNotFoundError, ValueError) as exc:
        st.session_state["category_list"] = []
        st.session_state["category_group_map"] = {}
        st.session_state["category_path"] = str(path)
        st.session_state["category_error"] = str(exc)
        return

    categories: list[str] = []
    group_map: dict[str, str] = {}
    for row in df.iter_rows(named=True):
        name = str(row.get("category_name", "") or "").strip()
        group = str(row.get("category_group", "") or "").strip()
        hidden = (
            str(row.get("hidden", "") or "").strip().casefold() in review_validation.TRUE_VALUES
            or bool(row.get("hidden", False))
        )
        if not name:
            continue
        if name not in group_map:
            group_map[name] = group
        if hidden:
            continue
        if name not in categories:
            categories.append(name)

    st.session_state["category_list"] = categories
    st.session_state["category_group_map"] = group_map
    st.session_state["category_path"] = str(path)
    st.session_state["category_error"] = ""


def _account_lookup_key(value: Any) -> str:
    return str(value or "").strip().casefold()


def _account_budget_lookup_from_accounts(
    accounts: list[dict[str, Any]],
) -> dict[str, bool]:
    return {
        _account_lookup_key(account.get("name", "")): bool(account.get("on_budget", False))
        for account in accounts
        if not bool(account.get("deleted", False))
        and _account_lookup_key(account.get("name", ""))
    }


def _lookup_account_on_budget(account_name: Any) -> bool | None:
    lookup = st.session_state.get("account_budget_lookup")
    if not isinstance(lookup, dict):
        return None
    key = _account_lookup_key(account_name)
    if not key or key not in lookup:
        return None
    return bool(lookup[key])


def _augment_with_account_budget_metadata(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    rows = df.to_dicts()
    for row in rows:
        source_present = bool(row.get("source_present", False))
        target_present = bool(row.get("target_present", False))
        target_payee = str(
            row.get("target_payee_selected")
            or row.get("target_payee_current")
            or row.get("payee_selected")
            or ""
        ).strip()
        source_payee = str(
            row.get("source_payee_selected")
            or row.get("source_payee_current")
            or ""
        ).strip()
        target_account = str(
            row.get("target_account")
            or (row.get("account_name") if target_present else "")
        ).strip()
        source_account = str(
            row.get("source_account")
            or (row.get("account_name") if source_present else "")
        ).strip()
        row["target_account_on_budget"] = _lookup_account_on_budget(target_account)
        row["source_account_on_budget"] = _lookup_account_on_budget(source_account)
        row["target_transfer_account_on_budget"] = _lookup_account_on_budget(
            review_model.transfer_target_account_name(target_payee)
        )
        row["source_transfer_account_on_budget"] = _lookup_account_on_budget(
            review_model.transfer_target_account_name(source_payee)
        )
    return pl.from_dicts(rows, infer_schema_length=None)


def _refresh_accounts_from_api(*, profile: str) -> None:
    if not str(profile or "").strip():
        raise ValueError("No workflow profile was provided for account refresh.")
    resolved_profile = workflow_profiles.resolve_profile(profile or None)
    budget_id = workflow_profiles.resolve_budget_id(profile=resolved_profile.name)
    if not budget_id:
        raise ValueError(f"No budget id configured for profile {resolved_profile.name!r}.")

    accounts = ynab_api.fetch_accounts(plan_id=budget_id or None)
    st.session_state["account_budget_lookup"] = _account_budget_lookup_from_accounts(
        accounts
    )
    st.session_state["account_notice"] = (
        f"Refreshed account metadata from YNAB for {resolved_profile.name}"
    )


def _refresh_categories_from_api(*, profile: str, categories_path: Path) -> None:
    if not str(profile or "").strip():
        raise ValueError("No workflow profile was provided for category refresh.")
    resolved_profile = workflow_profiles.resolve_profile(profile or None)
    budget_id = workflow_profiles.resolve_budget_id(profile=resolved_profile.name)
    if not budget_id:
        raise ValueError(f"No budget id configured for profile {resolved_profile.name!r}.")

    groups = ynab_api.fetch_categories(plan_id=budget_id or None)
    df = ynab_api.categories_to_dataframe(groups)
    if len(df) == 0:
        raise ValueError("No categories returned from YNAB API.")

    categories_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(categories_path)
    st.session_state["category_notice"] = (
        f"Refreshed categories from YNAB API to {categories_path}"
    )


def _init_from_cli() -> None:
    args = _parse_cli_args()
    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    categories_path = _effective_categories_path(
        categories_path=args.categories_path,
        profile=getattr(args, "profile", "") or "",
    )
    profile_name = str(getattr(args, "profile", "") or "").strip()
    if not _cli_has_flag("--out"):
        output_path = _default_reviewed_path(input_path)

    st.session_state.setdefault("source_path", str(input_path))
    st.session_state.setdefault("save_path", str(output_path))
    st.session_state.setdefault("category_path", str(categories_path))
    st.session_state.setdefault("profile_name", profile_name)
    st.session_state.setdefault("control_dir", str(getattr(args, "control_dir", "") or ""))

    refresh_key = f"categories_api_refreshed::{categories_path}"
    if not st.session_state.get(refresh_key, False):
        try:
            _refresh_categories_from_api(
                profile=profile_name,
                categories_path=categories_path,
            )
        except Exception as exc:
            st.session_state.setdefault(
                "category_notice",
                f"Using local categories file; YNAB refresh unavailable: {exc}",
            )
        finally:
            st.session_state[refresh_key] = True

    account_refresh_key = f"accounts_api_refreshed::{profile_name}"
    if not st.session_state.get(account_refresh_key, False):
        try:
            _refresh_accounts_from_api(profile=profile_name)
        except Exception as exc:
            st.session_state.setdefault(
                "account_notice",
                f"Using local review data only; YNAB account refresh unavailable: {exc}",
            )
            st.session_state.setdefault("account_budget_lookup", {})
        finally:
            st.session_state[account_refresh_key] = True

    if input_path.exists() and "df_base" not in st.session_state:
        _load_base(input_path)

    resume_path = getattr(args, "resume", None)
    if resume_path is not None and "df" not in st.session_state:
        if resume_path == DEFAULT_RESUME_SENTINEL:
            path = output_path
        else:
            path = Path(resume_path)
        if not path.exists():
            st.error(f"Resume file not found: {path}")
            st.stop()
        _load_df(path, set_source_path=False)
        st.session_state["save_path"] = str(path)


def _ensure_loaded() -> None:
    categories_path = Path(st.session_state.get("category_path", str(DEFAULT_CATEGORIES)))
    loaded_category_path = st.session_state.get("category_path_loaded", "")
    if categories_path.exists() and loaded_category_path != str(categories_path):
        _load_categories(categories_path)
        st.session_state["category_path_loaded"] = str(categories_path)
    elif not categories_path.exists() and loaded_category_path != str(categories_path):
        st.session_state["category_list"] = []
        st.session_state["category_group_map"] = {}
        st.session_state["category_error"] = f"Missing categories file: {categories_path}"
        st.session_state["category_path_loaded"] = str(categories_path)

    if "df" in st.session_state:
        return
    source_path = Path(st.session_state.get("source_path", str(DEFAULT_SOURCE)))
    if source_path.exists():
        if "df_base" not in st.session_state:
            _load_base(source_path)
        _load_df(source_path, set_source_path=True)


def _format_amount(row: dict[str, Any]) -> str:
    outflow = _parse_float_value(row.get("outflow_ils", 0.0))
    inflow = _parse_float_value(row.get("inflow_ils", 0.0))
    if outflow > 0:
        return f"-{outflow:g}"
    if inflow > 0:
        return f"+{inflow:g}"
    return ""


def _fp_key(fp: str) -> str:
    return hashlib.sha1(fp.encode("utf-8")).hexdigest()[:8]


def _parse_float_value(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        if isinstance(value, str) and not value.strip():
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _pick_summary_text(row: dict[str, Any]) -> str:
    for col in [
        "description_clean",
        "merchant_raw",
        "description_raw",
        "memo",
        "fingerprint",
        "source_memo",
        "target_memo",
        "source_payee_current",
        "target_payee_current",
        "source_category_current",
        "target_category_current",
    ]:
        value = str(row.get(col, "") or "").strip()
        if value:
            return value
    for side in ["source_splits", "target_splits"]:
        splits = row.get(side) or []
        if isinstance(splits, list):
            for split in splits:
                if not isinstance(split, dict):
                    continue
                for key in ["memo", "payee_raw", "category_raw"]:
                    value = str(split.get(key, "") or "").strip()
                    if value:
                        return value
    return ""


def _split_summary_suffix(helper_row: dict[str, Any] | None) -> str:
    if helper_row is None:
        return ""
    source_count = int(helper_row.get("source_split_count", 0) or 0)
    target_count = int(helper_row.get("target_split_count", 0) or 0)
    if source_count <= 0 and target_count <= 0:
        return ""
    parts: list[str] = []
    if source_count > 0:
        parts.append(f"Src split {source_count}")
    if target_count > 0:
        parts.append(f"Tgt split {target_count}")
    return " | " + " | ".join(parts)


def _helper_text(helper_row: dict[str, Any] | None, key: str) -> str:
    if helper_row is None:
        return ""
    return str(helper_row.get(key, "") or "").strip()


def _lookup_text(
    lookup: dict[Any, dict[str, Any]] | None,
    idx: Any,
    key: str,
) -> str:
    if not isinstance(lookup, dict):
        return ""
    row = lookup.get(idx)
    if not isinstance(row, dict):
        return ""
    return str(row.get(key, "") or "").strip()


def _lookup_bool(
    lookup: dict[Any, dict[str, Any]] | None,
    idx: Any,
    key: str,
) -> bool:
    if not isinstance(lookup, dict):
        return False
    row = lookup.get(idx)
    if not isinstance(row, dict):
        return False
    return bool(row.get(key, False))


def _lookup_rows(
    lookup: dict[Any, dict[str, Any]] | None,
    indices: list[Any],
) -> list[dict[str, Any]]:
    if not isinstance(lookup, dict):
        return []
    rows: list[dict[str, Any]] = []
    for idx in indices:
        row = lookup.get(idx)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _target_category_required(row: dict[str, Any], payee_value: str) -> bool:
    return review_model.category_required_for_payee(
        payee_value,
        current_account_on_budget=(
            bool(row.get("target_account_on_budget"))
            if row.get("target_account_on_budget") is not None
            else None
        ),
        transfer_target_on_budget=_lookup_account_on_budget(
            review_model.transfer_target_account_name(payee_value)
        ),
    )


def _most_common_lookup_value(
    lookup: dict[Any, dict[str, Any]] | None,
    indices: list[Any],
    key: str,
) -> str:
    values = [str(row.get(key, "") or "").strip() for row in _lookup_rows(lookup, indices)]
    return review_state.most_common_value(values)


def _group_status_counts(
    lookup: dict[Any, dict[str, Any]] | None,
    indices: list[Any],
) -> dict[str, int]:
    rows = _lookup_rows(lookup, indices)
    return {
        "unsaved": sum(1 for row in rows if str(row.get("save_state", "") or "").strip() == "Unsaved"),
        "saved": sum(1 for row in rows if str(row.get("save_state", "") or "").strip() == "Saved"),
        "changed": sum(1 for row in rows if bool(row.get("changed_bool", False))),
        "uncategorized": sum(1 for row in rows if bool(row.get("uncategorized_bool", False))),
    }


def _summary_date(row: dict[str, Any], helper_row: dict[str, Any] | None) -> str:
    return (
        _helper_text(helper_row, "source_display_date")
        or _helper_text(helper_row, "target_display_date")
        or str(row.get("date", "") or "").strip()
    )


def _summary_account(row: dict[str, Any], helper_row: dict[str, Any] | None) -> str:
    return (
        _helper_text(helper_row, "source_display_account")
        or _helper_text(helper_row, "target_display_account")
        or str(row.get("account_name", "") or "").strip()
    )


def _format_option_summary(
    options: list[str],
    *,
    formatter: Callable[[str], str] | None = None,
    limit: int = 2,
) -> str:
    if not options:
        return "—"
    formatter = formatter or (lambda v: v)
    shown = [formatter(opt) for opt in options[:limit]]
    remainder = len(options) - len(shown)
    summary = ", ".join(shown)
    if remainder > 0:
        summary += f" (+{remainder})"
    return summary


def _render_status_badges(
    *, unsaved: bool, changed: bool, reviewed: bool, uncategorized: bool = False
) -> None:
    badges: list[str] = []
    if unsaved:
        badges.append(
            "<span style='color:#b45309;font-weight:600;'>Unsaved</span>"
        )
    if changed:
        badges.append(
            "<span style='color:#2563eb;font-weight:600;'>Edited</span>"
        )
    if reviewed:
        badges.append(
            "<span style='color:#15803d;font-weight:600;'>Settled</span>"
        )
    if uncategorized:
        badges.append(
            "<span style='color:#b91c1c;font-weight:700;'>Uncategorized</span>"
        )
    if badges:
        st.markdown(" ".join(badges), unsafe_allow_html=True)


_PRIMARY_STATE_META: dict[tuple[str, str], dict[str, str]] = {
    ("Fix", "Unsaved"): {
        "short": "Fix",
        "label": "Fix • Unsaved",
        "color": "#b91c1c",
        "bg": "#fef2f2",
        "css": "txn-pri-fix-us",
    },
    ("Fix", "Saved"): {
        "short": "Fix",
        "label": "Fix • Saved",
        "color": "#b91c1c",
        "bg": "#fef2f2",
        "css": "txn-pri-fix-s",
    },
    ("Decide", "Unsaved"): {
        "short": "Decide",
        "label": "Decide • Unsaved",
        "color": "#b45309",
        "bg": "#fffbeb",
        "css": "txn-pri-decide-us",
    },
    ("Decide", "Saved"): {
        "short": "Decide",
        "label": "Decide • Saved",
        "color": "#b45309",
        "bg": "#fffbeb",
        "css": "txn-pri-decide-s",
    },
    ("Settled", "Unsaved"): {
        "short": "Settled",
        "label": "Settled • Unsaved",
        "color": "#15803d",
        "bg": "#f0fdf4",
        "css": "txn-pri-settled-us",
    },
    ("Settled", "Saved"): {
        "short": "Settled",
        "label": "Settled • Saved",
        "color": "#15803d",
        "bg": "#f0fdf4",
        "css": "txn-pri-settled-s",
    },
}


def _primary_state_meta(readiness: str, save_state: str) -> dict[str, str]:
    return _PRIMARY_STATE_META.get(
        (str(readiness or ""), str(save_state or "")),
        {
            "short": "UNK",
            "label": "Unknown state",
            "color": "#374151",
            "bg": "#f3f4f6",
            "css": "txn-pri-unk",
        },
    )


def _inject_primary_state_css() -> None:
    if st.session_state.get("_primary_state_css_injected"):
        return
    st.markdown(
        """
<style>
.stExpander details > summary {
  padding-top: 0.2rem;
  padding-bottom: 0.2rem;
}
.stExpander details > div[role="group"] {
  padding-top: 0.35rem;
}
.txn-compact p {
  margin: 0.05rem 0 0.18rem 0;
  line-height: 1.2;
}
.txn-compact .stCaption,
.txn-compact [data-testid="stCaptionContainer"] {
  margin-bottom: 0.1rem;
}
.txn-compact [data-testid="stHorizontalBlock"] {
  gap: 0.55rem;
}
.txn-compact [data-testid="column"] {
  padding-top: 0;
}
.txn-compact div[data-testid="stMarkdownContainer"] > p {
  margin-bottom: 0.2rem;
}
.txn-compact div[data-testid="stCheckbox"] {
  margin-top: -0.2rem;
  margin-bottom: -0.1rem;
}
.txn-compact div[data-testid="stTextInput"],
.txn-compact div[data-testid="stSelectbox"],
.txn-compact div[data-testid="stTextArea"],
.txn-compact div[data-testid="stMultiSelect"] {
  margin-bottom: -0.15rem;
}
.txn-compact .txn-detail-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}
.txn-compact .txn-detail-table td {
  padding: 0.08rem 0;
  vertical-align: top;
}
.txn-compact .txn-detail-label {
  width: 5.5rem;
  color: #6b7280;
  white-space: nowrap;
  padding-right: 0.4rem;
}
.txn-compact .txn-inline-note {
  color: #6b7280;
  font-size: 0.88rem;
  margin: 0.15rem 0 0.35rem 0;
}
.txn-state-anchor { display: none; }
.txn-state-anchor.txn-pri-fix-us + div[data-testid="stExpander"] {
  border: 2px solid #b91c1c;
  border-radius: 0.5rem;
}
.txn-state-anchor.txn-pri-fix-s + div[data-testid="stExpander"] {
  border: 2px solid #b91c1c;
  border-radius: 0.5rem;
}
.txn-state-anchor.txn-pri-decide-us + div[data-testid="stExpander"] {
  border: 2px solid #b45309;
  border-radius: 0.5rem;
}
.txn-state-anchor.txn-pri-decide-s + div[data-testid="stExpander"] {
  border: 2px solid #b45309;
  border-radius: 0.5rem;
}
.txn-state-anchor.txn-pri-settled-us + div[data-testid="stExpander"] {
  border: 2px solid #15803d;
  border-radius: 0.5rem;
}
.txn-state-anchor.txn-pri-settled-s + div[data-testid="stExpander"] {
  border: 2px solid #15803d;
  border-radius: 0.5rem;
}
</style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_primary_state_css_injected"] = True


def _render_primary_state_anchor(readiness: str, save_state: str) -> None:
    css = _primary_state_meta(readiness, save_state)["css"]
    st.markdown(f"<div class='txn-state-anchor {css}'></div>", unsafe_allow_html=True)


def _render_primary_state_banner(readiness: str, save_state: str) -> None:
    meta = _primary_state_meta(readiness, save_state)
    st.markdown(
        (
            "<div style='"
            f"border:1px solid {meta['color']};"
            f"background:{meta['bg']};"
            "border-radius:6px;"
            "padding:0.25rem 0.5rem;"
            "margin:0.1rem 0 0.5rem 0;"
            "'>"
            f"<span style='color:{meta['color']};font-weight:700;'>{meta['label']}</span>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_primary_state_strip(readiness: str, save_state: str) -> None:
    meta = _primary_state_meta(readiness, save_state)
    st.markdown(
        (
            "<div style='"
            f"height:0.28rem;background:{meta['color']};"
            "border-radius:4px;margin:0.05rem 0 0.15rem 0;"
            "'></div>"
        ),
        unsafe_allow_html=True,
    )


def _dominant_group_primary_state(
    readiness: pl.Series | list[str], save_state: pl.Series | list[str]
) -> tuple[str, str]:
    if isinstance(readiness, pl.Series):
        readiness_values = readiness.cast(pl.Utf8, strict=False).fill_null("").to_list()
    else:
        readiness_values = [str(value or "") for value in readiness]
    if isinstance(save_state, pl.Series):
        save_values = save_state.cast(pl.Utf8, strict=False).fill_null("").to_list()
    else:
        save_values = [str(value or "") for value in save_state]
    priority = [
        ("Fix", "Unsaved"),
        ("Fix", "Saved"),
        ("Decide", "Unsaved"),
        ("Decide", "Saved"),
        ("Settled", "Unsaved"),
        ("Settled", "Saved"),
    ]
    for ready_value, save_value in priority:
        if any(
            str(current_ready or "") == ready_value and str(current_save or "") == save_value
            for current_ready, current_save in zip(readiness_values, save_values, strict=False)
        ):
            return ready_value, save_value
    return "Decide", "Unsaved"


def _render_primary_state_legend() -> None:
    items = [
        ("Fix", "#b91c1c", "#fef2f2"),
        ("Decide", "#b45309", "#fffbeb"),
        ("Settled", "#15803d", "#f0fdf4"),
    ]
    chips = []
    for label, color, bg in items:
        chips.append(
            "<span style='"
            f"display:inline-block;margin-right:0.45rem;margin-bottom:0.25rem;"
            f"padding:0.2rem 0.55rem;border-radius:999px;border:1px solid {color};"
            f"background:{bg};color:{color};font-weight:700;"
            "'>"
            f"{label}"
            "</span>"
        )
    st.markdown(" ".join(chips), unsafe_allow_html=True)


def _render_secondary_tag_badges(
    *, inference: str, progress: str, persistence: str
) -> None:
    inference_colors = {
        "unrecognized": "#6b7280",
        "missing": "#b91c1c",
        "ambiguous": "#7c3aed",
        "unique": "#0369a1",
    }
    progress_colors = {"unchanged": "#6b7280", "resolved": "#2563eb"}
    persistence_colors = {"unsaved": "#b45309", "saved": "#15803d"}
    inf = str(inference or "").strip().lower()
    prog = str(progress or "").strip().lower()
    pers = str(persistence or "").strip().lower()

    spans = [
        (
            f"<span style='color:{inference_colors.get(inf, '#374151')};"
            "font-weight:600;'>"
            f"inference: {inf or 'unknown'}</span>"
        ),
        (
            f"<span style='color:{progress_colors.get(prog, '#374151')};"
            "font-weight:600;'>"
            f"progress: {prog or 'unknown'}</span>"
        ),
        (
            f"<span style='color:{persistence_colors.get(pers, '#374151')};"
            "font-weight:600;'>"
            f"persistence: {pers or 'unknown'}</span>"
        ),
    ]
    st.markdown(" | ".join(spans), unsafe_allow_html=True)


def _compute_derived_state(
    df: pl.DataFrame,
    original: pl.DataFrame | None,
    base: pl.DataFrame | None,
    *,
    review_table: pl.DataFrame | None = None,
    validation_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    counts = review_state.summary_counts(df)
    modified = review_state.modified_count(df, original)
    unsaved_mask = review_state.modified_mask(df, original)
    changed_mask = review_state.changed_mask(df, base)
    reviewed_mask = (
        df.get_column("reviewed").cast(pl.Boolean, strict=False).fill_null(False)
        if "reviewed" in df.columns
        else pl.Series([False] * len(df), dtype=pl.Boolean)
    )
    saved_mask = review_state.saved_mask(original, base, list(range(len(df))))
    updated_mask = changed_mask | reviewed_mask
    inconsistent = review_validation.inconsistent_fingerprints(df)
    uncategorized_mask = review_state.uncategorized_mask(df)
    if not isinstance(validation_state, dict):
        validation_state = review_validation.build_validation_state(df)
    component_map = validation_state.get("component_map", {})
    blocker_series = validation_state.get("blocker_series")
    if not isinstance(blocker_series, pl.Series) or len(blocker_series) != len(df):
        blocker_series = review_validation.build_validation_state(
            df,
            component_map=component_map if component_map else None,
        )["blocker_series"]
    save_state = pl.Series(
        ["Saved" if bool(value) else "Unsaved" for value in saved_mask.to_list()],
        dtype=pl.Utf8,
    )
    data_view = review_state.review_data_view(df)
    state_view = review_state.review_filter_state_view(
        data_view,
        blocker_series=blocker_series,
        save_state=save_state,
        changed_mask=changed_mask,
        uncategorized_mask=uncategorized_mask,
    )
    data_lookup = review_state.view_row_lookup(
        data_view.select(
            "_row_pos",
            "target_payee_selected",
            "target_category_selected",
            "action_label",
            "payee_options",
            "category_options",
            "workflow_type",
            "source_present",
            "target_present",
        ),
        list(range(len(df))),
    )
    state_lookup = review_state.view_row_lookup(
        state_view.select(
            "_row_pos",
            "primary_state",
            "save_state",
            "suggestion_label",
            "map_update_label",
            "changed_bool",
            "uncategorized_bool",
        ),
        list(range(len(df))),
    )
    inference_tag = review_state.initial_inference_tags(df, base)
    progress_tag = pl.Series(
        ["resolved" if bool(value) else "unchanged" for value in updated_mask.to_list()],
        dtype=pl.Utf8,
    )
    persistence_tag = save_state.str.to_lowercase()
    base_count = len(base) if isinstance(base, pl.DataFrame) and not base.is_empty() else len(df)
    updated_confirmed_count = int(updated_mask.sum())
    saved_reviewed_count = int(saved_mask.sum())
    uncategorized_count = int(uncategorized_mask.sum())
    return {
        "counts": counts,
        "modified": modified,
        "unsaved_mask": unsaved_mask,
        "changed_mask": changed_mask,
        "reviewed_mask": reviewed_mask,
        "saved_mask": saved_mask,
        "updated_mask": updated_mask,
        "inconsistent": inconsistent,
        "uncategorized_mask": uncategorized_mask,
        "blocker_series": blocker_series,
        "data_view": data_view,
        "data_lookup": data_lookup,
        "state_view": state_view,
        "state_lookup": state_lookup,
        "inference_tag": inference_tag,
        "progress_tag": progress_tag,
        "persistence_tag": persistence_tag,
        "base_count": base_count,
        "updated_confirmed_count": updated_confirmed_count,
        "saved_reviewed_count": saved_reviewed_count,
        "uncategorized_count": uncategorized_count,
        "component_map": component_map,
    }


def _get_cached_derived_state(
    cache: MutableMapping[str, Any],
    df: pl.DataFrame,
    original: pl.DataFrame | None,
    base: pl.DataFrame | None,
    *,
    review_table: pl.DataFrame | None = None,
    validation_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_generation = int(cache.get("_df_generation", 0))
    cached_generation = int(cache.get("_series_generation", -1))
    cached = cache.get("_cached_series")
    if cached_generation != current_generation or not isinstance(cached, dict):
        cached = _compute_derived_state(
            df,
            original,
            base,
            review_table=review_table,
            validation_state=validation_state,
        )
        cache["_cached_series"] = cached
        cache["_cached_component_map"] = cached.get("component_map", {})
        cache["_series_generation"] = current_generation
    return cached


def _ordered_filter_options(series: pl.Series | list[str], preferred: list[str]) -> list[str]:
    if isinstance(series, pl.Series):
        raw_values = series.cast(pl.Utf8, strict=False).fill_null("").to_list()
    else:
        raw_values = list(series)
    present = {str(value or "").strip() for value in raw_values if str(value or "").strip()}
    ordered = [value for value in preferred if value in present]
    extras = sorted(present - set(ordered))
    return ordered + extras


def _default_primary_state_selection(options: list[str]) -> list[str]:
    preferred = [value for value in ["Fix", "Decide"] if value in options]
    return preferred or options


def _merge_category_choices(*values: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            ordered.append(text)
            seen.add(text)
    return ordered


def _option_list(values: pl.Series | list[str]) -> list[str]:
    if isinstance(values, pl.Series):
        raw_values = values.cast(pl.Utf8, strict=False).fill_null("").to_list()
    else:
        raw_values = values
    ordered: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        for part in str(value or "").split(";"):
            text = str(part or "").strip()
            if text and text not in seen:
                ordered.append(text)
                seen.add(text)
    return ordered


def _category_choice_list(
    *,
    category_options: list[str],
    category_choices: list[str],
    selected_value: str,
    default_value: str,
    show_all: bool,
) -> list[str]:
    ordered_options = category_options.copy()
    if category_choices:
        option_set = set(category_options)
        ordered_options = [value for value in category_choices if value in option_set]
        for value in category_options:
            if value not in ordered_options:
                ordered_options.append(value)

    if show_all and category_choices:
        options = [""] + category_choices.copy()
    else:
        options = [""] + ordered_options

    for value in _merge_category_choices(selected_value, default_value):
        if value not in options:
            options.append(value)
    return options


def _selected_side_value(row: dict[str, Any], *, side: str, field: str) -> str:
    column = f"{side}_{field}_selected"
    value = str(row.get(column, "") or "").strip()
    if field == "category":
        return review_model.normalize_category_value(value)
    return value


def _apply_staged_row_widget_values(df: pl.DataFrame, indices: list[Any]) -> pl.DataFrame:
    if df.is_empty() or not indices:
        return df

    updated = df
    for idx in indices:
        if not isinstance(idx, int) or idx < 0 or idx >= len(updated):
            continue
        row = updated.row(idx, named=True)
        source_payee_key = _editor_key(f"source_payee_{idx}")
        source_category_key = _editor_key(f"source_category_{idx}")
        target_payee_select_key = _editor_key(f"target_payee_select_{idx}")
        target_payee_override_key = _editor_key(f"target_payee_override_{idx}")
        target_category_select_key = _editor_key(f"target_category_select_{idx}")
        memo_append_key = _editor_key(f"memo_append_{idx}")
        update_maps_key = _editor_key(f"update_maps_{idx}")
        decision_action_key = _editor_key(f"decision_action_{idx}")

        widget_keys = [
            source_payee_key,
            source_category_key,
            target_payee_select_key,
            target_payee_override_key,
            target_category_select_key,
            memo_append_key,
            update_maps_key,
            decision_action_key,
        ]
        if not any(key in st.session_state for key in widget_keys):
            continue

        source_payee_value = str(
            st.session_state.get(
                source_payee_key,
                _selected_side_value(row, side="source", field="payee"),
            )
            or ""
        )
        source_category_value = str(
            st.session_state.get(
                source_category_key,
                _selected_side_value(row, side="source", field="category"),
            )
            or ""
        )
        target_payee_select_value = str(
            st.session_state.get(
                target_payee_select_key,
                _selected_side_value(row, side="target", field="payee"),
            )
            or ""
        )
        target_payee_override_value = str(
            st.session_state.get(target_payee_override_key, "") or ""
        )
        target_category_value = str(
            st.session_state.get(
                target_category_select_key,
                _selected_side_value(row, side="target", field="category"),
            )
            or ""
        )
        memo_append_value = str(
            st.session_state.get(memo_append_key, str(row.get("memo_append", "") or "")) or ""
        )
        update_maps_tokens = st.session_state.get(
            update_maps_key,
            review_validation.parse_update_maps(row.get("update_maps", "")),
        ) or []
        decision_action_value = str(
            st.session_state.get(
                decision_action_key,
                review_validation.normalize_decision_action(row.get("decision_action", "")),
            )
            or review_validation.NO_DECISION
        )

        final_target_payee = review_model.resolve_selected_value(
            target_payee_select_value,
            target_payee_override_value,
        )
        final_update_maps = review_validation.join_update_maps(list(update_maps_tokens))

        updated = _call_apply_row_edit(
            updated,
            idx,
            source_payee=source_payee_value,
            source_category=source_category_value,
            target_payee=final_target_payee,
            target_category=target_category_value,
            memo_append=memo_append_value,
            update_maps=final_update_maps,
            decision_action=decision_action_value,
        )

    return updated


def _grouped_row_indices(filtered: pl.DataFrame) -> tuple[list[str], dict[str, list[int]]]:
    if filtered.is_empty():
        return [], {}
    return review_state.grouped_row_indices(filtered)


def _render_detail_section(title: str, entries: list[tuple[str, Any]]) -> None:
    rows: list[str] = []
    for label, value in entries:
        text = str(value or "").strip() or "—"
        rows.append(
            "<tr>"
            f"<td class='txn-detail-label'>{label}</td>"
            f"<td>{text}</td>"
            "</tr>"
        )
    st.markdown(f"**{title}**")
    st.markdown(
        "<table class='txn-detail-table'>"
        + "".join(rows)
        + "</table>",
        unsafe_allow_html=True,
    )


def _format_current_selected(current: Any, selected: Any) -> str:
    current_text = str(current or "").strip()
    selected_text = str(selected or "").strip()
    if current_text and selected_text and current_text != selected_text:
        return f"{current_text} -> {selected_text}"
    return selected_text or current_text or "—"


def _split_category_summary(splits: list[dict[str, Any]] | None) -> str:
    normalized_splits = review_io._normalize_split_records(splits)
    if not normalized_splits:
        return ""
    entries: list[str] = []
    seen: set[str] = set()
    for split in normalized_splits:
        if not isinstance(split, dict):
            continue
        category = str(split.get("category_raw", "") or "").strip()
        amount = _split_amount_text(split) or "—"
        entry = f"{category or '—'} {amount}"
        if entry in seen:
            continue
        entries.append(entry)
        seen.add(entry)
    if entries:
        return "; ".join(entries)
    return "Yes"


def _row_context_lines(row: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    source_context = str(row.get("source_context_kind", "") or "").strip()
    target_context = str(row.get("target_context_kind", "") or "").strip()
    if source_context:
        lines.append(f"Source context: {source_context}")
    if target_context:
        lines.append(f"Target context: {target_context}")
    return lines


def _split_id_set(value: str) -> set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    normalized = text.replace(",", ";")
    return {part.strip() for part in normalized.split(";") if part.strip()}


def _split_amount_text(split: dict[str, Any]) -> str:
    outflow = _parse_float_value(split.get("outflow_ils", 0.0))
    inflow = _parse_float_value(split.get("inflow_ils", 0.0))
    if outflow > 0:
        return f"-{outflow:g}"
    if inflow > 0:
        return f"+{inflow:g}"
    return ""


def _split_signed_amount(split: dict[str, Any]) -> float:
    outflow = _parse_float_value(split.get("outflow_ils", 0.0))
    inflow = _parse_float_value(split.get("inflow_ils", 0.0))
    return inflow - outflow


def _split_editor_amount_text(value: Any) -> str:
    text = str(value or "").strip()
    if text in {"", "-", "+"}:
        return ""
    try:
        numeric = float(text)
    except ValueError:
        return ""
    if math.isnan(numeric):
        return ""
    return f"{float(numeric):g}"


_SPLIT_EDITOR_WIDGET_PREFIXES = (
    "_split_editor_payee_",
    "_split_editor_category_",
    "_split_editor_memo_",
    "_split_editor_amount_",
    "_split_editor_remove_",
)


def _clear_split_line_widget_keys(line_id: int) -> None:
    for prefix in _SPLIT_EDITOR_WIDGET_PREFIXES:
        st.session_state.pop(f"{prefix}{line_id}", None)


def _collect_split_editor_lines() -> list[dict[str, Any]]:
    editor_state = st.session_state.get("_split_editor")
    if not isinstance(editor_state, dict):
        return []
    result: list[dict[str, Any]] = []
    for line in editor_state.get("lines", []):
        if not isinstance(line, dict):
            continue
        lid = line.get("_line_id")
        if lid is None:
            continue
        result.append(
            {
                "split_id": line.get("split_id", ""),
                "payee_raw": str(
                    st.session_state.get(f"_split_editor_payee_{lid}", line.get("payee_raw", ""))
                    or ""
                ).strip(),
                "category_raw": str(
                    st.session_state.get(
                        f"_split_editor_category_{lid}", line.get("category_raw", "")
                    )
                    or ""
                ).strip(),
                "memo": str(
                    st.session_state.get(f"_split_editor_memo_{lid}", line.get("memo", ""))
                    or ""
                ).strip(),
                "amount_ils": str(
                    st.session_state.get(f"_split_editor_amount_{lid}", line.get("amount_ils", ""))
                    or ""
                ).strip(),
            }
        )
    return result


def _source_context_caption(row: dict[str, Any]) -> str:
    context_kind = str(row.get("source_context_kind", "") or "").strip()
    category_name = str(row.get("source_context_category_name", "") or "").strip()
    matching_ids = str(row.get("source_context_matching_split_ids", "") or "").strip()
    if context_kind == "ynab_parent_category_match":
        if category_name:
            return f"Source included because the parent YNAB transaction matches category {category_name}."
        return "Source included because the parent YNAB transaction matches the selected category context."
    if context_kind == "ynab_split_category_match":
        matched_text = f" Matching split ids: {matching_ids}." if matching_ids else ""
        if category_name:
            return (
                "Source included because one or more YNAB split lines match "
                f"category {category_name}.{matched_text}"
            )
        return f"Source included because one or more YNAB split lines match the selected category context.{matched_text}"
    return context_kind


def _split_caption_lines(
    splits: list[dict[str, Any]] | None,
    *,
    matching_split_ids: str = "",
) -> list[str]:
    normalized_splits = review_io._normalize_split_records(splits)
    if not normalized_splits:
        return []
    split_ids = _split_id_set(matching_split_ids)
    lines: list[str] = []
    for index, split in enumerate(normalized_splits, start=1):
        if not isinstance(split, dict):
            continue
        split_id = str(split.get("split_id", "") or "").strip()
        matched = split_id and split_id in split_ids
        prefix = "Matching split" if matched else "Split"
        amount = _split_amount_text(split)
        payee = str(split.get("payee_raw", "") or "").strip() or "—"
        category = str(split.get("category_raw", "") or "").strip() or "—"
        memo = str(split.get("memo", "") or "").strip() or "—"
        split_label = split_id or str(index)
        lines.append(
            f"{prefix} {split_label}: {amount or '—'} | Payee: {payee} | Category: {category} | Memo: {memo}"
        )
    return lines


def _target_split_editor_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    splits = review_io._normalize_split_records(row.get("target_splits"))
    if splits:
        rows: list[dict[str, Any]] = []
        for split in splits:
            if not isinstance(split, dict):
                continue
            rows.append(
                {
                    "split_id": str(split.get("split_id", "") or "").strip(),
                    "payee_raw": str(split.get("payee_raw", "") or "").strip(),
                    "category_raw": str(split.get("category_raw", "") or "").strip(),
                    "memo": str(split.get("memo", "") or "").strip(),
                    "amount_ils": _split_signed_amount(split),
                }
            )
        if rows:
            return rows

    target_txn = review_state._target_transaction_for_split_edit(row)
    seeded_payee = str(target_txn.get("payee_raw", "") or "").strip() or str(
        row.get("target_payee_selected", "") or ""
    ).strip()
    seeded_category = str(target_txn.get("category_raw", "") or "").strip() or str(
        row.get("target_category_selected", "") or ""
    ).strip()
    seeded_memo = str(target_txn.get("memo", "") or "").strip() or str(
        row.get("target_memo", row.get("memo", "")) or ""
    ).strip()
    seeded_line = {
        "split_id": "",
        "payee_raw": seeded_payee,
        "category_raw": seeded_category,
        "memo": seeded_memo,
        "amount_ils": review_state._signed_amount_from_row_values(
            inflow=target_txn.get("inflow_ils", row.get("inflow_ils", 0.0)),
            outflow=target_txn.get("outflow_ils", row.get("outflow_ils", 0.0)),
        ),
    }
    blank_line = {
        "split_id": "",
        "payee_raw": "",
        "category_raw": "",
        "memo": "",
        "amount_ils": 0.0,
    }
    return [seeded_line, blank_line]


def _open_target_split_editor(
    row: dict[str, Any],
    *,
    idx: Any,
    group_fingerprint: str | None = None,
) -> None:
    lines = _target_split_editor_rows(row)
    for i, line in enumerate(lines):
        line["_line_id"] = i
    st.session_state["_split_editor"] = {
        "idx": idx,
        "group_fingerprint": group_fingerprint or "",
        "lines": lines,
        "_next_line_id": len(lines),
    }
    _preserve_expansion_context(idx=idx, group_fingerprint=group_fingerprint)


def _close_target_split_editor() -> None:
    _clear_split_editor_state()


@st.dialog("Split editor", width="large")
def _render_target_split_editor_dialog(
    *,
    df: pl.DataFrame,
    idx: Any,
    category_choices: list[str],
    group_fingerprint: str = "",
) -> None:
    editor_state = st.session_state.get("_split_editor")
    if not isinstance(editor_state, dict) or not isinstance(idx, int) or idx < 0 or idx >= len(df):
        _close_target_split_editor()
        return

    row = df.row(idx, named=True)
    parent_amount = review_state._signed_amount_from_row_values(
        inflow=row.get("inflow_ils", 0.0),
        outflow=row.get("outflow_ils", 0.0),
    )
    lines = editor_state.get("lines", [])

    # Build category choices for selectboxes
    category_options = review_model.parse_option_string(row.get("category_options", ""))
    split_line_categories = _merge_category_choices(
        *[
            str(line.get("category_raw", "") or "").strip()
            for line in lines
            if isinstance(line, dict)
        ]
    )
    split_category_choices = _category_choice_list(
        category_options=category_options,
        category_choices=category_choices or category_options,
        selected_value="",
        default_value="",
        show_all=True,
    )
    for category in split_line_categories:
        if category not in split_category_choices:
            split_category_choices.append(category)

    # Column headers
    header_cols = st.columns([3, 3, 2, 1.5, 0.5])
    header_cols[0].markdown("**Payee**")
    header_cols[1].markdown("**Category**")
    header_cols[2].markdown("**Memo**")
    header_cols[3].markdown("**Amount**")
    header_cols[4].markdown("")

    # Render per-line widgets
    remove_line_id: int | None = None
    for line in lines:
        if not isinstance(line, dict):
            continue
        lid = line.get("_line_id")
        if lid is None:
            continue
        split_id = str(line.get("split_id", "") or "").strip()

        cols = st.columns([3, 3, 2, 1.5, 0.5])
        with cols[0]:
            _ensure_widget_state(
                f"_split_editor_payee_{lid}", str(line.get("payee_raw", "") or "")
            )
            st.text_input(
                "Payee",
                key=f"_split_editor_payee_{lid}",
                label_visibility="collapsed",
                placeholder=f"id: {split_id}" if split_id else "Payee",
            )
        with cols[1]:
            cat_value = str(line.get("category_raw", "") or "").strip()
            cat_index = 0
            if cat_value and cat_value in split_category_choices:
                cat_index = split_category_choices.index(cat_value)
            _ensure_widget_state(f"_split_editor_category_{lid}", cat_value)
            st.selectbox(
                "Category",
                options=split_category_choices,
                index=cat_index,
                key=f"_split_editor_category_{lid}",
                label_visibility="collapsed",
            )
        with cols[2]:
            _ensure_widget_state(
                f"_split_editor_memo_{lid}", str(line.get("memo", "") or "")
            )
            st.text_input(
                "Memo",
                key=f"_split_editor_memo_{lid}",
                label_visibility="collapsed",
            )
        with cols[3]:
            _ensure_widget_state(
                f"_split_editor_amount_{lid}",
                _split_editor_amount_text(line.get("amount_ils", "")),
            )
            st.text_input(
                "Amount",
                key=f"_split_editor_amount_{lid}",
                label_visibility="collapsed",
            )
        with cols[4]:
            if st.button("\u2716", key=f"_split_editor_remove_{lid}"):
                remove_line_id = lid

    # Handle remove
    if remove_line_id is not None:
        editor_state["lines"] = [
            ln for ln in lines if ln.get("_line_id") != remove_line_id
        ]
        _clear_split_line_widget_keys(remove_line_id)
        st.rerun()

    # Add line button
    if st.button("+ Add line"):
        next_id = editor_state.get("_next_line_id", len(lines))
        editor_state["lines"].append(
            {
                "_line_id": next_id,
                "split_id": "",
                "payee_raw": "",
                "category_raw": "",
                "memo": "",
                "amount_ils": "",
            }
        )
        editor_state["_next_line_id"] = next_id + 1
        st.rerun()

    # Summary
    split_total = 0.0
    for line in lines:
        if not isinstance(line, dict):
            continue
        lid = line.get("_line_id")
        if lid is None:
            continue
        amount_text = st.session_state.get(
            f"_split_editor_amount_{lid}", line.get("amount_ils", "")
        )
        numeric = _parse_float_value(amount_text)
        if str(amount_text or "").strip():
            split_total += float(numeric)
    difference = split_total - parent_amount
    st.caption(
        f"Parent amount: {parent_amount:g} | Split total: {split_total:g} | Difference: {difference:g}"
    )

    # Save / Cancel
    save_col, cancel_col = st.columns(2)
    with save_col:
        if st.button("Save split", use_container_width=True):
            line_records = _collect_split_editor_lines()
            try:
                updated = review_state.apply_target_split_edit(
                    df,
                    idx,
                    lines=line_records,
                )
            except ValueError as exc:
                st.error(str(exc))
            else:
                _close_target_split_editor()
                _set_review_frames(df=updated, changed_indices=[idx])
                _preserve_expansion_context(
                    idx=idx,
                    group_fingerprint=group_fingerprint or None,
                )
                st.rerun()
    with cancel_col:
        if st.button("Cancel", use_container_width=True):
            _close_target_split_editor()
            _preserve_expansion_context(
                idx=idx,
                group_fingerprint=group_fingerprint or None,
            )
            st.rerun()


def _render_split_action_buttons(
    row: dict[str, Any],
    *,
    idx: Any,
    group_fingerprint: str | None = None,
) -> None:
    target_splits = review_io._normalize_split_records(row.get("target_splits"))
    is_split = bool(target_splits)
    action_label = "Edit split" if is_split else "Create split"
    if st.button(action_label, key=_editor_key(f"split_action_{idx}")):
        _open_target_split_editor(row, idx=idx, group_fingerprint=group_fingerprint)
        st.rerun()


def _maybe_render_target_split_editor_dialog(
    *,
    df: pl.DataFrame,
    category_choices: list[str],
) -> None:
    editor_state = st.session_state.get("_split_editor")
    if not isinstance(editor_state, dict):
        return
    idx = editor_state.get("idx")
    if not isinstance(idx, int) or idx < 0 or idx >= len(df):
        _close_target_split_editor()
        return
    _render_target_split_editor_dialog(
        df=df,
        idx=idx,
        category_choices=category_choices,
        group_fingerprint=str(editor_state.get("group_fingerprint", "") or ""),
    )


def _render_row_details(
    row: dict[str, Any],
    *,
    primary_state: str,
    blocker: str,
    category_group_map: dict[str, str],
    helper_row: dict[str, Any] | None = None,
) -> None:
    def _side_value(side: str, field: str, *, allow_summary_fallback: bool = False) -> str:
        present = bool(row.get(f"{side}_present", False))
        key = f"{side}_{field}"
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
        if present and allow_summary_fallback:
            return str(row.get(field, "") or "").strip()
        return ""

    def _side_transfer_counterpart(side: str) -> str:
        payee = (
            _side_value(side, "payee_current")
            or _side_value(side, "payee_selected")
        )
        return review_model.transfer_target_account_name(payee)

    source_splits = review_io._normalize_split_records(row.get("source_splits"))
    target_splits = review_io._normalize_split_records(row.get("target_splits"))
    source_category_current = _format_category_label(
        str(row.get("source_category_current", "") or "").strip(),
        category_group_map,
    )
    source_category_selected = _format_category_label(
        str(row.get("source_category_selected", "") or "").strip(),
        category_group_map,
    )
    target_category_current = _format_category_label(
        str(row.get("target_category_current", "") or "").strip(),
        category_group_map,
    )
    target_category_selected = _format_category_label(
        str(row.get("target_category_selected", "") or "").strip(),
        category_group_map,
    )

    source_payee = _format_current_selected(
        row.get("source_payee_current", ""),
        row.get("source_payee_selected", ""),
    )
    source_category = _format_current_selected(
        "Split" if source_splits else source_category_current,
        "Split" if source_splits else source_category_selected,
    )
    target_payee = _format_current_selected(
        row.get("target_payee_current", ""),
        row.get("target_payee_selected", ""),
    )
    target_category = _format_current_selected(
        "Split" if target_splits else target_category_current,
        "Split" if target_splits else target_category_selected,
    )

    overview_col, source_col, target_col = st.columns([1.1, 1.3, 1.3])
    with overview_col:
        _render_detail_section(
            "Overview",
            [
                ("Amount", _format_amount(row)),
                ("Match status", row.get("match_status", "")),
                ("Method", row.get("match_method", "")),
                ("Fingerprint", row.get("fingerprint", "")),
                ("Memo add", row.get("memo_append", "")),
            ],
        )
        if blocker and blocker != "None":
            st.caption(f"Blocker: {blocker}")
    with source_col:
        _render_detail_section(
            "Source",
            [
                ("Account", _side_value("source", "account", allow_summary_fallback=True)),
                ("Date", _side_value("source", "date", allow_summary_fallback=True)),
                ("Transfer with", _side_transfer_counterpart("source")),
                ("Payee", source_payee),
                ("Category", source_category),
                ("Split", _split_category_summary(source_splits)),
                ("Memo", row.get("source_memo", "")),
            ],
        )
    with target_col:
        _render_detail_section(
            "Target",
            [
                ("Account", _side_value("target", "account", allow_summary_fallback=True)),
                ("Date", _side_value("target", "date", allow_summary_fallback=True)),
                ("Transfer with", _side_transfer_counterpart("target")),
                ("Payee", target_payee),
                ("Category", target_category),
                ("Split", _split_category_summary(target_splits)),
                ("Memo", row.get("target_memo", "")),
            ],
        )
    for line in _row_context_lines(row):
        st.caption(line)


def _render_row_controls(
    df: pl.DataFrame,
    idx: Any,
    category_choices: list[str],
    category_group_map: dict[str, str],
    payee_defaults: dict[str, str],
    category_defaults: dict[str, str],
    show_apply: bool = True,
    group_fingerprint: str | None = None,
    updated_mask: pl.Series | None = None,
    component_map: dict[Any, int] | None = None,
    row_order: list[Any] | None = None,
    row_page_size: int | None = None,
) -> None:
    row = df.row(idx, named=True)
    fingerprint = str(row.get("fingerprint", "") or "")
    payee_options = review_model.parse_option_string(row.get("payee_options", ""))
    category_options = review_model.parse_option_string(row.get("category_options", ""))

    source_payee_selected = _selected_side_value(row, side="source", field="payee")
    source_category_selected = _selected_side_value(row, side="source", field="category")
    target_payee_selected = _selected_side_value(row, side="target", field="payee")
    target_category_selected = _selected_side_value(row, side="target", field="category")
    current_action = review_validation.normalize_decision_action(
        row.get("decision_action", review_validation.NO_DECISION)
    )
    source_present = bool(row["source_present"])
    target_present = bool(row["target_present"])
    create_target_default = current_action == "create_target" and not target_present
    uncategorized_default = "Uncategorized" if "Uncategorized" in category_choices else ""
    target_payee_default = (
        target_payee_selected
        or (fingerprint if create_target_default else "")
        or payee_defaults.get(fingerprint, "")
        or (payee_options[0] if payee_options else "")
    )
    no_category_default = (
        review_model.NO_CATEGORY_REQUIRED
        if (
            review_model.is_transfer_payee(target_payee_selected or target_payee_default)
            and not _target_category_required(
                row, target_payee_selected or target_payee_default
            )
        )
        else ""
    )
    target_category_default = (
        target_category_selected
        or (no_category_default if create_target_default else "")
        or (uncategorized_default if create_target_default else "")
        or category_defaults.get(fingerprint, "")
        or (category_options[0] if category_options else "")
    )

    show_all_categories_key = _editor_key(f"show_all_categories_{idx}")
    show_all_categories_default = bool(
        category_choices
        and (
            not category_options
            or (target_category_selected and target_category_selected not in category_options)
        )
    )
    _ensure_widget_state(show_all_categories_key, show_all_categories_default)
    show_all_categories = bool(
        st.session_state.get(show_all_categories_key, show_all_categories_default)
    )

    use_form = False
    form_context = st.form(key=_editor_key(f"row_form_{idx}")) if use_form else nullcontext()
    with form_context:
        source_payee_key = _editor_key(f"source_payee_{idx}")
        source_category_key = _editor_key(f"source_category_{idx}")
        _ensure_widget_state(source_payee_key, source_payee_selected)
        _ensure_widget_state(source_category_key, source_category_selected)

        source_category_choices = _category_choice_list(
            category_options=[],
            category_choices=category_choices,
            selected_value=source_category_selected,
            default_value=source_category_selected,
            show_all=True,
        )
        target_payee_override_key = _editor_key(f"target_payee_override_{idx}")
        _ensure_widget_state(target_payee_override_key, "")
        target_payee_override_current = str(
            st.session_state.get(target_payee_override_key, "") or ""
        )

        payee_choices = [""] + payee_options
        if target_payee_selected and target_payee_selected not in payee_choices:
            payee_choices = [target_payee_selected] + payee_choices
        if (
            target_payee_override_current
            and target_payee_override_current not in payee_choices
        ):
            payee_choices = [target_payee_override_current] + payee_choices
        if target_payee_default and target_payee_default not in payee_choices:
            payee_choices = [target_payee_default] + payee_choices

        payee_current = target_payee_selected or target_payee_default
        category_current = target_category_selected or target_category_default
        category_full = _category_choice_list(
            category_options=category_options,
            category_choices=category_choices,
            selected_value=target_category_selected,
            default_value=target_category_default,
            show_all=bool(show_all_categories),
        )
        target_payee_select_key = _editor_key(f"target_payee_select_{idx}")
        target_category_select_key = _editor_key(f"target_category_select_{idx}")
        _ensure_widget_state(target_payee_select_key, payee_current)
        _ensure_widget_state(target_category_select_key, category_current)

        memo_append_key = _editor_key(f"memo_append_{idx}")
        memo_append_default = str(row.get("memo_append", "") or "").strip()
        _ensure_widget_state(memo_append_key, memo_append_default)
        decision_action_key = _editor_key(f"decision_action_{idx}")
        decision_options = review_validation.allowed_decision_actions(row)
        if current_action not in decision_options:
            decision_options.append(current_action)
        _ensure_widget_state(decision_action_key, current_action)

        update_maps_key = _editor_key(f"update_maps_{idx}")
        update_maps_default = review_validation.parse_update_maps(row.get("update_maps", ""))
        _ensure_widget_state(update_maps_key, update_maps_default)
        st.markdown("<div class='txn-compact'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='txn-inline-note'>A non-ignore decision automatically pushes competing rows for the same source or target transaction to <code>ignore_row</code>.</div>",
            unsafe_allow_html=True,
        )

        source_col, target_col, decision_col = st.columns([1.1, 1.2, 1], vertical_alignment="top")
        with source_col:
            st.markdown("**Source**")
            if source_present:
                source_payee_input = st.text_input(
                    "Source payee",
                    value=str(
                        st.session_state.get(source_payee_key, source_payee_selected) or ""
                    ),
                    key=source_payee_key,
                )
                source_category_select = st.selectbox(
                    "Source category",
                    options=source_category_choices,
                    index=source_category_choices.index(source_category_selected)
                    if source_category_selected in source_category_choices
                    else 0,
                    format_func=lambda value: _format_category_label(
                        value, category_group_map
                    ),
                    key=source_category_key,
                )
            else:
                st.caption("No source transaction for this row.")
                source_payee_input = ""
                source_category_select = ""

        with target_col:
            st.markdown("**Target**")
            target_top_left, target_top_right = st.columns(2)
            with target_top_left:
                target_payee_override = st.text_input(
                    "Target payee override",
                    value=target_payee_override_current,
                    key=target_payee_override_key,
                )
            with target_top_right:
                target_payee_select = st.selectbox(
                    "Target payee option",
                    options=payee_choices,
                    index=payee_choices.index(payee_current) if payee_current in payee_choices else 0,
                    key=target_payee_select_key,
                )
            target_category_col, target_toggle_col = st.columns([5, 1], vertical_alignment="bottom")
            checkbox_kwargs: dict[str, Any] = {}
            if not use_form:
                checkbox_kwargs = {
                    "on_change": _preserve_expansion_context,
                    "kwargs": {"idx": idx, "group_fingerprint": group_fingerprint},
                }
            with target_toggle_col:
                st.checkbox(
                    "Show all",
                    value=bool(
                        st.session_state.get(
                            show_all_categories_key,
                            show_all_categories_default,
                        )
                    ),
                    key=show_all_categories_key,
                    **checkbox_kwargs,
                )
            with target_category_col:
                target_category_select = st.selectbox(
                    "Target category",
                    options=category_full,
                    index=category_full.index(category_current) if category_current in category_full else 0,
                    format_func=lambda value: _format_category_label(value, category_group_map),
                    key=target_category_select_key,
                )
            memo_append_value = st.text_area(
                "Memo add",
                value=str(st.session_state.get(memo_append_key, memo_append_default) or ""),
                key=memo_append_key,
                height=68,
            )

        with decision_col:
            st.markdown("**Decision**")
            decision_action = st.selectbox(
                "Decision",
                options=decision_options,
                index=decision_options.index(current_action),
                key=decision_action_key,
            )
            update_maps_value = st.multiselect(
                "Update maps",
                options=list(review_validation.UPDATE_MAP_TOKENS),
                default=update_maps_default,
                key=update_maps_key,
            )

        action_cols = st.columns(2)
        with action_cols[0]:
            if use_form:
                mark_reviewed = st.form_submit_button("Mark reviewed", use_container_width=True)
            else:
                mark_reviewed = st.button(
                    "Mark reviewed",
                    use_container_width=True,
                    key=_editor_key(f"row_mark_reviewed_{idx}"),
                )
        with action_cols[1]:
            if use_form:
                keep_open = st.form_submit_button(
                    "Mark open" if bool(row.get("reviewed", False)) else "Apply without review",
                    use_container_width=True,
                )
            else:
                keep_open = st.button(
                    "Mark open" if bool(row.get("reviewed", False)) else "Apply without review",
                    use_container_width=True,
                    key=_editor_key(f"row_keep_open_{idx}"),
                )
        if show_apply:
            if use_form:
                apply_all = st.form_submit_button(
                    "Apply to untouched rows with this fingerprint and mark reviewed",
                    use_container_width=True,
                )
            else:
                apply_all = st.button(
                    "Apply to untouched rows with this fingerprint and mark reviewed",
                    use_container_width=True,
                    key=_editor_key(f"row_apply_all_{idx}"),
                )
        else:
            apply_all = False
        st.markdown("</div>", unsafe_allow_html=True)

    source_payee_value = str(st.session_state.get(source_payee_key, source_payee_input) or "")
    source_category_value = str(st.session_state.get(source_category_key, source_category_select) or "")
    target_payee_select_value = str(
        st.session_state.get(target_payee_select_key, target_payee_select) or ""
    )
    target_payee_override_value = str(
        st.session_state.get(target_payee_override_key, target_payee_override) or ""
    )
    target_category_value = str(
        st.session_state.get(target_category_select_key, target_category_select) or ""
    )
    memo_append_value = str(st.session_state.get(memo_append_key, memo_append_value) or "")
    update_maps_tokens = st.session_state.get(update_maps_key, update_maps_value) or []
    decision_action_value = str(
        st.session_state.get(decision_action_key, decision_action) or review_validation.NO_DECISION
    )
    review_requested = bool(mark_reviewed or apply_all)
    submit_any = bool(mark_reviewed or keep_open or apply_all)

    final_target_payee = review_model.resolve_selected_value(
        target_payee_select_value, target_payee_override_value
    )
    final_update_maps = review_validation.join_update_maps(list(update_maps_tokens))

    if submit_any:
        if group_fingerprint:
            st.session_state["expanded_group_fp"] = group_fingerprint
            st.session_state["expanded_group_row_id"] = idx
        else:
            st.session_state["expanded_row_id"] = idx

        working_df = df
        working_df = _call_apply_row_edit(
            working_df,
            idx,
            source_payee=source_payee_value,
            source_category=source_category_value,
            target_payee=final_target_payee,
            target_category=target_category_value,
            memo_append=memo_append_value,
            update_maps=final_update_maps,
            decision_action=decision_action_value,
        )

        review_indices = [idx]
        working_df, competing_indices = _call_apply_competing_row_resolution(working_df, [idx])
        review_indices.extend(competing_indices)

        if apply_all and show_apply:
            untouched_mask = None
            if updated_mask is not None:
                untouched_mask = ~updated_mask.cast(pl.Boolean, strict=False).fill_null(False)
            applied_mask = review_state.series_or_default(working_df, "fingerprint").eq(
                str(row.get("fingerprint", "") or "").strip()
            )
            if untouched_mask is not None:
                applied_mask &= untouched_mask
            working_df = _call_apply_to_same_fingerprint(
                working_df,
                row.get("fingerprint", ""),
                payee=final_target_payee,
                category=target_category_value,
                memo_append=memo_append_value,
                decision_action=decision_action_value
                if decision_action_value != review_validation.NO_DECISION
                else None,
                eligible_mask=untouched_mask,
            )
            applied_indices = [
                current_idx
                for current_idx, flag in enumerate(applied_mask.to_list())
                if flag
            ]
            review_indices.extend(applied_indices)
            working_df, competing_indices = _call_apply_competing_row_resolution(
                working_df, applied_indices
            )
            review_indices.extend(competing_indices)

        row_errors, warnings = review_validation.validate_row(working_df.row(idx, named=True))
        final_df, review_errors = _call_apply_review_state(
            working_df,
            review_indices,
            reviewed=review_requested,
            component_map=component_map,
        )
        review_notice = "Review blocked: " + "; ".join(review_errors) if review_errors else ""

        if review_notice:
            st.error(review_notice)
        elif row_errors:
            st.warning("Pending issues: " + ", ".join(row_errors))
        if warnings:
            st.warning("Warnings: " + ", ".join(warnings))
        if apply_all and show_apply:
            st.success(
                "Applied target values in memory. Click Save to persist."
            )
        _set_review_frames(df=final_df, changed_indices=review_indices)
        if (
            review_requested
            and not review_errors
            and group_fingerprint is None
            and row_order is not None
            and row_page_size is not None
        ):
            next_idx, next_page = _next_row_cursor(row_order, idx, row_page_size)
            _focus_row_view(next_idx, next_page)
        # Recompute counters/badges from the updated dataframe in the same interaction.
        st.rerun()


def _format_category_label(value: str, group_map: dict[str, str]) -> str:
    if not value:
        return ""
    if review_model.is_no_category_required(value):
        return "None (no category required)"
    group = group_map.get(value, "")
    if group:
        return f"{group} / {value}"
    return value


def _default_row_kind_selection(options: list[str]) -> list[str]:
    selected = [value for value in options if value != "Matched cleared"]
    return selected or options


def main() -> None:
    st.set_page_config(page_title="YNAB Review", layout="wide")
    st.title("Proposed Transactions Review")
    _inject_primary_state_css()
    _consume_pending_scroll_to_top()

    _init_from_cli()
    _ensure_loaded()

    if "df" not in st.session_state:
        st.info("Load a proposed_transactions.csv file to begin.")
        source_path = st.text_input("Source path", value=str(DEFAULT_SOURCE))
        if st.button("Load"):
            _load_df(Path(source_path), set_source_path=True)
            st.session_state["save_path"] = str(_default_reviewed_path(Path(source_path)))
            st.rerun()
        return

    df: pl.DataFrame = st.session_state["df"]
    original: pl.DataFrame = st.session_state.get("df_original")
    base: pl.DataFrame = st.session_state.get("df_base")
    review_table: pl.DataFrame | None = st.session_state.get("review_table")
    review_helper_lookup: dict[Any, dict[str, Any]] | None = st.session_state.get("review_helper_lookup")
    category_list: list[str] = st.session_state.get("category_list", [])
    category_group_map: dict[str, str] = st.session_state.get("category_group_map", {})
    category_error = st.session_state.get("category_error", "")
    derived = _get_cached_derived_state(
        st.session_state,
        df,
        original,
        base,
        review_table=review_table,
        validation_state=st.session_state.get("_validation_state"),
    )
    counts = derived["counts"]
    modified = derived["modified"]
    unsaved_mask = derived["unsaved_mask"]
    changed_mask = derived["changed_mask"]
    reviewed_mask = derived["reviewed_mask"]
    updated_mask = derived["updated_mask"]
    inconsistent = derived["inconsistent"]
    uncategorized_mask = derived["uncategorized_mask"]
    blocker_series = derived["blocker_series"]
    data_view: pl.DataFrame = derived["data_view"]
    data_lookup: dict[Any, dict[str, Any]] = derived["data_lookup"]
    state_view: pl.DataFrame = derived["state_view"]
    state_lookup: dict[Any, dict[str, Any]] = derived["state_lookup"]
    inference_tag = derived["inference_tag"]
    progress_tag = derived["progress_tag"]
    persistence_tag = derived["persistence_tag"]
    base_count = derived["base_count"]
    updated_confirmed_count = derived["updated_confirmed_count"]
    saved_reviewed_count = derived["saved_reviewed_count"]
    uncategorized_count = derived["uncategorized_count"]
    component_map = derived["component_map"]

    with st.sidebar:
        st.header("Files")
        save_notice = _consume_notice("save_notice")
        if save_notice:
            st.success(save_notice)
        review_notice = _consume_notice("review_notice")
        if review_notice:
            st.success(review_notice)
        review_error = _consume_notice("review_error")
        if review_error:
            st.error(review_error)
        category_notice = _consume_notice("category_notice")
        if category_notice:
            st.info(category_notice)
        source_path = st.text_input("Source path", value=st.session_state.get("source_path", ""))
        save_path = st.text_input(
            "Save path", value=st.session_state.get("save_path", str(DEFAULT_SAVE))
        )
        st.session_state["save_path"] = save_path
        category_path = st.text_input(
            "Category list path", value=st.session_state.get("category_path", str(DEFAULT_CATEGORIES))
        )
        st.session_state["category_path"] = category_path
        if st.button("Refresh categories from YNAB", use_container_width=True):
            try:
                _refresh_categories_from_api(
                    profile=str(st.session_state.get("profile_name", "") or ""),
                    categories_path=Path(category_path),
                )
                _load_categories(Path(category_path))
                st.session_state["category_path_loaded"] = str(Path(category_path))
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to refresh categories: {exc}")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Reload original"):
                _close_target_split_editor()
                try:
                    _load_df(Path(source_path), set_source_path=True)
                    st.rerun()
                except (FileNotFoundError, ValueError) as exc:
                    st.error(f"Failed to load original: {exc}")
        with col2:
            if st.button("Reload saved"):
                _close_target_split_editor()
                try:
                    _load_df(Path(save_path), set_source_path=False)
                    st.rerun()
                except (FileNotFoundError, ValueError) as exc:
                    st.error(f"Failed to load saved: {exc}")
        save_action_col, save_button_col = st.columns([3, 2])
        with save_action_col:
            save_action = st.selectbox(
                "Save action",
                ["Save", "Save and quit", "Quit"],
                index=0,
                key="save_action",
                label_visibility="collapsed",
            )
        with save_button_col:
            save_pressed = st.button(save_action, use_container_width=True)
        map_updates_path = map_updates.default_map_updates_path(save_path)
        if save_pressed:
            _close_target_split_editor()
            if save_action == "Quit":
                if _request_quit("quit_without_saving"):
                    st.success("Quit requested. This tab can be closed.")
                    st.stop()
            review_io.save_reviewed_transactions(
                review_table if review_table is not None else df,
                save_path,
            )
            map_updates_df = map_updates.save_map_update_candidates(df, base, map_updates_path)
            st.session_state["last_saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _set_review_frames(original=df.clone())
            st.session_state["save_notice"] = (
                f"Saved to {save_path} and wrote {len(map_updates_df)} map updates to {map_updates_path}"
            )
            if save_action == "Save and quit":
                if _request_quit("save_and_quit"):
                    st.success("Saved and quit requested. This tab can be closed.")
                    st.stop()
            st.rerun()

        st.caption(f"Map updates path: {map_updates_path}")

        st.markdown(
            f"**Rows to review:** {base_count}\n"
            f"**Updated:** {updated_confirmed_count}\n"
            f"**Saved:** {saved_reviewed_count}\n"
            f"**Uncategorized:** <span style='color:#b91c1c;font-weight:700;'>{uncategorized_count}</span>",
            unsafe_allow_html=True,
        )

        st.header("View")
        view_mode = st.radio("Mode", ["Grouped", "Row"], index=0, key="view_mode")
        if st.button("Accept all set decisions", use_container_width=True):
            working_df = _apply_staged_row_widget_values(df, list(range(len(df))))
            working_actions = review_validation.normalize_decision_actions(
                working_df.get_column("decision_action")
                if "decision_action" in working_df.columns
                else pl.Series([""] * len(working_df), dtype=pl.Utf8)
            )
            reviewable_mask = working_actions.ne(review_validation.NO_DECISION)
            review_indices = [
                current_idx
                for current_idx, flag in enumerate(reviewable_mask.to_list())
                if flag
            ]
            if not review_indices:
                st.session_state["review_notice"] = "No rows have a decision to accept yet."
            else:
                final_df, review_errors, reviewed_indices = _accept_reviewed_components(
                    working_df,
                    review_indices,
                    component_map=component_map,
                )
                _set_review_frames(df=final_df, changed_indices=review_indices)
                if review_errors:
                    accepted_count = len(reviewed_indices)
                    blocked_count = len(review_indices) - accepted_count
                    message = (
                        f"Accepted {accepted_count} rows in memory. "
                        f"Blocked {blocked_count} rows: " + "; ".join(review_errors)
                    )
                    st.session_state["review_error"] = message
                else:
                    settled_count = int(
                        review_validation.normalize_flag_series(final_df.get_column("reviewed")).sum()
                    )
                    st.session_state["review_notice"] = (
                        f"Marked {settled_count} rows reviewed in memory. Click Save to persist."
                    )
                st.rerun()

        st.header("Filters")
        st.caption("Primary")
        primary_state = st.multiselect(
            "State",
            ["Fix", "Decide", "Settled"],
            default=_default_primary_state_selection(["Fix", "Decide", "Settled"]),
            key="filter_primary_state",
        )
        selected_save_state = st.multiselect(
            "Save status",
            ["Unsaved", "Saved"],
            default=["Unsaved", "Saved"],
            key="filter_save_state",
        )
        row_kind_options = _ordered_filter_options(
            data_view["row_kind"],
            ["Matched", "Matched cleared", "Source only", "Target only", "Ambiguous", "Unrecognized", "Other"],
        )
        selected_row_kind = st.multiselect(
            "Row kind",
            row_kind_options,
            default=_default_row_kind_selection(row_kind_options),
            key="filter_row_kind",
        )
        action_options = _ordered_filter_options(
            data_view["action_label"],
            [
                review_validation.NO_DECISION,
                "keep_match",
                "create_target",
                "create_source",
                "delete_target",
                "delete_source",
                "delete_both",
                "ignore_row",
            ],
        )
        selected_action = st.multiselect(
            "Action",
            action_options,
            default=action_options,
            key="filter_action",
        )

        st.caption("Diagnostics")
        blocker_options = _ordered_filter_options(
            blocker_series,
            [
                "None",
                "Contradiction in component",
                "Institutional source mutation",
                "No decision",
                "Missing payee",
                "Missing category",
                "Uncategorized",
            ],
        )
        selected_blockers = st.multiselect(
            "Blocker",
            blocker_options,
            default=blocker_options,
            key="filter_blocker",
        )
        suggestion_options = _ordered_filter_options(
            state_view["suggestion_label"],
            ["Has suggestions", "No suggestions"],
        )
        selected_suggestions = st.multiselect(
            "Suggestions",
            suggestion_options,
            default=suggestion_options,
            key="filter_suggestions",
        )
        map_update_options = _ordered_filter_options(
            state_view["map_update_label"],
            ["Has update_maps", "No update_maps"],
        )
        selected_map_updates = st.multiselect(
            "Map updates",
            map_update_options,
            default=map_update_options,
            key="filter_map_updates",
        )

        st.caption("Search")
        search_query = st.text_input("Search")

    last_saved_at = st.session_state.get("last_saved_at", "")
    if last_saved_at:
        st.caption(f"Last saved: {last_saved_at}")

    if category_error:
        st.warning(f"Category list not loaded: {category_error}")
    elif not category_list:
        st.warning("Category list is empty; category dropdowns will be limited.")
    st.caption("Legend")
    _render_primary_state_legend()

    changed_count = int(changed_mask.sum())
    reviewed_count = int(reviewed_mask.sum())
    matrix_counts = review_state.state_matrix_counts(state_view["primary_state"], state_view["save_state"])
    st.markdown(
        f"**Total:** {counts['total']} | "
        f"**Missing payee:** {counts['missing_payee']} | "
        f"**Missing category:** {counts['missing_category']} | "
        f"**Uncategorized:** {uncategorized_count} | "
        f"**Unresolved:** {counts['unresolved']} | "
        f"**update_maps:** {counts['update_maps']} | "
        f"**Changed vs original:** {changed_count} | "
        f"**Settled:** {reviewed_count} | "
        f"**Unsaved:** {modified}"
    )
    st.markdown(
        "**State Matrix:** "
        f"Fix/Unsaved={matrix_counts.get('Fix / Unsaved', 0)} | "
        f"Fix/Saved={matrix_counts.get('Fix / Saved', 0)} | "
        f"Decide/Unsaved={matrix_counts.get('Decide / Unsaved', 0)} | "
        f"Decide/Saved={matrix_counts.get('Decide / Saved', 0)} | "
        f"Settled/Unsaved={matrix_counts.get('Settled / Unsaved', 0)} | "
        f"Settled/Saved={matrix_counts.get('Settled / Saved', 0)}"
    )
    if counts["missing_payee"] == 0 and counts["missing_category"] == 0:
        st.success("Ready for upload: payee and category are filled for all rows.")
    else:
        st.warning("Some rows still need decisions or fixes before upload.")
    if uncategorized_count > 0:
        st.warning(f"Uncategorized still selected in {uncategorized_count} rows.")

    if not inconsistent.is_empty():
        st.warning(f"Inconsistent repeated transaction selections: {len(inconsistent)}")

    filtered_indices = review_state.filtered_row_indices_from_views(
        data_view,
        state_view,
        list(range(len(df))),
        primary_state=primary_state,
        row_kind=selected_row_kind,
        action_filter=selected_action,
        save_status=selected_save_state,
        blocker_filter=selected_blockers,
        suggestion_filter=selected_suggestions,
        map_update_filter=selected_map_updates,
        search_query=str(search_query or "").strip().casefold(),
    )
    filtered = pl.DataFrame(
        {
            "_row_pos": filtered_indices,
            "fingerprint": [
                str(df.row(idx, named=True).get("fingerprint", "") or "")
                for idx in filtered_indices
            ],
        }
    )

    working_defaults_view = df.select(["fingerprint", "payee_selected", "category_selected"])
    payee_defaults = review_state.most_common_by_fingerprint(working_defaults_view, "payee_selected")
    category_defaults = review_state.most_common_by_fingerprint(working_defaults_view, "category_selected")
    if view_mode == "Row":
        page_size = st.selectbox("Page size", [25, 50, 100], index=1, key="page_size")
        indices = filtered_indices
        total_pages = max(1, (len(indices) + page_size - 1) // page_size)
        page_key = _editor_key("row_page")
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=int(st.session_state[page_key]),
            step=1,
            key=page_key,
        )
        start = (page - 1) * page_size
        end = start + page_size
        for idx in indices[start:end]:
            row = df.row(idx, named=True)
            helper_row = (
                review_helper_lookup.get(idx)
                if isinstance(review_helper_lookup, dict)
                else None
            )
            row_readiness = _lookup_text(state_lookup, idx, "primary_state")
            row_save_state = _lookup_text(state_lookup, idx, "save_state")
            primary_meta = _primary_state_meta(row_readiness, row_save_state)
            summary_text = _pick_summary_text(row)
            memo_snip = summary_text[:80] + ("…" if len(summary_text) > 80 else "")
            payee_summary = _format_option_summary(
                review_model.parse_option_string(row.get("payee_options", "")),
                limit=2,
            )
            category_summary = _format_option_summary(
                review_model.parse_option_string(row.get("category_options", "")),
                formatter=lambda value: _format_category_label(value, category_group_map),
                limit=2,
            )
            summary = (
                f"[{primary_meta['short']}] {_summary_date(row, helper_row)} | {_format_amount(row)} | "
                f"{_summary_account(row, helper_row)} | "
                f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
                f"{_split_summary_suffix(helper_row)}"
            )
            expanded = st.session_state.get("expanded_row_id") == idx
            _render_primary_state_strip(row_readiness, row_save_state)
            _render_primary_state_anchor(row_readiness, row_save_state)
            with st.expander(summary, expanded=expanded):
                _render_primary_state_banner(row_readiness, row_save_state)
                _render_status_badges(
                    unsaved=bool(unsaved_mask[idx]),
                    changed=bool(changed_mask[idx]),
                    reviewed=bool(reviewed_mask[idx]),
                    uncategorized=bool(uncategorized_mask[idx]),
                )
                _render_secondary_tag_badges(
                    inference=str(inference_tag[idx] or ""),
                    progress=str(progress_tag[idx] or ""),
                    persistence=str(persistence_tag[idx] or ""),
                )
                _render_row_details(
                    row,
                    primary_state=row_readiness,
                    blocker=str(blocker_series[idx] or ""),
                    category_group_map=category_group_map,
                    helper_row=helper_row,
                )
                _render_split_action_buttons(row, idx=idx)
                _render_row_controls(
                    df,
                    idx,
                    category_choices=category_list,
                    category_group_map=category_group_map,
                    payee_defaults=payee_defaults,
                    category_defaults=category_defaults,
                    show_apply=True,
                    updated_mask=updated_mask,
                    component_map=component_map,
                    row_order=indices,
                    row_page_size=page_size,
                )

    else:
        group_page_size = st.selectbox(
            "Group page size", [10, 25, 50], index=0, key="group_page_size"
        )
        group_row_page_size = st.selectbox(
            "Rows per group", [10, 25, 50], index=0, key="group_row_page_size"
        )

        fingerprints, fp_to_indices = _grouped_row_indices(filtered)
        total_pages = max(1, (len(fingerprints) + group_page_size - 1) // group_page_size)
        page_key = _editor_key("group_page")
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=int(st.session_state[page_key]),
            step=1,
            key=page_key,
        )
        start = (page - 1) * group_page_size
        end = start + group_page_size
        for fp in fingerprints[start:end]:
            group_indices = fp_to_indices.get(fp, [])
            group_rows = [df.row(idx, named=True) for idx in group_indices]
            group = pl.from_dicts(group_rows, infer_schema_length=None)
            group_rows = _lookup_rows(data_lookup, group_indices)
            group_payee_options = _option_list(
                [str(row.get("payee_options", "") or "") for row in group_rows]
            )
            group_category_options = _option_list(
                [str(row.get("category_options", "") or "") for row in group_rows]
            )

            group_payee_summary = _format_option_summary(group_payee_options, limit=3)
            group_category_summary = _format_option_summary(
                group_category_options,
                formatter=lambda value: _format_category_label(value, category_group_map),
                limit=3,
            )
            header_fp = fp if len(fp) <= 80 else fp[:77] + "…"
            group_ready = [_lookup_text(state_lookup, idx, "primary_state") for idx in group_indices]
            group_save = [_lookup_text(state_lookup, idx, "save_state") for idx in group_indices]
            group_ready_value, group_save_value = _dominant_group_primary_state(
                group_ready, group_save
            )
            group_primary_meta = _primary_state_meta(group_ready_value, group_save_value)
            header = (
                f"[{group_primary_meta['short']}] {header_fp} ({len(group_rows)}) | "
                f"Payee: {group_payee_summary} | Cat: {group_category_summary}"
            )

            _render_primary_state_strip(group_ready_value, group_save_value)
            with st.expander(
                header, expanded=(st.session_state.get("expanded_group_fp") == fp)
            ):
                _render_primary_state_banner(group_ready_value, group_save_value)
                group_status = _group_status_counts(state_lookup, group_indices)
                group_unsaved = group_status["unsaved"]
                group_changed = group_status["changed"]
                group_saved = group_status["saved"]
                group_uncategorized = group_status["uncategorized"]
                if group_unsaved or group_changed or group_saved or group_uncategorized:
                    st.markdown(
                        " ".join(
                            [
                                f"<span style='color:#b45309;font-weight:600;'>Unsaved: {group_unsaved}</span>"
                                if group_unsaved
                                else "",
                                f"<span style='color:#2563eb;font-weight:600;'>Changed: {group_changed}</span>"
                                if group_changed
                                else "",
                                f"<span style='color:#15803d;font-weight:600;'>Saved: {group_saved}</span>"
                                if group_saved
                                else "",
                                (
                                    f"<span style='color:#b91c1c;font-weight:700;'>Uncategorized: "
                                    f"{group_uncategorized}</span>"
                                )
                                if group_uncategorized
                                else "",
                            ]
                        ).strip(),
                        unsafe_allow_html=True,
                    )
                payee_options = group_payee_options
                category_options = group_category_options

                group_payee_default = _most_common_lookup_value(
                    data_lookup,
                    group_indices,
                    "target_payee_selected",
                )
                if not group_payee_default:
                    group_payee_default = fp if fp else ""
                if not group_payee_default and payee_options:
                    group_payee_default = payee_options[0]

                group_category_default = _most_common_lookup_value(
                    data_lookup,
                    group_indices,
                    "target_category_selected",
                )
                if (
                    not group_category_default
                    and review_model.is_transfer_payee(group_payee_default)
                    and not any(
                        _target_category_required(group_row, group_payee_default)
                        for group_row in group_rows
                    )
                ):
                    group_category_default = review_model.NO_CATEGORY_REQUIRED
                if not group_category_default and "Uncategorized" in category_list:
                    group_category_default = "Uncategorized"
                if not group_category_default and category_options:
                    group_category_default = category_options[0]

                group_payee_choices = [""] + payee_options
                if group_payee_default and group_payee_default not in group_payee_choices:
                    group_payee_choices.insert(1, group_payee_default)

                group_category_choices = [""] + (category_list or category_options)
                if (
                    group_category_default
                    and group_category_default not in group_category_choices
                ):
                    group_category_choices.insert(1, group_category_default)

                group_payee_key = _editor_key(f"group_payee_select_{fp}")
                group_payee_override_key = _editor_key(f"group_payee_override_{fp}")
                group_category_key = _editor_key(f"group_category_{fp}")
                group_memo_append_key = _editor_key(f"group_memo_append_{fp}")
                group_decision_key = _editor_key(f"group_decision_{fp}")
                group_show_all_categories_key = _editor_key(f"group_show_all_categories_{fp}")
                _ensure_widget_state(group_payee_key, group_payee_default)
                _ensure_widget_state(group_payee_override_key, "")
                group_memo_append_default = _most_common_lookup_value(
                    data_lookup,
                    group_indices,
                    "memo_append",
                )
                _ensure_widget_state(group_memo_append_key, group_memo_append_default)
                group_show_all_categories_default = bool(
                    category_list
                    and (
                        not category_options
                        or (
                            group_category_default
                            and group_category_default not in category_options
                        )
                    )
                )
                _ensure_widget_state(
                    group_show_all_categories_key, group_show_all_categories_default
                )
                _ensure_widget_state(group_category_key, group_category_default)
                group_decision_options: list[str] = []
                for group_row in group_rows:
                    for action in review_validation.allowed_decision_actions(group_row):
                        if action not in group_decision_options:
                            group_decision_options.append(action)
                group_decision_default = _most_common_lookup_value(
                    data_lookup,
                    group_indices,
                    "action_label",
                )
                if group_decision_default not in group_decision_options:
                    group_decision_default = review_validation.NO_DECISION
                _ensure_widget_state(group_decision_key, group_decision_default)

                group_payee_select = st.selectbox(
                    "Group payee",
                    options=group_payee_choices,
                    index=group_payee_choices.index(group_payee_default)
                    if group_payee_default in group_payee_choices
                    else 0,
                    key=group_payee_key,
                )
                group_payee_override = st.text_input(
                    "Group payee override",
                    value=str(st.session_state.get(group_payee_override_key, "")),
                    key=group_payee_override_key,
                )

                group_category_col, group_toggle_col = st.columns([5, 1])
                with group_toggle_col:
                    group_show_all_categories = st.checkbox(
                        "Show all",
                        value=bool(
                            st.session_state.get(
                                group_show_all_categories_key,
                                group_show_all_categories_default,
                            )
                        ),
                        key=group_show_all_categories_key,
                        on_change=_preserve_expansion_context,
                        kwargs={"group_fingerprint": fp},
                    )
                group_category_choices = _category_choice_list(
                    category_options=category_options,
                    category_choices=category_list,
                    selected_value=group_category_default,
                    default_value=group_category_default,
                    show_all=bool(group_show_all_categories),
                )
                with group_category_col:
                    group_category_select = st.selectbox(
                        "Group category",
                        options=group_category_choices,
                        index=group_category_choices.index(group_category_default)
                        if group_category_default in group_category_choices
                        else 0,
                        format_func=lambda value: _format_category_label(
                            value, category_group_map
                        ),
                        key=group_category_key,
                    )
                group_memo_append = st.text_area(
                    "Group memo add",
                    value=str(
                        st.session_state.get(
                            group_memo_append_key, group_memo_append_default
                        )
                        or ""
                    ),
                    key=group_memo_append_key,
                    height=68,
                )
                group_decision = st.selectbox(
                    "Group decision",
                    options=group_decision_options,
                    index=group_decision_options.index(group_decision_default),
                    key=group_decision_key,
                )
                staged_group = _apply_staged_row_widget_values(df, group_indices)
                staged_group_actions = review_validation.normalize_decision_actions(
                    pl.Series([staged_group.row(idx, named=True).get("decision_action", "") for idx in group_indices], dtype=pl.Utf8)
                )
                ready_group_count = int(
                    staged_group_actions.ne(review_validation.NO_DECISION).sum()
                )
                unresolved_group_count = int(
                    staged_group_actions.eq(review_validation.NO_DECISION).sum()
                )
                st.caption(
                    "Ready to accept: "
                    f"{ready_group_count} | Still unresolved: {unresolved_group_count}"
                )
                if unresolved_group_count:
                    st.caption(
                        "Unresolved rows still need a decision before they can be accepted."
                    )
                group_action_cols = st.columns(2)
                with group_action_cols[0]:
                    apply_group = st.button(
                        "Apply group edits",
                        use_container_width=True,
                        key=_editor_key(f"group_apply_{fp}"),
                    )
                with group_action_cols[1]:
                    accept_group = st.button(
                        f"Accept set decisions in group ({ready_group_count})",
                        use_container_width=True,
                        key=_editor_key(f"group_accept_{fp}"),
                        disabled=ready_group_count == 0,
                    )

                if apply_group:
                    group_payee_select_value = str(
                        st.session_state.get(group_payee_key, group_payee_select) or ""
                    )
                    group_payee_override_value = str(
                        st.session_state.get(group_payee_override_key, group_payee_override)
                        or ""
                    )
                    group_category_select_value = str(
                        st.session_state.get(group_category_key, group_category_select) or ""
                    )
                    group_memo_append_value = str(
                        st.session_state.get(
                            group_memo_append_key, group_memo_append
                        )
                        or ""
                    )
                    group_decision_value = str(
                        st.session_state.get(group_decision_key, group_decision) or ""
                    )
                    final_payee = (
                        group_payee_override_value.strip()
                        or group_payee_select_value.strip()
                    )
                    payee_to_apply = final_payee if final_payee else None
                    category_to_apply = (
                        group_category_select_value.strip()
                        if group_category_select_value
                        else None
                    )
                    working_df = df
                    visible_group_mask = pl.Series(
                        [idx in set(group_indices) for idx in range(len(working_df))],
                        dtype=pl.Boolean,
                    )
                    eligible_mask = visible_group_mask
                    applied_mask = eligible_mask & review_state.series_or_default(
                        working_df, "fingerprint"
                    ).eq(fp)
                    working_df = _call_apply_to_same_fingerprint(
                        working_df,
                        fp,
                        payee=payee_to_apply,
                        category=category_to_apply,
                        memo_append=group_memo_append_value,
                        decision_action=group_decision_value
                        if group_decision_value != review_validation.NO_DECISION
                        else None,
                        eligible_mask=eligible_mask,
                    )
                    affected_indices = [
                        current_idx
                        for current_idx, flag in enumerate(applied_mask.to_list())
                        if flag
                    ]
                    working_df, competing_indices = _call_apply_competing_row_resolution(
                        working_df, affected_indices
                    )
                    affected_indices.extend(competing_indices)
                    final_df = working_df
                    review_errors: list[str] = []
                    if group_decision_value != review_validation.NO_DECISION:
                        final_df, review_errors = _call_apply_review_state(
                            working_df,
                            affected_indices,
                            reviewed=True,
                            component_map=component_map,
                        )
                    st.session_state["expanded_group_fp"] = fp
                    st.session_state["expanded_group_row_id"] = None
                    _set_review_frames(df=final_df, changed_indices=affected_indices)
                    if review_errors:
                        st.session_state["review_error"] = (
                            "Review blocked: " + "; ".join(review_errors)
                        )
                    elif group_decision_value == review_validation.NO_DECISION:
                        st.session_state["review_notice"] = (
                            "Applied group edits in memory. Choose a group decision, then "
                            "accept decided rows to settle them."
                        )
                    else:
                        st.session_state["review_notice"] = (
                            "Applied group values in memory. Click Save to persist."
                        )
                    # Recompute counters/badges from the updated dataframe in the same interaction.
                    st.rerun()

                if accept_group:
                    working_df = staged_group
                    working_group_rows = [working_df.row(idx, named=True) for idx in group_indices]
                    group_actions = review_validation.normalize_decision_actions(
                        pl.Series(
                            [row.get("decision_action", "") for row in working_group_rows],
                            dtype=pl.Utf8,
                        )
                    )
                    review_indices = [
                        idx
                        for idx, flag in zip(group_indices, group_actions.ne(review_validation.NO_DECISION).to_list(), strict=False)
                        if flag
                    ]
                    st.session_state["expanded_group_fp"] = fp
                    st.session_state["expanded_group_row_id"] = None
                    if not review_indices:
                        st.session_state["review_error"] = (
                            "No rows in this group have a decision to accept yet."
                        )
                    else:
                        working_df, competing_indices = _call_apply_competing_row_resolution(
                            working_df, review_indices
                        )
                        review_indices.extend(competing_indices)
                        final_df, review_errors, reviewed_indices = _accept_reviewed_components(
                            working_df,
                            review_indices,
                            component_map=component_map,
                        )
                        _set_review_frames(df=final_df, changed_indices=review_indices)
                        if review_errors:
                            accepted_count = len(reviewed_indices)
                            blocked_count = len(review_indices) - accepted_count
                            st.session_state["review_error"] = (
                                f"Accepted {accepted_count} group rows in memory. "
                                f"Blocked {blocked_count} rows: " + "; ".join(review_errors)
                            )
                        else:
                            st.session_state["review_notice"] = (
                                "Accepted set group decisions in memory. Click Save to persist."
                            )
                    st.rerun()

                st.markdown("**Rows**")
                row_indices = group_indices
                row_pages = max(1, (len(row_indices) + group_row_page_size - 1) // group_row_page_size)
                fp_key = _fp_key(fp)
                row_page_key = _editor_key(f"group_row_page_{fp_key}")
                if row_page_key not in st.session_state:
                    st.session_state[row_page_key] = 1
                row_page = st.number_input(
                    "Group page",
                    min_value=1,
                    max_value=row_pages,
                    value=int(st.session_state[row_page_key]),
                    step=1,
                    key=row_page_key,
                )
                row_start = (row_page - 1) * group_row_page_size
                row_end = row_start + group_row_page_size
                for idx in row_indices[row_start:row_end]:
                    row = df.row(idx, named=True)
                    helper_row = (
                        review_helper_lookup.get(idx)
                        if isinstance(review_helper_lookup, dict)
                        else None
                    )
                    row_readiness = _lookup_text(state_lookup, idx, "primary_state")
                    row_save_state = _lookup_text(state_lookup, idx, "save_state")
                    primary_meta = _primary_state_meta(row_readiness, row_save_state)
                    summary_text = _pick_summary_text(row)
                    memo_snip = summary_text[:60] + ("…" if len(summary_text) > 60 else "")
                    payee_summary = _format_option_summary(
                        review_model.parse_option_string(row.get("payee_options", "")),
                        limit=2,
                    )
                    category_summary = _format_option_summary(
                        review_model.parse_option_string(row.get("category_options", "")),
                        formatter=lambda value: _format_category_label(
                            value, category_group_map
                        ),
                        limit=2,
                    )
                    summary = (
                        f"[{primary_meta['short']}] {_summary_date(row, helper_row)} | {_format_amount(row)} | "
                        f"{_summary_account(row, helper_row)} | "
                        f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
                        f"{_split_summary_suffix(helper_row)}"
                    )
                    row_expanded = (
                        st.session_state.get("expanded_group_fp") == fp
                        and st.session_state.get("expanded_group_row_id") == idx
                    )
                    _render_primary_state_strip(row_readiness, row_save_state)
                    _render_primary_state_anchor(row_readiness, row_save_state)
                    with st.expander(summary, expanded=row_expanded):
                        _render_primary_state_banner(row_readiness, row_save_state)
                        _render_status_badges(
                            unsaved=bool(unsaved_mask[idx]),
                            changed=bool(changed_mask[idx]),
                            reviewed=bool(reviewed_mask[idx]),
                            uncategorized=bool(uncategorized_mask[idx]),
                        )
                        _render_secondary_tag_badges(
                            inference=str(inference_tag[idx] or ""),
                            progress=str(progress_tag[idx] or ""),
                            persistence=str(persistence_tag[idx] or ""),
                        )
                        _render_row_details(
                            row,
                            primary_state=row_readiness,
                            blocker=str(blocker_series[idx] or ""),
                            category_group_map=category_group_map,
                            helper_row=helper_row,
                        )
                        _render_split_action_buttons(
                            row,
                            idx=idx,
                            group_fingerprint=fp,
                        )
                        _render_row_controls(
                            df,
                            idx,
                            category_choices=category_list,
                            category_group_map=category_group_map,
                            payee_defaults=payee_defaults,
                            category_defaults=category_defaults,
                            show_apply=False,
                            group_fingerprint=fp,
                            updated_mask=updated_mask,
                            component_map=component_map,
                        )

    _maybe_render_target_split_editor_dialog(
        df=df,
        category_choices=category_list,
    )


if __name__ == "__main__":
    main()
