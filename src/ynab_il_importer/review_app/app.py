from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st

import ynab_il_importer.map_updates as map_updates
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.validation as review_validation
import ynab_il_importer.workflow_profiles as workflow_profiles


DEFAULT_SOURCE = Path("outputs/proposed_transactions.csv")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.csv")
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
    "group_show_all_categories_",
    "group_update_maps_",
    "group_row_page_",
    "group_page",
    "row_page",
)
EDITOR_STATE_KEYS = {
    "expanded_row_id",
    "expanded_group_fp",
    "expanded_group_row_id",
    "group_page",
    "row_page",
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
        help="Initial proposed_transactions CSV to load.",
    )
    parser.add_argument(
        "--out",
        dest="output_path",
        default=str(DEFAULT_SAVE),
        help="Save path for reviewed CSV. Defaults to <input>_reviewed.csv.",
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


def _load_df(path: Path, *, set_source_path: bool = False) -> None:
    df = review_io.load_proposed_transactions(path)
    st.session_state["df"] = df
    st.session_state["df_original"] = df.copy()
    if set_source_path:
        st.session_state["source_path"] = str(path)
    _clear_editor_state()


def _load_base(path: Path) -> None:
    base = review_io.load_proposed_transactions(path)
    st.session_state["df_base"] = base


def _load_categories(path: Path) -> None:
    try:
        df = review_io.load_category_list(path)
    except (FileNotFoundError, ValueError) as exc:
        st.session_state["category_list"] = []
        st.session_state["category_group_map"] = {}
        st.session_state["category_path"] = str(path)
        st.session_state["category_error"] = str(exc)
        return

    categories: list[str] = []
    group_map: dict[str, str] = {}
    for _, row in df.iterrows():
        name = str(row.get("category_name", "") or "").strip()
        group = str(row.get("category_group", "") or "").strip()
        if not name:
            continue
        if name not in group_map:
            group_map[name] = group
            categories.append(name)

    st.session_state["category_list"] = categories
    st.session_state["category_group_map"] = group_map
    st.session_state["category_path"] = str(path)
    st.session_state["category_error"] = ""


def _init_from_cli() -> None:
    args = _parse_cli_args()
    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    categories_path = _effective_categories_path(
        categories_path=args.categories_path,
        profile=getattr(args, "profile", "") or "",
    )
    if not _cli_has_flag("--out"):
        output_path = _default_reviewed_path(input_path)

    st.session_state.setdefault("source_path", str(input_path))
    st.session_state.setdefault("save_path", str(output_path))
    st.session_state.setdefault("category_path", str(categories_path))
    st.session_state.setdefault("control_dir", str(getattr(args, "control_dir", "") or ""))

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


def _format_amount(row: pd.Series) -> str:
    outflow = pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce")
    inflow = pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce")
    outflow = float(outflow) if pd.notna(outflow) else 0.0
    inflow = float(inflow) if pd.notna(inflow) else 0.0
    if outflow > 0:
        return f"-{outflow:g}"
    if inflow > 0:
        return f"+{inflow:g}"
    return ""


def _fp_key(fp: str) -> str:
    return hashlib.sha1(fp.encode("utf-8")).hexdigest()[:8]


def _pick_summary_text(row: pd.Series) -> str:
    for col in ["description_clean", "merchant_raw", "description_raw", "memo", "fingerprint"]:
        value = str(row.get(col, "") or "").strip()
        if value:
            return value
    return ""


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
    readiness: pd.Series, save_state: pd.Series
) -> tuple[str, str]:
    priority = [
        ("Fix", "Unsaved"),
        ("Fix", "Saved"),
        ("Decide", "Unsaved"),
        ("Decide", "Saved"),
        ("Settled", "Unsaved"),
        ("Settled", "Saved"),
    ]
    for ready_value, save_value in priority:
        mask = readiness.eq(ready_value) & save_state.eq(save_value)
        if bool(mask.any()):
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


def _row_key_series(df: pd.DataFrame) -> pd.Series:
    if "transaction_id" not in df.columns:
        return pd.Series(df.index.astype("string"), index=df.index, dtype="string")
    txn_id = df["transaction_id"].astype("string").fillna("")
    occurrence = txn_id.groupby(txn_id).cumcount().astype("string")
    return txn_id + "|" + occurrence


def _required_category_missing_mask(df: pd.DataFrame) -> pd.Series:
    payee = review_state.series_or_default(df, "payee_selected").str.strip()
    category = review_state.series_or_default(df, "category_selected").str.strip()
    transfer = payee.map(review_model.is_transfer_payee)
    return category.eq("") & ~transfer


def _uncategorized_mask(df: pd.DataFrame) -> pd.Series:
    category = review_state.series_or_default(df, "category_selected").str.strip().str.casefold()
    return category.str.contains("uncategorized", regex=False)


def _primary_state_series(df: pd.DataFrame) -> pd.Series:
    reviewed = df.get("reviewed", pd.Series([False] * len(df), index=df.index)).astype(bool)
    uncategorized = _uncategorized_mask(df)
    states: list[str] = []
    for idx, row in df.iterrows():
        row_errors, _ = review_validation.validate_row(row)
        if bool(uncategorized.loc[idx]) or row_errors:
            states.append("Fix")
        elif bool(reviewed.loc[idx]):
            states.append("Settled")
        else:
            states.append("Decide")
    return pd.Series(states, index=df.index, dtype="string")


def _derive_inference_tags(df: pd.DataFrame) -> pd.Series:
    match_status = review_state.series_or_default(df, "match_status").str.strip().str.lower()
    payee = review_state.series_or_default(df, "payee_selected").str.strip()
    missing_required = payee.eq("") | _required_category_missing_mask(df)

    inferred = pd.Series(["unique"] * len(df), index=df.index, dtype="string")
    inferred = inferred.where(~match_status.eq("none"), "unrecognized")
    inferred = inferred.where(~match_status.eq("ambiguous"), "ambiguous")
    inferred = inferred.where(
        ~(~match_status.isin(["none", "ambiguous"]) & missing_required), "missing"
    )
    unknown = (
        ~match_status.isin(["", "none", "ambiguous", "unique"])
        & ~missing_required
    )
    inferred = inferred.where(~unknown, match_status)
    return inferred


def _initial_inference_tags(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    fallback = _derive_inference_tags(df)
    if base is None or base.empty:
        return fallback

    base_keys = _row_key_series(base)
    base_inference = _derive_inference_tags(base)
    base_map = pd.Series(base_inference.to_numpy(), index=base_keys)

    current_keys = _row_key_series(df)
    aligned = current_keys.map(base_map)
    return aligned.fillna(fallback).astype("string")


def _apply_row_filters(
    df: pd.DataFrame,
    *,
    primary_state: list[str],
    primary_save: list[str],
    tag_inference: list[str],
    tag_progress: list[str],
    tag_persistence: list[str],
    primary_state_series: pd.Series,
    save_state: pd.Series,
    inference_tag: pd.Series,
    progress_tag: pd.Series,
    persistence_tag: pd.Series,
    fingerprint_query: str,
    payee_query: str,
    memo_query: str,
    source_query: str,
    account_query: str,
) -> pd.DataFrame:
    mask = pd.Series([True] * len(df), index=df.index)
    mask &= primary_state_series.isin(primary_state)
    mask &= save_state.isin(primary_save)
    mask &= inference_tag.isin(tag_inference)
    mask &= progress_tag.isin(tag_progress)
    mask &= persistence_tag.isin(tag_persistence)

    if fingerprint_query:
        mask &= (
            review_state.series_or_default(df, "fingerprint")
            .str.casefold()
            .str.contains(fingerprint_query, regex=False)
        )
    if payee_query:
        payee_text = (
            review_state.series_or_default(df, "payee_selected")
            + " "
            + review_state.series_or_default(df, "payee_options")
        )
        mask &= payee_text.str.casefold().str.contains(payee_query, regex=False)
    if memo_query:
        memo_text = (
            review_state.series_or_default(df, "memo")
            + " "
            + review_state.series_or_default(df, "description_raw")
            + " "
            + review_state.series_or_default(df, "description_clean")
        )
        mask &= memo_text.str.casefold().str.contains(memo_query, regex=False)
    if source_query:
        mask &= (
            review_state.series_or_default(df, "source")
            .str.casefold()
            .str.contains(source_query, regex=False)
        )
    if account_query:
        mask &= (
            review_state.series_or_default(df, "account_name")
            .str.casefold()
            .str.contains(account_query, regex=False)
        )

    return df[mask]


def _merge_category_choices(*values: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
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


def _selected_side_value(row: pd.Series, *, side: str, field: str) -> str:
    column = f"{side}_{field}_selected"
    return str(row.get(column, "") or "").strip()


def _allowed_decision_actions(row: pd.Series) -> list[str]:
    workflow_type = str(row.get("workflow_type", "") or "").strip().casefold()
    source_present = bool(row.get("source_present", False))
    target_present = bool(row.get("target_present", False))

    actions = [review_validation.NO_DECISION, "ignore_row"]
    if source_present and target_present:
        actions = [review_validation.NO_DECISION, "keep_match", "delete_source", "delete_target", "delete_both", "ignore_row"]
    elif source_present and not target_present:
        actions = [review_validation.NO_DECISION, "create_target", "delete_source", "ignore_row"]
    elif target_present and not source_present:
        actions = [review_validation.NO_DECISION, "create_source", "delete_target", "ignore_row"]

    if workflow_type == "institutional":
        actions = [
            action
            for action in actions
            if action not in review_validation.SOURCE_MUTATION_ACTIONS
        ]

    ordered: list[str] = []
    for action in actions:
        if action not in ordered:
            ordered.append(action)
    return ordered


def _apply_action_propagation(
    df: pd.DataFrame,
    idx: Any,
    *,
    decision_action: str,
    include_source: bool,
    include_target: bool,
) -> None:
    mask = review_state.related_rows_mask(
        df,
        idx,
        include_source=include_source,
        include_target=include_target,
    )
    if "decision_action" in df.columns:
        df.loc[mask, "decision_action"] = str(decision_action).strip()


def _render_row_controls(
    df: pd.DataFrame,
    idx: Any,
    category_choices: list[str],
    category_group_map: dict[str, str],
    payee_defaults: dict[str, str],
    category_defaults: dict[str, str],
    show_apply: bool = True,
    group_fingerprint: str | None = None,
    updated_mask: pd.Series | None = None,
) -> None:
    row = df.loc[idx]
    fingerprint = str(row.get("fingerprint", "") or "")
    payee_options = review_model.parse_option_string(row.get("payee_options", ""))
    category_options = review_model.parse_option_string(row.get("category_options", ""))

    source_payee_selected = _selected_side_value(row, side="source", field="payee")
    source_category_selected = _selected_side_value(row, side="source", field="category")
    target_payee_selected = _selected_side_value(row, side="target", field="payee")
    target_category_selected = _selected_side_value(row, side="target", field="category")
    target_payee_default = target_payee_selected or payee_defaults.get(fingerprint, "") or (
        payee_options[0] if payee_options else ""
    )
    target_category_default = (
        target_category_selected
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
    show_all_categories = st.checkbox(
        "Show all categories",
        value=bool(st.session_state.get(show_all_categories_key, show_all_categories_default)),
        key=show_all_categories_key,
    )

    with st.form(key=_editor_key(f"row_form_{idx}")):
        st.markdown("**Source**")
        source_payee_key = _editor_key(f"source_payee_{idx}")
        source_category_key = _editor_key(f"source_category_{idx}")
        _ensure_widget_state(source_payee_key, source_payee_selected)
        _ensure_widget_state(source_category_key, source_category_selected)

        source_payee_input = st.text_input(
            "Source payee",
            value=str(st.session_state.get(source_payee_key, source_payee_selected) or ""),
            key=source_payee_key,
        )
        source_category_choices = _category_choice_list(
            category_options=[],
            category_choices=category_choices,
            selected_value=source_category_selected,
            default_value=source_category_selected,
            show_all=True,
        )
        source_category_select = st.selectbox(
            "Source category",
            options=source_category_choices,
            index=source_category_choices.index(source_category_selected)
            if source_category_selected in source_category_choices
            else 0,
            format_func=lambda value: _format_category_label(value, category_group_map),
            key=source_category_key,
        )

        st.markdown("**Target**")
        target_payee_override_key = _editor_key(f"target_payee_override_{idx}")
        _ensure_widget_state(target_payee_override_key, "")
        target_payee_override = st.text_input(
            "Target payee override",
            value=st.session_state.get(target_payee_override_key, ""),
            key=target_payee_override_key,
        )

        payee_choices = [""] + payee_options
        if target_payee_selected and target_payee_selected not in payee_choices:
            payee_choices = [target_payee_selected] + payee_choices
        if target_payee_override and target_payee_override not in payee_choices:
            payee_choices = [target_payee_override] + payee_choices
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

        target_payee_select = st.selectbox(
            "Target payee option",
            options=payee_choices,
            index=payee_choices.index(payee_current) if payee_current in payee_choices else 0,
            key=target_payee_select_key,
        )
        target_category_select = st.selectbox(
            "Target category",
            options=category_full,
            index=category_full.index(category_current) if category_current in category_full else 0,
            format_func=lambda value: _format_category_label(value, category_group_map),
            key=target_category_select_key,
        )

        st.markdown("**Decision**")
        decision_action_key = _editor_key(f"decision_action_{idx}")
        decision_options = _allowed_decision_actions(row)
        current_action = str(row.get("decision_action", review_validation.NO_DECISION) or "").strip()
        if not current_action:
            current_action = review_validation.NO_DECISION
        if current_action not in decision_options:
            decision_options.append(current_action)
        _ensure_widget_state(decision_action_key, current_action)
        decision_action = st.selectbox(
            "Decision",
            options=decision_options,
            index=decision_options.index(current_action),
            key=decision_action_key,
        )

        update_maps_key = _editor_key(f"update_maps_{idx}")
        update_maps_default = review_validation.parse_update_maps(row.get("update_maps", ""))
        _ensure_widget_state(update_maps_key, update_maps_default)
        update_maps_value = st.multiselect(
            "Update maps",
            options=list(review_validation.UPDATE_MAP_TOKENS),
            default=update_maps_default,
            key=update_maps_key,
        )

        reviewed_key = _editor_key(f"reviewed_{idx}")
        _ensure_widget_state(reviewed_key, bool(row.get("reviewed", False)))
        reviewed_requested = st.checkbox(
            "Reviewed",
            value=bool(st.session_state.get(reviewed_key, row.get("reviewed", False))),
            key=reviewed_key,
        )

        propagate_source_key = _editor_key(f"propagate_action_source_{idx}")
        propagate_target_key = _editor_key(f"propagate_action_target_{idx}")
        _ensure_widget_state(propagate_source_key, False)
        _ensure_widget_state(propagate_target_key, False)
        propagate_cols = st.columns(2)
        with propagate_cols[0]:
            propagate_source = st.checkbox(
                "Propagate action to same source",
                value=bool(st.session_state.get(propagate_source_key, False)),
                key=propagate_source_key,
            )
        with propagate_cols[1]:
            propagate_target = st.checkbox(
                "Propagate action to same target",
                value=bool(st.session_state.get(propagate_target_key, False)),
                key=propagate_target_key,
            )

        submitted = st.form_submit_button("Save row", use_container_width=True)
        if show_apply:
            apply_all = st.form_submit_button(
                "Apply target values to untouched rows with this fingerprint",
                use_container_width=True,
            )
        else:
            apply_all = False

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
    update_maps_tokens = st.session_state.get(update_maps_key, update_maps_value) or []
    decision_action_value = str(
        st.session_state.get(decision_action_key, decision_action) or review_validation.NO_DECISION
    )
    reviewed_value = bool(st.session_state.get(reviewed_key, reviewed_requested))
    propagate_source_value = bool(st.session_state.get(propagate_source_key, propagate_source))
    propagate_target_value = bool(st.session_state.get(propagate_target_key, propagate_target))

    final_target_payee = review_model.resolve_selected_value(
        target_payee_select_value, target_payee_override_value
    )
    final_update_maps = review_validation.join_update_maps(list(update_maps_tokens))

    if submitted or apply_all:
        if group_fingerprint:
            st.session_state["expanded_group_fp"] = group_fingerprint
            st.session_state["expanded_group_row_id"] = idx
        else:
            st.session_state["expanded_row_id"] = idx

        working_df = df.copy()
        review_state.apply_row_edit(
            working_df,
            idx,
            source_payee=source_payee_value,
            source_category=source_category_value,
            target_payee=final_target_payee,
            target_category=target_category_value,
            update_maps=final_update_maps,
            decision_action=decision_action_value,
        )

        if propagate_source_value or propagate_target_value:
            _apply_action_propagation(
                working_df,
                idx,
                decision_action=decision_action_value,
                include_source=propagate_source_value,
                include_target=propagate_target_value,
            )

        if apply_all and show_apply:
            untouched_mask = None
            if updated_mask is not None:
                untouched_mask = ~updated_mask.astype(bool)
            review_model.apply_to_same_fingerprint(
                working_df,
                row.get("fingerprint", ""),
                payee=final_target_payee,
                category=target_category_value,
                eligible_mask=untouched_mask,
            )

        row_errors, warnings = review_validation.validate_row(working_df.loc[idx])
        final_df = working_df
        review_notice = ""
        if reviewed_value:
            reviewed_df = working_df.copy()
            review_state.apply_row_edit(reviewed_df, idx, reviewed=True)
            component_errors = review_validation.review_component_errors(reviewed_df, idx)
            if component_errors:
                final_df = working_df.copy()
                review_state.apply_row_edit(final_df, idx, reviewed=False)
                review_notice = "Review blocked: " + "; ".join(component_errors)
            else:
                final_df = reviewed_df
        else:
            review_state.apply_row_edit(final_df, idx, reviewed=False)

        if review_notice:
            st.error(review_notice)
        elif row_errors:
            st.warning("Pending issues: " + ", ".join(row_errors))
        if warnings:
            st.warning("Warnings: " + ", ".join(warnings))
        if apply_all and show_apply:
            st.success(
                "Applied target values to untouched rows with this fingerprint in memory. "
                "Click Save to persist."
            )
        st.session_state["df"] = final_df
        # Recompute counters/badges from the updated dataframe in the same interaction.
        st.rerun()


def _format_category_label(value: str, group_map: dict[str, str]) -> str:
    if not value:
        return ""
    group = group_map.get(value, "")
    if group:
        return f"{group} / {value}"
    return value


def main() -> None:
    st.set_page_config(page_title="YNAB Review", layout="wide")
    st.title("Proposed Transactions Review")
    _inject_primary_state_css()

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

    df: pd.DataFrame = st.session_state["df"]
    original: pd.DataFrame = st.session_state.get("df_original")
    base: pd.DataFrame = st.session_state.get("df_base")
    category_list: list[str] = st.session_state.get("category_list", [])
    category_group_map: dict[str, str] = st.session_state.get("category_group_map", {})
    category_error = st.session_state.get("category_error", "")

    counts = review_state.summary_counts(df)
    modified = review_state.modified_count(df, original)
    unsaved_mask = review_state.modified_mask(df, original)
    changed_mask = review_state.changed_mask(df, base)
    reviewed_mask = df.get("reviewed", pd.Series([False] * len(df), index=df.index)).astype(
        bool
    )
    saved_mask = review_state.saved_mask(original, base, df.index)
    updated_mask = (changed_mask | reviewed_mask).astype(bool)
    inconsistent = review_validation.inconsistent_fingerprints(df)
    uncategorized_mask = _uncategorized_mask(df)
    primary_state_series = _primary_state_series(df)
    save_state = pd.Series(
        ["Saved" if bool(value) else "Unsaved" for value in saved_mask],
        index=df.index,
        dtype="string",
    )
    inference_tag = _initial_inference_tags(df, base)
    progress_tag = pd.Series(
        ["resolved" if bool(value) else "unchanged" for value in updated_mask],
        index=df.index,
        dtype="string",
    )
    persistence_tag = save_state.str.lower()

    base_count = len(base) if isinstance(base, pd.DataFrame) and not base.empty else len(df)
    updated_confirmed_count = int(updated_mask.sum())
    saved_reviewed_count = int(saved_mask.sum())
    uncategorized_count = int(uncategorized_mask.sum())

    with st.sidebar:
        st.header("Files")
        save_notice = _consume_notice("save_notice")
        if save_notice:
            st.success(save_notice)
        source_path = st.text_input("Source path", value=st.session_state.get("source_path", ""))
        save_path = st.text_input(
            "Save path", value=st.session_state.get("save_path", str(DEFAULT_SAVE))
        )
        st.session_state["save_path"] = save_path
        category_path = st.text_input(
            "Category list path", value=st.session_state.get("category_path", str(DEFAULT_CATEGORIES))
        )
        st.session_state["category_path"] = category_path
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Reload original"):
                try:
                    _load_df(Path(source_path), set_source_path=True)
                    st.rerun()
                except (FileNotFoundError, ValueError) as exc:
                    st.error(f"Failed to load original: {exc}")
        with col2:
            if st.button("Reload saved"):
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
            if save_action == "Quit":
                if _request_quit("quit_without_saving"):
                    st.success("Quit requested. This tab can be closed.")
                    st.stop()
            review_io.save_reviewed_transactions(df, save_path)
            map_updates_df = map_updates.save_map_update_candidates(df, base, map_updates_path)
            st.session_state["last_saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["df_original"] = df.copy()
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

        st.header("Filters")
        st.caption("Primary dimensions")
        primary_state = st.multiselect(
            "State",
            ["Fix", "Decide", "Settled"],
            default=["Fix", "Decide", "Settled"],
            key="filter_primary_state",
        )
        primary_save = st.multiselect(
            "Save state",
            ["Unsaved", "Saved"],
            default=["Unsaved"],
            key="filter_primary_save",
        )

        st.caption("Secondary tags")
        inference_canonical = ["unrecognized", "missing", "ambiguous", "unique"]
        inference_seen = [
            value
            for value in inference_canonical
            if value in set(inference_tag.astype("string").tolist())
        ]
        inference_extra = sorted(
            set(inference_tag.astype("string").tolist()) - set(inference_seen)
        )
        inference_options = inference_seen + inference_extra
        if not inference_options:
            inference_options = inference_canonical.copy()
        selected_inference = st.multiselect(
            "Inference tag",
            inference_options,
            default=inference_options,
            key="filter_tag_inference",
        )
        selected_progress = st.multiselect(
            "Progress tag",
            ["unchanged", "resolved"],
            default=["unchanged", "resolved"],
            key="filter_tag_progress",
        )
        selected_persistence = st.multiselect(
            "Persistence tag",
            ["unsaved", "saved"],
            default=["unsaved", "saved"],
            key="filter_tag_persistence",
        )

        st.caption("Text filters")
        fingerprint_query = st.text_input("Fingerprint contains")
        payee_query = st.text_input("Payee contains")
        memo_query = st.text_input("Memo/description contains")
        source_query = st.text_input("Source contains")
        account_query = st.text_input("Account contains")

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
    matrix_counts = (
        pd.Series(
            [
                f"{str(primary_state_series.loc[idx])} / {str(save_state.loc[idx])}"
                for idx in df.index
            ],
            index=df.index,
        )
        .value_counts()
        .to_dict()
    )
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

    if not inconsistent.empty:
        st.warning(f"Inconsistent repeated transaction selections: {len(inconsistent)}")

    filtered = _apply_row_filters(
        df,
        primary_state=primary_state,
        primary_save=primary_save,
        tag_inference=selected_inference,
        tag_progress=selected_progress,
        tag_persistence=selected_persistence,
        primary_state_series=primary_state_series,
        save_state=save_state,
        inference_tag=inference_tag,
        progress_tag=progress_tag,
        persistence_tag=persistence_tag,
        fingerprint_query=str(fingerprint_query or "").strip().casefold(),
        payee_query=str(payee_query or "").strip().casefold(),
        memo_query=str(memo_query or "").strip().casefold(),
        source_query=str(source_query or "").strip().casefold(),
        account_query=str(account_query or "").strip().casefold(),
    )

    payee_defaults = review_state.most_common_by_fingerprint(df, "payee_selected")
    category_defaults = review_state.most_common_by_fingerprint(df, "category_selected")
    if view_mode == "Row":
        page_size = st.selectbox("Page size", [25, 50, 100], index=1, key="page_size")
        indices = filtered.index.tolist()
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
            row = df.loc[idx]
            row_readiness = str(primary_state_series.loc[idx] or "")
            row_save_state = str(save_state.loc[idx] or "")
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
                f"[{primary_meta['short']}] {row.get('date','')} | {_format_amount(row)} | "
                f"{str(row.get('account_name', '') or '').strip()} | "
                f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
            )
            expanded = st.session_state.get("expanded_row_id") == idx
            _render_primary_state_strip(row_readiness, row_save_state)
            _render_primary_state_anchor(row_readiness, row_save_state)
            with st.expander(summary, expanded=expanded):
                _render_primary_state_banner(row_readiness, row_save_state)
                _render_status_badges(
                    unsaved=bool(unsaved_mask.loc[idx]),
                    changed=bool(changed_mask.loc[idx]),
                    reviewed=bool(reviewed_mask.loc[idx]),
                    uncategorized=bool(uncategorized_mask.loc[idx]),
                )
                _render_secondary_tag_badges(
                    inference=str(inference_tag.loc[idx] or ""),
                    progress=str(progress_tag.loc[idx] or ""),
                    persistence=str(persistence_tag.loc[idx] or ""),
                )
                st.write(
                    {
                        "date": row.get("date", ""),
                        "amount": _format_amount(row),
                        "memo": str(row.get("memo", "") or ""),
                        "fingerprint": row.get("fingerprint", ""),
                        "match_status": row.get("match_status", ""),
                        "inference_tag_initial": str(inference_tag.loc[idx] or ""),
                        "progress_tag": str(progress_tag.loc[idx] or ""),
                        "persistence_tag": str(persistence_tag.loc[idx] or ""),
                        "primary_state": str(primary_state_series.loc[idx] or ""),
                        "source": row.get("source", ""),
                        "account": row.get("account_name", ""),
                    }
                )
                _render_row_controls(
                    df,
                    idx,
                    category_choices=category_list,
                    category_group_map=category_group_map,
                    payee_defaults=payee_defaults,
                    category_defaults=category_defaults,
                    show_apply=True,
                    updated_mask=updated_mask,
                )

    else:
        group_page_size = st.selectbox(
            "Group page size", [10, 25, 50], index=0, key="group_page_size"
        )
        group_row_page_size = st.selectbox(
            "Rows per group", [10, 25, 50], index=0, key="group_row_page_size"
        )

        filtered_fps = review_state.series_or_default(filtered, "fingerprint")
        filtered_fp_set = set(filtered_fps.tolist())
        all_sizes = (
            review_state.series_or_default(df, "fingerprint")
            .value_counts()
            .sort_values(ascending=False)
        )
        fingerprints = [fp for fp in all_sizes.index.tolist() if fp in filtered_fp_set]
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
            group = df[df["fingerprint"].astype("string").fillna("") == fp]
            group_payee_options: list[str] = []
            group_category_options: list[str] = []
            for _, row in group.iterrows():
                for opt in review_model.parse_option_string(row.get("payee_options", "")):
                    if opt not in group_payee_options:
                        group_payee_options.append(opt)
                for opt in review_model.parse_option_string(row.get("category_options", "")):
                    if opt not in group_category_options:
                        group_category_options.append(opt)

            group_payee_summary = _format_option_summary(group_payee_options, limit=3)
            group_category_summary = _format_option_summary(
                group_category_options,
                formatter=lambda value: _format_category_label(value, category_group_map),
                limit=3,
            )
            header_fp = fp if len(fp) <= 80 else fp[:77] + "…"
            group_ready = primary_state_series.loc[group.index].astype("string")
            group_save = save_state.loc[group.index].astype("string")
            group_ready_value, group_save_value = _dominant_group_primary_state(
                group_ready, group_save
            )
            group_primary_meta = _primary_state_meta(group_ready_value, group_save_value)
            header = (
                f"[{group_primary_meta['short']}] {header_fp} ({len(group)}) | "
                f"Payee: {group_payee_summary} | Cat: {group_category_summary}"
            )

            _render_primary_state_strip(group_ready_value, group_save_value)
            with st.expander(
                header, expanded=(st.session_state.get("expanded_group_fp") == fp)
            ):
                _render_primary_state_banner(group_ready_value, group_save_value)
                group_unsaved = int(unsaved_mask.loc[group.index].sum())
                group_changed = int(changed_mask.loc[group.index].sum())
                group_saved = int(saved_mask.loc[group.index].sum())
                group_uncategorized = int(uncategorized_mask.loc[group.index].sum())
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
                payee_options: list[str] = []
                category_options: list[str] = []
                for _, row in group.iterrows():
                    for opt in review_model.parse_option_string(row.get("payee_options", "")):
                        if opt not in payee_options:
                            payee_options.append(opt)
                    for opt in review_model.parse_option_string(row.get("category_options", "")):
                        if opt not in category_options:
                            category_options.append(opt)

                group_payee_default = review_state.most_common_value(group["payee_selected"])
                if not group_payee_default and payee_options:
                    group_payee_default = payee_options[0]

                group_category_default = review_state.most_common_value(group["category_selected"])
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
                group_show_all_categories_key = _editor_key(f"group_show_all_categories_{fp}")
                _ensure_widget_state(group_payee_key, group_payee_default)
                _ensure_widget_state(group_payee_override_key, "")
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
                apply_group = st.button(
                    "Apply to all in group",
                    use_container_width=True,
                    key=_editor_key(f"group_apply_{fp}"),
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
                    untouched_mask = ~updated_mask.astype(bool)
                    review_model.apply_to_same_fingerprint(
                        df,
                        fp,
                        payee=payee_to_apply,
                        category=category_to_apply,
                        eligible_mask=untouched_mask,
                    )
                    st.session_state["expanded_group_fp"] = fp
                    st.session_state["expanded_group_row_id"] = None
                    st.session_state["df"] = df
                    st.success(
                        "Applied group values to untouched rows in memory. "
                        "Click Save to persist."
                    )
                    # Recompute counters/badges from the updated dataframe in the same interaction.
                    st.rerun()

                st.markdown("**Rows**")
                row_indices = group.index.tolist()
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
                    row = df.loc[idx]
                    row_readiness = str(primary_state_series.loc[idx] or "")
                    row_save_state = str(save_state.loc[idx] or "")
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
                        f"[{primary_meta['short']}] {row.get('date','')} | {_format_amount(row)} | "
                        f"{str(row.get('account_name', '') or '').strip()} | "
                        f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
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
                            unsaved=bool(unsaved_mask.loc[idx]),
                            changed=bool(changed_mask.loc[idx]),
                            reviewed=bool(reviewed_mask.loc[idx]),
                            uncategorized=bool(uncategorized_mask.loc[idx]),
                        )
                        _render_secondary_tag_badges(
                            inference=str(inference_tag.loc[idx] or ""),
                            progress=str(progress_tag.loc[idx] or ""),
                            persistence=str(persistence_tag.loc[idx] or ""),
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
                        )


if __name__ == "__main__":
    main()
