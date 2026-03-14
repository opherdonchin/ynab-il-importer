from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
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


DEFAULT_SOURCE = Path("outputs/proposed_transactions.csv")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.csv")
DEFAULT_CATEGORIES = Path("outputs/ynab_categories.csv")
DEFAULT_RESUME_SENTINEL = "__DEFAULT_RESUME__"


EDITOR_STATE_PREFIXES = (
    "payee_override_",
    "payee_select_",
    "category_select_",
    "show_all_categories_",
    "update_map_",
    "group_payee_select_",
    "group_payee_override_",
    "group_category_",
    "group_show_all_categories_",
    "group_update_",
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
        "--resume",
        nargs="?",
        const=DEFAULT_RESUME_SENTINEL,
        help="Resume from a previously saved review CSV (optional path).",
    )
    return parser


def _parse_cli_args() -> argparse.Namespace:
    parser = _build_arg_parser()
    return parser.parse_known_args(sys.argv[1:])[0]


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


def _quit_app() -> None:
    time.sleep(0.1)
    os._exit(0)


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
    if not _cli_has_flag("--out"):
        output_path = _default_reviewed_path(input_path)

    st.session_state.setdefault("source_path", str(input_path))
    st.session_state.setdefault("save_path", str(output_path))
    st.session_state.setdefault("category_path", str(args.categories_path))

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


def _render_status_badges(*, unsaved: bool, changed: bool, reviewed: bool) -> None:
    badges: list[str] = []
    if unsaved:
        badges.append(
            "<span style='color:#b45309;font-weight:600;'>Unsaved</span>"
        )
    if changed:
        badges.append(
            "<span style='color:#2563eb;font-weight:600;'>Changed vs original</span>"
        )
    if reviewed:
        badges.append(
            "<span style='color:#15803d;font-weight:600;'>Reviewed</span>"
        )
    if badges:
        st.markdown(" ".join(badges), unsafe_allow_html=True)


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

    payee_selected = str(row.get("payee_selected", "") or "").strip()
    category_selected = str(row.get("category_selected", "") or "").strip()
    payee_default = payee_selected or payee_defaults.get(fingerprint, "") or (
        payee_options[0] if payee_options else ""
    )
    category_default = (
        category_selected
        or category_defaults.get(fingerprint, "")
        or (category_options[0] if category_options else "")
    )

    show_all_categories_key = _editor_key(f"show_all_categories_{idx}")
    show_all_categories_default = bool(
        category_choices
        and (
            not category_options
            or (category_selected and category_selected not in category_options)
        )
    )
    _ensure_widget_state(show_all_categories_key, show_all_categories_default)
    show_all_categories = st.checkbox(
        "Show all categories",
        value=bool(st.session_state.get(show_all_categories_key, show_all_categories_default)),
        key=show_all_categories_key,
    )

    with st.form(key=_editor_key(f"row_form_{idx}")):
        payee_override_key = _editor_key(f"payee_override_{idx}")
        _ensure_widget_state(payee_override_key, "")
        payee_override = st.text_input(
            "Payee override",
            value=st.session_state.get(payee_override_key, ""),
            key=payee_override_key,
        )

        payee_choices = [""] + payee_options
        if payee_selected and payee_selected not in payee_choices:
            payee_choices = [payee_selected] + payee_choices
        if payee_override and payee_override not in payee_choices:
            payee_choices = [payee_override] + payee_choices
        if payee_default and payee_default not in payee_choices:
            payee_choices = [payee_default] + payee_choices

        payee_current = payee_selected or payee_default
        category_current = category_selected or category_default
        category_full = _category_choice_list(
            category_options=category_options,
            category_choices=category_choices,
            selected_value=category_selected,
            default_value=category_default,
            show_all=bool(show_all_categories),
        )
        payee_select_key = _editor_key(f"payee_select_{idx}")
        category_select_key = _editor_key(f"category_select_{idx}")
        _ensure_widget_state(payee_select_key, payee_current)
        _ensure_widget_state(category_select_key, category_current)

        payee_select = st.selectbox(
            "Payee option",
            options=payee_choices,
            index=payee_choices.index(payee_current) if payee_current in payee_choices else 0,
            key=payee_select_key,
        )
        category_select = st.selectbox(
            "Category option",
            options=category_full,
            index=category_full.index(category_current) if category_current in category_full else 0,
            format_func=lambda value: _format_category_label(value, category_group_map),
            key=category_select_key,
        )

        update_key = _editor_key(f"update_map_{idx}")
        _ensure_widget_state(update_key, bool(row.get("update_map", False)))
        update_val = st.checkbox(
            "Update map", value=bool(row.get("update_map", False)), key=update_key
        )

        submitted = st.form_submit_button("Save row", use_container_width=True)
        if show_apply:
            apply_all = st.form_submit_button(
                "Apply to all with this fingerprint", use_container_width=True
            )
        else:
            apply_all = False

    payee_select_value = str(st.session_state.get(payee_select_key, payee_select) or "")
    payee_override_value = str(st.session_state.get(payee_override_key, payee_override) or "")
    category_select_value = str(st.session_state.get(category_select_key, category_select) or "")
    update_value = bool(st.session_state.get(update_key, update_val))

    final_payee = review_model.resolve_selected_value(payee_select_value, payee_override_value)
    final_category = review_model.resolve_selected_value(category_select_value, "")

    if submitted or apply_all:
        if group_fingerprint:
            st.session_state["expanded_group_fp"] = group_fingerprint
            st.session_state["expanded_group_row_id"] = idx
        else:
            st.session_state["expanded_row_id"] = idx
        review_state.apply_row_edit(
            df,
            idx,
            payee=final_payee,
            category=final_category,
            update_map=update_value,
            reviewed=True,
        )
        errors, warnings = review_validation.validate_row(df.loc[idx])
        if errors:
            st.error("Errors: " + ", ".join(errors))
        if warnings:
            st.warning("Warnings: " + ", ".join(warnings))
        if apply_all and show_apply:
            untouched_mask = None
            if updated_mask is not None:
                untouched_mask = ~updated_mask.astype(bool)
            review_model.apply_to_same_fingerprint(
                df,
                row.get("fingerprint", ""),
                payee=final_payee,
                category=final_category,
                update_map=update_value,
                reviewed=True,
                eligible_mask=untouched_mask,
            )
            st.success("Applied to untouched rows with this fingerprint.")
        st.session_state["df"] = df
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

    base_count = len(base) if isinstance(base, pd.DataFrame) and not base.empty else len(df)
    updated_confirmed_count = int(updated_mask.sum())
    saved_reviewed_count = int(saved_mask.sum())

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
                _quit_app()
            review_io.save_reviewed_transactions(df, save_path)
            map_updates_df = map_updates.save_map_update_candidates(df, base, map_updates_path)
            st.session_state["last_saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["df_original"] = df.copy()
            st.session_state["save_notice"] = (
                f"Saved to {save_path} and wrote {len(map_updates_df)} map updates to {map_updates_path}"
            )
            if save_action == "Save and quit":
                _quit_app()
            st.rerun()

        accept_defaults = review_state.accept_defaults_mask(df)
        accept_count = int(accept_defaults.sum())
        if st.button(
            f"Accept remaining defaults ({accept_count})",
            use_container_width=True,
            disabled=accept_count == 0,
        ):
            df.loc[accept_defaults, "reviewed"] = True
            st.session_state["df"] = df
            st.session_state["save_notice"] = (
                f"Accepted {accept_count} remaining default rows in memory. Save to persist."
            )
            st.rerun()

        st.caption(f"Map updates path: {map_updates_path}")

        st.markdown(
            f"**Rows to review:** {base_count}\n"
            f"**Updated:** {updated_confirmed_count}\n"
            f"**Saved:** {saved_reviewed_count}"
        )

        st.header("View")
        view_mode = st.radio("Mode", ["Grouped", "Row"], index=0, key="view_mode")

        st.header("Filters")
        statuses = sorted(df["match_status"].astype("string").fillna("").unique().tolist())
        default_status = [s for s in statuses if s in {"ambiguous", "none"}] or statuses
        match_status = st.multiselect("match_status", statuses, default=default_status)
        unresolved_only = st.checkbox("Unresolved only", value=True)
        missing_payee_only = st.checkbox("Missing payee only", value=False)
        missing_category_only = st.checkbox("Missing category only", value=False)
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

    changed_count = int(changed_mask.sum())
    reviewed_count = int(reviewed_mask.sum())
    st.markdown(
        f"**Total:** {counts['total']} | "
        f"**Missing payee:** {counts['missing_payee']} | "
        f"**Missing category:** {counts['missing_category']} | "
        f"**Unresolved:** {counts['unresolved']} | "
        f"**update_map:** {counts['update_map']} | "
        f"**Changed vs original:** {changed_count} | "
        f"**Reviewed:** {reviewed_count} | "
        f"**Unsaved:** {modified}"
    )
    if counts["missing_payee"] == 0 and counts["missing_category"] == 0:
        st.success("Ready for upload: payee and category are filled for all rows.")
    else:
        st.warning("Not ready: some rows are missing payee and/or category.")

    if not inconsistent.empty:
        st.warning(f"Inconsistent fingerprints: {len(inconsistent)}")

    filters = {
        "match_status": match_status,
        "unresolved_only": unresolved_only,
        "missing_payee_only": missing_payee_only,
        "missing_category_only": missing_category_only,
        "fingerprint_query": fingerprint_query,
        "payee_query": payee_query,
        "memo_query": memo_query,
        "source_query": source_query,
        "account_query": account_query,
    }

    filtered = review_state.apply_filters(df, filters)

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
                f"{row.get('date','')} | {_format_amount(row)} | "
                f"{str(row.get('account_name', '') or '').strip()} | "
                f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
            )
            expanded = st.session_state.get("expanded_row_id") == idx
            with st.expander(summary, expanded=expanded):
                _render_status_badges(
                    unsaved=bool(unsaved_mask.loc[idx]),
                    changed=bool(changed_mask.loc[idx]),
                    reviewed=bool(reviewed_mask.loc[idx]),
                )
                st.write(
                    {
                        "date": row.get("date", ""),
                        "amount": _format_amount(row),
                        "memo": str(row.get("memo", "") or ""),
                        "fingerprint": row.get("fingerprint", ""),
                        "match_status": row.get("match_status", ""),
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
            header = (
                f"{header_fp} ({len(group)}) | "
                f"Payee: {group_payee_summary} | Cat: {group_category_summary}"
            )

            with st.expander(
                header, expanded=(st.session_state.get("expanded_group_fp") == fp)
            ):
                group_unsaved = int(unsaved_mask.loc[group.index].sum())
                group_changed = int(changed_mask.loc[group.index].sum())
                group_saved = int(saved_mask.loc[group.index].sum())
                if group_unsaved or group_changed or group_saved:
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
                group_update_key = _editor_key(f"group_update_{fp}")
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
                _ensure_widget_state(group_update_key, False)

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
                group_update = st.checkbox(
                    "Set update_map for group", key=group_update_key
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
                    group_update_value = bool(
                        st.session_state.get(group_update_key, group_update)
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
                        update_map=group_update_value,
                        reviewed=True,
                        eligible_mask=untouched_mask,
                    )
                    st.session_state["expanded_group_fp"] = fp
                    st.session_state["expanded_group_row_id"] = None
                    st.session_state["df"] = df
                    st.success("Applied group values to untouched rows.")
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
                        f"{row.get('date','')} | {_format_amount(row)} | "
                        f"{str(row.get('account_name', '') or '').strip()} | "
                        f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
                    )
                    row_expanded = (
                        st.session_state.get("expanded_group_fp") == fp
                        and st.session_state.get("expanded_group_row_id") == idx
                    )
                    with st.expander(summary, expanded=row_expanded):
                        _render_status_badges(
                            unsaved=bool(unsaved_mask.loc[idx]),
                            changed=bool(changed_mask.loc[idx]),
                            reviewed=bool(reviewed_mask.loc[idx]),
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
