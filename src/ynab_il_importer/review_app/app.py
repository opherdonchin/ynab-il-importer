from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st

import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.validation as review_validation


DEFAULT_SOURCE = Path("outputs/proposed_transactions.csv")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.csv")
DEFAULT_CATEGORIES = Path("outputs/ynab_categories.csv")


def _series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


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
        const=str(DEFAULT_SAVE),
        help="Resume from a previously saved review CSV (optional path).",
    )
    return parser


def _parse_cli_args() -> argparse.Namespace:
    parser = _build_arg_parser()
    return parser.parse_known_args(sys.argv[1:])[0]


def _load_df(path: Path) -> None:
    df = review_io.load_proposed_transactions(path)
    st.session_state["df"] = df
    st.session_state["df_original"] = df.copy()
    st.session_state["source_path"] = str(path)


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
    if resume_path:
        path = Path(resume_path)
        if not path.exists():
            st.error(f"Resume file not found: {path}")
            st.stop()
        _load_df(path)
        st.session_state["save_path"] = str(path)


def _ensure_loaded() -> None:
    if "df" in st.session_state:
        return
    source_path = Path(st.session_state.get("source_path", str(DEFAULT_SOURCE)))
    if source_path.exists():
        if "df_base" not in st.session_state:
            _load_base(source_path)
        _load_df(source_path)
    categories_path = Path(st.session_state.get("category_path", str(DEFAULT_CATEGORIES)))
    if categories_path.exists():
        _load_categories(categories_path)


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


def _most_common_value(series: pd.Series) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    return str(clean.value_counts().idxmax())


def _summary_counts(df: pd.DataFrame) -> dict[str, int]:
    payee_blank = _series_or_default(df, "payee_selected").str.strip() == ""
    category_blank = _series_or_default(df, "category_selected").str.strip() == ""
    unresolved = payee_blank | category_blank
    update_map = df.get("update_map", pd.Series([False] * len(df), index=df.index)).astype(
        bool
    )
    return {
        "total": len(df),
        "missing_payee": int(payee_blank.sum()),
        "missing_category": int(category_blank.sum()),
        "unresolved": int(unresolved.sum()),
        "update_map": int(update_map.sum()),
    }


def _modified_count(df: pd.DataFrame, original: pd.DataFrame) -> int:
    if original is None or original.empty:
        return 0
    cols = ["payee_selected", "category_selected", "update_map"]
    for col in cols:
        if col not in df.columns:
            return 0
    current = df[cols].copy()
    base = original[cols].copy()
    base["update_map"] = base["update_map"].astype(bool)
    current["update_map"] = current["update_map"].astype(bool)
    return int((current != base).any(axis=1).sum())


def _modified_mask(df: pd.DataFrame, original: pd.DataFrame) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = ["payee_selected", "category_selected", "update_map"]
    for col in cols:
        if col not in df.columns or col not in original.columns:
            return pd.Series([False] * len(df), index=df.index)
    current = df[cols].copy()
    base = original[cols].copy()
    base["update_map"] = base["update_map"].astype(bool)
    current["update_map"] = current["update_map"].astype(bool)
    return (current != base).any(axis=1)


def _changed_mask(df: pd.DataFrame, base: pd.DataFrame) -> pd.Series:
    if base is None or base.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = ["payee_selected", "category_selected"]
    for col in cols:
        if col not in df.columns or col not in base.columns:
            return pd.Series([False] * len(df), index=df.index)
    if "transaction_id" in df.columns and "transaction_id" in base.columns:
        df_ids = df["transaction_id"].astype("string").fillna("")
        base_ids = base["transaction_id"].astype("string").fillna("")
        current = df.assign(_tid=df_ids).set_index("_tid")[cols].copy()
        baseline = base.assign(_tid=base_ids).set_index("_tid")[cols].copy()
        aligned = baseline.reindex(current.index)
        changed = (current != aligned).any(axis=1)
        return df_ids.map(changed).fillna(False).astype(bool)

    current = df[cols].copy()
    baseline = base[cols].reindex(df.index)
    return (current != baseline).any(axis=1)


def _apply_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()

    match_status = filters.get("match_status")
    if match_status:
        filtered = filtered[filtered["match_status"].isin(match_status)]

    payee_blank = _series_or_default(filtered, "payee_selected").str.strip() == ""
    category_blank = _series_or_default(filtered, "category_selected").str.strip() == ""
    if filters.get("unresolved_only"):
        filtered = filtered[payee_blank | category_blank]
    if filters.get("missing_payee_only"):
        filtered = filtered[payee_blank]
    if filters.get("missing_category_only"):
        filtered = filtered[category_blank]

    fingerprint_query = str(filters.get("fingerprint_query", "") or "").strip().casefold()
    if fingerprint_query:
        filtered = filtered[
            _series_or_default(filtered, "fingerprint")
            .str.casefold()
            .str.contains(fingerprint_query, regex=False)
        ]

    payee_query = str(filters.get("payee_query", "") or "").strip().casefold()
    if payee_query:
        payee_text = (
            _series_or_default(filtered, "payee_selected")
            + " "
            + _series_or_default(filtered, "payee_options")
        )
        filtered = filtered[
            payee_text.str.casefold().str.contains(payee_query, regex=False)
        ]

    memo_query = str(filters.get("memo_query", "") or "").strip().casefold()
    if memo_query:
        memo_text = (
            _series_or_default(filtered, "memo")
            + " "
            + _series_or_default(filtered, "description_raw")
            + " "
            + _series_or_default(filtered, "description_clean")
        )
        filtered = filtered[
            memo_text.str.casefold().str.contains(memo_query, regex=False)
        ]

    source_query = str(filters.get("source_query", "") or "").strip().casefold()
    if source_query:
        filtered = filtered[
            _series_or_default(filtered, "source")
            .str.casefold()
            .str.contains(source_query, regex=False)
        ]

    account_query = str(filters.get("account_query", "") or "").strip().casefold()
    if account_query:
        filtered = filtered[
            _series_or_default(filtered, "account_name")
            .str.casefold()
            .str.contains(account_query, regex=False)
        ]

    return filtered


def _most_common_by_fingerprint(df: pd.DataFrame, column: str) -> dict[str, str]:
    if "fingerprint" not in df.columns or column not in df.columns:
        return {}
    result: dict[str, str] = {}
    for fp, grp in df.groupby("fingerprint"):
        values = grp[column].astype("string").fillna("").str.strip()
        values = values[values != ""]
        if values.empty:
            continue
        result[fp] = values.value_counts().idxmax()
    return result


def _render_row_controls(
    df: pd.DataFrame,
    idx: Any,
    category_choices: list[str],
    category_group_map: dict[str, str],
    payee_defaults: dict[str, str],
    category_defaults: dict[str, str],
    show_apply: bool = True,
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

    with st.form(key=f"row_form_{idx}"):
        payee_override_key = f"payee_override_{idx}"
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

        category_full = [""] + category_choices
        if category_selected and category_selected not in category_full:
            category_full = [category_selected] + category_full
        if category_default and category_default not in category_full:
            category_full = [category_default] + category_full
        for option in category_options:
            if option and option not in category_full:
                category_full.append(option)

        payee_select_key = f"payee_select_{idx}"
        category_select_key = f"category_select_{idx}"

        payee_current = payee_selected or payee_default
        category_current = category_selected or category_default

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

        confirm_payee_key = f"confirm_payee_{idx}"
        confirm_category_key = f"confirm_category_{idx}"

        confirm_payee_default = st.checkbox(
            "Confirm payee default",
            value=bool(payee_selected and payee_selected == payee_default),
            key=confirm_payee_key,
        )
        confirm_category_default = st.checkbox(
            "Confirm category default",
            value=bool(category_selected and category_selected == category_default),
            key=confirm_category_key,
        )

        update_key = f"update_map_{idx}"
        update_val = st.checkbox(
            "Update map", value=bool(row.get("update_map", False)), key=update_key
        )

        submitted = st.form_submit_button("Save row", use_container_width=True)
        apply_all = st.form_submit_button(
            "Apply to all with this fingerprint", use_container_width=True
        )

    final_payee = _resolve_default_choice(
        selected=payee_select,
        override=payee_override,
        existing=payee_selected,
        default=payee_default,
        confirmed=confirm_payee_default,
    )
    final_category = _resolve_default_choice(
        selected=category_select,
        override="",
        existing=category_selected,
        default=category_default,
        confirmed=confirm_category_default,
    )

    if submitted or apply_all:
        st.session_state["expanded_row_id"] = idx
        df.at[idx, "payee_selected"] = final_payee
        df.at[idx, "category_selected"] = final_category
        df.at[idx, "update_map"] = bool(update_val)
        df.at[idx, "reviewed"] = True
        errors, warnings = review_validation.validate_row(df.loc[idx])
        if errors:
            st.error("Errors: " + ", ".join(errors))
        if warnings:
            st.warning("Warnings: " + ", ".join(warnings))
        if apply_all and show_apply:
            review_model.apply_to_same_fingerprint(
                df,
                row.get("fingerprint", ""),
                payee=final_payee,
                category=final_category,
                update_map=update_val,
                reviewed=True,
            )
            st.success("Applied to all rows with this fingerprint.")


def _format_category_label(value: str, group_map: dict[str, str]) -> str:
    if not value:
        return ""
    group = group_map.get(value, "")
    if group:
        return f"{group} — {value}"
    return value


def _resolve_default_choice(
    *,
    selected: str,
    override: str,
    existing: str,
    default: str,
    confirmed: bool,
) -> str:
    _ = confirmed
    if override:
        return override.strip()
    if selected:
        return selected.strip()
    if existing:
        return existing.strip()
    return default.strip()


def main() -> None:
    st.set_page_config(page_title="YNAB Review", layout="wide")
    st.title("Proposed Transactions Review")

    _init_from_cli()
    _ensure_loaded()

    if "df" not in st.session_state:
        st.info("Load a proposed_transactions.csv file to begin.")
        source_path = st.text_input("Source path", value=str(DEFAULT_SOURCE))
        if st.button("Load"):
            _load_df(Path(source_path))
            st.session_state["save_path"] = str(DEFAULT_SAVE)
        return

    df: pd.DataFrame = st.session_state["df"]
    original: pd.DataFrame = st.session_state.get("df_original")
    base: pd.DataFrame = st.session_state.get("df_base")
    category_list: list[str] = st.session_state.get("category_list", [])
    category_group_map: dict[str, str] = st.session_state.get("category_group_map", {})
    category_error = st.session_state.get("category_error", "")

    counts = _summary_counts(df)
    modified = _modified_count(df, original)
    unsaved_mask = _modified_mask(df, original)
    changed_mask = _changed_mask(df, base)
    reviewed_mask = df.get("reviewed", pd.Series([False] * len(df), index=df.index)).astype(
        bool
    )
    inconsistent = review_validation.inconsistent_fingerprints(df)

    base_count = len(base) if isinstance(base, pd.DataFrame) and not base.empty else len(df)
    updated_confirmed_count = int((changed_mask | reviewed_mask).sum())
    saved_reviewed_count = 0
    if isinstance(original, pd.DataFrame) and "reviewed" in original.columns:
        saved_reviewed_count = int(
            original["reviewed"].astype(bool).fillna(False).sum()
        )

    with st.sidebar:
        st.header("Files")
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
                    _load_df(Path(source_path))
                except (FileNotFoundError, ValueError) as exc:
                    st.error(f"Failed to load original: {exc}")
        with col2:
            if st.button("Reload saved"):
                try:
                    _load_df(Path(save_path))
                except (FileNotFoundError, ValueError) as exc:
                    st.error(f"Failed to load saved: {exc}")
        if st.button("Save"):
            review_io.save_reviewed_transactions(df, save_path)
            st.session_state["last_saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["df_original"] = df.copy()
            st.success(f"Saved to {save_path}")

        st.markdown(
            f"**Rows to review:** {base_count}\n"
            f"**Updated:** {updated_confirmed_count}\n"
            f"**Saved:** {saved_reviewed_count}"
        )
        if st.button("Reload categories"):
            _load_categories(Path(category_path))
            category_list = st.session_state.get("category_list", [])
            category_group_map = st.session_state.get("category_group_map", {})
            category_error = st.session_state.get("category_error", "")

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

    filtered = _apply_filters(df, filters)

    payee_defaults = _most_common_by_fingerprint(df, "payee_selected")
    category_defaults = _most_common_by_fingerprint(df, "category_selected")
    if view_mode == "Row":
        page_size = st.selectbox("Page size", [25, 50, 100], index=1, key="page_size")
        indices = filtered.index.tolist()
        total_pages = max(1, (len(indices) + page_size - 1) // page_size)
        page_key = "row_page"
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
                )

    else:
        group_page_size = st.selectbox(
            "Group page size", [10, 25, 50], index=0, key="group_page_size"
        )
        group_row_page_size = st.selectbox(
            "Rows per group", [10, 25, 50], index=0, key="group_row_page_size"
        )

        filtered_fps = _series_or_default(filtered, "fingerprint")
        filtered_fp_set = set(filtered_fps.tolist())
        all_sizes = (
            _series_or_default(df, "fingerprint").value_counts().sort_values(ascending=False)
        )
        fingerprints = [fp for fp in all_sizes.index.tolist() if fp in filtered_fp_set]
        total_pages = max(1, (len(fingerprints) + group_page_size - 1) // group_page_size)
        page_key = "group_page"
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

            with st.expander(header, expanded=False):
                group_unsaved = int(unsaved_mask.loc[group.index].sum())
                group_changed = int(changed_mask.loc[group.index].sum())
                group_reviewed = int(reviewed_mask.loc[group.index].sum())
                if group_unsaved or group_changed or group_reviewed:
                    st.markdown(
                        " ".join(
                            [
                                f"<span style='color:#b45309;font-weight:600;'>Unsaved: {group_unsaved}</span>"
                                if group_unsaved
                                else "",
                                f"<span style='color:#2563eb;font-weight:600;'>Changed: {group_changed}</span>"
                                if group_changed
                                else "",
                                f"<span style='color:#15803d;font-weight:600;'>Reviewed: {group_reviewed}</span>"
                                if group_reviewed
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

                group_payee_default = _most_common_value(group["payee_selected"])
                if not group_payee_default and payee_options:
                    group_payee_default = payee_options[0]

                group_category_default = _most_common_value(group["category_selected"])
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

                with st.form(key=f"group_form_{fp}"):
                    group_payee_select = st.selectbox(
                        "Group payee",
                        options=group_payee_choices,
                        index=group_payee_choices.index(group_payee_default)
                        if group_payee_default in group_payee_choices
                        else 0,
                        key=f"group_payee_select_{fp}",
                    )
                    group_payee_override = st.text_input(
                        "Group payee override",
                        value="",
                        key=f"group_payee_override_{fp}",
                    )
                    group_category_select = st.selectbox(
                        "Group category",
                        options=group_category_choices,
                        index=group_category_choices.index(group_category_default)
                        if group_category_default in group_category_choices
                        else 0,
                        format_func=lambda value: _format_category_label(
                            value, category_group_map
                        ),
                        key=f"group_category_{fp}",
                    )
                    group_update = st.checkbox(
                        "Set update_map for group", key=f"group_update_{fp}"
                    )
                    apply_group = st.form_submit_button(
                        "Apply to all in group", use_container_width=True
                    )

                if apply_group:
                    final_payee = group_payee_override.strip() or group_payee_select.strip()
                    payee_to_apply = final_payee if final_payee else None
                    category_to_apply = (
                        group_category_select.strip() if group_category_select else None
                    )
                    review_model.apply_to_same_fingerprint(
                        df,
                        fp,
                        payee=payee_to_apply,
                        category=category_to_apply,
                        update_map=group_update,
                        reviewed=True,
                    )
                    st.success("Applied group values.")

                st.markdown("**Rows**")
                row_indices = group.index.tolist()
                row_pages = max(1, (len(row_indices) + group_row_page_size - 1) // group_row_page_size)
                fp_key = _fp_key(fp)
                row_page_key = f"group_row_page_{fp_key}"
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
                        f"{memo_snip} | Payee: {payee_summary} | Cat: {category_summary}"
                    )
                    with st.expander(summary, expanded=False):
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
                        )


if __name__ == "__main__":
    main()
