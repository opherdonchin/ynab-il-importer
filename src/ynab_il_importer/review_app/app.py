from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from ynab_il_importer.review_app.io import (
    load_category_list,
    load_proposed_transactions,
    save_reviewed_transactions,
)
from ynab_il_importer.review_app.model import (
    apply_to_same_fingerprint,
    parse_option_string,
    resolve_selected_value,
)
from ynab_il_importer.review_app.validation import inconsistent_fingerprints, validate_row


DEFAULT_SOURCE = Path("outputs/proposed_transactions.csv")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.csv")
DEFAULT_CATEGORIES = Path("outputs/ynab_categories.csv")


def _series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _load_df(path: Path) -> None:
    df = load_proposed_transactions(path)
    st.session_state["df"] = df
    st.session_state["df_original"] = df.copy()
    st.session_state["source_path"] = str(path)


def _load_categories(path: Path) -> None:
    try:
        df = load_category_list(path)
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


def _ensure_loaded() -> None:
    if "df" in st.session_state:
        return
    if DEFAULT_SOURCE.exists():
        _load_df(DEFAULT_SOURCE)
        st.session_state["save_path"] = str(DEFAULT_SAVE)
    if DEFAULT_CATEGORIES.exists():
        _load_categories(DEFAULT_CATEGORIES)


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
    payee_options = parse_option_string(row.get("payee_options", ""))
    category_options = parse_option_string(row.get("category_options", ""))

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
        errors, warnings = validate_row(df.loc[idx])
        if errors:
            st.error("Errors: " + ", ".join(errors))
        if warnings:
            st.warning("Warnings: " + ", ".join(warnings))
        if apply_all and show_apply:
            apply_to_same_fingerprint(
                df,
                row.get("fingerprint", ""),
                payee=final_payee,
                category=final_category,
                update_map=update_val,
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
    category_list: list[str] = st.session_state.get("category_list", [])
    category_group_map: dict[str, str] = st.session_state.get("category_group_map", {})
    category_error = st.session_state.get("category_error", "")

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
            if st.button("Reload"):
                _load_df(Path(source_path))
        with col2:
            if st.button("Save"):
                save_reviewed_transactions(df, save_path)
                st.success(f"Saved to {save_path}")
        if st.button("Reload categories"):
            _load_categories(Path(category_path))
            category_list = st.session_state.get("category_list", [])
            category_group_map = st.session_state.get("category_group_map", {})
            category_error = st.session_state.get("category_error", "")

        st.header("View")
        view_mode = st.radio("Mode", ["Row", "Grouped"], index=0, key="view_mode")

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

    counts = _summary_counts(df)
    modified = _modified_count(df, original)
    inconsistent = inconsistent_fingerprints(df)

    if category_error:
        st.warning(f"Category list not loaded: {category_error}")
    elif not category_list:
        st.warning("Category list is empty; category dropdowns will be limited.")

    st.markdown(
        f"**Total:** {counts['total']} | "
        f"**Missing payee:** {counts['missing_payee']} | "
        f"**Missing category:** {counts['missing_category']} | "
        f"**Unresolved:** {counts['unresolved']} | "
        f"**update_map:** {counts['update_map']} | "
        f"**Modified:** {modified}"
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

    page_size = st.selectbox("Page size", [25, 50, 100], index=1, key="page_size")

    payee_defaults = _most_common_by_fingerprint(df, "payee_selected")
    category_defaults = _most_common_by_fingerprint(df, "category_selected")
    if view_mode == "Row":
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
            memo = str(row.get("memo", "") or "")
            memo_snip = memo[:80] + ("…" if len(memo) > 80 else "")
            summary = (
                f"{row.get('date','')} | {_format_amount(row)} | "
                f"{memo_snip} | {row.get('fingerprint','')} | {row.get('match_status','')}"
            )
            expanded = st.session_state.get("expanded_row_id") == idx
            with st.expander(summary, expanded=expanded):
                st.write(
                    {
                        "date": row.get("date", ""),
                        "amount": _format_amount(row),
                        "memo": memo,
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
        fingerprints: list[str] = []
        seen: set[str] = set()
        for fp in filtered["fingerprint"].astype("string").fillna("").tolist():
            if fp in seen:
                continue
            fingerprints.append(fp)
            seen.add(fp)
        total_pages = max(1, (len(fingerprints) + page_size - 1) // page_size)
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
        start = (page - 1) * page_size
        end = start + page_size
        for fp in fingerprints[start:end]:
            group = df[df["fingerprint"].astype("string").fillna("") == fp]
            with st.expander(f"{fp} ({len(group)})", expanded=False):
                payee_options: list[str] = []
                category_options: list[str] = []
                for _, row in group.iterrows():
                    for opt in parse_option_string(row.get("payee_options", "")):
                        if opt not in payee_options:
                            payee_options.append(opt)
                    for opt in parse_option_string(row.get("category_options", "")):
                        if opt not in category_options:
                            category_options.append(opt)

                group_payee = st.text_input("Group payee override", key=f"group_payee_{fp}")
                group_category_options = category_list or category_options
                group_category = st.selectbox(
                    "Group category",
                    options=[""] + group_category_options,
                    format_func=lambda value: _format_category_label(value, category_group_map),
                    key=f"group_category_{fp}",
                )
                group_update = st.checkbox("Set update_map for group", key=f"group_update_{fp}")
                if st.button("Apply to all in group", key=f"group_apply_{fp}"):
                    apply_to_same_fingerprint(df, fp, group_payee, group_category, group_update)
                    st.success("Applied group values.")

                st.markdown("**Rows**")
                for idx in group.index:
                    row = df.loc[idx]
                    memo = str(row.get("memo", "") or "")
                    memo_snip = memo[:60] + ("…" if len(memo) > 60 else "")
                    summary = (
                        f"{row.get('date','')} | {_format_amount(row)} | "
                        f"{memo_snip} | {row.get('match_status','')}"
                    )
                    with st.expander(summary, expanded=False):
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
