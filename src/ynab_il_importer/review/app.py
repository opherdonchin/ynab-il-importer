from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from ynab_il_importer.review.io import load_proposed_transactions, save_reviewed_transactions
from ynab_il_importer.review.model import (
    apply_to_same_fingerprint,
    parse_option_string,
    resolve_selected_value,
)
from ynab_il_importer.review.validation import inconsistent_fingerprints, validate_row


DEFAULT_SOURCE = Path("outputs/proposed_transactions.csv")
DEFAULT_SAVE = Path("outputs/proposed_transactions_reviewed.csv")


def _series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _load_df(path: Path) -> None:
    df = load_proposed_transactions(path)
    st.session_state["df"] = df
    st.session_state["df_original"] = df.copy()
    st.session_state["source_path"] = str(path)


def _ensure_loaded() -> None:
    if "df" in st.session_state:
        return
    if DEFAULT_SOURCE.exists():
        _load_df(DEFAULT_SOURCE)
        st.session_state["save_path"] = str(DEFAULT_SAVE)


def _format_amount(row: pd.Series) -> str:
    outflow = str(row.get("outflow_ils", "") or "").strip()
    inflow = str(row.get("inflow_ils", "") or "").strip()
    if outflow and outflow != "0":
        return f"-{outflow}"
    if inflow and inflow != "0":
        return f"+{inflow}"
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


def _render_row_controls(df: pd.DataFrame, idx: Any, show_apply: bool = True) -> None:
    row = df.loc[idx]
    payee_options = parse_option_string(row.get("payee_options", ""))
    category_options = parse_option_string(row.get("category_options", ""))

    payee_override_key = f"payee_override_{idx}"
    category_override_key = f"category_override_{idx}"

    payee_override = st.text_input(
        "Payee override", value=st.session_state.get(payee_override_key, ""), key=payee_override_key
    )
    category_override = st.text_input(
        "Category override",
        value=st.session_state.get(category_override_key, ""),
        key=category_override_key,
    )

    payee_selected = str(row.get("payee_selected", "") or "").strip()
    category_selected = str(row.get("category_selected", "") or "").strip()

    payee_choices = [""] + payee_options
    if payee_selected and payee_selected not in payee_choices:
        payee_choices = [payee_selected] + payee_choices
    if payee_override and payee_override not in payee_choices:
        payee_choices = [payee_override] + payee_choices

    category_choices = [""] + category_options
    if category_selected and category_selected not in category_choices:
        category_choices = [category_selected] + category_choices
    if category_override and category_override not in category_choices:
        category_choices = [category_override] + category_choices

    payee_select_key = f"payee_select_{idx}"
    category_select_key = f"category_select_{idx}"

    payee_select = st.selectbox(
        "Payee option",
        options=payee_choices,
        index=payee_choices.index(payee_selected) if payee_selected in payee_choices else 0,
        key=payee_select_key,
    )
    category_select = st.selectbox(
        "Category option",
        options=category_choices,
        index=category_choices.index(category_selected) if category_selected in category_choices else 0,
        key=category_select_key,
    )

    final_payee = resolve_selected_value(payee_select, payee_override)
    final_category = resolve_selected_value(category_select, category_override)

    df.at[idx, "payee_selected"] = final_payee
    df.at[idx, "category_selected"] = final_category

    update_key = f"update_map_{idx}"
    update_val = st.checkbox(
        "Update map", value=bool(row.get("update_map", False)), key=update_key
    )
    df.at[idx, "update_map"] = bool(update_val)

    errors, warnings = validate_row(df.loc[idx])
    if errors:
        st.error("Errors: " + ", ".join(errors))
    if warnings:
        st.warning("Warnings: " + ", ".join(warnings))

    if show_apply:
        if st.button("Apply to all with this fingerprint", key=f"apply_fp_{idx}"):
            apply_to_same_fingerprint(
                df,
                row.get("fingerprint", ""),
                payee=final_payee,
                category=final_category,
                update_map=update_val,
            )
            st.success("Applied to all rows with this fingerprint.")


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

    with st.sidebar:
        st.header("Files")
        source_path = st.text_input("Source path", value=st.session_state.get("source_path", ""))
        save_path = st.text_input(
            "Save path", value=st.session_state.get("save_path", str(DEFAULT_SAVE))
        )
        st.session_state["save_path"] = save_path
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Reload"):
                _load_df(Path(source_path))
        with col2:
            if st.button("Save"):
                save_reviewed_transactions(df, save_path)
                st.success(f"Saved to {save_path}")

        st.header("View")
        view_mode = st.radio("Mode", ["Row", "Grouped"], index=0)

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

    page_size = st.selectbox("Page size", [25, 50, 100], index=1)
    if view_mode == "Row":
        indices = filtered.index.tolist()
        total_pages = max(1, (len(indices) + page_size - 1) // page_size)
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
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
            with st.expander(summary, expanded=False):
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
                _render_row_controls(df, idx, show_apply=True)

    else:
        fingerprints: list[str] = []
        seen: set[str] = set()
        for fp in filtered["fingerprint"].astype("string").fillna("").tolist():
            if fp in seen:
                continue
            fingerprints.append(fp)
            seen.add(fp)
        total_pages = max(1, (len(fingerprints) + page_size - 1) // page_size)
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
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
                group_category = st.text_input("Group category override", key=f"group_category_{fp}")
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
                        _render_row_controls(df, idx, show_apply=False)


if __name__ == "__main__":
    main()
