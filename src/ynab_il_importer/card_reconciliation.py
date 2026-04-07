from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from ynab_il_importer.artifacts.transaction_io import read_transactions_polars
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.normalize as normalize

STAMP_CARD_SYNC_RESOLVED_VIAS = {
    "legacy_import_id",
    "memo_exact",
    "date_amount_unique",
    "date_amount_unique_memo_exact",
    "secondary_date_amount_unique",
    "secondary_date_amount_unique_memo_exact",
}

CARD_SYNC_REPORT_COLUMNS = [
    "row_index",
    "account_name",
    "date",
    "secondary_date",
    "description_raw",
    "fingerprint",
    "outflow_ils",
    "inflow_ils",
    "card_txn_id",
    "legacy_import_id",
    "resolved_transaction_id",
    "resolved_via",
    "prior_cleared",
    "candidate_count",
    "candidate_reconciled_count",
    "candidate_status",
    "candidate_summary",
    "lineage_conflict_summary",
    "action",
    "reason",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _truncate_text(value: Any, limit: int = 80) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _normalize_match_text(value: Any) -> str:
    return normalize.normalize_text(card_identity.strip_card_txn_id_markers(value))


def _signed_amount_ils(df: pd.DataFrame) -> pd.Series:
    outflow = pd.to_numeric(df.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
    inflow = pd.to_numeric(df.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
    return (inflow - outflow).round(2)


def _row_identity_hash(row: pd.Series) -> str:
    parts = [
        _normalize_text(row.get("account_name", "")),
        _normalize_text(row.get("source_account", "")),
        _normalize_text(row.get("date", "")),
        _normalize_text(row.get("secondary_date", "")),
        _normalize_text(row.get("outflow_ils", "")),
        _normalize_text(row.get("inflow_ils", "")),
        _normalize_text(row.get("fingerprint", "")),
        _normalize_text(row.get("description_raw", row.get("memo", ""))),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _legacy_import_ids(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")

    work = df.reset_index().copy()
    work["account_key"] = _normalize_text_series(work["account_name"])
    work["date_key"] = (
        pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    )
    work["amount_milliunits"] = (_signed_amount_ils(work) * 1000).round().astype(int)
    work["stable_key"] = work.apply(_row_identity_hash, axis=1)
    ordered = work.sort_values(
        ["account_key", "date_key", "amount_milliunits", "stable_key", "index"]
    ).copy()
    ordered["import_occurrence"] = (
        ordered.groupby(["account_key", "date_key", "amount_milliunits"], dropna=False)
        .cumcount()
        .add(1)
    )
    ordered["legacy_import_id"] = ordered.apply(
        lambda row: f"YNAB:{int(row['amount_milliunits'])}:{row['date_key']}:{int(row['import_occurrence'])}",
        axis=1,
    )
    return (
        ordered.set_index("index")["legacy_import_id"]
        .reindex(df.index)
        .astype("string")
    )


def load_card_source(path: str | Path) -> pl.DataFrame:
    source_path = Path(path)
    if source_path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Card reconciliation requires canonical parquet input, got: {source_path}"
        )
    return read_transactions_polars(source_path)


def _target_source_rows(df: pl.DataFrame, account_name: str) -> pd.DataFrame:
    work = (
        df.select(
            "account_name",
            "source_account",
            "transaction_id",
            "date",
            "secondary_date",
            "outflow_ils",
            "inflow_ils",
            "signed_amount_ils",
            "description_raw",
            "fingerprint",
        )
        .with_columns(
            pl.col("account_name").fill_null("").str.strip_chars(),
            pl.col("source_account").fill_null("").str.strip_chars(),
            pl.col("transaction_id").fill_null("").str.strip_chars().alias("card_txn_id"),
            pl.col("description_raw").fill_null("").str.strip_chars(),
            pl.col("fingerprint").fill_null("").str.strip_chars(),
        )
    )
    mask = (pl.col("account_name") == account_name) | (
        pl.col("source_account") == account_name
    )
    filtered = work.filter(mask)
    if filtered.is_empty():
        available = sorted(
            (set(work["account_name"].to_list()) | set(work["source_account"].to_list()))
            - {""}
        )
        raise ValueError(
            f"Account {account_name!r} not found in card source. Available accounts: {available}"
        )
    filtered = filtered.with_row_index("row_index")
    prepared = filtered.to_pandas()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date
    prepared["secondary_date"] = pd.to_datetime(
        prepared["secondary_date"], errors="coerce"
    ).dt.date
    prepared["outflow_ils"] = (
        pd.to_numeric(prepared["outflow_ils"], errors="coerce").fillna(0.0).round(2)
    )
    prepared["inflow_ils"] = (
        pd.to_numeric(prepared["inflow_ils"], errors="coerce").fillna(0.0).round(2)
    )
    prepared["card_txn_id"] = _normalize_text_series(prepared["card_txn_id"])
    prepared["legacy_import_id"] = _legacy_import_ids(prepared)
    prepared["description_match"] = prepared["description_raw"].map(
        _normalize_match_text
    )
    prepared["signed_ils"] = (
        pd.to_numeric(prepared["signed_amount_ils"], errors="coerce").fillna(0.0).round(2)
    )
    prepared = prepared[prepared["signed_ils"] != 0].copy()
    prepared["row_index"] = range(len(prepared))
    return prepared


def _coerce_optional_date(value: Any, *, field_name: str) -> date | None:
    text = _normalize_text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(
            f"Invalid {field_name} value {value!r}; expected YYYY-MM-DD."
        )
    return parsed.date()


def _filter_rows_by_date_range(
    rows: pd.DataFrame,
    *,
    date_from: Any = None,
    date_to: Any = None,
    range_name: str,
    allow_empty: bool = True,
) -> tuple[pd.DataFrame, int]:
    parsed_from = _coerce_optional_date(date_from, field_name=f"{range_name}_date_from")
    parsed_to = _coerce_optional_date(date_to, field_name=f"{range_name}_date_to")
    if parsed_from is None and parsed_to is None:
        return rows.copy(), 0
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        raise ValueError(
            f"{range_name}_date_from {parsed_from} is after {range_name}_date_to {parsed_to}."
        )
    if rows.empty:
        if allow_empty:
            return rows.copy(), 0
        raise ValueError(
            f"No {range_name} rows are available for filtering in the requested account."
        )

    row_dates = pd.to_datetime(rows["date"], errors="coerce").dt.date
    mask = pd.Series([True] * len(rows), index=rows.index)
    if parsed_from is not None:
        mask = mask & (row_dates >= parsed_from)
    if parsed_to is not None:
        mask = mask & (row_dates <= parsed_to)
    filtered = rows.loc[mask].copy()
    dropped_count = int(len(rows) - len(filtered))

    if filtered.empty and not allow_empty:
        bounds = []
        if parsed_from is not None:
            bounds.append(f"from {parsed_from}")
        if parsed_to is not None:
            bounds.append(f"to {parsed_to}")
        bounds_txt = " ".join(bounds).strip()
        raise ValueError(
            f"No {range_name} rows remain after applying date filter {bounds_txt}."
        )

    return filtered, dropped_count


def _account_lookup(
    accounts: list[dict[str, Any]], account_name: str
) -> dict[str, str]:
    normalized_target = _normalize_text(account_name)
    matches = [
        acc
        for acc in accounts
        if not bool(acc.get("deleted", False))
        and _normalize_text(acc.get("name", "")) == normalized_target
    ]
    if not matches:
        raise ValueError(f"YNAB account not found: {account_name}")
    if len(matches) > 1:
        raise ValueError(f"Duplicate YNAB accounts named {account_name!r}")
    return {
        "account_id": _normalize_text(matches[0].get("id", "")),
        "account_name": normalized_target,
    }


def _account_name_map(accounts: list[dict[str, Any]]) -> dict[str, str]:
    return {
        _normalize_text(acc.get("id", "")): _normalize_text(acc.get("name", ""))
        for acc in accounts
        if not bool(acc.get("deleted", False))
    }


def _all_ynab_transactions_frame(
    transactions: list[dict[str, Any]],
    *,
    account_names: dict[str, str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        if bool(txn.get("deleted", False)):
            continue
        account_id = _normalize_text(txn.get("account_id", ""))
        amount_milliunits = int(txn.get("amount", 0) or 0)
        parsed_date = pd.to_datetime(txn.get("date", ""), errors="coerce")
        rows.append(
            {
                "id": _normalize_text(txn.get("id", "")),
                "account_id": account_id,
                "account_name": _normalize_text(
                    (account_names or {}).get(account_id, "")
                ),
                "date": parsed_date.date() if pd.notna(parsed_date) else pd.NaT,
                "amount_milliunits": amount_milliunits,
                "signed_ils": round(amount_milliunits / 1000.0, 2),
                "memo": _normalize_text(txn.get("memo", "")),
                "memo_match": _normalize_match_text(txn.get("memo", "")),
                "import_id": _normalize_text(txn.get("import_id", "")),
                "card_txn_id_marker": card_identity.extract_card_txn_id_from_memo(
                    txn.get("memo", "")
                ),
                "cleared": _normalize_text(txn.get("cleared", "")),
                "approved": bool(txn.get("approved", False)),
                "payee_name": _normalize_text(txn.get("payee_name", "")),
                "transfer_account_id": _normalize_text(
                    txn.get("transfer_account_id", "")
                ),
                "transfer_transaction_id": _normalize_text(
                    txn.get("transfer_transaction_id", "")
                ),
            }
        )
    return pd.DataFrame(rows)


def _ynab_transactions_frame(
    transactions: list[dict[str, Any]],
    *,
    account_id: str,
    account_names: dict[str, str] | None = None,
) -> pd.DataFrame:
    all_rows = _all_ynab_transactions_frame(transactions, account_names=account_names)
    if all_rows.empty:
        return all_rows
    return all_rows[all_rows["account_id"] == account_id].copy()


def _card_lineage_maps(
    ynab_df: pd.DataFrame,
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    import_map: dict[str, list[int]] = {}
    memo_map: dict[str, list[int]] = {}
    for idx, row in ynab_df.iterrows():
        import_id = _normalize_text(row.get("import_id", ""))
        if card_identity.is_card_txn_id(import_id):
            import_map.setdefault(import_id, []).append(idx)
        memo_card_txn_id = _normalize_text(row.get("card_txn_id_marker", ""))
        if memo_card_txn_id:
            memo_map.setdefault(memo_card_txn_id, []).append(idx)
    return import_map, memo_map


def _card_date_amount_candidates_for_field(
    source_row: pd.Series, ynab_df: pd.DataFrame, *, date_field: str
) -> pd.DataFrame:
    target_date = pd.to_datetime(source_row.get(date_field, ""), errors="coerce")
    if pd.isna(target_date):
        return ynab_df.iloc[0:0].copy()
    candidates = ynab_df.copy()
    candidates = candidates[candidates["date"] == target_date.date()]
    candidates = candidates[
        (candidates["signed_ils"] - source_row["signed_ils"]).abs() < 0.001
    ]
    return candidates


def _card_date_amount_candidates(
    source_row: pd.Series, ynab_df: pd.DataFrame
) -> pd.DataFrame:
    return _card_date_amount_candidates_for_field(source_row, ynab_df, date_field="date")


def _card_secondary_date_amount_candidates(
    source_row: pd.Series, ynab_df: pd.DataFrame
) -> pd.DataFrame:
    primary = pd.to_datetime(source_row.get("date", ""), errors="coerce")
    secondary = pd.to_datetime(source_row.get("secondary_date", ""), errors="coerce")
    if pd.isna(secondary):
        return ynab_df.iloc[0:0].copy()
    if pd.notna(primary) and primary.date() == secondary.date():
        return ynab_df.iloc[0:0].copy()
    return _card_date_amount_candidates_for_field(
        source_row, ynab_df, date_field="secondary_date"
    )


def _card_unlinked_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    unlinked = candidates[
        candidates["card_txn_id_marker"].astype("string").fillna("").str.strip() == ""
    ].copy()
    if unlinked.empty:
        return unlinked
    has_card_import_id = unlinked["import_id"].map(card_identity.is_card_txn_id)
    return unlinked.loc[~has_card_import_id].copy()


def _summarize_card_candidate(row: pd.Series) -> str:
    payee = _truncate_text(row.get("payee_name", "") or "<blank>", limit=40)
    memo = _truncate_text(row.get("memo", "") or "<blank>", limit=60)
    import_id = _truncate_text(row.get("import_id", "") or "<blank>", limit=40)
    return (
        f"{_normalize_text(row.get('id', ''))} | "
        f"payee={payee} | "
        f"cleared={_normalize_text(row.get('cleared', '')) or '<blank>'} | "
        f"import_id={import_id} | "
        f"memo={memo}"
    )


def _summarize_card_candidate_rows(candidates: pd.DataFrame) -> str:
    if candidates.empty:
        return ""
    ordered = candidates.sort_values(["date", "signed_ils", "id"], na_position="last")
    return " || ".join(_summarize_card_candidate(row) for _, row in ordered.iterrows())


def _card_lineage_conflict_summary(
    source_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> str:
    card_txn_id = source_row["card_txn_id"]
    parts: list[str] = []
    import_hits = import_map.get(card_txn_id, [])
    if len(import_hits) == 1:
        parts.append(
            f"import_id -> {_summarize_card_candidate(ynab_df.loc[import_hits[0]])}"
        )
    memo_hits = memo_map.get(card_txn_id, [])
    if len(memo_hits) == 1:
        parts.append(
            f"memo_marker -> {_summarize_card_candidate(ynab_df.loc[memo_hits[0]])}"
        )
    return " || ".join(parts)


def _resolve_exact_card_lineage(
    source_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[pd.Series | None, str, str]:
    card_txn_id = _normalize_text(source_row.get("card_txn_id", ""))
    source_date = source_row["date"]
    source_amount = round(float(source_row["signed_ils"]), 2)
    mismatch_reasons: list[str] = []

    if card_txn_id:
        import_hits = import_map.get(card_txn_id, [])
        if len(import_hits) > 1:
            return None, "", f"duplicate YNAB import_id matches for {card_txn_id}"
        if len(import_hits) == 1:
            candidate = ynab_df.loc[import_hits[0]]
            if (
                candidate["date"] == source_date
                and abs(float(candidate["signed_ils"]) - source_amount) < 0.001
            ):
                return candidate, "import_id", ""
            mismatch_reasons.append(
                "card_txn_id import_id is attached to a YNAB transaction with different date/amount"
            )

        memo_hits = memo_map.get(card_txn_id, [])
        if len(memo_hits) > 1:
            return None, "", f"duplicate YNAB memo markers for {card_txn_id}"
        if len(memo_hits) == 1:
            candidate = ynab_df.loc[memo_hits[0]]
            if (
                candidate["date"] == source_date
                and abs(float(candidate["signed_ils"]) - source_amount) < 0.001
            ):
                return candidate, "memo_marker", ""
            mismatch_reasons.append(
                "card_txn_id memo marker is attached to a YNAB transaction with different date/amount"
            )

    legacy_import_id = _normalize_text(source_row.get("legacy_import_id", ""))
    if legacy_import_id:
        legacy_hits = ynab_df.index[ynab_df["import_id"] == legacy_import_id].tolist()
        if len(legacy_hits) > 1:
            return (
                None,
                "",
                f"duplicate YNAB legacy import_id matches for {legacy_import_id}",
            )
        if len(legacy_hits) == 1:
            candidate = ynab_df.loc[legacy_hits[0]]
            if (
                candidate["date"] == source_date
                and abs(float(candidate["signed_ils"]) - source_amount) < 0.001
            ):
                return candidate, "legacy_import_id", ""
            mismatch_reasons.append(
                "legacy import_id is attached to a YNAB transaction with different date/amount"
            )

    if mismatch_reasons:
        return None, "", "; ".join(mismatch_reasons)
    return None, "", "no exact lineage match"


def _card_candidate_diagnostics(
    source_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[int, int, str, str, str]:
    lineage_conflict = _card_lineage_conflict_summary(
        source_row, ynab_df, import_map, memo_map
    )

    def _diagnose(candidates: pd.DataFrame, *, prefix: str = "") -> tuple[int, int, str, str]:
        candidate_count = int(len(candidates))
        candidate_reconciled_count = (
            int((candidates["cleared"] == "reconciled").sum()) if candidate_count else 0
        )
        candidate_summary = _summarize_card_candidate_rows(candidates)
        if candidate_count == 0:
            return 0, 0, "", candidate_summary

        unlinked = _card_unlinked_candidates(candidates)
        if unlinked.empty:
            return (
                candidate_count,
                candidate_reconciled_count,
                f"only_linked_{prefix}date_amount_candidates",
                candidate_summary,
            )

        memo_exact = unlinked[unlinked["memo_match"] == source_row["description_match"]]
        if len(memo_exact) == 1:
            return (
                candidate_count,
                candidate_reconciled_count,
                f"unique_{prefix}memo_exact_candidate",
                candidate_summary,
            )
        if len(memo_exact) > 1:
            return (
                candidate_count,
                candidate_reconciled_count,
                f"ambiguous_{prefix}memo_exact_candidates",
                candidate_summary,
            )

        if len(unlinked) == 1:
            return (
                candidate_count,
                candidate_reconciled_count,
                f"unique_{prefix}date_amount_candidate",
                candidate_summary,
            )
        return (
            candidate_count,
            candidate_reconciled_count,
            f"ambiguous_{prefix}date_amount_candidates",
            candidate_summary,
        )

    primary = _diagnose(_card_date_amount_candidates(source_row, ynab_df))
    if primary[0] > 0:
        return primary + (lineage_conflict,)

    secondary = _diagnose(
        _card_secondary_date_amount_candidates(source_row, ynab_df),
        prefix="secondary_",
    )
    if secondary[0] > 0:
        return secondary + (lineage_conflict,)
    return 0, 0, "no_date_amount_match", "", lineage_conflict


def _card_sync_fallback_candidate(
    source_row: pd.Series,
    ynab_df: pd.DataFrame,
) -> tuple[pd.Series | None, str, str]:
    def _resolve_candidates(
        candidates: pd.DataFrame, *, resolved_via: str, empty_reason: str, many_reason: str
    ) -> tuple[pd.Series | None, str, str]:
        memo_exact = candidates[candidates["memo_match"] == source_row["description_match"]]
        if len(memo_exact) == 1:
            return memo_exact.iloc[0], f"{resolved_via}_memo_exact", ""
        if len(memo_exact) > 1:
            return None, "", many_reason
        if len(candidates) == 1:
            return candidates.iloc[0], resolved_via, ""
        if candidates.empty:
            return None, "", empty_reason
        return None, "", many_reason

    primary = _resolve_candidates(
        _card_unlinked_candidates(_card_date_amount_candidates(source_row, ynab_df)),
        resolved_via="date_amount_unique",
        empty_reason="no unique same-date/same-amount candidate",
        many_reason="multiple same-date/same-amount candidates",
    )
    if primary[0] is not None or primary[2] != "no unique same-date/same-amount candidate":
        return primary

    return _resolve_candidates(
        _card_unlinked_candidates(_card_secondary_date_amount_candidates(source_row, ynab_df)),
        resolved_via="secondary_date_amount_unique",
        empty_reason="no unique billing-date/same-amount candidate",
        many_reason="multiple billing-date/same-amount candidates",
    )


def _card_sync_unmatched_reason(candidate_status: str, base_reason: str) -> str:
    if candidate_status == "only_linked_date_amount_candidates":
        return "same date/amount candidate is already linked to a different card_txn_id"
    if candidate_status == "only_linked_secondary_date_amount_candidates":
        return "same billing-date/amount candidate is already linked to a different card_txn_id"
    if candidate_status == "ambiguous_memo_exact_candidates":
        return "multiple same-date/same-amount memo-confirmed candidates"
    if candidate_status == "ambiguous_secondary_memo_exact_candidates":
        return "multiple billing-date/same-amount memo-confirmed candidates"
    if candidate_status == "ambiguous_date_amount_candidates":
        return "multiple YNAB transactions share this date/amount"
    if candidate_status == "ambiguous_secondary_date_amount_candidates":
        return "multiple YNAB transactions share this billing-date/amount"
    return base_reason


@dataclass
class _ResolvedMatch:
    ynab_row: pd.Series | None
    resolved_via: str
    candidate_status: str
    reason: str


@dataclass
class _PaymentTransferMatch:
    ok: bool
    reason: str
    card_transaction_id: str = ""
    card_transfer_account_id: str = ""
    card_transfer_account_name: str = ""
    card_date: str = ""
    card_amount_ils: float = 0.0
    bank_transaction_id: str = ""
    bank_account_id: str = ""
    bank_account_name: str = ""
    bank_date: str = ""
    bank_amount_ils: float = 0.0


def _expected_statement_date(previous_rows: pd.DataFrame) -> pd.Timestamp | pd.NaT:
    if previous_rows.empty or "secondary_date" not in previous_rows.columns:
        return pd.NaT
    secondary = pd.to_datetime(
        previous_rows["secondary_date"], errors="coerce"
    ).dropna()
    if secondary.empty:
        return pd.NaT
    return secondary.max()


def _identify_billing_date_groups(
    previous_rows: pd.DataFrame,
) -> tuple[object, pd.DataFrame, pd.DataFrame]:
    """Split previous rows into main billing date and separately-settled groups.

    The main billing date is the latest secondary_date, which corresponds to the
    monthly bundled payment transfer from the bank. Rows with earlier secondary_dates
    were settled individually by the card company and do not roll into the monthly
    payment transfer.

    Returns (main_billing_date, main_rows, sep_rows).
    sep_rows is empty when all rows share the same secondary_date.
    """
    if "secondary_date" not in previous_rows.columns or previous_rows.empty:
        return None, previous_rows.copy(), pd.DataFrame(columns=previous_rows.columns)
    sec_dates = pd.to_datetime(previous_rows["secondary_date"], errors="coerce")
    main_date = sec_dates.max()
    if pd.isna(main_date):
        return None, previous_rows.copy(), pd.DataFrame(columns=previous_rows.columns)
    mask = sec_dates == main_date
    return main_date, previous_rows[mask].copy(), previous_rows[~mask].copy()


def _validate_payment_transfer(
    *,
    previous_rows: pd.DataFrame,
    all_ynab_df: pd.DataFrame,
    card_account_id: str,
    account_names: dict[str, str],
) -> _PaymentTransferMatch:
    expected_total = round(abs(float(previous_rows["signed_ils"].sum())), 2)
    expected_milliunits = int(round(expected_total * 1000))
    statement_date = _expected_statement_date(previous_rows)

    candidates = all_ynab_df[
        (all_ynab_df["account_id"] == card_account_id)
        & (all_ynab_df["transfer_account_id"] != "")
        & (all_ynab_df["amount_milliunits"] == expected_milliunits)
    ].copy()
    if candidates.empty:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"No card payment transfer found for previous total {expected_total:.2f} ILS.",
        )

    if pd.notna(statement_date):
        dates = pd.to_datetime(candidates["date"], errors="coerce")
        window_mask = (dates - statement_date).abs().dt.days <= 7
        window_candidates = candidates[window_mask].copy()
        if not window_candidates.empty:
            candidates = window_candidates

    if len(candidates) != 1:
        return _PaymentTransferMatch(
            ok=False,
            reason=(
                f"Expected exactly one card payment transfer for previous total {expected_total:.2f} ILS; "
                f"found {len(candidates)}."
            ),
        )

    card_txn = candidates.iloc[0]
    bank_txn_id = _normalize_text(card_txn.get("transfer_transaction_id", ""))
    if not bank_txn_id:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"Card payment transfer {card_txn['id']} has no linked bank transfer transaction.",
        )

    bank_candidates = all_ynab_df[all_ynab_df["id"] == bank_txn_id].copy()
    if len(bank_candidates) != 1:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"Linked bank transfer {bank_txn_id} was not found in YNAB transactions.",
        )

    bank_txn = bank_candidates.iloc[0]
    if int(bank_txn["amount_milliunits"]) != -expected_milliunits:
        return _PaymentTransferMatch(
            ok=False,
            reason=(
                f"Linked bank transfer amount {bank_txn['signed_ils']:.2f} ILS does not match "
                f"expected {-expected_total:.2f} ILS."
            ),
        )
    if _normalize_text(bank_txn.get("transfer_account_id", "")) != card_account_id:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"Linked bank transfer {bank_txn_id} does not point back to the card account.",
        )

    card_transfer_account_id = _normalize_text(card_txn.get("transfer_account_id", ""))
    bank_account_id = _normalize_text(bank_txn.get("account_id", ""))
    return _PaymentTransferMatch(
        ok=True,
        reason="",
        card_transaction_id=_normalize_text(card_txn.get("id", "")),
        card_transfer_account_id=card_transfer_account_id,
        card_transfer_account_name=_normalize_text(
            account_names.get(card_transfer_account_id, "")
        ),
        card_date=_normalize_text(card_txn.get("date", "")),
        card_amount_ils=round(float(card_txn["signed_ils"]), 2),
        bank_transaction_id=bank_txn_id,
        bank_account_id=bank_account_id,
        bank_account_name=_normalize_text(account_names.get(bank_account_id, "")),
        bank_date=_normalize_text(bank_txn.get("date", "")),
        bank_amount_ils=round(float(bank_txn["signed_ils"]), 2),
    )


def _resolve_card_match(source_row: pd.Series, ynab_df: pd.DataFrame) -> _ResolvedMatch:
    if ynab_df.empty:
        return _ResolvedMatch(
            None, "", "no_candidates", "no YNAB transaction candidates"
        )

    card_txn_id = _normalize_text(source_row.get("card_txn_id", ""))
    if card_txn_id:
        import_hits = ynab_df.index[ynab_df["import_id"] == card_txn_id].tolist()
        if len(import_hits) == 1:
            return _ResolvedMatch(
                ynab_df.loc[import_hits[0]], "import_id", "exact_lineage", ""
            )
        if len(import_hits) > 1:
            return _ResolvedMatch(
                None,
                "",
                "duplicate_import_id",
                f"duplicate YNAB import_id matches for {card_txn_id}",
            )

        memo_hits = ynab_df.index[ynab_df["card_txn_id_marker"] == card_txn_id].tolist()
        if len(memo_hits) == 1:
            return _ResolvedMatch(
                ynab_df.loc[memo_hits[0]], "memo_marker", "exact_lineage", ""
            )
        if len(memo_hits) > 1:
            return _ResolvedMatch(
                None,
                "",
                "duplicate_memo_marker",
                f"duplicate YNAB memo markers for {card_txn_id}",
            )

    legacy_import_id = _normalize_text(source_row.get("legacy_import_id", ""))
    if legacy_import_id:
        legacy_hits = ynab_df.index[ynab_df["import_id"] == legacy_import_id].tolist()
        if len(legacy_hits) == 1:
            candidate = ynab_df.loc[legacy_hits[0]]
            if (
                candidate["date"] == source_row["date"]
                and abs(candidate["signed_ils"] - source_row["signed_ils"]) < 0.001
            ):
                return _ResolvedMatch(
                    candidate, "legacy_import_id", "legacy_import_id", ""
                )

    same_key = _card_date_amount_candidates(source_row, ynab_df)
    if same_key.empty:
        secondary_key = _card_secondary_date_amount_candidates(source_row, ynab_df)
        if secondary_key.empty:
            return _ResolvedMatch(
                None,
                "",
                "no_date_amount_match",
                "no date/amount or billing-date/amount match",
            )

        memo_exact = secondary_key[
            secondary_key["memo_match"] == source_row["description_match"]
        ]
        if len(memo_exact) == 1:
            return _ResolvedMatch(
                memo_exact.iloc[0],
                "secondary_date_memo_exact",
                "unique_secondary_memo_exact_candidate",
                "",
            )
        if len(memo_exact) > 1:
            return _ResolvedMatch(
                None,
                "",
                "ambiguous_secondary_memo_exact_candidates",
                "multiple billing-date/same-amount memo-confirmed candidates",
            )
        if len(secondary_key) == 1:
            return _ResolvedMatch(
                None,
                "",
                "weak_unique_secondary_date_amount",
                "unique billing-date/amount candidate exists but memo does not confirm it",
            )
        return _ResolvedMatch(
            None,
            "",
            "ambiguous_secondary_date_amount_candidates",
            "multiple billing-date/same-amount candidates",
        )

    memo_exact = same_key[same_key["memo_match"] == source_row["description_match"]]
    if len(memo_exact) == 1:
        return _ResolvedMatch(
            memo_exact.iloc[0], "memo_exact", "unique_memo_exact_candidate", ""
        )
    if len(memo_exact) > 1:
        return _ResolvedMatch(
            None,
            "",
            "ambiguous_memo_exact_candidates",
            "multiple same-date/same-amount memo-confirmed candidates",
        )
    if len(same_key) == 1:
        return _ResolvedMatch(
            None,
            "",
            "weak_unique_date_amount",
            "unique date/amount candidate exists but memo does not confirm it",
        )
    return _ResolvedMatch(
        None,
        "",
        "ambiguous_date_amount_candidates",
        "multiple same-date/same-amount candidates",
    )


def _row_report(source_row: pd.Series, snapshot_role: str) -> dict[str, object]:
    return {
        "snapshot_role": snapshot_role,
        "row_index": int(source_row.get("row_index", -1)),
        "account_name": _normalize_text(source_row.get("account_name", "")),
        "date": _normalize_text(source_row.get("date", "")),
        "secondary_date": _normalize_text(source_row.get("secondary_date", "")),
        "description_raw": _normalize_text(
            source_row.get("description_raw", source_row.get("memo", ""))
        ),
        "fingerprint": _normalize_text(source_row.get("fingerprint", "")),
        "outflow_ils": float(
            pd.to_numeric(source_row.get("outflow_ils", 0.0), errors="coerce") or 0.0
        ),
        "inflow_ils": float(
            pd.to_numeric(source_row.get("inflow_ils", 0.0), errors="coerce") or 0.0
        ),
        "card_txn_id": _normalize_text(source_row.get("card_txn_id", "")),
        "legacy_import_id": _normalize_text(source_row.get("legacy_import_id", "")),
        "resolved_via": "",
        "candidate_status": "",
        "reason": "",
        "ynab_transaction_id": "",
        "ynab_import_id": "",
        "ynab_date": "",
        "ynab_signed_ils": 0.0,
        "prior_cleared": "",
        "action": "",
    }


def _summarize_open_older_rows(
    ynab_df: pd.DataFrame, first_source_date: Any
) -> pd.DataFrame:
    cutoff = pd.to_datetime(first_source_date, errors="coerce")
    if pd.isna(cutoff):
        return pd.DataFrame()
    return ynab_df[
        (ynab_df["cleared"] == "cleared")
        & (ynab_df["date"].notna())
        & (pd.to_datetime(ynab_df["date"], errors="coerce") < cutoff)
    ].copy()


def _apply_updates_for_rows(
    rows: pd.DataFrame, *, target_cleared: str
) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        if _normalize_text(row.get("prior_cleared", "")) == target_cleared:
            continue
        updates.append(
            {
                "id": _normalize_text(row.get("ynab_transaction_id", "")),
                "cleared": target_cleared,
            }
        )
    return updates


def _evaluate_snapshot_rows(
    source_df: pd.DataFrame, ynab_df: pd.DataFrame, *, snapshot_role: str
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, source_row in source_df.iterrows():
        report_row = _row_report(source_row, snapshot_role)
        resolved = _resolve_card_match(source_row, ynab_df)
        report_row["resolved_via"] = resolved.resolved_via
        report_row["candidate_status"] = resolved.candidate_status
        report_row["reason"] = resolved.reason
        if resolved.ynab_row is not None:
            report_row["ynab_transaction_id"] = _normalize_text(
                resolved.ynab_row.get("id", "")
            )
            report_row["ynab_import_id"] = _normalize_text(
                resolved.ynab_row.get("import_id", "")
            )
            report_row["ynab_date"] = _normalize_text(resolved.ynab_row.get("date", ""))
            report_row["ynab_signed_ils"] = round(
                float(resolved.ynab_row.get("signed_ils", 0.0)), 2
            )
            report_row["prior_cleared"] = _normalize_text(
                resolved.ynab_row.get("cleared", "")
            )
            report_row["action"] = "matched"
        else:
            report_row["action"] = "blocked"
        rows.append(report_row)
    return pd.DataFrame(rows)


def _card_sync_report_row(
    source_row: pd.Series,
    *,
    resolved_transaction_id: str = "",
    resolved_via: str = "",
    prior_cleared: str = "",
    candidate_count: int = 0,
    candidate_reconciled_count: int = 0,
    candidate_status: str = "",
    candidate_summary: str = "",
    lineage_conflict_summary: str = "",
    action: str = "",
    reason: str = "",
) -> dict[str, object]:
    return {
        "row_index": int(source_row.get("row_index", -1)),
        "account_name": _normalize_text(source_row.get("account_name", "")),
        "date": _normalize_text(source_row.get("date", "")),
        "secondary_date": _normalize_text(source_row.get("secondary_date", "")),
        "description_raw": _normalize_text(
            source_row.get("description_raw", source_row.get("memo", ""))
        ),
        "fingerprint": _normalize_text(source_row.get("fingerprint", "")),
        "outflow_ils": float(
            pd.to_numeric(source_row.get("outflow_ils", 0.0), errors="coerce") or 0.0
        ),
        "inflow_ils": float(
            pd.to_numeric(source_row.get("inflow_ils", 0.0), errors="coerce") or 0.0
        ),
        "card_txn_id": _normalize_text(source_row.get("card_txn_id", "")),
        "legacy_import_id": _normalize_text(source_row.get("legacy_import_id", "")),
        "resolved_transaction_id": resolved_transaction_id,
        "resolved_via": resolved_via,
        "prior_cleared": prior_cleared,
        "candidate_count": candidate_count,
        "candidate_reconciled_count": candidate_reconciled_count,
        "candidate_status": candidate_status,
        "candidate_summary": candidate_summary,
        "lineage_conflict_summary": lineage_conflict_summary,
        "action": action,
        "reason": reason,
    }


def plan_card_match_sync(
    *,
    account_name: str,
    source_df: pl.DataFrame,
    accounts: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    source_date_from: Any = None,
    source_date_to: Any = None,
) -> dict[str, object]:
    account = _account_lookup(accounts, account_name)
    source_rows = _target_source_rows(source_df, account_name)
    source_rows, source_filtered_out_count = _filter_rows_by_date_range(
        source_rows,
        date_from=source_date_from,
        date_to=source_date_to,
        range_name="source",
        allow_empty=False,
    )
    ynab_df = _ynab_transactions_frame(transactions, account_id=account["account_id"])
    if ynab_df.empty:
        raise ValueError(
            f"No live YNAB transactions found for account {account_name!r}."
        )

    import_map, memo_map = _card_lineage_maps(ynab_df)
    report_rows: list[dict[str, object]] = []
    updates: list[dict[str, str]] = []

    for _, source_row in source_rows.iterrows():
        matched, resolved_via, reason = _resolve_exact_card_lineage(
            source_row, ynab_df, import_map, memo_map
        )
        (
            candidate_count,
            candidate_reconciled_count,
            candidate_status,
            candidate_summary,
            lineage_conflict_summary,
        ) = _card_candidate_diagnostics(source_row, ynab_df, import_map, memo_map)

        if matched is None and reason == "no exact lineage match":
            matched, fallback_via, fallback_reason = _card_sync_fallback_candidate(
                source_row, ynab_df
            )
            if matched is not None:
                resolved_via = fallback_via
                reason = ""
            else:
                reason = _card_sync_unmatched_reason(candidate_status, fallback_reason)

        if matched is None:
            report_rows.append(
                _card_sync_report_row(
                    source_row,
                    candidate_count=candidate_count,
                    candidate_reconciled_count=candidate_reconciled_count,
                    candidate_status=candidate_status,
                    candidate_summary=candidate_summary,
                    lineage_conflict_summary=lineage_conflict_summary,
                    action="unmatched",
                    reason=reason,
                )
            )
            continue

        transaction_id = _normalize_text(matched.get("id", ""))
        prior_cleared = _normalize_text(matched.get("cleared", ""))
        patch: dict[str, str] = {"id": transaction_id}
        actions: list[str] = []

        if resolved_via in STAMP_CARD_SYNC_RESOLVED_VIAS:
            try:
                new_memo = card_identity.append_card_txn_id_marker(
                    matched.get("memo", ""), source_row["card_txn_id"]
                )
            except ValueError as exc:
                report_rows.append(
                    _card_sync_report_row(
                        source_row,
                        resolved_transaction_id=transaction_id,
                        resolved_via=resolved_via,
                        prior_cleared=prior_cleared,
                        candidate_count=candidate_count,
                        candidate_reconciled_count=candidate_reconciled_count,
                        candidate_status=candidate_status,
                        candidate_summary=candidate_summary,
                        lineage_conflict_summary=lineage_conflict_summary,
                        action="blocked",
                        reason=str(exc),
                    )
                )
                continue
            if new_memo != _normalize_text(matched.get("memo", "")):
                patch["memo"] = new_memo
                actions.append("stamp")

        if prior_cleared == "uncleared":
            patch["cleared"] = "cleared"
            actions.append("clear")

        action = "noop"
        if len(patch) > 1:
            updates.append(patch)
            action = "+".join(actions)

        report_rows.append(
            _card_sync_report_row(
                source_row,
                resolved_transaction_id=transaction_id,
                resolved_via=resolved_via,
                prior_cleared=prior_cleared,
                candidate_count=candidate_count,
                candidate_reconciled_count=candidate_reconciled_count,
                candidate_status=candidate_status,
                candidate_summary=candidate_summary,
                lineage_conflict_summary=lineage_conflict_summary,
                action=action,
            )
        )

    report = pd.DataFrame(report_rows, columns=CARD_SYNC_REPORT_COLUMNS)
    return {
        "account_id": account["account_id"],
        "account_name": account["account_name"],
        "source_row_count": int(len(source_rows)),
        "source_filtered_out_count": source_filtered_out_count,
        "updates": updates,
        "report": report,
        "matched_count": (
            int(report["action"].ne("unmatched").sum()) if not report.empty else 0
        ),
        "update_count": len(updates),
    }


def plan_card_cycle_reconciliation(
    *,
    account_name: str,
    source_df: pl.DataFrame,
    accounts: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    previous_df: pl.DataFrame | None = None,
    allow_reconciled_source: bool = False,
    source_date_from: Any = None,
    source_date_to: Any = None,
    previous_date_from: Any = None,
    previous_date_to: Any = None,
) -> dict[str, object]:
    account = _account_lookup(accounts, account_name)
    account_names = _account_name_map(accounts)
    source_rows = _target_source_rows(source_df, account_name)
    source_rows, source_filtered_out_count = _filter_rows_by_date_range(
        source_rows,
        date_from=source_date_from,
        date_to=source_date_to,
        range_name="source",
        allow_empty=False,
    )
    if source_rows.empty:
        raise ValueError(
            "Source file has no in-scope non-pending rows for the requested account."
        )
    previous_filtered_out_count = 0
    previous_rows = (
        _target_source_rows(previous_df, account_name)
        if previous_df is not None
        else pd.DataFrame()
    )
    if previous_df is not None:
        previous_rows, previous_filtered_out_count = _filter_rows_by_date_range(
            previous_rows,
            date_from=previous_date_from,
            date_to=previous_date_to,
            range_name="previous",
            allow_empty=False,
        )

    all_ynab_df = _all_ynab_transactions_frame(
        transactions, account_names=account_names
    )
    ynab_df = all_ynab_df[all_ynab_df["account_id"] == account["account_id"]].copy()
    if ynab_df.empty:
        raise ValueError(
            f"No live YNAB transactions found for account {account_name!r}."
        )

    mode = "transition" if previous_df is not None else "source_only"
    source_report = _evaluate_snapshot_rows(
        source_rows, ynab_df, snapshot_role="source"
    )
    previous_report = (
        _evaluate_snapshot_rows(previous_rows, ynab_df, snapshot_role="previous")
        if previous_df is not None
        else pd.DataFrame()
    )
    report = pd.concat([previous_report, source_report], ignore_index=True)

    result: dict[str, object] = {
        "ok": True,
        "mode": mode,
        "account_id": account["account_id"],
        "account_name": account["account_name"],
        "source_row_count": int(len(source_rows)),
        "previous_row_count": int(len(previous_rows)),
        "source_filtered_out_count": source_filtered_out_count,
        "previous_filtered_out_count": previous_filtered_out_count,
        "source_total_ils": round(float(source_rows["signed_ils"].sum()), 2),
        "previous_total_ils": (
            round(float(previous_rows["signed_ils"].sum()), 2)
            if previous_df is not None
            else 0.0
        ),
        "matched_source_count": int((source_report["ynab_transaction_id"] != "").sum()),
        "matched_previous_count": (
            int((previous_report["ynab_transaction_id"] != "").sum())
            if previous_df is not None
            else 0
        ),
        "matched_source_total_ils": round(
            float(
                pd.to_numeric(
                    source_report.get("ynab_signed_ils", 0.0), errors="coerce"
                )
                .fillna(0.0)
                .sum()
            ),
            2,
        ),
        "matched_previous_total_ils": (
            round(
                float(
                    pd.to_numeric(
                        previous_report.get("ynab_signed_ils", 0.0), errors="coerce"
                    )
                    .fillna(0.0)
                    .sum()
                ),
                2,
            )
            if previous_df is not None
            else 0.0
        ),
        "update_count": 0,
        "updates": [],
        "reason": "",
        "report": report,
        "warning": "",
        "separately_settled_count": 0,
        "separately_settled_dates": [],
        "payment_transfer_card_transaction_id": "",
        "payment_transfer_card_date": "",
        "payment_transfer_card_amount_ils": 0.0,
        "payment_transfer_bank_transaction_id": "",
        "payment_transfer_bank_account_id": "",
        "payment_transfer_bank_account_name": "",
        "payment_transfer_bank_date": "",
        "payment_transfer_bank_amount_ils": 0.0,
    }

    if (report["action"] == "blocked").any():
        result["ok"] = False
        blocked = report[report["action"] == "blocked"].copy()
        first = blocked.iloc[0]
        result["reason"] = (
            f"{int(len(blocked))} rows could not be matched exactly; "
            f"first blocked {first['snapshot_role']} row {int(first['row_index'])}: {first['reason']}"
        )
        return result

    source_reconciled = source_report[
        source_report["prior_cleared"] == "reconciled"
    ].copy()
    if not source_reconciled.empty and not allow_reconciled_source:
        result["ok"] = False
        first = source_reconciled.iloc[0]
        result["reason"] = (
            "Source file contains transactions that are already reconciled; "
            f"first source row {int(first['row_index'])} is already reconciled. "
            "Pass the older settled file as --previous and a newer current file as --source."
        )
        return result
    if not source_reconciled.empty and allow_reconciled_source:
        report.loc[
            (report["snapshot_role"] == "source")
            & (report["prior_cleared"] == "reconciled"),
            "action",
        ] = "already_reconciled"
        result["warning"] = (
            f"{len(source_reconciled)} source rows are already reconciled and will be skipped."
        )

    if mode == "source_only":
        older_open = _summarize_open_older_rows(ynab_df, source_rows["date"].min())
        if not older_open.empty:
            result["ok"] = False
            first = older_open.iloc[0]
            result["reason"] = (
                "Older cleared-but-unreconciled transactions exist before the first source row; "
                f"first older open row is {first['date']} amount {first['signed_ils']:.2f}. "
                "Provide --previous for month-transition reconciliation."
            )
            return result

        matched_source_rows = source_report.copy()
        report.loc[
            (report["snapshot_role"] == "source")
            & (report["prior_cleared"] == "cleared"),
            "action",
        ] = "keep_cleared"
        to_clear = matched_source_rows[
            matched_source_rows["prior_cleared"] == "uncleared"
        ].copy()
        for _, row in to_clear.iterrows():
            report.loc[
                (report["snapshot_role"] == "source")
                & (report["row_index"] == row["row_index"]),
                "action",
            ] = "clear"
        result["updates"] = _apply_updates_for_rows(to_clear, target_cleared="cleared")
        result["update_count"] = len(result["updates"])
        result["report"] = report
        return result

    previous_reconciled = previous_report[
        previous_report["prior_cleared"] == "reconciled"
    ].copy()

    previous_total = round(float(previous_rows["signed_ils"].sum()), 2)
    current_total = round(float(source_rows["signed_ils"].sum()), 2)
    result["previous_total_ils"] = previous_total
    result["source_total_ils"] = current_total

    _, main_previous_rows, sep_previous_rows = _identify_billing_date_groups(
        previous_rows
    )
    result["separately_settled_count"] = int(len(sep_previous_rows))
    result["separately_settled_dates"] = (
        sorted(str(d) for d in sep_previous_rows["secondary_date"].unique())
        if not sep_previous_rows.empty
        else []
    )
    payment_match = _validate_payment_transfer(
        previous_rows=main_previous_rows,
        all_ynab_df=all_ynab_df,
        card_account_id=account["account_id"],
        account_names=account_names,
    )
    if not payment_match.ok:
        result["ok"] = False
        result["reason"] = payment_match.reason
        return result
    result["payment_transfer_card_transaction_id"] = payment_match.card_transaction_id
    result["payment_transfer_card_date"] = payment_match.card_date
    result["payment_transfer_card_amount_ils"] = payment_match.card_amount_ils
    result["payment_transfer_bank_transaction_id"] = payment_match.bank_transaction_id
    result["payment_transfer_bank_account_id"] = payment_match.bank_account_id
    result["payment_transfer_bank_account_name"] = payment_match.bank_account_name
    result["payment_transfer_bank_date"] = payment_match.bank_date
    result["payment_transfer_bank_amount_ils"] = payment_match.bank_amount_ils

    # All previous rows should settle; current rows should remain open.
    previous_all_reconciled = len(previous_report) > 0 and len(
        previous_reconciled
    ) == len(previous_report)
    previous_to_reconcile = previous_report[
        previous_report["prior_cleared"] != "reconciled"
    ].copy()
    source_to_clear = source_report[
        source_report["prior_cleared"] == "uncleared"
    ].copy()
    if previous_report.empty:
        pass
    elif previous_all_reconciled:
        result["warning"] = "All previous-file transactions are already reconciled."
        report.loc[report["snapshot_role"] == "previous", "action"] = (
            "already_reconciled"
        )
    else:
        report.loc[report["snapshot_role"] == "previous", "action"] = "reconcile"
        if not previous_reconciled.empty:
            report.loc[
                (report["snapshot_role"] == "previous")
                & (report["prior_cleared"] == "reconciled"),
                "action",
            ] = "already_reconciled"
            result["warning"] = (
                f"{len(previous_reconciled)} previous-file rows are already reconciled; "
                f"reconciling remaining {len(previous_to_reconcile)} rows."
            )
        if not sep_previous_rows.empty:
            sep_date_strs = set(
                str(d) for d in sep_previous_rows["secondary_date"].unique()
            )
            sep_mask = (
                (report["snapshot_role"] == "previous")
                & (report["action"] == "reconcile")
                & (report["secondary_date"].isin(sep_date_strs))
            )
            report.loc[sep_mask, "action"] = "reconcile_separate"
    report.loc[
        (report["snapshot_role"] == "source")
        & (report["prior_cleared"] == "uncleared"),
        "action",
    ] = "clear"
    report.loc[
        (report["snapshot_role"] == "source") & (report["prior_cleared"] == "cleared"),
        "action",
    ] = "keep_cleared"

    updates = _apply_updates_for_rows(
        previous_to_reconcile, target_cleared="reconciled"
    )
    updates.extend(_apply_updates_for_rows(source_to_clear, target_cleared="cleared"))

    # In card reconciliation, only reconcile the card-side payment transfer.
    card_transfer_id = _normalize_text(payment_match.card_transaction_id)
    transfer_rows = all_ynab_df[
        all_ynab_df["id"].isin([card_transfer_id] if card_transfer_id else [])
    ].copy()
    if not transfer_rows.empty:
        transfer_to_reconcile = transfer_rows[
            transfer_rows["cleared"] != "reconciled"
        ].copy()
        transfer_to_reconcile = transfer_to_reconcile.rename(
            columns={"id": "ynab_transaction_id", "cleared": "prior_cleared"}
        )
        updates.extend(
            _apply_updates_for_rows(transfer_to_reconcile, target_cleared="reconciled")
        )

    result["updates"] = updates
    result["update_count"] = len(updates)
    result["report"] = report
    return result
