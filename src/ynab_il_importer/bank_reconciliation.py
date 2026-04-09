from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd  # kept for report DataFrame construction only
import polars as pl

from ynab_il_importer.artifacts.transaction_io import read_transactions_polars
import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.normalize as normalize


SYNC_REPORT_COLUMNS = [
    "row_index",
    "date",
    "secondary_date",
    "outflow_ils",
    "inflow_ils",
    "balance_ils",
    "bank_txn_id",
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

UNCLEARED_TRIAGE_COLUMNS = [
    "ynab_row_index",
    "ynab_transaction_id",
    "date",
    "amount_ils",
    "payee_name",
    "memo",
    "import_id",
    "matched_transaction_id",
    "exact_bank_row_count",
    "exact_unlinked_bank_row_count",
    "exact_linked_elsewhere_count",
    "near_bank_row_count",
    "near_unlinked_bank_row_count",
    "near_linked_elsewhere_count",
    "near_window_days",
    "days_from_latest_bank_row",
    "exact_bank_dates",
    "near_bank_dates",
    "exact_bank_summary",
    "near_bank_summary",
    "triage",
    "reason",
    "suggested_action",
]

RECONCILIATION_REPORT_COLUMNS = [
    "row_index",
    "date",
    "secondary_date",
    "outflow_ils",
    "inflow_ils",
    "balance_ils",
    "bank_txn_id",
    "legacy_import_id",
    "resolved_transaction_id",
    "resolved_via",
    "prior_cleared",
    "candidate_count",
    "candidate_reconciled_count",
    "candidate_status",
    "candidate_summary",
    "lineage_conflict_summary",
    "replayed_balance_ils",
    "balance_match",
    "action",
    "reason",
]


@dataclass(frozen=True)
class ResolvedAccount:
    account_id: str
    account_name: str
    last_reconciled_at: str
    account_payload: dict[str, Any]


@dataclass(frozen=True)
class ReconciliationResolution:
    matched_row: dict[str, Any] | None
    resolved_transaction_id: str
    resolved_via: str
    prior_cleared: str
    reason: str
    candidate_count: int
    candidate_reconciled_count: int
    candidate_status: str
    candidate_summary: str
    lineage_conflict_summary: str


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _truncate_text(value: Any, limit: int = 80) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _parse_date(value: Any) -> date | None:
    """Parse a date string or datetime.date object to datetime.date, or None on failure."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()[:10]
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _amount_milliunits(outflow_ils: Any, inflow_ils: Any) -> int:
    return bank_identity.signed_amount_milliunits(outflow_ils, inflow_ils)


def _amount_ils(outflow_ils: Any, inflow_ils: Any) -> float:
    return round(_amount_milliunits(outflow_ils, inflow_ils) / 1000.0, 2)


def _row_identity_key(row: dict[str, Any]) -> str:
    parts = [
        _normalize_text(row.get("account_name", "")),
        _normalize_text(row.get("source_account", "")),
        _normalize_text(row.get("date", "")),
        _normalize_text(row.get("secondary_date", "")),
        _normalize_text(row.get("outflow_ils", "")),
        _normalize_text(row.get("inflow_ils", "")),
        _normalize_text(row.get("fingerprint", "")),
        _normalize_text(row.get("description_raw", "")),
        _normalize_text(row.get("ref", "")),
    ]
    return normalize.normalize_text("|".join(parts))


def _compute_bank_legacy_import_ids(df: pl.DataFrame) -> pl.Series:
    """Compute YNAB-format legacy import IDs: YNAB:<milliunits>:<date>:<1-indexed-occurrence>.

    Occurrence is 1-indexed within (account_name, date, amount_milliunits) group,
    ordered by (account_key, date_key, amount_milliunits, stable_key, _row_nr).
    The stable_key is the normalised row identity string used to break ties.
    """
    if df.is_empty():
        return pl.Series("legacy_import_id", [], dtype=pl.Utf8)

    work = (
        df.with_row_index("_row_nr")
        .with_columns(
            [
                pl.col("account_name").fill_null("").str.strip_chars().alias("_account_key"),
                pl.col("date").cast(pl.Utf8).fill_null("").alias("_date_key"),
                pl.struct(
                    [
                        "account_name",
                        "source_account",
                        "date",
                        "secondary_date",
                        "outflow_ils",
                        "inflow_ils",
                        "fingerprint",
                        "description_raw",
                        "ref",
                    ]
                )
                .map_elements(
                    lambda r: normalize.normalize_text(
                        "|".join(
                            [
                                str(r.get("account_name") or ""),
                                str(r.get("source_account") or ""),
                                str(r.get("date") or ""),
                                str(r.get("secondary_date") or ""),
                                str(r.get("outflow_ils") or ""),
                                str(r.get("inflow_ils") or ""),
                                str(r.get("fingerprint") or ""),
                                str(r.get("description_raw") or ""),
                                str(r.get("ref") or ""),
                            ]
                        )
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("_stable_key"),
            ]
        )
    )
    sorted_work = work.sort(
        ["_account_key", "_date_key", "amount_milliunits", "_stable_key", "_row_nr"]
    ).with_columns(
        pl.col("_account_key")
        .cum_count()
        .over(["_account_key", "_date_key", "amount_milliunits"])
        .alias("_import_occurrence")
    )
    with_id = sorted_work.with_columns(
        pl.concat_str(
            [
                pl.lit("YNAB:"),
                pl.col("amount_milliunits").cast(pl.Utf8),
                pl.lit(":"),
                pl.col("_date_key"),
                pl.lit(":"),
                pl.col("_import_occurrence").cast(pl.Utf8),
            ]
        ).alias("legacy_import_id")
    )
    return with_id.sort("_row_nr")["legacy_import_id"]


def _same_balance(left: float, right: float) -> bool:
    return abs(round(float(left), 2) - round(float(right), 2)) <= 0.005


def _active_accounts(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [account for account in accounts if not bool(account.get("deleted", False))]


def load_bank_transactions(path: str | Path) -> pl.DataFrame:
    source_path = Path(path)
    if source_path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Bank reconciliation requires canonical parquet input, got: {source_path}"
        )
    return read_transactions_polars(source_path)


def _build_bank_source_frame(bank_df: pl.DataFrame) -> pl.DataFrame:
    """Prepare the canonical bank parquet as a Polars working frame.

    Returns a pl.DataFrame with a derived row_index, bank_txn_id,
    amount_milliunits, amount_ils, description_match_key, fingerprint_match_key,
    and legacy_import_id columns added alongside the canonical columns.
    """
    prepped = (
        bank_df.with_row_index("row_index")
        .select(
            "row_index",
            "account_id",
            "account_name",
            "source_account",
            "date",
            "secondary_date",
            "outflow_ils",
            "inflow_ils",
            "signed_amount_ils",
            "balance_ils",
            "description_raw",
            "ref",
            "transaction_id",
            "fingerprint",
        )
        .with_columns(
            pl.col("account_id").fill_null("").str.strip_chars(),
            pl.col("account_name").fill_null("").str.strip_chars(),
            pl.col("source_account").fill_null("").str.strip_chars(),
            pl.col("description_raw").fill_null("").str.strip_chars(),
            pl.col("ref").fill_null("").str.strip_chars(),
            pl.col("fingerprint").fill_null("").str.strip_chars(),
            pl.col("date").cast(pl.Date),
            pl.col("secondary_date").cast(pl.Date),
            pl.col("transaction_id")
            .fill_null("")
            .map_elements(
                bank_identity.validate_bank_txn_id,
                return_dtype=pl.String,
            )
            .alias("bank_txn_id"),
            (pl.col("signed_amount_ils") * 1000.0)
            .round(0)
            .cast(pl.Int64)
            .alias("amount_milliunits"),
            pl.col("signed_amount_ils").round(2).alias("amount_ils"),
            pl.col("description_raw")
            .map_elements(
                bank_identity.normalize_bank_memo_match_text,
                return_dtype=pl.String,
            )
            .alias("description_match_key"),
            pl.col("fingerprint")
            .map_elements(normalize.normalize_text, return_dtype=pl.String)
            .alias("fingerprint_match_key"),
        )
    )
    return prepped.with_columns(
        _compute_bank_legacy_import_ids(prepped).alias("legacy_import_id")
    )


def _build_bank_ynab_frame(transactions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the working ynab-row list from YNAB API transaction dicts.

    Uses stdlib date parsing; sets memo_bank_txn_id and memo_ref from memo content.
    Returns list[dict]; row_index is added later by _filter_account_transactions.
    """
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        if bool(txn.get("deleted", False)):
            continue
        memo = _normalize_text(txn.get("memo", ""))
        try:
            memo_bank_txn_id = bank_identity.extract_bank_txn_id_from_memo(memo)
            memo_marker_error = ""
        except ValueError as exc:
            memo_bank_txn_id = ""
            memo_marker_error = str(exc)
        memo_ref = bank_identity.extract_bank_ref_from_memo(memo)
        parsed_date = _parse_date(txn.get("date", ""))
        rows.append(
            {
                "id": _normalize_text(txn.get("id", "")),
                "account_id": _normalize_text(txn.get("account_id", "")),
                "date": parsed_date,
                "amount_milliunits": int(txn.get("amount", 0) or 0),
                "amount_ils": round(int(txn.get("amount", 0) or 0) / 1000.0, 2),
                "memo": memo,
                "memo_match_key": normalize.normalize_text(
                    bank_identity.strip_bank_txn_id_markers(memo)
                ),
                "payee_name": _normalize_text(txn.get("payee_name", "")),
                "payee_match_key": normalize.normalize_text(
                    _normalize_text(txn.get("payee_name", ""))
                ),
                "import_id": _normalize_text(txn.get("import_id", "")),
                "memo_bank_txn_id": memo_bank_txn_id,
                "memo_ref": memo_ref,
                "memo_marker_error": memo_marker_error,
                "cleared": _normalize_text(txn.get("cleared", "")),
                "approved": bool(txn.get("approved", False)),
                "matched_transaction_id": _normalize_text(
                    txn.get("matched_transaction_id", "")
                ),
                "deleted": False,
            }
        )
    return rows


def _resolve_account(
    bank_df: pl.DataFrame, accounts: list[dict[str, Any]]
) -> ResolvedAccount:
    active_accounts = _active_accounts(accounts)
    account_by_id = {
        _normalize_text(account.get("id", "")): account for account in active_accounts
    }
    account_by_name = {
        _normalize_text(account.get("name", "")): account for account in active_accounts
    }

    mapped_ids = sorted(
        {
            v
            for v in bank_df["account_id"].fill_null("").str.strip_chars().to_list()
            if v
        }
    )
    if len(mapped_ids) > 1:
        raise ValueError(
            f"Bank source resolves to multiple account_id values: {mapped_ids}"
        )
    if mapped_ids:
        account_id = mapped_ids[0]
        if account_id not in account_by_id:
            raise ValueError(f"Unknown or deleted YNAB account id: {account_id}")
        account = account_by_id[account_id]
        return ResolvedAccount(
            account_id=account_id,
            account_name=_normalize_text(account.get("name", "")),
            last_reconciled_at=_normalize_text(account.get("last_reconciled_at", "")),
            account_payload=account,
        )

    account_names = sorted(
        {
            v
            for v in bank_df["account_name"].fill_null("").str.strip_chars().to_list()
            if v
        }
    )
    if len(account_names) != 1:
        raise ValueError(
            "Bank source must resolve to exactly one account_name when account_id is absent."
        )
    account_name = account_names[0]
    if account_name not in account_by_name:
        raise ValueError(f"Unknown or deleted YNAB account name: {account_name}")
    account = account_by_name[account_name]
    return ResolvedAccount(
        account_id=_normalize_text(account.get("id", "")),
        account_name=account_name,
        last_reconciled_at=_normalize_text(account.get("last_reconciled_at", "")),
        account_payload=account,
    )


def _filter_account_transactions(
    ynab_rows: list[dict[str, Any]],
    account_id: str,
) -> list[dict[str, Any]]:
    """Return ynab rows for account_id with a sequential row_index added."""
    filtered = [r for r in ynab_rows if r.get("account_id") == account_id]
    return [{"row_index": i, **r} for i, r in enumerate(filtered)]


def _lineage_maps(
    ynab_rows: list[dict[str, Any]],
) -> tuple[dict[str, list[int]], dict[str, list[int]], dict[str, list[int]]]:
    import_map: dict[str, list[int]] = {}
    memo_map: dict[str, list[int]] = {}
    ref_map: dict[str, list[int]] = {}
    for idx, row in enumerate(ynab_rows):
        import_id = _normalize_text(row.get("import_id", ""))
        if bank_identity.is_bank_txn_id(import_id):
            import_map.setdefault(import_id, []).append(idx)
        memo_bank_txn_id = _normalize_text(row.get("memo_bank_txn_id", ""))
        if memo_bank_txn_id:
            memo_map.setdefault(memo_bank_txn_id, []).append(idx)
        memo_ref = _normalize_text(row.get("memo_ref", ""))
        if memo_ref:
            ref_map.setdefault(memo_ref, []).append(idx)
    return import_map, memo_map, ref_map


def _linked_row_indexes_for_bank_txn_id(
    bank_txn_id: str,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> list[int]:
    linked = set(import_map.get(bank_txn_id, []))
    linked.update(memo_map.get(bank_txn_id, []))
    return sorted(linked)


def _resolve_exact_lineage(
    bank_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
    ref_map: dict[str, list[int]] | None = None,
) -> tuple[dict[str, Any] | None, str, str]:
    bank_txn_id = bank_row["bank_txn_id"]
    bank_date = bank_row["date"]
    bank_amount = int(bank_row["amount_milliunits"])
    bank_ref = _normalize_text(bank_row.get("ref", ""))
    mismatch_reasons: list[str] = []

    import_hits = import_map.get(bank_txn_id, [])
    if len(import_hits) > 1:
        return None, "", f"duplicate YNAB import_id matches for {bank_txn_id}"
    if len(import_hits) == 1:
        candidate = ynab_rows[import_hits[0]]
        candidate_date = candidate.get("date")
        candidate_amount = int(candidate.get("amount_milliunits", 0) or 0)
        if candidate_date == bank_date and candidate_amount == bank_amount:
            return candidate, "import_id", ""
        mismatch_reasons.append(
            "bank_txn_id import_id is attached to a YNAB transaction with different date/amount"
        )

    memo_hits = memo_map.get(bank_txn_id, [])
    if len(memo_hits) > 1:
        return None, "", f"duplicate YNAB memo markers for {bank_txn_id}"
    if len(memo_hits) == 1:
        candidate = ynab_rows[memo_hits[0]]
        candidate_date = candidate.get("date")
        candidate_amount = int(candidate.get("amount_milliunits", 0) or 0)
        if candidate_date == bank_date and candidate_amount == bank_amount:
            return candidate, "memo_marker", ""
        mismatch_reasons.append(
            "bank_txn_id memo marker is attached to a YNAB transaction with different date/amount"
        )

    # ref-in-memo lookup: if a previous sync stamped the bank ref into the YNAB
    # memo, use that as confirmation together with a date+amount check.
    if bank_ref and ref_map is not None:
        ref_hits = ref_map.get(bank_ref, [])
        if len(ref_hits) == 1:
            candidate = ynab_rows[ref_hits[0]]
            candidate_date = candidate.get("date")
            candidate_amount = int(candidate.get("amount_milliunits", 0) or 0)
            if candidate_date == bank_date and candidate_amount == bank_amount:
                return candidate, "memo_ref", ""
            mismatch_reasons.append(
                "memo ref marker is attached to a YNAB transaction with different date/amount"
            )

    legacy_import_id = _normalize_text(bank_row.get("legacy_import_id", ""))
    if legacy_import_id:
        legacy_hits = [
            i for i, r in enumerate(ynab_rows) if r.get("import_id") == legacy_import_id
        ]
        if len(legacy_hits) > 1:
            return None, "", f"duplicate YNAB legacy import_id matches for {legacy_import_id}"
        if len(legacy_hits) == 1:
            candidate = ynab_rows[legacy_hits[0]]
            candidate_date = candidate.get("date")
            candidate_amount = int(candidate.get("amount_milliunits", 0) or 0)
            if candidate_date == bank_date and candidate_amount == bank_amount:
                return candidate, "legacy_import_id", ""
            mismatch_reasons.append(
                "legacy import_id is attached to a YNAB transaction with different date/amount"
            )

    if mismatch_reasons:
        return None, "", "; ".join(mismatch_reasons)
    return None, "", "no exact lineage match"


def _date_amount_candidates(bank_row: dict[str, Any], ynab_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bank_date = bank_row["date"]
    bank_amount = int(bank_row["amount_milliunits"])
    return [
        r for r in ynab_rows
        if r.get("date") == bank_date and int(r.get("amount_milliunits", 0)) == bank_amount
    ]


def _unlinked_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        r for r in candidates
        if not _normalize_text(r.get("memo_bank_txn_id", ""))
        and not bank_identity.is_bank_txn_id(_normalize_text(r.get("import_id", "")))
    ]


def _summarize_ynab_candidate(row: dict[str, Any]) -> str:
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


def _summarize_candidate_rows(candidates: list[dict[str, Any]]) -> str:
    if not candidates:
        return ""
    ordered = sorted(
        candidates,
        key=lambda r: (r.get("date") or date.min, r.get("amount_milliunits") or 0, r.get("id") or ""),
    )
    return " || ".join(_summarize_ynab_candidate(row) for row in ordered)


def _lineage_conflict_summary(
    bank_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> str:
    bank_txn_id = bank_row["bank_txn_id"]
    parts: list[str] = []
    import_hits = import_map.get(bank_txn_id, [])
    if len(import_hits) == 1:
        parts.append(
            f"import_id -> {_summarize_ynab_candidate(ynab_rows[import_hits[0]])}"
        )
    memo_hits = memo_map.get(bank_txn_id, [])
    if len(memo_hits) == 1:
        parts.append(
            f"memo_marker -> {_summarize_ynab_candidate(ynab_rows[memo_hits[0]])}"
        )
    return " || ".join(parts)


def _summarize_bank_row(
    row: dict[str, Any],
    *,
    linkage_status: str = "",
    linked_ynab_summary: str = "",
) -> str:
    fingerprint = _truncate_text(row.get("fingerprint", "") or "<blank>", limit=40)
    description = _truncate_text(row.get("description_raw", "") or "<blank>", limit=60)
    summary = (
        f"{row.get('date')} | "
        f"{float(row.get('amount_ils', 0.0)):.2f} | "
        f"status={linkage_status or '<unknown>'} | "
        f"fingerprint={fingerprint} | "
        f"description={description}"
    )
    if linked_ynab_summary:
        summary += f" | linked_ynab={linked_ynab_summary}"
    return summary


def _summarize_bank_candidates_for_triage(
    bank_candidates: list[dict[str, Any]],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
    *,
    ynab_transaction_id: str,
) -> tuple[int, int, str]:
    if not bank_candidates:
        return 0, 0, ""

    unlinked_count = 0
    linked_elsewhere_count = 0
    parts: list[str] = []

    ordered = sorted(
        bank_candidates,
        key=lambda r: (r.get("date") or date.min, r.get("amount_milliunits") or 0, r.get("bank_txn_id") or ""),
    )
    for bank_row in ordered:
        bank_txn_id = _normalize_text(bank_row.get("bank_txn_id", ""))
        linked_row_indexes = _linked_row_indexes_for_bank_txn_id(
            bank_txn_id, import_map, memo_map
        )
        if not linked_row_indexes:
            linkage_status = "unlinked"
            unlinked_count += 1
            linked_ynab_summary = ""
        else:
            linked_transaction_ids = {
                _normalize_text(ynab_rows[idx].get("id", ""))
                for idx in linked_row_indexes
            }
            if linked_transaction_ids == {ynab_transaction_id}:
                linkage_status = "linked_to_self"
                linked_ynab_summary = ""
            else:
                linkage_status = "linked_elsewhere"
                linked_elsewhere_count += 1
                linked_ynab_summary = " || ".join(
                    _summarize_ynab_candidate(ynab_rows[idx]) for idx in linked_row_indexes
                )
        parts.append(
            _summarize_bank_row(
                bank_row,
                linkage_status=linkage_status,
                linked_ynab_summary=linked_ynab_summary,
            )
        )

    return unlinked_count, linked_elsewhere_count, " || ".join(parts)


def _candidate_diagnostics(
    bank_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[int, int, str, str, str]:
    candidates = _date_amount_candidates(bank_row, ynab_rows)
    candidate_count = len(candidates)
    candidate_reconciled_count = sum(
        1 for r in candidates if r.get("cleared") == "reconciled"
    )
    candidate_summary = _summarize_candidate_rows(candidates)
    lineage_conflict = _lineage_conflict_summary(
        bank_row, ynab_rows, import_map, memo_map
    )
    if candidate_count == 0:
        return 0, 0, "no_date_amount_match", candidate_summary, lineage_conflict

    unlinked = _unlinked_candidates(candidates)
    if not unlinked:
        return (
            candidate_count,
            candidate_reconciled_count,
            "only_linked_date_amount_candidates",
            candidate_summary,
            lineage_conflict,
        )

    memo_exact = [
        r for r in unlinked if r.get("memo_match_key") == bank_row.get("description_match_key")
    ]
    if len(memo_exact) == 1:
        return (
            candidate_count,
            candidate_reconciled_count,
            "unique_memo_exact_candidate",
            candidate_summary,
            lineage_conflict,
        )
    if len(memo_exact) > 1:
        return (
            candidate_count,
            candidate_reconciled_count,
            "ambiguous_memo_exact_candidates",
            candidate_summary,
            lineage_conflict,
        )

    fingerprint_match_key = _normalize_text(bank_row.get("fingerprint_match_key", ""))
    if fingerprint_match_key:
        payee_exact = [r for r in unlinked if r.get("payee_match_key") == fingerprint_match_key]
        if len(payee_exact) == 1:
            return (
                candidate_count,
                candidate_reconciled_count,
                "unique_payee_match_date_amount_candidate",
                candidate_summary,
                lineage_conflict,
            )
        if len(payee_exact) > 1:
            return (
                candidate_count,
                candidate_reconciled_count,
                "ambiguous_payee_match_date_amount_candidates",
                candidate_summary,
                lineage_conflict,
            )

    if len(unlinked) == 1:
        cleared = _normalize_text(unlinked[0].get("cleared", ""))
        if cleared == "reconciled":
            return (
                candidate_count,
                candidate_reconciled_count,
                "unique_reconciled_date_amount_candidate",
                candidate_summary,
                lineage_conflict,
            )
        return (
            candidate_count,
            candidate_reconciled_count,
            "unique_nonreconciled_date_amount_candidate",
            candidate_summary,
            lineage_conflict,
        )

    reconciled_unlinked_count = sum(1 for r in unlinked if r.get("cleared") == "reconciled")
    if reconciled_unlinked_count:
        return (
            candidate_count,
            candidate_reconciled_count,
            "ambiguous_reconciled_date_amount_candidates",
            candidate_summary,
            lineage_conflict,
        )
    return (
        candidate_count,
        candidate_reconciled_count,
        "ambiguous_date_amount_candidates",
        candidate_summary,
        lineage_conflict,
    )


def _uncleared_triage_row(
    ynab_row: dict[str, Any],
    *,
    exact_bank_row_count: int = 0,
    exact_unlinked_bank_row_count: int = 0,
    exact_linked_elsewhere_count: int = 0,
    near_bank_row_count: int = 0,
    near_unlinked_bank_row_count: int = 0,
    near_linked_elsewhere_count: int = 0,
    near_window_days: int = 0,
    days_from_latest_bank_row: int | None = None,
    exact_bank_dates: str = "",
    near_bank_dates: str = "",
    exact_bank_summary: str = "",
    near_bank_summary: str = "",
    triage: str = "",
    reason: str = "",
    suggested_action: str = "",
) -> dict[str, Any]:
    return {
        "ynab_row_index": int(ynab_row["row_index"]),
        "ynab_transaction_id": _normalize_text(ynab_row.get("id", "")),
        "date": ynab_row["date"],
        "amount_ils": round(float(ynab_row["amount_ils"]), 2),
        "payee_name": _normalize_text(ynab_row.get("payee_name", "")),
        "memo": _normalize_text(ynab_row.get("memo", "")),
        "import_id": _normalize_text(ynab_row.get("import_id", "")),
        "matched_transaction_id": _normalize_text(
            ynab_row.get("matched_transaction_id", "")
        ),
        "exact_bank_row_count": exact_bank_row_count,
        "exact_unlinked_bank_row_count": exact_unlinked_bank_row_count,
        "exact_linked_elsewhere_count": exact_linked_elsewhere_count,
        "near_bank_row_count": near_bank_row_count,
        "near_unlinked_bank_row_count": near_unlinked_bank_row_count,
        "near_linked_elsewhere_count": near_linked_elsewhere_count,
        "near_window_days": near_window_days,
        "days_from_latest_bank_row": days_from_latest_bank_row,
        "exact_bank_dates": exact_bank_dates,
        "near_bank_dates": near_bank_dates,
        "exact_bank_summary": exact_bank_summary,
        "near_bank_summary": near_bank_summary,
        "triage": triage,
        "reason": reason,
        "suggested_action": suggested_action,
    }


def plan_uncleared_ynab_triage(
    bank_df: pl.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
    *,
    near_window_days: int = 7,
    pending_window_days: int = 3,
) -> dict[str, Any]:
    if near_window_days < 0:
        raise ValueError("near_window_days must be >= 0.")
    if pending_window_days < 0:
        raise ValueError("pending_window_days must be >= 0.")

    prepared_bank = _build_bank_source_frame(bank_df)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_rows = _filter_account_transactions(
        _build_bank_ynab_frame(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map, _ = _lineage_maps(ynab_rows)

    uncleared = [r for r in ynab_rows if r.get("cleared") == "uncleared"]
    if not uncleared:
        report = pd.DataFrame(columns=UNCLEARED_TRIAGE_COLUMNS)
        return {
            "account_id": resolved_account.account_id,
            "account_name": resolved_account.account_name,
            "report": report,
            "recent_pending_count": 0,
            "candidate_source_match_count": 0,
            "stale_orphan_count": 0,
        }

    latest_bank_date = prepared_bank["date"].max()
    report_rows: list[dict[str, Any]] = []

    for ynab_row in uncleared:
        ynab_date = ynab_row.get("date")
        ynab_amount = ynab_row.get("amount_milliunits")
        exact_frame = prepared_bank.filter(
            (pl.col("date") == ynab_date) & (pl.col("amount_milliunits") == ynab_amount)
        )
        near_frame = prepared_bank.filter(
            (pl.col("amount_milliunits") == ynab_amount)
            & (pl.col("date") >= ynab_date - timedelta(days=near_window_days))
            & (pl.col("date") <= ynab_date + timedelta(days=near_window_days))
        )
        exact_candidates = exact_frame.to_dicts()
        near_candidates = near_frame.to_dicts()

        exact_unlinked_count, exact_linked_elsewhere_count, exact_summary = (
            _summarize_bank_candidates_for_triage(
                exact_candidates,
                ynab_rows,
                import_map,
                memo_map,
                ynab_transaction_id=_normalize_text(ynab_row.get("id", "")),
            )
        )
        near_unlinked_count, near_linked_elsewhere_count, near_summary = (
            _summarize_bank_candidates_for_triage(
                near_candidates,
                ynab_rows,
                import_map,
                memo_map,
                ynab_transaction_id=_normalize_text(ynab_row.get("id", "")),
            )
        )

        exact_dates = " | ".join(sorted({str(r.get("date")) for r in exact_candidates}))
        near_dates = " | ".join(sorted({str(r.get("date")) for r in near_candidates}))
        days_from_latest_bank_row = (
            int((latest_bank_date - ynab_date).days)
            if latest_bank_date is not None and ynab_date is not None
            else None
        )

        if exact_unlinked_count > 0:
            triage = "candidate_source_match"
            reason = "exact date+amount bank row exists and is not yet linked"
            suggested_action = "run_sync_or_accept_match"
        elif exact_linked_elsewhere_count > 0:
            triage = "candidate_source_match"
            reason = "exact date+amount bank row exists but is already linked elsewhere"
            suggested_action = "investigate_link_conflict"
        elif near_unlinked_count > 0:
            triage = "candidate_source_match"
            reason = (
                f"same-amount bank row exists within {near_window_days} days and is not yet linked"
            )
            suggested_action = "review_nearby_match"
        elif near_linked_elsewhere_count > 0:
            triage = "candidate_source_match"
            reason = (
                f"same-amount bank row exists within {near_window_days} days but is already linked elsewhere"
            )
            suggested_action = "investigate_link_conflict"
        elif (
            days_from_latest_bank_row is not None
            and days_from_latest_bank_row <= pending_window_days
        ):
            triage = "recent_pending"
            reason = "transaction is close to the end of the available bank window"
            suggested_action = "wait_for_bank"
        else:
            triage = "stale_orphan"
            reason = f"no same-amount bank row exists within {near_window_days} days"
            suggested_action = "review_for_delete"

        report_rows.append(
            _uncleared_triage_row(
                ynab_row,
                exact_bank_row_count=len(exact_candidates),
                exact_unlinked_bank_row_count=exact_unlinked_count,
                exact_linked_elsewhere_count=exact_linked_elsewhere_count,
                near_bank_row_count=len(near_candidates),
                near_unlinked_bank_row_count=near_unlinked_count,
                near_linked_elsewhere_count=near_linked_elsewhere_count,
                near_window_days=near_window_days,
                days_from_latest_bank_row=days_from_latest_bank_row,
                exact_bank_dates=exact_dates,
                near_bank_dates=near_dates,
                exact_bank_summary=exact_summary,
                near_bank_summary=near_summary,
                triage=triage,
                reason=reason,
                suggested_action=suggested_action,
            )
        )

    report = pd.DataFrame(report_rows, columns=UNCLEARED_TRIAGE_COLUMNS).sort_values(
        ["date", "amount_ils", "ynab_transaction_id"], na_position="last"
    )
    report = report.reset_index(drop=True)

    return {
        "account_id": resolved_account.account_id,
        "account_name": resolved_account.account_name,
        "report": report,
        "recent_pending_count": int((report["triage"] == "recent_pending").sum()),
        "candidate_source_match_count": int(
            (report["triage"] == "candidate_source_match").sum()
        ),
        "stale_orphan_count": int((report["triage"] == "stale_orphan").sum()),
    }


def _memo_exact_fallback_candidate(
    bank_row: dict[str, Any], ynab_rows: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, str]:
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_rows))
    candidates = [
        r for r in candidates if r.get("memo_match_key") == bank_row.get("description_match_key")
    ]

    if not candidates:
        return None, "no conservative memo match"
    if len(candidates) > 1:
        return None, "ambiguous conservative memo match"
    return candidates[0], ""


def _payee_exact_fallback_candidate(
    bank_row: dict[str, Any], ynab_rows: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, str]:
    fingerprint_match_key = _normalize_text(bank_row.get("fingerprint_match_key", ""))
    if not fingerprint_match_key:
        return None, "no conservative payee match"
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_rows))
    candidates = [r for r in candidates if r.get("payee_match_key") == fingerprint_match_key]

    if not candidates:
        return None, "no conservative payee match"
    if len(candidates) > 1:
        return None, "ambiguous conservative payee match"
    return candidates[0], ""


def _legacy_reconciled_fallback_candidate(
    bank_row: dict[str, Any], ynab_rows: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, str]:
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_rows))
    if not candidates:
        return None, "no legacy reconciled date+amount match"
    reconciled = [r for r in candidates if r.get("cleared") == "reconciled"]
    if not reconciled:
        return None, "no legacy reconciled date+amount match"
    if len(reconciled) > 1:
        return None, "ambiguous legacy reconciled date+amount match"
    if len(candidates) > 1:
        return None, "ambiguous legacy date+amount match"
    return reconciled[0], ""


def _sync_unmatched_reason(
    candidate_status: str,
    base_reason: str,
) -> str:
    if candidate_status == "only_linked_date_amount_candidates":
        return "same date/amount candidate is already linked to a different bank_txn_id"
    if candidate_status == "ambiguous_reconciled_date_amount_candidates":
        return "multiple reconciled YNAB transactions share this date/amount"
    if candidate_status == "ambiguous_date_amount_candidates":
        return "multiple YNAB transactions share this date/amount"
    if candidate_status == "unique_nonreconciled_date_amount_candidate":
        return "unique date/amount candidate exists but memo/payee does not confirm it"
    if candidate_status == "no_date_amount_match":
        return base_reason
    return base_reason


def _candidate_reason(
    candidate_status: str,
    base_reason: str,
) -> str:
    if base_reason != "no exact lineage match":
        return base_reason
    return _sync_unmatched_reason(candidate_status, base_reason)


def _sync_report_row(
    bank_row: dict[str, Any],
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
) -> dict[str, Any]:
    return {
        "row_index": int(bank_row["row_index"]),
        "date": bank_row["date"],
        "secondary_date": bank_row["secondary_date"],
        "outflow_ils": round(float(bank_row["outflow_ils"]), 2),
        "inflow_ils": round(float(bank_row["inflow_ils"]), 2),
        "balance_ils": bank_row["balance_ils"],
        "bank_txn_id": bank_row["bank_txn_id"],
        "legacy_import_id": _normalize_text(bank_row.get("legacy_import_id", "")),
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


def plan_bank_match_sync(
    bank_df: pl.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    prepared_bank = _build_bank_source_frame(bank_df)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_rows = _filter_account_transactions(
        _build_bank_ynab_frame(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map, ref_map = _lineage_maps(ynab_rows)

    report_rows: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []

    for bank_row in prepared_bank.iter_rows(named=True):
        matched, resolved_via, reason = _resolve_exact_lineage(
            bank_row, ynab_rows, import_map, memo_map, ref_map
        )
        (
            candidate_count,
            candidate_reconciled_count,
            candidate_status,
            candidate_summary,
            lineage_conflict_summary,
        ) = _candidate_diagnostics(bank_row, ynab_rows, import_map, memo_map)
        if matched is None and reason == "no exact lineage match":
            matched, fallback_reason = _memo_exact_fallback_candidate(bank_row, ynab_rows)
            if matched is not None:
                resolved_via = "memo_exact"
                reason = ""
            else:
                matched, payee_reason = _payee_exact_fallback_candidate(
                    bank_row, ynab_rows
                )
                if matched is not None:
                    resolved_via = "payee_exact"
                    reason = ""
                else:
                    matched, legacy_reason = _legacy_reconciled_fallback_candidate(
                        bank_row, ynab_rows
                    )
                    if matched is not None:
                        resolved_via = "date_amount_reconciled"
                        reason = ""
                    else:
                        # Last resort: accept a unique unlinked date+amount candidate so we
                        # can stamp the bank_txn_id + ref into its memo.  Future reconciliation
                        # runs will then recognise it via the stronger memo_ref lookup.
                        unlinked = _unlinked_candidates(
                            _date_amount_candidates(bank_row, ynab_rows)
                        )
                        if len(unlinked) == 1:
                            matched = unlinked[0]
                            resolved_via = "unique_date_amount"
                            reason = ""
                        else:
                            base_reason = fallback_reason
                            if (
                                candidate_status
                                == "unique_reconciled_date_amount_candidate"
                            ):
                                base_reason = legacy_reason
                            elif (
                                candidate_status
                                == "unique_payee_match_date_amount_candidate"
                            ):
                                base_reason = payee_reason
                            reason = _sync_unmatched_reason(
                                candidate_status, base_reason
                            )

        if matched is None:
            report_rows.append(
                _sync_report_row(
                    bank_row,
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
        patch: dict[str, Any] = {"id": transaction_id}
        actions: list[str] = []

        if resolved_via in {
            "legacy_import_id",
            "memo_exact",
            "payee_exact",
            "date_amount_reconciled",
            "unique_date_amount",
        }:
            try:
                patch["memo"] = bank_identity.append_bank_txn_id_marker(
                    matched.get("memo", ""),
                    bank_row["bank_txn_id"],
                    ref=_normalize_text(bank_row.get("ref", "")),
                )
                actions.append("stamp")
            except ValueError as exc:
                report_rows.append(
                    _sync_report_row(
                        bank_row,
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

        if prior_cleared == "uncleared":
            patch["cleared"] = "cleared"
            actions.append("clear")

        if len(patch) > 1:
            updates.append(patch)
            action = "+".join(actions)
        else:
            action = "noop"

        report_rows.append(
            _sync_report_row(
                bank_row,
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

    report = pd.DataFrame(report_rows, columns=SYNC_REPORT_COLUMNS)
    return {
        "account_id": resolved_account.account_id,
        "account_name": resolved_account.account_name,
        "updates": updates,
        "report": report,
        "matched_count": (
            int((report["action"] != "unmatched").sum()) if not report.empty else 0
        ),
        "update_count": len(updates),
    }


def _require_balance_column(bank_df: pl.DataFrame) -> None:
    if bank_df["balance_ils"].is_null().any():
        missing_rows = bank_df.filter(pl.col("balance_ils").is_null())["row_index"].to_list()
        raise ValueError(f"Bank source is missing balance_ils on row(s): {missing_rows}")


def _last_reconciled_date(last_reconciled_at: str) -> date | None:
    if not _normalize_text(last_reconciled_at):
        return None
    return _parse_date(last_reconciled_at)


def _starting_balance_transaction(ynab_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not ynab_rows:
        raise ValueError("No YNAB transactions found for the target account.")
    ordered = sorted(
        ynab_rows,
        key=lambda r: (r.get("date") or date.min, r.get("id") or ""),
    )
    first = ordered[0]
    if first.get("date") is None:
        raise ValueError("Could not determine starting balance transaction date.")
    return first


def _reconciliation_report_row(
    bank_row: dict[str, Any],
    *,
    resolved_transaction_id: str = "",
    resolved_via: str = "",
    prior_cleared: str = "",
    candidate_count: int = 0,
    candidate_reconciled_count: int = 0,
    candidate_status: str = "",
    candidate_summary: str = "",
    lineage_conflict_summary: str = "",
    replayed_balance_ils: float | None = None,
    balance_match: bool | None = None,
    action: str = "",
    reason: str = "",
) -> dict[str, Any]:
    return {
        "row_index": int(bank_row["row_index"]),
        "date": bank_row["date"],
        "secondary_date": bank_row["secondary_date"],
        "outflow_ils": round(float(bank_row["outflow_ils"]), 2),
        "inflow_ils": round(float(bank_row["inflow_ils"]), 2),
        "balance_ils": round(float(bank_row["balance_ils"]), 2),
        "bank_txn_id": bank_row["bank_txn_id"],
        "legacy_import_id": _normalize_text(bank_row.get("legacy_import_id", "")),
        "resolved_transaction_id": resolved_transaction_id,
        "resolved_via": resolved_via,
        "prior_cleared": prior_cleared,
        "candidate_count": candidate_count,
        "candidate_reconciled_count": candidate_reconciled_count,
        "candidate_status": candidate_status,
        "candidate_summary": candidate_summary,
        "lineage_conflict_summary": lineage_conflict_summary,
        "replayed_balance_ils": replayed_balance_ils,
        "balance_match": balance_match,
        "action": action,
        "reason": reason,
    }


def _resolve_reconciliation_rows(
    prepared_bank: pl.DataFrame,
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
    ref_map: dict[str, list[int]] | None = None,
) -> tuple[list[ReconciliationResolution], list[dict[str, Any]]]:
    resolutions: list[ReconciliationResolution] = []
    report_rows: list[dict[str, Any]] = []

    for bank_row in prepared_bank.iter_rows(named=True):
        matched, resolved_via, reason = _resolve_exact_lineage(
            bank_row, ynab_rows, import_map, memo_map, ref_map
        )
        transaction_id = (
            _normalize_text(matched.get("id", "")) if matched is not None else ""
        )
        prior_cleared = (
            _normalize_text(matched.get("cleared", "")) if matched is not None else ""
        )
        (
            candidate_count,
            candidate_reconciled_count,
            candidate_status,
            candidate_summary,
            lineage_conflict_summary,
        ) = _candidate_diagnostics(bank_row, ynab_rows, import_map, memo_map)
        if matched is None:
            reason = _candidate_reason(candidate_status, reason)
        resolutions.append(
            ReconciliationResolution(
                matched_row=matched,
                resolved_transaction_id=transaction_id,
                resolved_via=resolved_via,
                prior_cleared=prior_cleared,
                reason=reason,
                candidate_count=candidate_count,
                candidate_reconciled_count=candidate_reconciled_count,
                candidate_status=candidate_status,
                candidate_summary=candidate_summary,
                lineage_conflict_summary=lineage_conflict_summary,
            )
        )
        report_rows.append(
            _reconciliation_report_row(
                bank_row,
                resolved_transaction_id=transaction_id,
                resolved_via=resolved_via,
                prior_cleared=prior_cleared,
                candidate_count=candidate_count,
                candidate_reconciled_count=candidate_reconciled_count,
                candidate_status=candidate_status,
                candidate_summary=candidate_summary,
                lineage_conflict_summary=lineage_conflict_summary,
                action="matched_preview" if matched is not None else "unmatched",
                reason="" if matched is not None else reason,
            )
        )

    return resolutions, report_rows


def _is_anchor_candidate(resolution: ReconciliationResolution) -> bool:
    if resolution.matched_row is not None and resolution.prior_cleared == "reconciled":
        return True
    return resolution.candidate_status == "unique_reconciled_date_amount_candidate"


def _find_anchor_window(
    resolutions: list[ReconciliationResolution],
    anchor_streak: int,
) -> tuple[int | None, int | None, int]:
    if len(resolutions) < anchor_streak:
        return None, None, 0

    found_start: int | None = None
    best_start: int | None = None
    best_count = -1
    for start in range(0, len(resolutions) - anchor_streak + 1):
        window = resolutions[start : start + anchor_streak]
        eligible_count = sum(
            1 for resolution in window if _is_anchor_candidate(resolution)
        )
        if eligible_count > best_count or (
            eligible_count == best_count
            and best_start is not None
            and start > best_start
        ):
            best_start = start
            best_count = eligible_count
        # Keep the first fully eligible streak as the anchor. Choosing a later
        # streak can silently skip unresolved historical rows as pre-anchor.
        if eligible_count == anchor_streak and found_start is None:
            found_start = start

    return found_start, best_start, best_count


def _reconciliation_result(
    *,
    resolved_account: ResolvedAccount,
    prepared_bank: pl.DataFrame,
    report_rows: list[dict[str, Any]],
    anchor_streak: int,
    last_reconciled_exists: bool,
    ok: bool,
    reason: str = "",
    anchor_type: str = "",
    anchor_transaction_id: str = "",
    anchor_balance_ils: float = 0.0,
    anchor_window_start: int | None = None,
    updates: list[dict[str, Any]] | None = None,
    final_balance_ils: float | None = None,
) -> dict[str, Any]:
    report = pd.DataFrame(report_rows, columns=RECONCILIATION_REPORT_COLUMNS)
    resolved_ids = (
        report["resolved_transaction_id"].astype("string").fillna("").str.strip()
    )
    matched_mask = resolved_ids != ""
    matched_count = int(matched_mask.sum())
    reconciled_match_count = int(
        (
            matched_mask
            & report["prior_cleared"]
            .astype("string")
            .fillna("")
            .str.strip()
            .eq("reconciled")
        ).sum()
    )
    probable_legacy_match_count = int(
        report["candidate_status"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("unique_reconciled_date_amount_candidate")
        .sum()
    )

    anchor_expected_count = anchor_streak if last_reconciled_exists else 0
    if anchor_window_start is None:
        anchor_window = report.head(min(anchor_streak, len(report))).copy()
    else:
        anchor_window = report.iloc[
            anchor_window_start : anchor_window_start + anchor_streak
        ].copy()
    anchor_matched_count = int(
        anchor_window["resolved_transaction_id"]
        .astype("string")
        .fillna("")
        .str.strip()
        .ne("")
        .sum()
    )
    anchor_reconciled_count = int(
        anchor_window["prior_cleared"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("reconciled")
        .sum()
    )
    anchor_probable_legacy_count = int(
        anchor_window["candidate_status"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("unique_reconciled_date_amount_candidate")
        .sum()
    )
    anchor_eligible_mask = (
        anchor_window["resolved_transaction_id"]
        .astype("string")
        .fillna("")
        .str.strip()
        .ne("")
        & anchor_window["prior_cleared"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("reconciled")
    ) | (
        anchor_window["candidate_status"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("unique_reconciled_date_amount_candidate")
    )
    anchor_eligible_count = int(anchor_eligible_mask.sum())
    if anchor_window.empty:
        anchor_window_row_start = -1
        anchor_window_row_end = -1
    else:
        anchor_window_row_start = int(anchor_window.iloc[0]["row_index"])
        anchor_window_row_end = int(anchor_window.iloc[-1]["row_index"])
    if anchor_window_row_end >= 0:
        post_anchor = report.loc[report["row_index"] > anchor_window_row_end].copy()
    else:
        post_anchor = report.iloc[0:0].copy()
    post_anchor_unresolved = post_anchor[
        post_anchor["resolved_transaction_id"]
        .astype("string")
        .fillna("")
        .str.strip()
        .eq("")
    ]
    post_anchor_unresolved_count = int(len(post_anchor_unresolved))
    first_post_anchor_unresolved_row = (
        int(post_anchor_unresolved.iloc[0]["row_index"])
        if not post_anchor_unresolved.empty
        else -1
    )

    if final_balance_ils is None:
        final_balance_ils = (
            round(float(prepared_bank.row(-1, named=True)["balance_ils"]), 2)
            if not prepared_bank.is_empty()
            else 0.0
        )

    return {
        "ok": ok,
        "reason": reason,
        "account_id": resolved_account.account_id,
        "account_name": resolved_account.account_name,
        "last_reconciled_at": resolved_account.last_reconciled_at,
        "anchor_type": anchor_type,
        "anchor_transaction_id": anchor_transaction_id,
        "anchor_balance_ils": anchor_balance_ils,
        "updates": updates or [],
        "report": report,
        "update_count": len(updates or []),
        "final_balance_ils": final_balance_ils,
        "matched_count": matched_count,
        "reconciled_match_count": reconciled_match_count,
        "probable_legacy_match_count": probable_legacy_match_count,
        "anchor_expected_count": anchor_expected_count,
        "anchor_matched_count": anchor_matched_count,
        "anchor_reconciled_count": anchor_reconciled_count,
        "anchor_eligible_count": anchor_eligible_count,
        "anchor_probable_legacy_count": anchor_probable_legacy_count,
        "anchor_window_row_start": anchor_window_row_start,
        "anchor_window_row_end": anchor_window_row_end,
        "post_anchor_unresolved_count": post_anchor_unresolved_count,
        "first_post_anchor_unresolved_row": first_post_anchor_unresolved_row,
        "anchor_streak": anchor_streak,
    }


def plan_bank_statement_reconciliation(
    bank_df: pl.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
    *,
    anchor_streak: int = 7,
    use_ynab_reconciled_date: bool = False,
) -> dict[str, Any]:
    if anchor_streak < 1:
        raise ValueError("anchor_streak must be at least 1.")

    prepared_bank = _build_bank_source_frame(bank_df)
    _require_balance_column(prepared_bank)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_rows = _filter_account_transactions(
        _build_bank_ynab_frame(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map, ref_map = _lineage_maps(ynab_rows)

    earliest_bank_date = prepared_bank["date"].min()
    updates: list[dict[str, Any]] = []
    resolutions, report_rows = _resolve_reconciliation_rows(
        prepared_bank,
        ynab_rows,
        import_map,
        memo_map,
        ref_map,
    )

    last_reconciled = _last_reconciled_date(resolved_account.last_reconciled_at)
    anchor_type = ""
    anchor_balance = 0.0
    anchor_row_index = -1
    anchor_window_start = None
    anchor_transaction_id = ""
    last_reconciled_exists = last_reconciled is not None

    if last_reconciled is not None:
        if use_ynab_reconciled_date:
            required_start = last_reconciled - timedelta(days=7)
            if earliest_bank_date > required_start:
                return _reconciliation_result(
                    resolved_account=resolved_account,
                    prepared_bank=prepared_bank,
                    report_rows=report_rows,
                    anchor_streak=anchor_streak,
                    last_reconciled_exists=last_reconciled_exists,
                    ok=False,
                    anchor_type="last_reconciled_at",
                    reason=(
                        "Bank source starts too late for auto-reconciliation: "
                        f"{earliest_bank_date} > {required_start}."
                    ),
                )
        if len(prepared_bank) < anchor_streak:
            return _reconciliation_result(
                resolved_account=resolved_account,
                prepared_bank=prepared_bank,
                report_rows=report_rows,
                anchor_streak=anchor_streak,
                last_reconciled_exists=last_reconciled_exists,
                ok=False,
                anchor_type="last_reconciled_at",
                reason=f"Bank source has fewer than {anchor_streak} rows; cannot establish anchor.",
            )
        anchor_start_index, best_anchor_start_index, best_anchor_count = (
            _find_anchor_window(resolutions, anchor_streak)
        )
        diagnostic_anchor_start = (
            anchor_start_index
            if anchor_start_index is not None
            else best_anchor_start_index
        )
        if anchor_start_index is None:
            return _reconciliation_result(
                resolved_account=resolved_account,
                prepared_bank=prepared_bank,
                report_rows=report_rows,
                anchor_streak=anchor_streak,
                last_reconciled_exists=last_reconciled_exists,
                ok=False,
                anchor_type="last_reconciled_at",
                anchor_window_start=diagnostic_anchor_start,
                reason=(
                    "Could not establish reconciliation anchor: best candidate streak "
                    f"covered {best_anchor_count} / {anchor_streak} rows."
                ),
            )
        for i in range(anchor_start_index):
            report_rows[i]["action"] = "pre_anchor_history"
        anchor_window_start = anchor_start_index
        for i in range(anchor_start_index, anchor_start_index + anchor_streak):
            bank_row = prepared_bank.row(i, named=True)
            resolution = resolutions[i]
            if not _is_anchor_candidate(resolution):
                report_rows[i] = _reconciliation_report_row(
                    bank_row,
                    candidate_count=resolution.candidate_count,
                    candidate_reconciled_count=resolution.candidate_reconciled_count,
                    candidate_status=resolution.candidate_status,
                    candidate_summary=resolution.candidate_summary,
                    lineage_conflict_summary=resolution.lineage_conflict_summary,
                    action="anchor_failed",
                    reason=resolution.reason
                    or "row is not eligible as a reconciled anchor",
                )
                return _reconciliation_result(
                    resolved_account=resolved_account,
                    prepared_bank=prepared_bank,
                    report_rows=report_rows,
                    anchor_streak=anchor_streak,
                    last_reconciled_exists=last_reconciled_exists,
                    ok=False,
                    anchor_type="last_reconciled_at",
                    anchor_window_start=anchor_start_index,
                    reason=(
                        "Could not establish reconciliation anchor because the selected streak "
                        f"contains an ineligible row at {int(bank_row['row_index'])}."
                    ),
                )
            action = "anchor_history"
            resolved_via = resolution.resolved_via
            resolved_transaction_id = resolution.resolved_transaction_id
            prior_cleared = resolution.prior_cleared
            reason = ""
            if resolution.matched_row is None:
                action = "anchor_history_legacy"
                resolved_via = "date_amount_reconciled"
                resolved_transaction_id = ""
                prior_cleared = "reconciled"
                reason = "unique reconciled date+amount anchor"
            report_rows[i] = _reconciliation_report_row(
                bank_row,
                resolved_transaction_id=resolved_transaction_id,
                resolved_via=resolved_via,
                prior_cleared=prior_cleared,
                candidate_count=resolution.candidate_count,
                candidate_reconciled_count=resolution.candidate_reconciled_count,
                candidate_status=resolution.candidate_status,
                candidate_summary=resolution.candidate_summary,
                lineage_conflict_summary=resolution.lineage_conflict_summary,
                replayed_balance_ils=round(float(bank_row["balance_ils"]), 2),
                balance_match=True,
                action=action,
                reason=reason,
            )
            anchor_balance = round(float(bank_row["balance_ils"]), 2)
            anchor_row_index = i
            if resolved_transaction_id:
                anchor_transaction_id = resolved_transaction_id
        anchor_type = "last_reconciled_at"
    else:
        starting_balance_txn = _starting_balance_transaction(ynab_rows)
        starting_balance_date = starting_balance_txn["date"]
        if earliest_bank_date != starting_balance_date:
            return _reconciliation_result(
                resolved_account=resolved_account,
                prepared_bank=prepared_bank,
                report_rows=report_rows,
                anchor_streak=anchor_streak,
                last_reconciled_exists=last_reconciled_exists,
                ok=False,
                anchor_type="starting_balance",
                reason=(
                    "Bank source must start on the starting balance date when last_reconciled_at is missing: "
                    f"{earliest_bank_date} != {starting_balance_date}."
                ),
            )
        anchor_type = "starting_balance"
        anchor_balance = round(float(starting_balance_txn["amount_ils"]), 2)
        anchor_window_start = 0
        anchor_transaction_id = _normalize_text(starting_balance_txn.get("id", ""))

    running_balance = anchor_balance
    for i in range(anchor_row_index + 1, len(prepared_bank)):
        bank_row = prepared_bank.row(i, named=True)
        resolution = resolutions[i]
        if resolution.matched_row is None:
            report_rows[i] = _reconciliation_report_row(
                bank_row,
                candidate_count=resolution.candidate_count,
                candidate_reconciled_count=resolution.candidate_reconciled_count,
                candidate_status=resolution.candidate_status,
                candidate_summary=resolution.candidate_summary,
                lineage_conflict_summary=resolution.lineage_conflict_summary,
                action="blocked",
                reason=resolution.reason,
            )
            return _reconciliation_result(
                resolved_account=resolved_account,
                prepared_bank=prepared_bank,
                report_rows=report_rows,
                anchor_streak=anchor_streak,
                last_reconciled_exists=last_reconciled_exists,
                ok=False,
                anchor_type=anchor_type,
                anchor_transaction_id=anchor_transaction_id,
                anchor_balance_ils=anchor_balance,
                anchor_window_start=anchor_window_start,
                reason=f"Could not reconcile row {int(bank_row['row_index'])}: {resolution.reason}",
            )

        running_balance = round(running_balance + float(bank_row["amount_ils"]), 2)
        balance_match = _same_balance(running_balance, float(bank_row["balance_ils"]))
        action = (
            "already_reconciled"
            if resolution.prior_cleared == "reconciled"
            else "reconcile"
        )
        report_rows[i] = _reconciliation_report_row(
            bank_row,
            resolved_transaction_id=resolution.resolved_transaction_id,
            resolved_via=resolution.resolved_via,
            prior_cleared=resolution.prior_cleared,
            candidate_count=resolution.candidate_count,
            candidate_reconciled_count=resolution.candidate_reconciled_count,
            candidate_status=resolution.candidate_status,
            candidate_summary=resolution.candidate_summary,
            lineage_conflict_summary=resolution.lineage_conflict_summary,
            replayed_balance_ils=running_balance,
            balance_match=balance_match,
            action=action if balance_match else "blocked",
            reason="" if balance_match else "running balance mismatch",
        )
        if not balance_match:
            return _reconciliation_result(
                resolved_account=resolved_account,
                prepared_bank=prepared_bank,
                report_rows=report_rows,
                anchor_streak=anchor_streak,
                last_reconciled_exists=last_reconciled_exists,
                ok=False,
                anchor_type=anchor_type,
                anchor_transaction_id=anchor_transaction_id,
                anchor_balance_ils=anchor_balance,
                anchor_window_start=anchor_window_start,
                reason=(
                    "Running balance mismatch at row "
                    f"{int(bank_row['row_index'])}: expected {float(bank_row['balance_ils']):.2f}, "
                    f"replayed {running_balance:.2f}."
                ),
            )

        if resolution.prior_cleared != "reconciled":
            updates.append(
                {"id": resolution.resolved_transaction_id, "cleared": "reconciled"}
            )

    final_bank_balance = round(float(prepared_bank.row(-1, named=True)["balance_ils"]), 2)
    if not _same_balance(running_balance, final_bank_balance):
        return _reconciliation_result(
            resolved_account=resolved_account,
            prepared_bank=prepared_bank,
            report_rows=report_rows,
            anchor_streak=anchor_streak,
            last_reconciled_exists=last_reconciled_exists,
            ok=False,
            reason=(
                f"Final balance mismatch: replayed {running_balance:.2f} vs bank {final_bank_balance:.2f}."
            ),
            anchor_type=anchor_type,
            anchor_transaction_id=anchor_transaction_id,
            anchor_balance_ils=anchor_balance,
            anchor_window_start=anchor_window_start,
            updates=[],
            final_balance_ils=final_bank_balance,
        )

    return _reconciliation_result(
        resolved_account=resolved_account,
        prepared_bank=prepared_bank,
        report_rows=report_rows,
        anchor_streak=anchor_streak,
        last_reconciled_exists=last_reconciled_exists,
        ok=True,
        anchor_type=anchor_type,
        anchor_transaction_id=anchor_transaction_id,
        anchor_balance_ils=anchor_balance,
        anchor_window_start=anchor_window_start,
        updates=updates,
        final_balance_ils=final_bank_balance,
    )
