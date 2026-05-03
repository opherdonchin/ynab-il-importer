from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from pathlib import Path
import re
from typing import Any

import pandas as pd  # kept for report DataFrame construction only
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


def _parse_date(value: Any) -> date | None:
    """Parse a value to datetime.date, returning None on failure."""
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


def _truncate_text(value: Any, limit: int = 80) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _normalize_match_text(value: Any) -> str:
    return normalize.normalize_text(card_identity.strip_card_txn_id_markers(value))


def _optional_source_expr(df: pl.DataFrame, name: str, *, dtype: pl.DataType) -> pl.Expr:
    if name in df.columns:
        return pl.col(name).cast(dtype, strict=False)
    return pl.lit(None, dtype=dtype).alias(name)


def _source_card_txn_id_variants(source_row: dict[str, Any]) -> list[str]:
    current = _normalize_text(source_row.get("card_txn_id", ""))
    source_account = _normalize_text(source_row.get("source_account", ""))
    suffix_match = re.search(r"(\d{4})", source_account)
    if suffix_match is None:
        return [current] if current else []

    aliases = card_identity.make_card_txn_id_aliases(
        source="card",
        source_account=source_account,
        card_suffix=suffix_match.group(1),
        date=source_row.get("date", ""),
        secondary_date=source_row.get("secondary_date", ""),
        outflow_ils=source_row.get("outflow_ils", 0.0),
        inflow_ils=source_row.get("inflow_ils", 0.0),
        description_raw=source_row.get("description_raw", ""),
        max_sheet=source_row.get("max_sheet", ""),
        max_txn_type=source_row.get("max_txn_type", ""),
        max_original_amount=source_row.get("max_original_amount"),
        max_original_currency=source_row.get("max_original_currency", ""),
    )
    if current and current not in aliases:
        return [current, *aliases]
    return aliases


def _row_identity_hash(row: dict[str, Any]) -> str:
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


def _compute_card_legacy_import_ids(df: pl.DataFrame) -> pl.Series:
    """Compute YNAB-format legacy import IDs: YNAB:<milliunits>:<date>:<1-indexed-occurrence>."""
    if df.is_empty():
        return pl.Series("legacy_import_id", [], dtype=pl.Utf8)

    work = df.with_row_index("_row_nr").with_columns(
        pl.col("account_name").fill_null("").str.strip_chars().alias("_account_key"),
        pl.col("date").cast(pl.Utf8).fill_null("").alias("_date_key"),
        pl.struct(
            ["account_name", "source_account", "date", "secondary_date",
             "outflow_ils", "inflow_ils", "fingerprint", "description_raw"]
        ).map_elements(
            lambda r: normalize.normalize_text(
                "|".join([
                    str(r.get("account_name") or ""),
                    str(r.get("source_account") or ""),
                    str(r.get("date") or ""),
                    str(r.get("secondary_date") or ""),
                    str(r.get("outflow_ils") or ""),
                    str(r.get("inflow_ils") or ""),
                    str(r.get("fingerprint") or ""),
                    str(r.get("description_raw") or ""),
                ])
            ),
            return_dtype=pl.Utf8,
        ).alias("_stable_key"),
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
        pl.concat_str([
            pl.lit("YNAB:"),
            pl.col("amount_milliunits").cast(pl.Utf8),
            pl.lit(":"),
            pl.col("_date_key"),
            pl.lit(":"),
            pl.col("_import_occurrence").cast(pl.Utf8),
        ]).alias("legacy_import_id")
    )
    return with_id.sort("_row_nr")["legacy_import_id"]


def load_card_source(path: str | Path) -> pl.DataFrame:
    source_path = Path(path)
    if source_path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Card reconciliation requires canonical parquet input, got: {source_path}"
        )
    return read_transactions_polars(source_path)


def _build_card_source_frame(df: pl.DataFrame, account_name: str) -> pl.DataFrame:
    work = (
        df.select(
            pl.col("account_name"),
            pl.col("source_account"),
            pl.col("transaction_id"),
            pl.col("date"),
            pl.col("secondary_date"),
            pl.col("outflow_ils"),
            pl.col("inflow_ils"),
            pl.col("signed_amount_ils"),
            pl.col("description_raw"),
            pl.col("fingerprint"),
            _optional_source_expr(df, "max_sheet", dtype=pl.Utf8),
            _optional_source_expr(df, "max_txn_type", dtype=pl.Utf8),
            _optional_source_expr(df, "max_original_amount", dtype=pl.Float64),
            _optional_source_expr(df, "max_original_currency", dtype=pl.Utf8),
        )
        .with_columns(
            pl.col("account_name").fill_null("").str.strip_chars(),
            pl.col("source_account").fill_null("").str.strip_chars(),
            pl.col("transaction_id").fill_null("").str.strip_chars().alias("card_txn_id"),
            pl.col("description_raw").fill_null("").str.strip_chars(),
            pl.col("fingerprint").fill_null("").str.strip_chars(),
            pl.col("max_sheet").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
            pl.col("max_txn_type").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
            pl.col("max_original_currency").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
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
    filtered = (
        filtered
        .with_columns(
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("secondary_date").cast(pl.Date, strict=False),
            pl.col("outflow_ils").cast(pl.Float64, strict=False).fill_null(0.0).round(2),
            pl.col("inflow_ils").cast(pl.Float64, strict=False).fill_null(0.0).round(2),
            pl.col("signed_amount_ils").cast(pl.Float64, strict=False).fill_null(0.0).round(2).alias("signed_ils"),
        )
        .filter(pl.col("signed_ils") != 0.0)
    )
    amount_milliunits = (filtered["signed_ils"] * 1000.0).round(0).cast(pl.Int64)
    legacy_ids = _compute_card_legacy_import_ids(
        filtered.with_columns(amount_milliunits.alias("amount_milliunits"))
    )
    description_match = filtered["description_raw"].map_elements(
        _normalize_match_text, return_dtype=pl.Utf8
    )
    return (
        filtered
        .with_columns(
            amount_milliunits.alias("amount_milliunits"),
            legacy_ids.alias("legacy_import_id"),
            description_match.alias("description_match"),
        )
        .with_row_index("row_index")
    )


def _parse_date_range_bound(value: Any, *, field_name: str) -> date | None:
    text = _normalize_text(value)
    if not text:
        return None
    parsed = _parse_date(text)
    if parsed is None:
        raise ValueError(
            f"Invalid {field_name} value {value!r}; expected YYYY-MM-DD."
        )
    return parsed


def _filter_rows_by_date_range(
    rows: pl.DataFrame,
    *,
    date_from: Any = None,
    date_to: Any = None,
    range_name: str,
    allow_empty: bool = True,
) -> tuple[pl.DataFrame, int]:
    parsed_from = _parse_date_range_bound(date_from, field_name=f"{range_name}_date_from")
    parsed_to = _parse_date_range_bound(date_to, field_name=f"{range_name}_date_to")
    if parsed_from is None and parsed_to is None:
        return rows, 0
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        raise ValueError(
            f"{range_name}_date_from {parsed_from} is after {range_name}_date_to {parsed_to}."
        )
    if rows.is_empty():
        if allow_empty:
            return rows, 0
        raise ValueError(
            f"No {range_name} rows are available for filtering in the requested account."
        )

    mask = pl.lit(True)
    if parsed_from is not None:
        mask = mask & (pl.col("date") >= pl.lit(parsed_from))
    if parsed_to is not None:
        mask = mask & (pl.col("date") <= pl.lit(parsed_to))
    filtered = rows.filter(mask)
    dropped_count = len(rows) - len(filtered)

    if filtered.is_empty() and not allow_empty:
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


def _build_card_ynab_rows(
    transactions: list[dict[str, Any]],
    *,
    account_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        if bool(txn.get("deleted", False)):
            continue
        account_id = _normalize_text(txn.get("account_id", ""))
        amount_milliunits = int(txn.get("amount", 0) or 0)
        parsed_date = _parse_date(txn.get("date", ""))
        rows.append(
            {
                "id": _normalize_text(txn.get("id", "")),
                "account_id": account_id,
                "account_name": _normalize_text(
                    (account_names or {}).get(account_id, "")
                ),
                "date": parsed_date,
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
    return rows


def _build_card_account_ynab_rows(
    transactions: list[dict[str, Any]],
    *,
    account_id: str,
    account_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    all_rows = _build_card_ynab_rows(transactions, account_names=account_names)
    return [r for r in all_rows if r["account_id"] == account_id]


def _card_lineage_maps(
    ynab_rows: list[dict[str, Any]],
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    import_map: dict[str, list[int]] = {}
    memo_map: dict[str, list[int]] = {}
    for idx, row in enumerate(ynab_rows):
        import_id = _normalize_text(row.get("import_id", ""))
        if card_identity.is_card_txn_id(import_id):
            import_map.setdefault(import_id, []).append(idx)
        memo_card_txn_id = _normalize_text(row.get("card_txn_id_marker", ""))
        if memo_card_txn_id:
            memo_map.setdefault(memo_card_txn_id, []).append(idx)
    return import_map, memo_map


def _card_date_amount_candidates_for_field(
    source_row: dict[str, Any], ynab_rows: list[dict[str, Any]], *, date_field: str
) -> list[dict[str, Any]]:
    target_date = source_row.get(date_field)
    if target_date is None:
        return []
    signed_ils = float(source_row.get("signed_ils", 0.0) or 0.0)
    return [
        r for r in ynab_rows
        if r.get("date") == target_date
        and abs(float(r.get("signed_ils", 0.0) or 0.0) - signed_ils) < 0.001
    ]


def _card_date_amount_candidates(
    source_row: dict[str, Any], ynab_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return _card_date_amount_candidates_for_field(source_row, ynab_rows, date_field="date")


def _card_secondary_date_amount_candidates(
    source_row: dict[str, Any], ynab_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    primary = source_row.get("date")
    secondary = source_row.get("secondary_date")
    if secondary is None:
        return []
    if primary is not None and primary == secondary:
        return []
    return _card_date_amount_candidates_for_field(
        source_row, ynab_rows, date_field="secondary_date"
    )


def _card_unlinked_candidates(
    candidates: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return [
        r for r in candidates
        if not r.get("card_txn_id_marker", "").strip()
        and not card_identity.is_card_txn_id(r.get("import_id", ""))
    ]


def _summarize_card_candidate(row: dict[str, Any]) -> str:
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


def _summarize_card_candidate_rows(candidates: list[dict[str, Any]]) -> str:
    if not candidates:
        return ""
    ordered = sorted(
        candidates,
        key=lambda r: (r.get("date") or date.min, float(r.get("signed_ils", 0.0) or 0.0), r.get("id", "")),
    )
    return " || ".join(_summarize_card_candidate(r) for r in ordered)


def _card_lineage_conflict_summary(
    source_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> str:
    card_txn_id = source_row.get("card_txn_id", "")
    parts: list[str] = []
    import_hits = import_map.get(card_txn_id, [])
    if len(import_hits) == 1:
        parts.append(
            f"import_id -> {_summarize_card_candidate(ynab_rows[import_hits[0]])}"
        )
    memo_hits = memo_map.get(card_txn_id, [])
    if len(memo_hits) == 1:
        parts.append(
            f"memo_marker -> {_summarize_card_candidate(ynab_rows[memo_hits[0]])}"
        )
    return " || ".join(parts)


def _resolve_exact_card_lineage(
    source_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[dict[str, Any] | None, str, str]:
    card_txn_ids = _source_card_txn_id_variants(source_row)
    source_date = source_row.get("date")
    source_amount = round(float(source_row.get("signed_ils", 0.0) or 0.0), 2)
    mismatch_reasons: list[str] = []

    for index, card_txn_id in enumerate(card_txn_ids):
        via_suffix = "" if index == 0 else "_alias"
        import_hits = import_map.get(card_txn_id, [])
        if len(import_hits) > 1:
            return None, "", f"duplicate YNAB import_id matches for {card_txn_id}"
        if len(import_hits) == 1:
            candidate = ynab_rows[import_hits[0]]
            if (
                candidate.get("date") == source_date
                and abs(float(candidate.get("signed_ils", 0.0) or 0.0) - source_amount) < 0.001
            ):
                return candidate, f"import_id{via_suffix}", ""
            mismatch_reasons.append(
                "card_txn_id import_id is attached to a YNAB transaction with different date/amount"
            )

        memo_hits = memo_map.get(card_txn_id, [])
        if len(memo_hits) > 1:
            return None, "", f"duplicate YNAB memo markers for {card_txn_id}"
        if len(memo_hits) == 1:
            candidate = ynab_rows[memo_hits[0]]
            if (
                candidate.get("date") == source_date
                and abs(float(candidate.get("signed_ils", 0.0) or 0.0) - source_amount) < 0.001
            ):
                return candidate, f"memo_marker{via_suffix}", ""
            mismatch_reasons.append(
                "card_txn_id memo marker is attached to a YNAB transaction with different date/amount"
            )

    legacy_import_id = _normalize_text(source_row.get("legacy_import_id", ""))
    if legacy_import_id:
        legacy_hits = [
            i for i, r in enumerate(ynab_rows) if r.get("import_id") == legacy_import_id
        ]
        if len(legacy_hits) > 1:
            return (
                None,
                "",
                f"duplicate YNAB legacy import_id matches for {legacy_import_id}",
            )
        if len(legacy_hits) == 1:
            candidate = ynab_rows[legacy_hits[0]]
            if (
                candidate.get("date") == source_date
                and abs(float(candidate.get("signed_ils", 0.0) or 0.0) - source_amount) < 0.001
            ):
                return candidate, "legacy_import_id", ""
            mismatch_reasons.append(
                "legacy import_id is attached to a YNAB transaction with different date/amount"
            )

    if mismatch_reasons:
        return None, "", "; ".join(mismatch_reasons)
    return None, "", "no exact lineage match"


def _card_candidate_diagnostics(
    source_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[int, int, str, str, str]:
    lineage_conflict = _card_lineage_conflict_summary(
        source_row, ynab_rows, import_map, memo_map
    )

    def _diagnose(candidates: list[dict[str, Any]], *, prefix: str = "") -> tuple[int, int, str, str]:
        candidate_count = len(candidates)
        candidate_reconciled_count = (
            sum(1 for r in candidates if r.get("cleared") == "reconciled")
            if candidate_count else 0
        )
        candidate_summary = _summarize_card_candidate_rows(candidates)
        if candidate_count == 0:
            return 0, 0, "", candidate_summary

        unlinked = _card_unlinked_candidates(candidates)
        if not unlinked:
            return (
                candidate_count,
                candidate_reconciled_count,
                f"only_linked_{prefix}date_amount_candidates",
                candidate_summary,
            )

        memo_exact = [r for r in unlinked if r.get("memo_match") == source_row.get("description_match")]
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

    primary = _diagnose(_card_date_amount_candidates(source_row, ynab_rows))
    if primary[0] > 0:
        return primary + (lineage_conflict,)

    secondary = _diagnose(
        _card_secondary_date_amount_candidates(source_row, ynab_rows),
        prefix="secondary_",
    )
    if secondary[0] > 0:
        return secondary + (lineage_conflict,)
    return 0, 0, "no_date_amount_match", "", lineage_conflict


def _card_sync_fallback_candidate(
    source_row: dict[str, Any],
    ynab_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str, str]:
    def _resolve_candidates(
        candidates: list[dict[str, Any]], *, resolved_via: str, empty_reason: str, many_reason: str
    ) -> tuple[dict[str, Any] | None, str, str]:
        memo_exact = [r for r in candidates if r.get("memo_match") == source_row.get("description_match")]
        if len(memo_exact) == 1:
            return memo_exact[0], f"{resolved_via}_memo_exact", ""
        if len(memo_exact) > 1:
            return None, "", many_reason
        if len(candidates) == 1:
            return candidates[0], resolved_via, ""
        if not candidates:
            return None, "", empty_reason
        return None, "", many_reason

    primary = _resolve_candidates(
        _card_unlinked_candidates(_card_date_amount_candidates(source_row, ynab_rows)),
        resolved_via="date_amount_unique",
        empty_reason="no unique same-date/same-amount candidate",
        many_reason="multiple same-date/same-amount candidates",
    )
    if primary[0] is not None or primary[2] != "no unique same-date/same-amount candidate":
        return primary

    return _resolve_candidates(
        _card_unlinked_candidates(_card_secondary_date_amount_candidates(source_row, ynab_rows)),
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
    ynab_row: dict[str, Any] | None
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


def _expected_statement_date(previous_rows: pl.DataFrame) -> date | None:
    if previous_rows.is_empty() or "secondary_date" not in previous_rows.columns:
        return None
    max_val = previous_rows["secondary_date"].drop_nulls().max()
    return max_val  # already a date | None in Polars


def _identify_billing_date_groups(
    previous_rows: pl.DataFrame,
) -> tuple[date | None, pl.DataFrame, pl.DataFrame]:
    """Split previous rows into main billing date and separately-settled groups.

    The main billing date is the latest secondary_date, which corresponds to the
    monthly bundled payment transfer from the bank. Rows with earlier secondary_dates
    were settled individually by the card company and do not roll into the monthly
    payment transfer.

    Returns (main_billing_date, main_rows, sep_rows).
    sep_rows is empty when all rows share the same secondary_date.
    """
    if "secondary_date" not in previous_rows.columns or previous_rows.is_empty():
        return None, previous_rows, pl.DataFrame(schema=previous_rows.schema)
    main_date = previous_rows["secondary_date"].drop_nulls().max()
    if main_date is None:
        return None, previous_rows, pl.DataFrame(schema=previous_rows.schema)
    main_rows = previous_rows.filter(pl.col("secondary_date") == pl.lit(main_date))
    sep_rows = previous_rows.filter(
        pl.col("secondary_date").is_null() | (pl.col("secondary_date") != pl.lit(main_date))
    )
    return main_date, main_rows, sep_rows


def _validate_payment_transfer(
    *,
    previous_rows: pl.DataFrame,
    all_ynab_rows: list[dict[str, Any]],
    card_account_id: str,
    account_names: dict[str, str],
) -> _PaymentTransferMatch:
    expected_total = round(abs(float(previous_rows["signed_ils"].sum())), 2)
    expected_milliunits = int(round(expected_total * 1000))
    statement_date = _expected_statement_date(previous_rows)

    candidates = [
        r for r in all_ynab_rows
        if r.get("account_id") == card_account_id
        and r.get("transfer_account_id", "")
        and r.get("amount_milliunits") == expected_milliunits
    ]
    if not candidates:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"No card payment transfer found for previous total {expected_total:.2f} ILS.",
        )

    if statement_date is not None:
        windowed = [
            r for r in candidates
            if r.get("date") is not None
            and abs((r["date"] - statement_date).days) <= 7
        ]
        if windowed:
            candidates = windowed

    if len(candidates) != 1:
        return _PaymentTransferMatch(
            ok=False,
            reason=(
                f"Expected exactly one card payment transfer for previous total {expected_total:.2f} ILS; "
                f"found {len(candidates)}."
            ),
        )

    card_txn = candidates[0]
    bank_txn_id = _normalize_text(card_txn.get("transfer_transaction_id", ""))
    if not bank_txn_id:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"Card payment transfer {card_txn['id']} has no linked bank transfer transaction.",
        )

    bank_candidates = [r for r in all_ynab_rows if r.get("id") == bank_txn_id]
    if len(bank_candidates) != 1:
        return _PaymentTransferMatch(
            ok=False,
            reason=f"Linked bank transfer {bank_txn_id} was not found in YNAB transactions.",
        )

    bank_txn = bank_candidates[0]
    if int(bank_txn.get("amount_milliunits", 0) or 0) != -expected_milliunits:
        return _PaymentTransferMatch(
            ok=False,
            reason=(
                f"Linked bank transfer amount {bank_txn.get('signed_ils', 0.0):.2f} ILS does not match "
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
        card_amount_ils=round(float(card_txn.get("signed_ils", 0.0) or 0.0), 2),
        bank_transaction_id=bank_txn_id,
        bank_account_id=bank_account_id,
        bank_account_name=_normalize_text(account_names.get(bank_account_id, "")),
        bank_date=_normalize_text(bank_txn.get("date", "")),
        bank_amount_ils=round(float(bank_txn.get("signed_ils", 0.0) or 0.0), 2),
    )


def _resolve_card_match(source_row: dict[str, Any], ynab_rows: list[dict[str, Any]]) -> _ResolvedMatch:
    if not ynab_rows:
        return _ResolvedMatch(
            None, "", "no_candidates", "no YNAB transaction candidates"
        )

    for index, card_txn_id in enumerate(_source_card_txn_id_variants(source_row)):
        via_suffix = "" if index == 0 else "_alias"
        import_hits = [i for i, r in enumerate(ynab_rows) if r.get("import_id") == card_txn_id]
        if len(import_hits) == 1:
            return _ResolvedMatch(
                ynab_rows[import_hits[0]], f"import_id{via_suffix}", "exact_lineage", ""
            )
        if len(import_hits) > 1:
            return _ResolvedMatch(
                None,
                "",
                "duplicate_import_id",
                f"duplicate YNAB import_id matches for {card_txn_id}",
            )

        memo_hits = [i for i, r in enumerate(ynab_rows) if r.get("card_txn_id_marker") == card_txn_id]
        if len(memo_hits) == 1:
            return _ResolvedMatch(
                ynab_rows[memo_hits[0]], f"memo_marker{via_suffix}", "exact_lineage", ""
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
        legacy_hits = [i for i, r in enumerate(ynab_rows) if r.get("import_id") == legacy_import_id]
        if len(legacy_hits) == 1:
            candidate = ynab_rows[legacy_hits[0]]
            if (
                candidate.get("date") == source_row.get("date")
                and abs(float(candidate.get("signed_ils", 0.0) or 0.0) - float(source_row.get("signed_ils", 0.0) or 0.0)) < 0.001
            ):
                return _ResolvedMatch(
                    candidate, "legacy_import_id", "legacy_import_id", ""
                )

    same_key = _card_date_amount_candidates(source_row, ynab_rows)
    if not same_key:
        secondary_key = _card_secondary_date_amount_candidates(source_row, ynab_rows)
        if not secondary_key:
            return _ResolvedMatch(
                None,
                "",
                "no_date_amount_match",
                "no date/amount or billing-date/amount match",
            )

        memo_exact = [r for r in secondary_key if r.get("memo_match") == source_row.get("description_match")]
        if len(memo_exact) == 1:
            return _ResolvedMatch(
                memo_exact[0],
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

    memo_exact = [r for r in same_key if r.get("memo_match") == source_row.get("description_match")]
    if len(memo_exact) == 1:
        return _ResolvedMatch(
            memo_exact[0], "memo_exact", "unique_memo_exact_candidate", ""
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


def _row_report(source_row: dict[str, Any], snapshot_role: str) -> dict[str, object]:
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
        "outflow_ils": float(source_row.get("outflow_ils") or 0.0),
        "inflow_ils": float(source_row.get("inflow_ils") or 0.0),
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
    ynab_rows: list[dict[str, Any]], first_source_date: date | None
) -> list[dict[str, Any]]:
    if first_source_date is None:
        return []
    return [
        r for r in ynab_rows
        if r.get("cleared") == "cleared"
        and r.get("date") is not None
        and r["date"] < first_source_date
    ]


def _apply_updates_for_rows(
    rows: list[dict[str, Any]], *, target_cleared: str
) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for row in rows:
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
    source_df: pl.DataFrame, ynab_rows: list[dict[str, Any]], *, snapshot_role: str
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source_row in source_df.iter_rows(named=True):
        report_row = _row_report(source_row, snapshot_role)
        resolved = _resolve_card_match(source_row, ynab_rows)
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
                float(resolved.ynab_row.get("signed_ils", 0.0) or 0.0), 2
            )
            report_row["prior_cleared"] = _normalize_text(
                resolved.ynab_row.get("cleared", "")
            )
            report_row["action"] = "matched"
        else:
            report_row["action"] = "blocked"
        rows.append(report_row)
    return rows


def _card_sync_report_row(
    source_row: dict[str, Any],
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
        "outflow_ils": float(source_row.get("outflow_ils") or 0.0),
        "inflow_ils": float(source_row.get("inflow_ils") or 0.0),
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
    source_rows = _build_card_source_frame(source_df, account_name)
    source_rows, source_filtered_out_count = _filter_rows_by_date_range(
        source_rows,
        date_from=source_date_from,
        date_to=source_date_to,
        range_name="source",
        allow_empty=False,
    )
    ynab_rows = _build_card_account_ynab_rows(
        transactions, account_id=account["account_id"]
    )
    if not ynab_rows:
        raise ValueError(
            f"No live YNAB transactions found for account {account_name!r}."
        )

    import_map, memo_map = _card_lineage_maps(ynab_rows)
    report_rows: list[dict[str, object]] = []
    updates: list[dict[str, str]] = []

    for source_row in source_rows.iter_rows(named=True):
        matched, resolved_via, reason = _resolve_exact_card_lineage(
            source_row, ynab_rows, import_map, memo_map
        )
        (
            candidate_count,
            candidate_reconciled_count,
            candidate_status,
            candidate_summary,
            lineage_conflict_summary,
        ) = _card_candidate_diagnostics(source_row, ynab_rows, import_map, memo_map)

        if matched is None and reason == "no exact lineage match":
            matched, fallback_via, fallback_reason = _card_sync_fallback_candidate(
                source_row, ynab_rows
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
    source_rows = _build_card_source_frame(source_df, account_name)
    source_rows, source_filtered_out_count = _filter_rows_by_date_range(
        source_rows,
        date_from=source_date_from,
        date_to=source_date_to,
        range_name="source",
        allow_empty=False,
    )
    if source_rows.is_empty():
        raise ValueError(
            "Source file has no in-scope non-pending rows for the requested account."
        )
    previous_filtered_out_count = 0
    previous_rows: pl.DataFrame = (
        _build_card_source_frame(previous_df, account_name)
        if previous_df is not None
        else pl.DataFrame()
    )
    if previous_df is not None:
        previous_rows, previous_filtered_out_count = _filter_rows_by_date_range(
            previous_rows,
            date_from=previous_date_from,
            date_to=previous_date_to,
            range_name="previous",
            allow_empty=False,
        )

    all_ynab_rows = _build_card_ynab_rows(
        transactions, account_names=account_names
    )
    ynab_rows = [r for r in all_ynab_rows if r["account_id"] == account["account_id"]]
    if not ynab_rows:
        raise ValueError(
            f"No live YNAB transactions found for account {account_name!r}."
        )

    mode = "transition" if previous_df is not None else "source_only"
    source_report = _evaluate_snapshot_rows(
        source_rows, ynab_rows, snapshot_role="source"
    )
    previous_report: list[dict[str, object]] = (
        _evaluate_snapshot_rows(previous_rows, ynab_rows, snapshot_role="previous")
        if previous_df is not None
        else []
    )
    report = pd.DataFrame(previous_report + source_report)

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
        "matched_source_count": int(
            sum(1 for r in source_report if r.get("ynab_transaction_id"))
        ),
        "matched_previous_count": (
            int(sum(1 for r in previous_report if r.get("ynab_transaction_id")))
            if previous_df is not None
            else 0
        ),
        "matched_source_total_ils": round(
            sum(float(r.get("ynab_signed_ils") or 0.0) for r in source_report), 2
        ),
        "matched_previous_total_ils": (
            round(
                sum(float(r.get("ynab_signed_ils") or 0.0) for r in previous_report), 2
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

    if report.empty or not (report["action"] == "blocked").any():
        pass
    elif (report["action"] == "blocked").any():
        result["ok"] = False
        blocked = report[report["action"] == "blocked"].copy()
        first = blocked.iloc[0]
        result["reason"] = (
            f"{int(len(blocked))} rows could not be matched exactly; "
            f"first blocked {first['snapshot_role']} row {int(first['row_index'])}: {first['reason']}"
        )
        return result

    source_reconciled = [r for r in source_report if r.get("prior_cleared") == "reconciled"]
    if source_reconciled and not allow_reconciled_source:
        result["ok"] = False
        first = source_reconciled[0]
        result["reason"] = (
            "Source file contains transactions that are already reconciled; "
            f"first source row {int(first['row_index'])} is already reconciled. "
            "Pass the older settled file as --previous and a newer current file as --source."
        )
        return result
    if source_reconciled and allow_reconciled_source:
        report.loc[
            (report["snapshot_role"] == "source")
            & (report["prior_cleared"] == "reconciled"),
            "action",
        ] = "already_reconciled"
        result["warning"] = (
            f"{len(source_reconciled)} source rows are already reconciled and will be skipped."
        )

    if mode == "source_only":
        older_open = _summarize_open_older_rows(ynab_rows, source_rows["date"].min())
        if older_open:
            result["ok"] = False
            first = older_open[0]
            result["reason"] = (
                "Older cleared-but-unreconciled transactions exist before the first source row; "
                f"first older open row is {first['date']} amount {first['signed_ils']:.2f}. "
                "Provide --previous for month-transition reconciliation."
            )
            return result

        report.loc[
            (report["snapshot_role"] == "source")
            & (report["prior_cleared"] == "cleared"),
            "action",
        ] = "keep_cleared"
        to_clear = [r for r in source_report if r.get("prior_cleared") == "uncleared"]
        for row in to_clear:
            report.loc[
                (report["snapshot_role"] == "source")
                & (report["row_index"] == row["row_index"]),
                "action",
            ] = "clear"
        result["updates"] = _apply_updates_for_rows(to_clear, target_cleared="cleared")
        result["update_count"] = len(result["updates"])
        result["report"] = report
        return result

    previous_reconciled = [r for r in previous_report if r.get("prior_cleared") == "reconciled"]

    previous_total = round(float(previous_rows["signed_ils"].sum()), 2)
    current_total = round(float(source_rows["signed_ils"].sum()), 2)
    result["previous_total_ils"] = previous_total
    result["source_total_ils"] = current_total

    _, main_previous_rows, sep_previous_rows = _identify_billing_date_groups(
        previous_rows
    )
    result["separately_settled_count"] = int(len(sep_previous_rows))
    result["separately_settled_dates"] = (
        sorted(
            str(d)
            for d in sep_previous_rows["secondary_date"].drop_nulls().unique().to_list()
        )
        if not sep_previous_rows.is_empty()
        else []
    )
    payment_match = _validate_payment_transfer(
        previous_rows=main_previous_rows,
        all_ynab_rows=all_ynab_rows,
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
    previous_to_reconcile = [r for r in previous_report if r.get("prior_cleared") != "reconciled"]
    source_to_clear = [r for r in source_report if r.get("prior_cleared") == "uncleared"]
    if previous_report:
        if previous_all_reconciled:
            result["warning"] = "All previous-file transactions are already reconciled."
            report.loc[report["snapshot_role"] == "previous", "action"] = (
                "already_reconciled"
            )
        else:
            report.loc[report["snapshot_role"] == "previous", "action"] = "reconcile"
            if previous_reconciled:
                report.loc[
                    (report["snapshot_role"] == "previous")
                    & (report["prior_cleared"] == "reconciled"),
                    "action",
                ] = "already_reconciled"
                result["warning"] = (
                    f"{len(previous_reconciled)} previous-file rows are already reconciled; "
                    f"reconciling remaining {len(previous_to_reconcile)} rows."
                )
            if not sep_previous_rows.is_empty():
                sep_date_strs = set(
                    str(d)
                    for d in sep_previous_rows["secondary_date"].drop_nulls().unique().to_list()
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
    transfer_rows = [r for r in all_ynab_rows if r.get("id") == card_transfer_id] if card_transfer_id else []
    if transfer_rows:
        transfer_to_reconcile = [
            {**r, "ynab_transaction_id": r["id"], "prior_cleared": r["cleared"]}
            for r in transfer_rows
            if r.get("cleared") != "reconciled"
        ]
        updates.extend(
            _apply_updates_for_rows(transfer_to_reconcile, target_cleared="reconciled")
        )

    result["updates"] = updates
    result["update_count"] = len(updates)
    result["report"] = report
    return result
