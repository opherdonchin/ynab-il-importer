from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

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

RECONCILIATION_REPORT_COLUMNS = [
    "row_index",
    "date",
    "secondary_date",
    "outflow_ils",
    "inflow_ils",
    "balance_ils",
    "bank_txn_id",
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
    matched_row: pd.Series | None
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


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _truncate_text(value: Any, limit: int = 80) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _coerce_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _coerce_money_series(
    series: pd.Series, *, allow_missing: bool = False
) -> pd.Series:
    converted = pd.to_numeric(series, errors="coerce")
    if allow_missing:
        return converted.round(2)
    return converted.fillna(0.0).round(2)


def _amount_milliunits(outflow_ils: Any, inflow_ils: Any) -> int:
    return bank_identity.signed_amount_milliunits(outflow_ils, inflow_ils)


def _amount_ils(outflow_ils: Any, inflow_ils: Any) -> float:
    return round(_amount_milliunits(outflow_ils, inflow_ils) / 1000.0, 2)


def _same_balance(left: float, right: float) -> bool:
    return abs(round(float(left), 2) - round(float(right), 2)) <= 0.005


def _active_accounts(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [account for account in accounts if not bool(account.get("deleted", False))]


def _load_bank_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(Path(path))
    return _prepare_bank_dataframe(df)


def _prepare_bank_dataframe(bank_df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "account_name",
        "date",
        "outflow_ils",
        "inflow_ils",
        "bank_txn_id",
    ]
    missing = [column for column in required if column not in bank_df.columns]
    if missing:
        raise ValueError(f"Bank CSV missing required columns: {missing}")

    prepared = bank_df.copy().reset_index(drop=True)
    prepared["row_index"] = prepared.index
    prepared["account_name"] = _normalize_text_series(prepared["account_name"])
    if "ynab_account_id" in prepared.columns:
        prepared["ynab_account_id"] = _normalize_text_series(
            prepared["ynab_account_id"]
        )
    else:
        prepared["ynab_account_id"] = ""
    prepared["source_account"] = _normalize_text_series(
        prepared.get(
            "source_account", pd.Series([""] * len(prepared), index=prepared.index)
        )
    )
    prepared["secondary_date"] = _coerce_date_series(
        prepared.get(
            "secondary_date", pd.Series([None] * len(prepared), index=prepared.index)
        )
    )
    prepared["date"] = _coerce_date_series(prepared["date"])
    prepared["outflow_ils"] = _coerce_money_series(prepared["outflow_ils"])
    prepared["inflow_ils"] = _coerce_money_series(prepared["inflow_ils"])
    prepared["balance_ils"] = _coerce_money_series(
        prepared.get(
            "balance_ils", pd.Series([pd.NA] * len(prepared), index=prepared.index)
        ),
        allow_missing=True,
    )
    prepared["description_raw"] = _normalize_text_series(
        prepared.get(
            "description_raw",
            prepared.get("memo", pd.Series([""] * len(prepared), index=prepared.index)),
        )
    )
    prepared["ref"] = _normalize_text_series(
        prepared.get("ref", pd.Series([""] * len(prepared), index=prepared.index))
    )
    prepared["bank_txn_id"] = prepared["bank_txn_id"].map(
        bank_identity.validate_bank_txn_id
    )
    prepared["amount_milliunits"] = prepared.apply(
        lambda row: _amount_milliunits(row["outflow_ils"], row["inflow_ils"]),
        axis=1,
    )
    prepared["amount_ils"] = prepared["amount_milliunits"].div(1000.0).round(2)
    prepared["description_match_key"] = prepared["description_raw"].map(
        bank_identity.normalize_bank_memo_match_text
    )
    prepared["fingerprint_match_key"] = _normalize_text_series(
        prepared.get(
            "fingerprint", pd.Series([""] * len(prepared), index=prepared.index)
        )
    ).map(normalize.normalize_text)
    return prepared


def _prepare_ynab_transactions(transactions: list[dict[str, Any]]) -> pd.DataFrame:
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
        parsed_date = pd.to_datetime(txn.get("date", ""), errors="coerce")
        rows.append(
            {
                "id": _normalize_text(txn.get("id", "")),
                "account_id": _normalize_text(txn.get("account_id", "")),
                "date": parsed_date.date() if not pd.isna(parsed_date) else pd.NaT,
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
    return pd.DataFrame(rows)


def _resolve_account(
    bank_df: pd.DataFrame, accounts: list[dict[str, Any]]
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
            value
            for value in bank_df["ynab_account_id"]
            .astype("string")
            .fillna("")
            .str.strip()
            .tolist()
            if value
        }
    )
    if len(mapped_ids) > 1:
        raise ValueError(
            f"Bank CSV resolves to multiple ynab_account_id values: {mapped_ids}"
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
            value
            for value in bank_df["account_name"]
            .astype("string")
            .fillna("")
            .str.strip()
            .tolist()
            if value
        }
    )
    if len(account_names) != 1:
        raise ValueError(
            "Bank CSV must resolve to exactly one account_name when ynab_account_id is absent."
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
    ynab_df: pd.DataFrame,
    account_id: str,
) -> pd.DataFrame:
    if ynab_df.empty:
        return ynab_df.copy()
    filtered = (
        ynab_df[ynab_df["account_id"] == account_id].copy().reset_index(drop=True)
    )
    filtered["row_index"] = filtered.index
    return filtered


def _lineage_maps(
    ynab_df: pd.DataFrame,
) -> tuple[dict[str, list[int]], dict[str, list[int]], dict[str, list[int]]]:
    import_map: dict[str, list[int]] = {}
    memo_map: dict[str, list[int]] = {}
    ref_map: dict[str, list[int]] = {}
    for idx, row in ynab_df.iterrows():
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


def _resolve_exact_lineage(
    bank_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
    ref_map: dict[str, list[int]] | None = None,
) -> tuple[pd.Series | None, str, str]:
    bank_txn_id = bank_row["bank_txn_id"]
    bank_date = bank_row["date"]
    bank_amount = int(bank_row["amount_milliunits"])
    bank_ref = _normalize_text(bank_row.get("ref", ""))
    mismatch_reasons: list[str] = []

    import_hits = import_map.get(bank_txn_id, [])
    if len(import_hits) > 1:
        return None, "", f"duplicate YNAB import_id matches for {bank_txn_id}"
    if len(import_hits) == 1:
        candidate = ynab_df.loc[import_hits[0]]
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
        candidate = ynab_df.loc[memo_hits[0]]
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
            candidate = ynab_df.loc[ref_hits[0]]
            candidate_date = candidate.get("date")
            candidate_amount = int(candidate.get("amount_milliunits", 0) or 0)
            if candidate_date == bank_date and candidate_amount == bank_amount:
                return candidate, "memo_ref", ""
            mismatch_reasons.append(
                "memo ref marker is attached to a YNAB transaction with different date/amount"
            )

    if mismatch_reasons:
        return None, "", "; ".join(mismatch_reasons)
    return None, "", "no exact lineage match"


def _date_amount_candidates(bank_row: pd.Series, ynab_df: pd.DataFrame) -> pd.DataFrame:
    candidates = ynab_df.copy()
    candidates = candidates[candidates["date"] == bank_row["date"]]
    candidates = candidates[
        candidates["amount_milliunits"] == bank_row["amount_milliunits"]
    ]
    return candidates


def _unlinked_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    unlinked = candidates[
        candidates["memo_bank_txn_id"].astype("string").fillna("").str.strip() == ""
    ].copy()
    if unlinked.empty:
        return unlinked
    has_bank_import_id = unlinked["import_id"].map(bank_identity.is_bank_txn_id)
    return unlinked.loc[~has_bank_import_id].copy()


def _summarize_ynab_candidate(row: pd.Series) -> str:
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


def _summarize_candidate_rows(candidates: pd.DataFrame) -> str:
    if candidates.empty:
        return ""
    ordered = candidates.sort_values(
        ["date", "amount_milliunits", "id"],
        na_position="last",
    )
    return " || ".join(_summarize_ynab_candidate(row) for _, row in ordered.iterrows())


def _lineage_conflict_summary(
    bank_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> str:
    bank_txn_id = bank_row["bank_txn_id"]
    parts: list[str] = []
    import_hits = import_map.get(bank_txn_id, [])
    if len(import_hits) == 1:
        parts.append(
            f"import_id -> {_summarize_ynab_candidate(ynab_df.loc[import_hits[0]])}"
        )
    memo_hits = memo_map.get(bank_txn_id, [])
    if len(memo_hits) == 1:
        parts.append(
            f"memo_marker -> {_summarize_ynab_candidate(ynab_df.loc[memo_hits[0]])}"
        )
    return " || ".join(parts)


def _candidate_diagnostics(
    bank_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[int, int, str, str, str]:
    candidates = _date_amount_candidates(bank_row, ynab_df)
    candidate_count = int(len(candidates))
    candidate_reconciled_count = (
        int((candidates["cleared"] == "reconciled").sum()) if candidate_count else 0
    )
    candidate_summary = _summarize_candidate_rows(candidates)
    lineage_conflict = _lineage_conflict_summary(
        bank_row, ynab_df, import_map, memo_map
    )
    if candidate_count == 0:
        return 0, 0, "no_date_amount_match", candidate_summary, lineage_conflict

    unlinked = _unlinked_candidates(candidates)
    if unlinked.empty:
        return (
            candidate_count,
            candidate_reconciled_count,
            "only_linked_date_amount_candidates",
            candidate_summary,
            lineage_conflict,
        )

    memo_exact = unlinked[
        unlinked["memo_match_key"] == bank_row["description_match_key"]
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
        payee_exact = unlinked[unlinked["payee_match_key"] == fingerprint_match_key]
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
        cleared = _normalize_text(unlinked.iloc[0].get("cleared", ""))
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

    reconciled_unlinked_count = int((unlinked["cleared"] == "reconciled").sum())
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


def _memo_exact_fallback_candidate(
    bank_row: pd.Series, ynab_df: pd.DataFrame
) -> tuple[pd.Series | None, str]:
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_df))
    candidates = candidates[
        candidates["memo_match_key"] == bank_row["description_match_key"]
    ]

    if candidates.empty:
        return None, "no conservative memo match"
    if len(candidates) > 1:
        return None, "ambiguous conservative memo match"
    return candidates.iloc[0], ""


def _payee_exact_fallback_candidate(
    bank_row: pd.Series, ynab_df: pd.DataFrame
) -> tuple[pd.Series | None, str]:
    fingerprint_match_key = _normalize_text(bank_row.get("fingerprint_match_key", ""))
    if not fingerprint_match_key:
        return None, "no conservative payee match"
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_df))
    candidates = candidates[candidates["payee_match_key"] == fingerprint_match_key]

    if candidates.empty:
        return None, "no conservative payee match"
    if len(candidates) > 1:
        return None, "ambiguous conservative payee match"
    return candidates.iloc[0], ""


def _legacy_reconciled_fallback_candidate(
    bank_row: pd.Series, ynab_df: pd.DataFrame
) -> tuple[pd.Series | None, str]:
    candidates = _unlinked_candidates(_date_amount_candidates(bank_row, ynab_df))
    if candidates.empty:
        return None, "no legacy reconciled date+amount match"
    reconciled = candidates[candidates["cleared"] == "reconciled"]
    if reconciled.empty:
        return None, "no legacy reconciled date+amount match"
    if len(reconciled) > 1:
        return None, "ambiguous legacy reconciled date+amount match"
    if len(candidates) > 1:
        return None, "ambiguous legacy date+amount match"
    return reconciled.iloc[0], ""


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
    bank_row: pd.Series,
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
    bank_df: pd.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    prepared_bank = _prepare_bank_dataframe(bank_df)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_df = _filter_account_transactions(
        _prepare_ynab_transactions(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map, ref_map = _lineage_maps(ynab_df)

    report_rows: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []

    for _, bank_row in prepared_bank.iterrows():
        matched, resolved_via, reason = _resolve_exact_lineage(
            bank_row, ynab_df, import_map, memo_map, ref_map
        )
        (
            candidate_count,
            candidate_reconciled_count,
            candidate_status,
            candidate_summary,
            lineage_conflict_summary,
        ) = _candidate_diagnostics(bank_row, ynab_df, import_map, memo_map)
        if matched is None and reason == "no exact lineage match":
            matched, fallback_reason = _memo_exact_fallback_candidate(bank_row, ynab_df)
            if matched is not None:
                resolved_via = "memo_exact"
                reason = ""
            else:
                matched, payee_reason = _payee_exact_fallback_candidate(
                    bank_row, ynab_df
                )
                if matched is not None:
                    resolved_via = "payee_exact"
                    reason = ""
                else:
                    matched, legacy_reason = _legacy_reconciled_fallback_candidate(
                        bank_row, ynab_df
                    )
                    if matched is not None:
                        resolved_via = "date_amount_reconciled"
                        reason = ""
                    else:
                        # Last resort: accept a unique unlinked date+amount candidate so we
                        # can stamp the bank_txn_id + ref into its memo.  Future reconciliation
                        # runs will then recognise it via the stronger memo_ref lookup.
                        unlinked = _unlinked_candidates(
                            _date_amount_candidates(bank_row, ynab_df)
                        )
                        if len(unlinked) == 1:
                            matched = unlinked.iloc[0]
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


def _require_balance_column(bank_df: pd.DataFrame) -> None:
    if bank_df["balance_ils"].isna().any():
        missing_rows = bank_df.loc[bank_df["balance_ils"].isna(), "row_index"].tolist()
        raise ValueError(f"Bank CSV is missing balance_ils on row(s): {missing_rows}")


def _last_reconciled_date(last_reconciled_at: str) -> pd.Timestamp | None:
    if not _normalize_text(last_reconciled_at):
        return None
    parsed = pd.to_datetime(last_reconciled_at, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.normalize()


def _starting_balance_transaction(ynab_df: pd.DataFrame) -> pd.Series:
    if ynab_df.empty:
        raise ValueError("No YNAB transactions found for the target account.")
    ordered = ynab_df.sort_values(["date", "id"], na_position="last").reset_index(
        drop=True
    )
    first = ordered.iloc[0]
    if pd.isna(first["date"]):
        raise ValueError("Could not determine starting balance transaction date.")
    return first


def _reconciliation_report_row(
    bank_row: pd.Series,
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
    prepared_bank: pd.DataFrame,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
    ref_map: dict[str, list[int]] | None = None,
) -> tuple[list[ReconciliationResolution], list[dict[str, Any]]]:
    resolutions: list[ReconciliationResolution] = []
    report_rows: list[dict[str, Any]] = []

    for _, bank_row in prepared_bank.iterrows():
        matched, resolved_via, reason = _resolve_exact_lineage(
            bank_row, ynab_df, import_map, memo_map, ref_map
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
        ) = _candidate_diagnostics(bank_row, ynab_df, import_map, memo_map)
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
        elif eligible_count > best_count:
            best_start = start
            best_count = eligible_count
        if eligible_count == anchor_streak:
            found_start = start

    return found_start, best_start, best_count


def _reconciliation_result(
    *,
    resolved_account: ResolvedAccount,
    prepared_bank: pd.DataFrame,
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
            round(float(prepared_bank.iloc[-1]["balance_ils"]), 2)
            if not prepared_bank.empty
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
    bank_df: pd.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
    *,
    anchor_streak: int = 7,
    use_ynab_reconciled_date: bool = False,
) -> dict[str, Any]:
    if anchor_streak < 1:
        raise ValueError("anchor_streak must be at least 1.")

    prepared_bank = _prepare_bank_dataframe(bank_df)
    _require_balance_column(prepared_bank)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_df = _filter_account_transactions(
        _prepare_ynab_transactions(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map, ref_map = _lineage_maps(ynab_df)

    earliest_bank_date = prepared_bank["date"].min()
    updates: list[dict[str, Any]] = []
    resolutions, report_rows = _resolve_reconciliation_rows(
        prepared_bank,
        ynab_df,
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
            required_start = (last_reconciled - timedelta(days=7)).date()
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
                        "Bank CSV starts too late for auto-reconciliation: "
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
                reason=f"Bank CSV has fewer than {anchor_streak} rows; cannot establish anchor.",
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
            bank_row = prepared_bank.iloc[i]
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
        starting_balance_txn = _starting_balance_transaction(ynab_df)
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
                    "Bank CSV must start on the starting balance date when last_reconciled_at is missing: "
                    f"{earliest_bank_date} != {starting_balance_date}."
                ),
            )
        anchor_type = "starting_balance"
        anchor_balance = round(float(starting_balance_txn["amount_ils"]), 2)
        anchor_window_start = 0
        anchor_transaction_id = _normalize_text(starting_balance_txn.get("id", ""))

    running_balance = anchor_balance
    for i in range(anchor_row_index + 1, len(prepared_bank)):
        bank_row = prepared_bank.iloc[i]
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

    final_bank_balance = round(float(prepared_bank.iloc[-1]["balance_ils"]), 2)
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


def load_bank_csv(path: str | Path) -> pd.DataFrame:
    return _load_bank_csv(path)
