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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _coerce_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _coerce_money_series(series: pd.Series, *, allow_missing: bool = False) -> pd.Series:
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
        prepared["ynab_account_id"] = _normalize_text_series(prepared["ynab_account_id"])
    else:
        prepared["ynab_account_id"] = ""
    prepared["source_account"] = _normalize_text_series(
        prepared.get("source_account", pd.Series([""] * len(prepared), index=prepared.index))
    )
    prepared["secondary_date"] = _coerce_date_series(
        prepared.get("secondary_date", pd.Series([None] * len(prepared), index=prepared.index))
    )
    prepared["date"] = _coerce_date_series(prepared["date"])
    prepared["outflow_ils"] = _coerce_money_series(prepared["outflow_ils"])
    prepared["inflow_ils"] = _coerce_money_series(prepared["inflow_ils"])
    prepared["balance_ils"] = _coerce_money_series(
        prepared.get("balance_ils", pd.Series([pd.NA] * len(prepared), index=prepared.index)),
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
    prepared["bank_txn_id"] = prepared["bank_txn_id"].map(bank_identity.validate_bank_txn_id)
    prepared["amount_milliunits"] = prepared.apply(
        lambda row: _amount_milliunits(row["outflow_ils"], row["inflow_ils"]),
        axis=1,
    )
    prepared["amount_ils"] = prepared["amount_milliunits"].div(1000.0).round(2)
    prepared["description_match_key"] = prepared["description_raw"].map(
        bank_identity.normalize_bank_memo_match_text
    )
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
                "import_id": _normalize_text(txn.get("import_id", "")),
                "memo_bank_txn_id": memo_bank_txn_id,
                "memo_marker_error": memo_marker_error,
                "cleared": _normalize_text(txn.get("cleared", "")),
                "approved": bool(txn.get("approved", False)),
                "matched_transaction_id": _normalize_text(txn.get("matched_transaction_id", "")),
                "deleted": False,
            }
        )
    return pd.DataFrame(rows)


def _resolve_account(bank_df: pd.DataFrame, accounts: list[dict[str, Any]]) -> ResolvedAccount:
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
            for value in bank_df["ynab_account_id"].astype("string").fillna("").str.strip().tolist()
            if value
        }
    )
    if len(mapped_ids) > 1:
        raise ValueError(f"Bank CSV resolves to multiple ynab_account_id values: {mapped_ids}")
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
            for value in bank_df["account_name"].astype("string").fillna("").str.strip().tolist()
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
    filtered = ynab_df[ynab_df["account_id"] == account_id].copy().reset_index(drop=True)
    filtered["row_index"] = filtered.index
    return filtered


def _lineage_maps(ynab_df: pd.DataFrame) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    import_map: dict[str, list[int]] = {}
    memo_map: dict[str, list[int]] = {}
    for idx, row in ynab_df.iterrows():
        import_id = _normalize_text(row.get("import_id", ""))
        if bank_identity.is_bank_txn_id(import_id):
            import_map.setdefault(import_id, []).append(idx)
        memo_bank_txn_id = _normalize_text(row.get("memo_bank_txn_id", ""))
        if memo_bank_txn_id:
            memo_map.setdefault(memo_bank_txn_id, []).append(idx)
    return import_map, memo_map


def _resolve_exact_lineage(
    bank_row: pd.Series,
    ynab_df: pd.DataFrame,
    import_map: dict[str, list[int]],
    memo_map: dict[str, list[int]],
) -> tuple[pd.Series | None, str, str]:
    bank_txn_id = bank_row["bank_txn_id"]

    import_hits = import_map.get(bank_txn_id, [])
    if len(import_hits) > 1:
        return None, "", f"duplicate YNAB import_id matches for {bank_txn_id}"
    if len(import_hits) == 1:
        return ynab_df.loc[import_hits[0]], "import_id", ""

    memo_hits = memo_map.get(bank_txn_id, [])
    if len(memo_hits) > 1:
        return None, "", f"duplicate YNAB memo markers for {bank_txn_id}"
    if len(memo_hits) == 1:
        return ynab_df.loc[memo_hits[0]], "memo_marker", ""

    return None, "", "no exact lineage match"


def _sync_fallback_candidate(bank_row: pd.Series, ynab_df: pd.DataFrame) -> tuple[pd.Series | None, str]:
    candidates = ynab_df.copy()
    candidates = candidates[candidates["date"] == bank_row["date"]]
    candidates = candidates[candidates["amount_milliunits"] == bank_row["amount_milliunits"]]
    candidates = candidates[candidates["import_id"].astype("string").fillna("").str.strip() == ""]
    candidates = candidates[
        candidates["memo_bank_txn_id"].astype("string").fillna("").str.strip() == ""
    ]
    candidates = candidates[candidates["memo_match_key"] == bank_row["description_match_key"]]

    if candidates.empty:
        return None, "no conservative memo match"
    if len(candidates) > 1:
        return None, "ambiguous conservative memo match"
    return candidates.iloc[0], ""


def _sync_report_row(
    bank_row: pd.Series,
    *,
    resolved_transaction_id: str = "",
    resolved_via: str = "",
    prior_cleared: str = "",
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
    import_map, memo_map = _lineage_maps(ynab_df)

    report_rows: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []

    for _, bank_row in prepared_bank.iterrows():
        matched, resolved_via, reason = _resolve_exact_lineage(bank_row, ynab_df, import_map, memo_map)
        if matched is None and reason == "no exact lineage match":
            matched, fallback_reason = _sync_fallback_candidate(bank_row, ynab_df)
            if matched is not None:
                resolved_via = "memo_exact"
                reason = ""
            else:
                reason = fallback_reason

        if matched is None:
            report_rows.append(
                _sync_report_row(bank_row, action="unmatched", reason=reason)
            )
            continue

        transaction_id = _normalize_text(matched.get("id", ""))
        prior_cleared = _normalize_text(matched.get("cleared", ""))
        patch: dict[str, Any] = {"id": transaction_id}
        actions: list[str] = []

        if resolved_via == "memo_exact":
            try:
                patch["memo"] = bank_identity.append_bank_txn_id_marker(
                    matched.get("memo", ""),
                    bank_row["bank_txn_id"],
                )
                actions.append("stamp")
            except ValueError as exc:
                report_rows.append(
                    _sync_report_row(
                        bank_row,
                        resolved_transaction_id=transaction_id,
                        resolved_via=resolved_via,
                        prior_cleared=prior_cleared,
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
                action=action,
            )
        )

    report = pd.DataFrame(report_rows, columns=SYNC_REPORT_COLUMNS)
    return {
        "account_id": resolved_account.account_id,
        "account_name": resolved_account.account_name,
        "updates": updates,
        "report": report,
        "matched_count": int((report["action"] != "unmatched").sum()) if not report.empty else 0,
        "update_count": len(updates),
    }


def _require_balance_column(bank_df: pd.DataFrame) -> None:
    if bank_df["balance_ils"].isna().any():
        missing_rows = bank_df.loc[bank_df["balance_ils"].isna(), "row_index"].tolist()
        raise ValueError(
            f"Bank CSV is missing balance_ils on row(s): {missing_rows}"
        )


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
    ordered = ynab_df.sort_values(["date", "id"], na_position="last").reset_index(drop=True)
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
        "replayed_balance_ils": replayed_balance_ils,
        "balance_match": balance_match,
        "action": action,
        "reason": reason,
    }


def plan_bank_statement_reconciliation(
    bank_df: pd.DataFrame,
    accounts: list[dict[str, Any]],
    ynab_transactions: list[dict[str, Any]],
    *,
    anchor_streak: int = 7,
) -> dict[str, Any]:
    if anchor_streak < 1:
        raise ValueError("anchor_streak must be at least 1.")

    prepared_bank = _prepare_bank_dataframe(bank_df)
    _require_balance_column(prepared_bank)
    resolved_account = _resolve_account(prepared_bank, accounts)
    ynab_df = _filter_account_transactions(
        _prepare_ynab_transactions(ynab_transactions), resolved_account.account_id
    )
    import_map, memo_map = _lineage_maps(ynab_df)

    earliest_bank_date = prepared_bank["date"].min()
    report_rows: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []

    last_reconciled = _last_reconciled_date(resolved_account.last_reconciled_at)
    anchor_type = ""
    anchor_balance = 0.0
    anchor_row_index = -1
    anchor_transaction_id = ""

    if last_reconciled is not None:
        required_start = (last_reconciled - timedelta(days=7)).date()
        if earliest_bank_date > required_start:
            raise ValueError(
                "Bank CSV starts too late for auto-reconciliation: "
                f"{earliest_bank_date} > {required_start}."
            )
        if len(prepared_bank) < anchor_streak:
            raise ValueError(
                f"Bank CSV has fewer than {anchor_streak} rows; cannot establish anchor."
            )
        for i in range(anchor_streak):
            bank_row = prepared_bank.iloc[i]
            matched, resolved_via, reason = _resolve_exact_lineage(
                bank_row, ynab_df, import_map, memo_map
            )
            if matched is None:
                raise ValueError(
                    f"Could not establish reconciliation anchor at row {int(bank_row['row_index'])}: {reason}"
                )
            if _normalize_text(matched.get("cleared", "")) != "reconciled":
                raise ValueError(
                    "Could not establish reconciliation anchor because the opening streak "
                    f"contains a non-reconciled YNAB transaction at row {int(bank_row['row_index'])}."
                )
            report_rows.append(
                _reconciliation_report_row(
                    bank_row,
                    resolved_transaction_id=_normalize_text(matched.get("id", "")),
                    resolved_via=resolved_via,
                    prior_cleared=_normalize_text(matched.get("cleared", "")),
                    replayed_balance_ils=round(float(bank_row["balance_ils"]), 2),
                    balance_match=True,
                    action="anchor_history",
                )
            )
            anchor_balance = round(float(bank_row["balance_ils"]), 2)
            anchor_row_index = i
            anchor_transaction_id = _normalize_text(matched.get("id", ""))
        anchor_type = "last_reconciled_at"
    else:
        starting_balance_txn = _starting_balance_transaction(ynab_df)
        starting_balance_date = starting_balance_txn["date"]
        if earliest_bank_date != starting_balance_date:
            raise ValueError(
                "Bank CSV must start on the starting balance date when last_reconciled_at is missing: "
                f"{earliest_bank_date} != {starting_balance_date}."
            )
        anchor_type = "starting_balance"
        anchor_balance = round(float(starting_balance_txn["amount_ils"]), 2)
        anchor_transaction_id = _normalize_text(starting_balance_txn.get("id", ""))

    running_balance = anchor_balance
    for i in range(anchor_row_index + 1, len(prepared_bank)):
        bank_row = prepared_bank.iloc[i]
        matched, resolved_via, reason = _resolve_exact_lineage(
            bank_row, ynab_df, import_map, memo_map
        )
        if matched is None:
            report_rows.append(
                _reconciliation_report_row(
                    bank_row,
                    action="blocked",
                    reason=reason,
                )
            )
            raise ValueError(
                f"Could not reconcile row {int(bank_row['row_index'])}: {reason}"
            )

        running_balance = round(running_balance + float(bank_row["amount_ils"]), 2)
        balance_match = _same_balance(running_balance, float(bank_row["balance_ils"]))
        prior_cleared = _normalize_text(matched.get("cleared", ""))
        action = "already_reconciled" if prior_cleared == "reconciled" else "reconcile"
        report_rows.append(
            _reconciliation_report_row(
                bank_row,
                resolved_transaction_id=_normalize_text(matched.get("id", "")),
                resolved_via=resolved_via,
                prior_cleared=prior_cleared,
                replayed_balance_ils=running_balance,
                balance_match=balance_match,
                action=action if balance_match else "blocked",
                reason="" if balance_match else "running balance mismatch",
            )
        )
        if not balance_match:
            raise ValueError(
                "Running balance mismatch at row "
                f"{int(bank_row['row_index'])}: expected {float(bank_row['balance_ils']):.2f}, "
                f"replayed {running_balance:.2f}."
            )

        if prior_cleared != "reconciled":
            updates.append({"id": _normalize_text(matched.get("id", "")), "cleared": "reconciled"})

    final_bank_balance = round(float(prepared_bank.iloc[-1]["balance_ils"]), 2)
    if not _same_balance(running_balance, final_bank_balance):
        raise ValueError(
            f"Final balance mismatch: replayed {running_balance:.2f} vs bank {final_bank_balance:.2f}."
        )

    report = pd.DataFrame(report_rows, columns=RECONCILIATION_REPORT_COLUMNS)
    return {
        "account_id": resolved_account.account_id,
        "account_name": resolved_account.account_name,
        "anchor_type": anchor_type,
        "anchor_transaction_id": anchor_transaction_id,
        "anchor_balance_ils": anchor_balance,
        "updates": updates,
        "report": report,
        "update_count": len(updates),
        "final_balance_ils": final_bank_balance,
    }


def load_bank_csv(path: str | Path) -> pd.DataFrame:
    return _load_bank_csv(path)
