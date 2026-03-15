from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.io_max as io_max
import ynab_il_importer.normalize as normalize


PENDING_SHEET_NAME = "עסקאות שאושרו וטרם נקלטו"


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


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
    work["date_key"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
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
    return ordered.set_index("index")["legacy_import_id"].reindex(df.index).astype("string")


def _ensure_card_txn_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "card_txn_id" in out.columns:
        existing = _normalize_text_series(out["card_txn_id"])
    else:
        existing = pd.Series([""] * len(out), index=out.index, dtype="string")
        out["card_txn_id"] = existing

    missing = existing == ""
    if not missing.any():
        out["card_txn_id"] = existing
        return out

    out.loc[missing, "card_txn_id"] = out.loc[missing].apply(
        lambda row: card_identity.make_card_txn_id(
            source=row.get("source", "card"),
            source_account=row.get("source_account", ""),
            card_suffix=row.get("card_suffix", ""),
            date=row.get("date", ""),
            secondary_date=row.get("secondary_date", ""),
            outflow_ils=row.get("outflow_ils", 0.0),
            inflow_ils=row.get("inflow_ils", 0.0),
            description_raw=row.get("description_raw", row.get("memo", "")),
            max_sheet=row.get("max_sheet", ""),
            max_txn_type=row.get("max_txn_type", ""),
            max_original_amount=row.get("max_original_amount", ""),
            max_original_currency=row.get("max_original_currency", ""),
        ),
        axis=1,
    )
    out["card_txn_id"] = _normalize_text_series(out["card_txn_id"])
    return out


def _drop_pending_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["max_sheet"] = _normalize_text_series(
        out.get("max_sheet", pd.Series([""] * len(out), index=out.index))
    )
    out["outflow_ils"] = pd.to_numeric(out.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
    out["inflow_ils"] = pd.to_numeric(out.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
    signed = _signed_amount_ils(out)
    return out[(out["max_sheet"] != PENDING_SHEET_NAME) & (signed != 0)].copy()


def load_card_source(path: str | Path) -> pd.DataFrame:
    source_path = Path(path)
    if source_path.suffix.lower() == ".csv":
        df = pd.read_csv(source_path, dtype="string").fillna("")
        for col in ["outflow_ils", "inflow_ils", "max_original_amount", "max_exchange_rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        if "secondary_date" in df.columns:
            df["secondary_date"] = pd.to_datetime(df["secondary_date"], errors="coerce").dt.date
        return _drop_pending_rows(_ensure_card_txn_id(df))

    if source_path.suffix.lower() in {".xlsx", ".xls"}:
        return _drop_pending_rows(_ensure_card_txn_id(io_max.read_raw(source_path)))

    raise ValueError(f"Unsupported card source file type: {source_path}")


def _target_source_rows(df: pd.DataFrame, account_name: str) -> pd.DataFrame:
    work = _ensure_card_txn_id(df).copy()
    work["account_name"] = _normalize_text_series(work["account_name"])
    work["source_account"] = _normalize_text_series(
        work.get("source_account", pd.Series([""] * len(work), index=work.index))
    )
    mask = (work["account_name"] == account_name) | (work["source_account"] == account_name)
    filtered = work.loc[mask].copy()
    if filtered.empty:
        available = sorted((set(work["account_name"].tolist()) | set(work["source_account"].tolist())) - {""})
        raise ValueError(
            f"Account {account_name!r} not found in card source. Available accounts: {available}"
        )
    filtered["date"] = pd.to_datetime(filtered["date"], errors="coerce").dt.date
    filtered["secondary_date"] = pd.to_datetime(filtered["secondary_date"], errors="coerce").dt.date
    filtered["outflow_ils"] = pd.to_numeric(filtered["outflow_ils"], errors="coerce").fillna(0.0).round(2)
    filtered["inflow_ils"] = pd.to_numeric(filtered["inflow_ils"], errors="coerce").fillna(0.0).round(2)
    filtered["max_sheet"] = _normalize_text_series(
        filtered.get("max_sheet", pd.Series([""] * len(filtered), index=filtered.index))
    )
    filtered["card_txn_id"] = _normalize_text_series(filtered["card_txn_id"])
    filtered["legacy_import_id"] = _legacy_import_ids(filtered)
    filtered["description_match"] = filtered.apply(
        lambda row: _normalize_match_text(row.get("description_raw", row.get("memo", ""))),
        axis=1,
    )
    filtered["row_index"] = range(len(filtered))
    filtered["signed_ils"] = _signed_amount_ils(filtered)
    filtered = filtered[
        filtered["max_sheet"] != PENDING_SHEET_NAME
    ].copy()
    filtered = filtered[filtered["signed_ils"] != 0].copy()
    filtered.reset_index(drop=True, inplace=True)
    filtered["row_index"] = range(len(filtered))
    return filtered


def _account_lookup(accounts: list[dict[str, Any]], account_name: str) -> dict[str, str]:
    normalized_target = _normalize_text(account_name)
    matches = [
        acc
        for acc in accounts
        if not bool(acc.get("deleted", False)) and _normalize_text(acc.get("name", "")) == normalized_target
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
                "account_name": _normalize_text((account_names or {}).get(account_id, "")),
                "date": parsed_date.date() if pd.notna(parsed_date) else pd.NaT,
                "amount_milliunits": amount_milliunits,
                "signed_ils": round(amount_milliunits / 1000.0, 2),
                "memo": _normalize_text(txn.get("memo", "")),
                "memo_match": _normalize_match_text(txn.get("memo", "")),
                "import_id": _normalize_text(txn.get("import_id", "")),
                "card_txn_id_marker": card_identity.extract_card_txn_id_from_memo(txn.get("memo", "")),
                "cleared": _normalize_text(txn.get("cleared", "")),
                "approved": bool(txn.get("approved", False)),
                "payee_name": _normalize_text(txn.get("payee_name", "")),
                "transfer_account_id": _normalize_text(txn.get("transfer_account_id", "")),
                "transfer_transaction_id": _normalize_text(txn.get("transfer_transaction_id", "")),
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
    secondary = pd.to_datetime(previous_rows["secondary_date"], errors="coerce").dropna()
    if secondary.empty:
        return pd.NaT
    return secondary.max()


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
        card_transfer_account_name=_normalize_text(account_names.get(card_transfer_account_id, "")),
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
        return _ResolvedMatch(None, "", "no_candidates", "no YNAB transaction candidates")

    card_txn_id = _normalize_text(source_row.get("card_txn_id", ""))
    if card_txn_id:
        import_hits = ynab_df.index[ynab_df["import_id"] == card_txn_id].tolist()
        if len(import_hits) == 1:
            return _ResolvedMatch(ynab_df.loc[import_hits[0]], "import_id", "exact_lineage", "")
        if len(import_hits) > 1:
            return _ResolvedMatch(None, "", "duplicate_import_id", f"duplicate YNAB import_id matches for {card_txn_id}")

        memo_hits = ynab_df.index[ynab_df["card_txn_id_marker"] == card_txn_id].tolist()
        if len(memo_hits) == 1:
            return _ResolvedMatch(ynab_df.loc[memo_hits[0]], "memo_marker", "exact_lineage", "")
        if len(memo_hits) > 1:
            return _ResolvedMatch(None, "", "duplicate_memo_marker", f"duplicate YNAB memo markers for {card_txn_id}")

    legacy_import_id = _normalize_text(source_row.get("legacy_import_id", ""))
    if legacy_import_id:
        legacy_hits = ynab_df.index[ynab_df["import_id"] == legacy_import_id].tolist()
        if len(legacy_hits) == 1:
            candidate = ynab_df.loc[legacy_hits[0]]
            if candidate["date"] == source_row["date"] and abs(candidate["signed_ils"] - source_row["signed_ils"]) < 0.001:
                return _ResolvedMatch(candidate, "legacy_import_id", "legacy_import_id", "")

    same_key = ynab_df[
        (ynab_df["date"] == source_row["date"])
        & ((ynab_df["signed_ils"] - source_row["signed_ils"]).abs() < 0.001)
    ].copy()
    if same_key.empty:
        return _ResolvedMatch(None, "", "no_date_amount_match", "no date/amount match")

    memo_exact = same_key[same_key["memo_match"] == source_row["description_match"]]
    if len(memo_exact) == 1:
        return _ResolvedMatch(memo_exact.iloc[0], "memo_exact", "unique_memo_exact_candidate", "")
    if len(memo_exact) > 1:
        return _ResolvedMatch(None, "", "ambiguous_memo_exact_candidates", "multiple same-date/same-amount memo-confirmed candidates")
    if len(same_key) == 1:
        return _ResolvedMatch(None, "", "weak_unique_date_amount", "unique date/amount candidate exists but memo does not confirm it")
    return _ResolvedMatch(None, "", "ambiguous_date_amount_candidates", "multiple same-date/same-amount candidates")


def _row_report(source_row: pd.Series, snapshot_role: str) -> dict[str, object]:
    return {
        "snapshot_role": snapshot_role,
        "row_index": int(source_row.get("row_index", -1)),
        "account_name": _normalize_text(source_row.get("account_name", "")),
        "date": _normalize_text(source_row.get("date", "")),
        "secondary_date": _normalize_text(source_row.get("secondary_date", "")),
        "description_raw": _normalize_text(source_row.get("description_raw", source_row.get("memo", ""))),
        "fingerprint": _normalize_text(source_row.get("fingerprint", "")),
        "outflow_ils": float(pd.to_numeric(source_row.get("outflow_ils", 0.0), errors="coerce") or 0.0),
        "inflow_ils": float(pd.to_numeric(source_row.get("inflow_ils", 0.0), errors="coerce") or 0.0),
        "card_txn_id": _normalize_text(source_row.get("card_txn_id", "")),
        "legacy_import_id": _normalize_text(source_row.get("legacy_import_id", "")),
        "resolved_via": "",
        "candidate_status": "",
        "reason": "",
        "ynab_transaction_id": "",
        "ynab_import_id": "",
        "prior_cleared": "",
        "action": "",
    }


def _summarize_open_older_rows(ynab_df: pd.DataFrame, first_source_date: Any) -> pd.DataFrame:
    cutoff = pd.to_datetime(first_source_date, errors="coerce")
    if pd.isna(cutoff):
        return pd.DataFrame()
    return ynab_df[
        (ynab_df["cleared"] == "cleared")
        & (ynab_df["date"].notna())
        & (pd.to_datetime(ynab_df["date"], errors="coerce") < cutoff)
    ].copy()


def _apply_updates_for_rows(rows: pd.DataFrame, *, target_cleared: str) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for _, row in rows.iterrows():
        if _normalize_text(row.get("prior_cleared", "")) == target_cleared:
            continue
        updates.append({"id": _normalize_text(row.get("ynab_transaction_id", "")), "cleared": target_cleared})
    return updates


def _evaluate_snapshot_rows(source_df: pd.DataFrame, ynab_df: pd.DataFrame, *, snapshot_role: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, source_row in source_df.iterrows():
        report_row = _row_report(source_row, snapshot_role)
        resolved = _resolve_card_match(source_row, ynab_df)
        report_row["resolved_via"] = resolved.resolved_via
        report_row["candidate_status"] = resolved.candidate_status
        report_row["reason"] = resolved.reason
        if resolved.ynab_row is not None:
            report_row["ynab_transaction_id"] = _normalize_text(resolved.ynab_row.get("id", ""))
            report_row["ynab_import_id"] = _normalize_text(resolved.ynab_row.get("import_id", ""))
            report_row["prior_cleared"] = _normalize_text(resolved.ynab_row.get("cleared", ""))
            report_row["action"] = "matched"
        else:
            report_row["action"] = "blocked"
        rows.append(report_row)
    return pd.DataFrame(rows)


def plan_card_cycle_reconciliation(
    *,
    account_name: str,
    source_df: pd.DataFrame,
    accounts: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    previous_df: pd.DataFrame | None = None,
) -> dict[str, object]:
    account = _account_lookup(accounts, account_name)
    account_names = _account_name_map(accounts)
    source_rows = _target_source_rows(source_df, account_name)
    if source_rows.empty:
        raise ValueError("Source file has no in-scope non-pending rows for the requested account.")
    previous_rows = (
        _target_source_rows(previous_df, account_name) if previous_df is not None else pd.DataFrame()
    )

    all_ynab_df = _all_ynab_transactions_frame(transactions, account_names=account_names)
    ynab_df = all_ynab_df[all_ynab_df["account_id"] == account["account_id"]].copy()
    if ynab_df.empty:
        raise ValueError(f"No live YNAB transactions found for account {account_name!r}.")

    mode = "transition" if previous_df is not None else "source_only"
    source_report = _evaluate_snapshot_rows(source_rows, ynab_df, snapshot_role="source")
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
        "source_total_ils": round(float(source_rows["signed_ils"].sum()), 2),
        "previous_total_ils": round(float(previous_rows["signed_ils"].sum()), 2) if previous_df is not None else 0.0,
        "matched_source_count": int((source_report["ynab_transaction_id"] != "").sum()),
        "matched_previous_count": int((previous_report["ynab_transaction_id"] != "").sum()) if previous_df is not None else 0,
        "update_count": 0,
        "updates": [],
        "reason": "",
        "report": report,
        "warning": "",
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

    source_reconciled = source_report[source_report["prior_cleared"] == "reconciled"].copy()
    if not source_reconciled.empty:
        result["ok"] = False
        first = source_reconciled.iloc[0]
        result["reason"] = (
            "Source file contains transactions that are already reconciled; "
            f"first source row {int(first['row_index'])} is already reconciled. "
            "Pass the older settled file as --previous and a newer current file as --source."
        )
        return result

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
            (report["snapshot_role"] == "source") & (report["prior_cleared"] == "cleared"),
            "action",
        ] = "keep_cleared"
        to_clear = matched_source_rows[matched_source_rows["prior_cleared"] == "uncleared"].copy()
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

    previous_reconciled = previous_report[previous_report["prior_cleared"] == "reconciled"].copy()
    if len(previous_reconciled) == len(previous_report):
        result["warning"] = "All previous-file transactions are already reconciled."
        result["report"] = report
        return result
    if not previous_reconciled.empty:
        result["ok"] = False
        first = previous_reconciled.iloc[0]
        result["reason"] = (
            "Previous file has a mixed reconciled state; "
            f"first already-reconciled previous row is {int(first['row_index'])}. "
            "Clean up the account manually before retrying."
        )
        return result

    previous_total = round(float(previous_rows["signed_ils"].sum()), 2)
    current_total = round(float(source_rows["signed_ils"].sum()), 2)
    result["previous_total_ils"] = previous_total
    result["source_total_ils"] = current_total

    payment_match = _validate_payment_transfer(
        previous_rows=previous_rows,
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
    previous_to_reconcile = previous_report.copy()
    source_to_clear = source_report[source_report["prior_cleared"] == "uncleared"].copy()
    report.loc[report["snapshot_role"] == "previous", "action"] = "reconcile"
    report.loc[
        (report["snapshot_role"] == "source") & (report["prior_cleared"] == "uncleared"),
        "action",
    ] = "clear"
    report.loc[
        (report["snapshot_role"] == "source") & (report["prior_cleared"] == "cleared"),
        "action",
    ] = "keep_cleared"

    updates = _apply_updates_for_rows(previous_to_reconcile, target_cleared="reconciled")
    updates.extend(_apply_updates_for_rows(source_to_clear, target_cleared="cleared"))

    result["updates"] = updates
    result["update_count"] = len(updates)
    result["report"] = report
    return result
