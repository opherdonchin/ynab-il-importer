from __future__ import annotations

from collections import Counter
import re
from typing import Any

import pandas as pd

import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.review_app.model as review_model


REQUIRED_REVIEW_COLUMNS = [
    "transaction_id",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "payee_selected",
    "category_selected",
]
_LEADING_SYMBOL_RE = re.compile(r"^[^\w\u0590-\u05FF]+")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _amount_milliunits(row: pd.Series) -> int:
    outflow = float(pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce") or 0.0)
    inflow = float(pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce") or 0.0)
    return int(round((inflow - outflow) * 1000))


def _nonzero_amount_mask(df: pd.DataFrame) -> pd.Series:
    outflow = pd.to_numeric(df["outflow_ils"], errors="coerce").fillna(0.0)
    inflow = pd.to_numeric(df["inflow_ils"], errors="coerce").fillna(0.0)
    return (outflow != 0.0) | (inflow != 0.0)


def _transfer_target(payee: str) -> str:
    if not review_model.is_transfer_payee(payee):
        return ""
    _, _, target = payee.partition(":")
    return target.strip()


def _source_import_id(row: pd.Series) -> str:
    bank_txn_id = _normalize_text(row.get("bank_txn_id", ""))
    if not bank_txn_id:
        card_txn_id = _normalize_text(row.get("card_txn_id", ""))
        if not card_txn_id:
            return ""
        return card_identity.validate_card_txn_id(card_txn_id)
    return bank_identity.validate_bank_txn_id(bank_txn_id)


def _validate_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _account_lookup(
    accounts: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str]]:
    active = [acc for acc in accounts if not bool(acc.get("deleted", False))]
    name_counts = Counter(_normalize_text(acc.get("name", "")) for acc in active)
    duplicates = sorted(
        name for name, count in name_counts.items() if name and count > 1
    )
    if duplicates:
        raise ValueError(f"Duplicate YNAB account names: {duplicates}")

    account_ids = {
        _normalize_text(acc.get("name", "")): _normalize_text(acc.get("id", ""))
        for acc in active
        if _normalize_text(acc.get("name", ""))
    }
    transfer_payees = {
        _normalize_text(acc.get("name", "")): _normalize_text(
            acc.get("transfer_payee_id", "")
        )
        for acc in active
        if _normalize_text(acc.get("name", ""))
    }
    return account_ids, transfer_payees


def uploadable_account_mask(
    df: pd.DataFrame, accounts: list[dict[str, Any]]
) -> pd.Series:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    account_ids, _ = _account_lookup(accounts)
    account_names = _normalize_text_series(df["account_name"])
    return account_names.isin(set(account_ids.keys()))


def _category_lookup(categories_df: pd.DataFrame) -> dict[str, str]:
    active = categories_df.copy()
    if "hidden" in active.columns:
        active = active[~active["hidden"].astype(bool)]

    names = _normalize_text_series(active["category_name"])
    name_counts = Counter(names.tolist())
    duplicates = sorted(
        name for name, count in name_counts.items() if name and count > 1
    )
    if duplicates:
        raise ValueError(f"Duplicate YNAB category names: {duplicates}")

    return {
        _normalize_text(row.get("category_name", "")): _normalize_text(
            row.get("category_id", "")
        )
        for _, row in active.iterrows()
        if _normalize_text(row.get("category_name", ""))
    }


def _category_alias(name: str) -> str:
    text = _normalize_text(name)
    text = _LEADING_SYMBOL_RE.sub("", text).strip()
    if text.casefold().startswith("inflow:"):
        _, _, text = text.partition(":")
        text = text.strip()
    return text.casefold()


def _category_alias_lookup(categories_df: pd.DataFrame) -> dict[str, str]:
    active = categories_df.copy()
    if "hidden" in active.columns:
        active = active[~active["hidden"].astype(bool)]

    alias_to_id: dict[str, str] = {}
    duplicate_aliases: list[str] = []
    for _, row in active.iterrows():
        name = _normalize_text(row.get("category_name", ""))
        category_id = _normalize_text(row.get("category_id", ""))
        alias = _category_alias(name)
        if not alias or not category_id:
            continue
        if alias in alias_to_id and alias_to_id[alias] != category_id:
            duplicate_aliases.append(alias)
            continue
        alias_to_id[alias] = category_id

    if duplicate_aliases:
        raise ValueError(
            f"Ambiguous simplified YNAB category aliases: {sorted(set(duplicate_aliases))}"
        )
    return alias_to_id


def validate_ready_for_upload(df: pd.DataFrame) -> None:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)

    payee = _normalize_text_series(df["payee_selected"])
    category = _normalize_text_series(df["category_selected"])
    transfer = payee.map(review_model.is_transfer_payee)
    nonzero_amount = _nonzero_amount_mask(df)

    missing_payee = df.index[payee == ""].tolist()
    missing_category = df.index[(category == "") & ~transfer].tolist()
    zero_amount = df.index[~nonzero_amount].tolist()
    if missing_payee or missing_category or zero_amount:
        raise ValueError(
            "Reviewed file is not ready for upload: "
            f"{len(missing_payee)} rows missing payee, "
            f"{len(missing_category)} rows missing category, "
            f"{len(zero_amount)} rows with zero amount."
        )


def ready_mask(df: pd.DataFrame) -> pd.Series:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    payee = _normalize_text_series(df["payee_selected"])
    category = _normalize_text_series(df["category_selected"])
    transfer = payee.map(review_model.is_transfer_payee)
    nonzero_amount = _nonzero_amount_mask(df)
    return (payee != "") & ((category != "") | transfer) & nonzero_amount


def prepare_upload_transactions(
    reviewed_df: pd.DataFrame,
    *,
    accounts: list[dict[str, Any]],
    categories_df: pd.DataFrame,
    cleared: str = "cleared",
    approved: bool = False,
) -> pd.DataFrame:
    validate_ready_for_upload(reviewed_df)

    df = reviewed_df.copy()
    for col in [
        "transaction_id",
        "account_name",
        "date",
        "memo",
        "payee_selected",
        "category_selected",
    ]:
        df[col] = _normalize_text_series(df[col])
    for col in ["outflow_ils", "inflow_ils"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)

    account_ids, transfer_payees = _account_lookup(accounts)
    category_ids = _category_lookup(categories_df)
    category_alias_ids = _category_alias_lookup(categories_df)

    df["account_id"] = df["account_name"].map(account_ids).astype("string").fillna("")
    missing_accounts = sorted(
        df.loc[df["account_id"] == "", "account_name"].unique().tolist()
    )
    if missing_accounts:
        raise ValueError(
            f"Missing YNAB account ids for account_name values: {missing_accounts}"
        )

    df["transfer_target"] = df["payee_selected"].map(_transfer_target)
    is_transfer = df["payee_selected"].map(review_model.is_transfer_payee)

    transfer_target_ids = (
        df["transfer_target"].map(transfer_payees).astype("string").fillna("")
    )
    transfer_target_account_ids = (
        df["transfer_target"].map(account_ids).astype("string").fillna("")
    )
    missing_transfer_targets = sorted(
        df.loc[is_transfer & (transfer_target_ids == ""), "transfer_target"]
        .unique()
        .tolist()
    )
    if missing_transfer_targets:
        raise ValueError(
            f"Missing transfer payee ids for target accounts: {missing_transfer_targets}"
        )

    df["category_id"] = (
        df["category_selected"].map(category_ids).astype("string").fillna("")
    )
    unresolved_category = (~is_transfer) & (df["category_id"] == "")
    if unresolved_category.any():
        aliases = df.loc[unresolved_category, "category_selected"].map(_category_alias)
        df.loc[unresolved_category, "category_id"] = (
            aliases.map(category_alias_ids).astype("string").fillna("")
        )
    missing_categories = sorted(
        df.loc[~is_transfer & (df["category_id"] == ""), "category_selected"]
        .unique()
        .tolist()
    )
    if missing_categories:
        raise ValueError(
            f"Missing YNAB category ids for category_selected values: {missing_categories}"
        )

    df["payee_id"] = ""
    df.loc[is_transfer, "payee_id"] = transfer_target_ids.loc[is_transfer]
    df["payee_name_upload"] = df["payee_selected"].where(~is_transfer, "")
    df.loc[is_transfer, "category_id"] = ""
    df["transfer_target_account_id"] = ""
    df.loc[is_transfer, "transfer_target_account_id"] = transfer_target_account_ids.loc[
        is_transfer
    ]

    df["amount_milliunits"] = df.apply(_amount_milliunits, axis=1)
    df["bank_txn_id"] = (
        df.get("bank_txn_id", pd.Series([""] * len(df), index=df.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    df["card_txn_id"] = (
        df.get("card_txn_id", pd.Series([""] * len(df), index=df.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    occurrence_order = (
        df.reset_index()
        .sort_values(
            ["account_id", "date", "amount_milliunits", "transaction_id", "index"]
        )
        .copy()
    )
    occurrence_order["import_occurrence"] = (
        occurrence_order.groupby(
            ["account_id", "date", "amount_milliunits"], dropna=False
        )
        .cumcount()
        .add(1)
    )
    occurrence_order["import_id"] = occurrence_order.apply(
        lambda row: _source_import_id(row)
        or f"YNAB:{int(row['amount_milliunits'])}:{row['date']}:{int(row['import_occurrence'])}",
        axis=1,
    )
    occurrence_map = occurrence_order.set_index("index")["import_id"]
    df["import_id"] = (
        df.index.to_series().map(occurrence_map).astype("string").fillna("")
    )

    df["cleared"] = cleared
    df["approved"] = bool(approved)

    df["upload_kind"] = "regular"
    df.loc[is_transfer, "upload_kind"] = "transfer"
    columns = [
        "transaction_id",
        "account_name",
        "account_id",
        "date",
        "outflow_ils",
        "inflow_ils",
        "amount_milliunits",
        "memo",
        "payee_selected",
        "payee_name_upload",
        "payee_id",
        "transfer_target",
        "transfer_target_account_id",
        "category_selected",
        "category_id",
        "cleared",
        "approved",
        "import_id",
        "upload_kind",
    ]
    optional_columns = [
        "source",
        "source_account",
        "card_suffix",
        "secondary_date",
        "ref",
        "balance_ils",
        "ynab_account_id",
        "bank_txn_id",
        "card_txn_id",
        "max_sheet",
        "max_txn_type",
        "max_original_amount",
        "max_original_currency",
        "max_report_period",
        "max_report_scope",
    ]
    columns.extend([col for col in optional_columns if col in df.columns])
    return df[columns].copy()


def upload_payload_records(prepared_df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for _, row in prepared_df.iterrows():
        payload: dict[str, Any] = {
            "account_id": _normalize_text(row.get("account_id", "")),
            "date": _normalize_text(row.get("date", "")),
            "amount": int(row.get("amount_milliunits", 0)),
            "memo": _normalize_text(row.get("memo", "")) or None,
            "cleared": _normalize_text(row.get("cleared", "")) or "cleared",
            "approved": bool(row.get("approved", True)),
            "import_id": _normalize_text(row.get("import_id", "")),
        }
        payee_id = _normalize_text(row.get("payee_id", ""))
        payee_name = _normalize_text(row.get("payee_name_upload", ""))
        category_id = _normalize_text(row.get("category_id", ""))
        if payee_id:
            payload["payee_id"] = payee_id
        elif payee_name:
            payload["payee_name"] = payee_name
        if category_id:
            payload["category_id"] = category_id
        records.append(payload)
    return records


def _transactions_frame(transactions: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        rows.append(
            {
                "id": _normalize_text(txn.get("id", "")),
                "account_id": _normalize_text(txn.get("account_id", "")),
                "date": _normalize_text(txn.get("date", "")),
                "amount_milliunits": int(txn.get("amount", 0) or 0),
                "memo": _normalize_text(txn.get("memo", "")),
                "cleared": _normalize_text(txn.get("cleared", "")),
                "approved": bool(txn.get("approved", False)),
                "import_id": _normalize_text(txn.get("import_id", "")),
                "matched_transaction_id": _normalize_text(
                    txn.get("matched_transaction_id", "")
                ),
                "transfer_account_id": _normalize_text(
                    txn.get("transfer_account_id", "")
                ),
                "deleted": bool(txn.get("deleted", False)),
                "payee_name": _normalize_text(txn.get("payee_name", "")),
                "category_name": _normalize_text(txn.get("category_name", "")),
                "category_id": _normalize_text(txn.get("category_id", "")),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date_key"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _account_import_label(account_id: str, import_id: str) -> str:
    return f"{_normalize_text(account_id)}::{_normalize_text(import_id)}"


def upload_preflight(
    prepared_df: pd.DataFrame,
    existing_transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    prepared = prepared_df.copy()
    prepared["import_id"] = _normalize_text_series(prepared["import_id"])
    prepared["account_id"] = _normalize_text_series(prepared["account_id"])
    prepared["upload_kind"] = _normalize_text_series(prepared["upload_kind"])
    prepared["payee_id"] = _normalize_text_series(prepared["payee_id"])
    prepared["payee_name_upload"] = _normalize_text_series(
        prepared["payee_name_upload"]
    )
    prepared["category_id"] = _normalize_text_series(prepared["category_id"])
    prepared["date_key"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["amount_milliunits"] = (
        pd.to_numeric(prepared["amount_milliunits"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    payload_duplicates = prepared.groupby(
        ["account_id", "import_id"], dropna=False
    ).size()
    payload_duplicate_keys = [
        (account_id, import_id)
        for (account_id, import_id), count in payload_duplicates.items()
        if import_id and count > 1
    ]

    existing_df = _transactions_frame(existing_transactions)
    existing_import_id_hits: list[tuple[str, str]] = []
    potential_match_import_ids: list[str] = []
    if not existing_df.empty:
        existing_with_import = existing_df[existing_df["import_id"] != ""].copy()
        if not existing_with_import.empty:
            existing_keys = {
                (row["account_id"], row["import_id"])
                for _, row in existing_with_import.iterrows()
            }
            existing_import_id_hits = sorted(
                {
                    (row["account_id"], row["import_id"])
                    for _, row in prepared.iterrows()
                    if row["import_id"]
                    and (row["account_id"], row["import_id"]) in existing_keys
                }
            )

        candidates = existing_df[existing_df["import_id"] == ""].copy()
        if not candidates.empty:
            merged = prepared.reset_index(drop=True).merge(
                candidates,
                on=["account_id", "amount_milliunits"],
                suffixes=("_prepared", "_existing"),
            )
            if not merged.empty:
                merged["date_gap_days"] = (
                    (merged["date_key_prepared"] - merged["date_key_existing"])
                    .abs()
                    .dt.days
                )
                merged = merged[merged["date_gap_days"] <= 10]
                if not merged.empty:
                    potential_match_import_ids = sorted(
                        set(merged["import_id_prepared"].tolist())
                        - set(existing_import_id_hits)
                    )

    is_transfer = prepared["upload_kind"] == "transfer"
    transfer_payload_issue_mask = (
        (is_transfer & (prepared["payee_id"] == ""))
        | (is_transfer & (prepared["category_id"] != ""))
        | (is_transfer & (prepared["payee_name_upload"] != ""))
        | (~is_transfer & (prepared["payee_id"] != ""))
    )
    transfer_payload_issue_ids = sorted(
        prepared.loc[transfer_payload_issue_mask, "import_id"].tolist()
    )

    return {
        "prepared_count": len(prepared),
        "transfer_count": int(is_transfer.sum()),
        "payload_duplicate_import_keys": payload_duplicate_keys,
        "existing_import_id_hits": existing_import_id_hits,
        "potential_match_import_ids": potential_match_import_ids,
        "transfer_payload_issue_ids": transfer_payload_issue_ids,
    }


def summarize_upload_response(response: dict[str, Any]) -> dict[str, int]:
    transactions = response.get("transactions", []) or []
    saved_ids = response.get("transaction_ids", []) or []
    duplicate_ids = response.get("duplicate_import_ids", []) or []
    tx_df = _transactions_frame(transactions)

    matched_existing = 0
    transfer_saved = 0
    if not tx_df.empty:
        matched_existing = int((tx_df["matched_transaction_id"] != "").sum())
        transfer_saved = int((tx_df["transfer_account_id"] != "").sum())

    return {
        "saved": len(saved_ids),
        "duplicate_import_ids": len(duplicate_ids),
        "matched_existing": matched_existing,
        "transfer_saved": transfer_saved,
    }


def classify_upload_result(
    summary: dict[str, int], *, prepared_count: int
) -> dict[str, Any]:
    saved = int(summary.get("saved", 0))
    duplicate_import_ids = int(summary.get("duplicate_import_ids", 0))
    matched_existing = int(summary.get("matched_existing", 0))
    transfer_saved = int(summary.get("transfer_saved", 0))

    idempotent_rerun = (
        prepared_count > 0 and saved == 0 and duplicate_import_ids == prepared_count
    )
    verification_needed = saved > 0

    if idempotent_rerun:
        status = "idempotent rerun confirmed"
    elif saved > 0:
        status = "new transactions saved"
    elif duplicate_import_ids > 0 or matched_existing > 0:
        status = "no new transactions saved"
    else:
        status = "no transactions saved"

    return {
        "saved": saved,
        "duplicate_import_ids": duplicate_import_ids,
        "matched_existing": matched_existing,
        "transfer_saved": transfer_saved,
        "idempotent_rerun": idempotent_rerun,
        "verification_needed": verification_needed,
        "status": status,
    }


def verify_upload_response(
    prepared_df: pd.DataFrame,
    response: dict[str, Any],
) -> dict[str, Any]:
    prepared = prepared_df.copy()
    prepared["import_id"] = _normalize_text_series(prepared["import_id"])
    prepared["account_id"] = _normalize_text_series(prepared["account_id"])
    prepared["category_id"] = _normalize_text_series(prepared["category_id"])
    prepared["transfer_target_account_id"] = _normalize_text_series(
        prepared["transfer_target_account_id"]
    )
    prepared["date"] = _normalize_text_series(prepared["date"])
    prepared["amount_milliunits"] = (
        pd.to_numeric(prepared["amount_milliunits"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    prepared["upload_kind"] = _normalize_text_series(prepared["upload_kind"])

    response_df = _transactions_frame(response.get("transactions", []) or [])
    if response_df.empty:
        return {
            "checked": 0,
            "missing_saved_transactions": [],
            "amount_mismatches": [],
            "date_mismatches": [],
            "account_mismatches": [],
            "transfer_mismatches": [],
            "category_mismatches": [],
        }

    response_df = response_df[response_df["import_id"] != ""].copy()
    prepared_indexed = prepared.set_index(["account_id", "import_id"], drop=False)
    response_indexed = response_df.set_index(["account_id", "import_id"], drop=False)

    if prepared_indexed.index.has_duplicates:
        prepared_indexed = prepared_indexed[
            ~prepared_indexed.index.duplicated(keep="first")
        ]
    if response_indexed.index.has_duplicates:
        response_indexed = response_indexed[
            ~response_indexed.index.duplicated(keep="first")
        ]

    missing_saved = sorted(
        set(response.get("transaction_ids", []) or []) - set(response_df["id"].tolist())
    )

    shared_keys = sorted(
        set(prepared_indexed.index).intersection(set(response_indexed.index))
    )
    amount_mismatches: list[str] = []
    date_mismatches: list[str] = []
    account_mismatches: list[str] = []
    transfer_mismatches: list[str] = []
    category_mismatches: list[str] = []

    for account_id, import_id in shared_keys:
        prepared_row = prepared_indexed.loc[(account_id, import_id)]
        response_row = response_indexed.loc[(account_id, import_id)]
        label = _account_import_label(account_id, import_id)

        if int(prepared_row["amount_milliunits"]) != int(
            response_row["amount_milliunits"]
        ):
            amount_mismatches.append(label)
        if str(prepared_row["date"]) != str(response_row["date"]):
            date_mismatches.append(label)
        if str(prepared_row["account_id"]) != str(response_row["account_id"]):
            account_mismatches.append(label)

        is_transfer = str(prepared_row["upload_kind"]) == "transfer"
        response_transfer_account_id = str(
            response_row.get("transfer_account_id", "") or ""
        )
        if is_transfer:
            if not response_transfer_account_id or (
                str(prepared_row["transfer_target_account_id"])
                and response_transfer_account_id
                != str(prepared_row["transfer_target_account_id"])
            ):
                transfer_mismatches.append(label)
        else:
            if response_transfer_account_id:
                transfer_mismatches.append(label)

        prepared_category_id = str(prepared_row.get("category_id", "") or "")
        response_category_id = str(response_row.get("category_id", "") or "")
        response_category_name = str(response_row.get("category_name", "") or "")
        prepared_category_name = str(prepared_row.get("category_selected", "") or "")
        category_matches = prepared_category_id == response_category_id
        if (
            not category_matches
            and not response_category_id
            and prepared_category_name
            and prepared_category_name == response_category_name
        ):
            category_matches = True
        if (
            not is_transfer
            and prepared_category_id
            and not category_matches
        ):
            category_mismatches.append(label)

    return {
        "checked": len(shared_keys),
        "missing_saved_transactions": missing_saved,
        "amount_mismatches": sorted(amount_mismatches),
        "date_mismatches": sorted(date_mismatches),
        "account_mismatches": sorted(account_mismatches),
        "transfer_mismatches": sorted(transfer_mismatches),
        "category_mismatches": sorted(category_mismatches),
    }
