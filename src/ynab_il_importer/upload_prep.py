from __future__ import annotations

from collections import Counter
import re
from typing import Any

import pandas as pd

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


def _transfer_target(payee: str) -> str:
    if not review_model.is_transfer_payee(payee):
        return ""
    _, _, target = payee.partition(":")
    return target.strip()


def _validate_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _account_lookup(accounts: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
    active = [acc for acc in accounts if not bool(acc.get("deleted", False))]
    name_counts = Counter(_normalize_text(acc.get("name", "")) for acc in active)
    duplicates = sorted(name for name, count in name_counts.items() if name and count > 1)
    if duplicates:
        raise ValueError(f"Duplicate YNAB account names: {duplicates}")

    account_ids = {
        _normalize_text(acc.get("name", "")): _normalize_text(acc.get("id", ""))
        for acc in active
        if _normalize_text(acc.get("name", ""))
    }
    transfer_payees = {
        _normalize_text(acc.get("name", "")): _normalize_text(acc.get("transfer_payee_id", ""))
        for acc in active
        if _normalize_text(acc.get("name", ""))
    }
    return account_ids, transfer_payees


def uploadable_account_mask(df: pd.DataFrame, accounts: list[dict[str, Any]]) -> pd.Series:
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
    duplicates = sorted(name for name, count in name_counts.items() if name and count > 1)
    if duplicates:
        raise ValueError(f"Duplicate YNAB category names: {duplicates}")

    return {
        _normalize_text(row.get("category_name", "")): _normalize_text(row.get("category_id", ""))
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
        raise ValueError(f"Ambiguous simplified YNAB category aliases: {sorted(set(duplicate_aliases))}")
    return alias_to_id


def validate_ready_for_upload(df: pd.DataFrame) -> None:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)

    payee = _normalize_text_series(df["payee_selected"])
    category = _normalize_text_series(df["category_selected"])
    transfer = payee.map(review_model.is_transfer_payee)

    missing_payee = df.index[payee == ""].tolist()
    missing_category = df.index[(category == "") & ~transfer].tolist()
    if missing_payee or missing_category:
        raise ValueError(
            "Reviewed file is not ready for upload: "
            f"{len(missing_payee)} rows missing payee, "
            f"{len(missing_category)} rows missing category."
        )


def ready_mask(df: pd.DataFrame) -> pd.Series:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    payee = _normalize_text_series(df["payee_selected"])
    category = _normalize_text_series(df["category_selected"])
    transfer = payee.map(review_model.is_transfer_payee)
    return (payee != "") & ((category != "") | transfer)


def prepare_upload_transactions(
    reviewed_df: pd.DataFrame,
    *,
    accounts: list[dict[str, Any]],
    categories_df: pd.DataFrame,
    cleared: str = "cleared",
    approved: bool = True,
) -> pd.DataFrame:
    validate_ready_for_upload(reviewed_df)

    df = reviewed_df.copy()
    for col in ["transaction_id", "account_name", "date", "memo", "payee_selected", "category_selected"]:
        df[col] = _normalize_text_series(df[col])
    for col in ["outflow_ils", "inflow_ils"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)

    account_ids, transfer_payees = _account_lookup(accounts)
    category_ids = _category_lookup(categories_df)
    category_alias_ids = _category_alias_lookup(categories_df)

    df["account_id"] = df["account_name"].map(account_ids).astype("string").fillna("")
    missing_accounts = sorted(df.loc[df["account_id"] == "", "account_name"].unique().tolist())
    if missing_accounts:
        raise ValueError(f"Missing YNAB account ids for account_name values: {missing_accounts}")

    df["transfer_target"] = df["payee_selected"].map(_transfer_target)
    is_transfer = df["payee_selected"].map(review_model.is_transfer_payee)

    transfer_target_ids = df["transfer_target"].map(transfer_payees).astype("string").fillna("")
    missing_transfer_targets = sorted(
        df.loc[is_transfer & (transfer_target_ids == ""), "transfer_target"].unique().tolist()
    )
    if missing_transfer_targets:
        raise ValueError(
            f"Missing transfer payee ids for target accounts: {missing_transfer_targets}"
        )

    df["category_id"] = df["category_selected"].map(category_ids).astype("string").fillna("")
    unresolved_category = (~is_transfer) & (df["category_id"] == "")
    if unresolved_category.any():
        aliases = df.loc[unresolved_category, "category_selected"].map(_category_alias)
        df.loc[unresolved_category, "category_id"] = (
            aliases.map(category_alias_ids).astype("string").fillna("")
        )
    missing_categories = sorted(
        df.loc[~is_transfer & (df["category_id"] == ""), "category_selected"].unique().tolist()
    )
    if missing_categories:
        raise ValueError(f"Missing YNAB category ids for category_selected values: {missing_categories}")

    df["payee_id"] = ""
    df.loc[is_transfer, "payee_id"] = transfer_target_ids.loc[is_transfer]
    df["payee_name_upload"] = df["payee_selected"].where(~is_transfer, "")
    df.loc[is_transfer, "category_id"] = ""

    df["amount_milliunits"] = df.apply(_amount_milliunits, axis=1)
    occurrence_order = (
        df.reset_index()
        .sort_values(["account_id", "date", "amount_milliunits", "transaction_id", "index"])
        .copy()
    )
    occurrence_order["import_occurrence"] = (
        occurrence_order.groupby(["account_id", "date", "amount_milliunits"], dropna=False)
        .cumcount()
        .add(1)
    )
    occurrence_order["import_id"] = occurrence_order.apply(
        lambda row: (
            f"YNAB:{int(row['amount_milliunits'])}:{row['date']}:{int(row['import_occurrence'])}"
        ),
        axis=1,
    )
    occurrence_map = occurrence_order.set_index("index")["import_id"]
    df["import_id"] = df.index.to_series().map(occurrence_map).astype("string").fillna("")

    df["cleared"] = cleared
    df["approved"] = bool(approved)

    df["upload_kind"] = "regular"
    df.loc[is_transfer, "upload_kind"] = "transfer"
    return df[
        [
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
            "category_selected",
            "category_id",
            "cleared",
            "approved",
            "import_id",
            "upload_kind",
        ]
    ].copy()


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
