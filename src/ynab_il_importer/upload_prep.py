from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
from typing import Any

import pandas as pd
import polars as pl

import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.working_schema as working_schema
from ynab_il_importer.safe_types import normalize_flag_series


REQUIRED_REVIEW_COLUMNS = [
    "transaction_id",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "target_payee_selected",
    "target_category_selected",
    "decision_action",
    "reviewed",
]
_LEADING_SYMBOL_RE = re.compile(r"^[^\w\u0590-\u05FF]+")
_CREATE_TARGET_ACTION = "create_target"
TRANSACTION_UNIT_COLUMNS = [
    "upload_transaction_id",
    "source_row_count",
    "upload_kind",
    "unsupported_reason",
    "account_id",
    "account_name",
    "date",
    "amount_milliunits",
    "memo",
    "cleared",
    "approved",
    "import_id",
    "payee_id",
    "payee_name_upload",
    "category_id",
    "target_category_selected",
    "transfer_target_account_id",
    "subtransactions",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _is_selected_category(value: Any) -> bool:
    text = review_model.normalize_category_value(value)
    return bool(text) and not review_model.is_no_category_required(text)


def _amount_milliunits(row: pd.Series) -> int:
    outflow = float(pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce") or 0.0)
    inflow = float(pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce") or 0.0)
    return int(round((inflow - outflow) * 1000))


def _amount_milliunits_from_values(*, inflow_ils: Any, outflow_ils: Any) -> int:
    outflow = float(pd.to_numeric(pd.Series([outflow_ils]), errors="coerce").fillna(0.0).iloc[0])
    inflow = float(pd.to_numeric(pd.Series([inflow_ils]), errors="coerce").fillna(0.0).iloc[0])
    return int(round((inflow - outflow) * 1000))


def _nonzero_amount_mask(df: pd.DataFrame) -> pd.Series:
    outflow = pd.to_numeric(df["outflow_ils"], errors="coerce").fillna(0.0)
    inflow = pd.to_numeric(df["inflow_ils"], errors="coerce").fillna(0.0)
    return (outflow != 0.0) | (inflow != 0.0)


def _decision_action_mask(df: pd.DataFrame) -> pd.Series:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    action = _normalize_text_series(df["decision_action"]).str.casefold()
    reviewed = normalize_flag_series(df["reviewed"])
    return action.eq(_CREATE_TARGET_ACTION) & reviewed


def _target_payee_series(df: pd.DataFrame) -> pd.Series:
    if "target_payee_selected" in df.columns:
        return _normalize_text_series(df["target_payee_selected"])
    return _normalize_text_series(df["payee_selected"])


def _parent_target_payee_series(df: pd.DataFrame) -> pd.Series:
    if "parent_target_payee_selected" in df.columns:
        return _normalize_text_series(df["parent_target_payee_selected"])
    return _target_payee_series(df)


def _target_category_series(df: pd.DataFrame) -> pd.Series:
    if "target_category_selected" in df.columns:
        return _normalize_text_series(df["target_category_selected"]).map(
            review_model.normalize_category_value
        )
    return _normalize_text_series(df["category_selected"]).map(review_model.normalize_category_value)


def _memo_append_series(df: pd.DataFrame) -> pd.Series:
    return _normalize_text_series(
        df.get("memo_append", pd.Series([""] * len(df), index=df.index))
    )


def _combined_memo_series(df: pd.DataFrame) -> pd.Series:
    base = _normalize_text_series(df["memo"])
    extra = _memo_append_series(df)
    combined = base.copy()
    extra_only = combined.eq("") & extra.ne("")
    combined.loc[extra_only] = extra.loc[extra_only]
    both = combined.ne("") & extra.ne("")
    combined.loc[both] = combined.loc[both] + "\n" + extra.loc[both]
    return combined


def _canonical_context_text(row: pd.Series, *names: str) -> str:
    for name in names:
        value = _normalize_text(row.get(name, ""))
        if value:
            return value
    return ""


def _review_artifact_to_working_frame(
    reviewed_source: pd.DataFrame | Any,
) -> pd.DataFrame:
    original_index = reviewed_source.index if isinstance(reviewed_source, pd.DataFrame) else None
    if isinstance(reviewed_source, (str, Path)):
        working = review_io.project_review_artifact_to_working_dataframe(
            review_io.load_review_artifact(Path(reviewed_source))
        ).to_pandas()
    elif isinstance(reviewed_source, pd.DataFrame):
        working = working_schema.build_working_dataframe(
            pl.from_pandas(reviewed_source, include_index=False)
        ).to_pandas()
    else:
        working = review_io.project_review_artifact_to_working_dataframe(
            review_io.coerce_review_artifact_table(reviewed_source)
        ).to_pandas()
    if working.empty:
        return pd.DataFrame(columns=REQUIRED_REVIEW_COLUMNS)
    if original_index is not None and len(original_index) == len(working):
        working.index = original_index
    return working


def _normalize_split_records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        if isinstance(value, dict):
            return []
        try:
            value = list(value)
        except TypeError:
            return []
    return [line for line in value if isinstance(line, dict)]


def _explode_target_splits_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        row_payload = row.to_dict()
        target_current = row_payload.get("target_current_transaction", {})
        row_payload["parent_target_payee_selected"] = _normalize_text(
            row_payload.get("target_payee_selected", "")
        )
        row_payload["parent_memo"] = _normalize_text(
            target_current.get("memo", "") if isinstance(target_current, dict) else ""
        ) or _normalize_text(row_payload.get("memo", ""))
        row_payload["upload_is_split"] = False
        split_lines = _normalize_split_records(row_payload.get("target_splits"))
        if not split_lines:
            row_payload["subtransaction_memo"] = ""
            rows.append(row_payload)
            continue

        row_payload["upload_is_split"] = True
        for line in split_lines:
            split_row = dict(row_payload)
            split_row["target_payee_selected"] = _normalize_text(line.get("payee_raw", ""))
            split_row["target_category_selected"] = review_model.normalize_category_value(
                line.get("category_raw", "")
            )
            split_row["target_category_id"] = _normalize_text(line.get("category_id", ""))
            split_row["memo"] = row_payload["parent_memo"]
            split_row["subtransaction_memo"] = _normalize_text(line.get("memo", ""))
            split_row["inflow_ils"] = float(line.get("inflow_ils", 0.0) or 0.0)
            split_row["outflow_ils"] = float(line.get("outflow_ils", 0.0) or 0.0)
            rows.append(split_row)

    return pd.DataFrame(rows, columns=list(dict.fromkeys([*df.columns, "parent_target_payee_selected", "parent_memo", "subtransaction_memo", "upload_is_split"])))


def _transfer_target(payee: str) -> str:
    if not review_model.is_transfer_payee(payee):
        return ""
    _, _, target = payee.partition(":")
    return target.strip()


def _source_import_id(row: pd.Series) -> str:
    bank_txn_id = _normalize_text(row.get("bank_txn_id", ""))
    if not bank_txn_id:
        source_system = _normalize_text(
            row.get("source", row.get("source_source_system", ""))
        ).casefold()
        source_transaction_id = _normalize_text(
            row.get("source_transaction_id", row.get("transaction_id", ""))
        )
        if source_system == "bank" and source_transaction_id:
            bank_txn_id = source_transaction_id
    if not bank_txn_id:
        card_txn_id = _normalize_text(row.get("card_txn_id", ""))
        if not card_txn_id:
            source_system = _normalize_text(
                row.get("source", row.get("source_source_system", ""))
            ).casefold()
            source_transaction_id = _normalize_text(
                row.get("source_transaction_id", row.get("transaction_id", ""))
            )
            if source_system == "card" and source_transaction_id:
                card_txn_id = source_transaction_id
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
    reviewed_source: pd.DataFrame | Any, accounts: list[dict[str, Any]]
) -> pd.Series:
    df = _review_artifact_to_working_frame(reviewed_source)
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    account_ids, _ = _account_lookup(accounts)
    account_names = _normalize_text_series(df["account_name"])
    upload_mask = _decision_action_mask(df)
    account_ok = account_names.isin(set(account_ids.keys()))
    return (~upload_mask) | account_ok


def _category_lookup(categories_df: pd.DataFrame) -> dict[str, str]:
    active = categories_df.copy()
    if "hidden" in active.columns:
        active = active[~normalize_flag_series(active["hidden"])]

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
        active = active[~normalize_flag_series(active["hidden"])]

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


def _uncategorized_category_id(categories_df: pd.DataFrame) -> str:
    category_ids = _category_lookup(categories_df)
    if "Uncategorized" in category_ids:
        return category_ids["Uncategorized"]
    category_alias_ids = _category_alias_lookup(categories_df)
    return category_alias_ids.get("uncategorized", "")


def _upload_rows_for_validation(reviewed_source: pd.DataFrame | Any) -> pd.DataFrame:
    df = _review_artifact_to_working_frame(reviewed_source)
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    upload_df = df.loc[_decision_action_mask(df)].copy()
    if upload_df.empty:
        return upload_df
    upload_df["upload_row_position"] = range(len(upload_df))
    upload_df["target_payee_selected"] = _target_payee_series(upload_df)
    upload_df["target_category_selected"] = _target_category_series(upload_df)
    upload_df["memo"] = _combined_memo_series(upload_df)
    for col in ["outflow_ils", "inflow_ils"]:
        upload_df[col] = pd.to_numeric(upload_df[col], errors="coerce").fillna(0.0).round(2)
    return _explode_target_splits_for_upload(upload_df)


def validate_ready_for_upload(reviewed_source: pd.DataFrame | Any) -> None:
    upload_df = _upload_rows_for_validation(reviewed_source)
    if upload_df.empty:
        return

    payee = _parent_target_payee_series(upload_df)
    category = _target_category_series(upload_df)
    transfer = _target_payee_series(upload_df).map(review_model.is_transfer_payee)
    category_selected = category.map(_is_selected_category)
    nonzero_amount = _nonzero_amount_mask(upload_df)

    missing_payee = upload_df.index[payee == ""].tolist()
    missing_category = upload_df.index[(~category_selected) & ~transfer].tolist()
    zero_amount = upload_df.index[~nonzero_amount].tolist()
    if missing_payee or missing_category or zero_amount:
        raise ValueError(
            "Rows selected for create_target are not ready for upload: "
            f"{len(missing_payee)} rows missing payee, "
            f"{len(missing_category)} rows missing category, "
            f"{len(zero_amount)} rows with zero amount."
        )


def ready_mask(reviewed_source: pd.DataFrame | Any) -> pd.Series:
    df = _review_artifact_to_working_frame(reviewed_source)
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    upload_mask = _decision_action_mask(df)
    if not upload_mask.any():
        return upload_mask

    upload_df = _upload_rows_for_validation(df)
    payee = _parent_target_payee_series(upload_df)
    category = _target_category_series(upload_df)
    transfer = _target_payee_series(upload_df).map(review_model.is_transfer_payee)
    category_selected = category.map(_is_selected_category)
    nonzero_amount = _nonzero_amount_mask(upload_df)
    upload_row_ready = (payee != "") & (category_selected | transfer) & nonzero_amount

    if "upload_row_position" not in upload_df.columns:
        return upload_mask & False

    row_readiness = upload_row_ready.groupby(upload_df["upload_row_position"], dropna=False).all()
    result = pd.Series(False, index=df.index, dtype=bool)
    positions = pd.Series(range(len(df)), index=df.index)
    result.loc[upload_mask] = positions.loc[upload_mask].map(row_readiness).fillna(False)
    return result


def prepare_upload_transactions(
    reviewed_source: pd.DataFrame | Any,
    *,
    accounts: list[dict[str, Any]],
    categories_df: pd.DataFrame,
    cleared: str = "cleared",
    approved: bool = False,
) -> pd.DataFrame:
    reviewed_df = _review_artifact_to_working_frame(reviewed_source)
    validate_ready_for_upload(reviewed_df)

    df = reviewed_df.loc[_decision_action_mask(reviewed_df)].copy()
    if df.empty:
        return pd.DataFrame()

    df["upload_row_position"] = range(len(df))
    for col in [
        "transaction_id",
        "account_name",
        "date",
        "target_payee_selected",
        "target_category_selected",
        "decision_action",
    ]:
        df[col] = _normalize_text_series(df[col])
    df["target_category_selected"] = df["target_category_selected"].map(
        review_model.normalize_category_value
    )
    df["memo"] = _combined_memo_series(df)
    for col in ["outflow_ils", "inflow_ils"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)
    df["upload_transaction_id"] = _normalize_text_series(df["transaction_id"])
    blank_upload_id = df["upload_transaction_id"] == ""
    if blank_upload_id.any():
        generated = pd.Series(
            [f"upload_txn_{idx}" for idx in range(len(df))],
            index=df.index,
            dtype="string",
        )
        df.loc[blank_upload_id, "upload_transaction_id"] = generated.loc[blank_upload_id]
    df["import_amount_milliunits"] = df.apply(_amount_milliunits, axis=1)
    df = _explode_target_splits_for_upload(df)

    account_ids, transfer_payees = _account_lookup(accounts)
    category_ids = _category_lookup(categories_df)
    category_alias_ids = _category_alias_lookup(categories_df)
    uncategorized_category_id = _uncategorized_category_id(categories_df)

    df["account_id"] = df["account_name"].map(account_ids).astype("string").fillna("")
    missing_accounts = sorted(
        df.loc[df["account_id"] == "", "account_name"].unique().tolist()
    )
    if missing_accounts:
        raise ValueError(
            f"Missing YNAB account ids for account_name values: {missing_accounts}"
        )

    df["transfer_target"] = df["target_payee_selected"].map(_transfer_target)
    is_transfer = df["target_payee_selected"].map(review_model.is_transfer_payee)
    df["parent_transfer_target"] = _parent_target_payee_series(df).map(_transfer_target)
    parent_is_transfer = _parent_target_payee_series(df).map(review_model.is_transfer_payee)

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
    parent_transfer_target_ids = (
        df["parent_transfer_target"].map(transfer_payees).astype("string").fillna("")
    )

    upload_category = df["target_category_selected"].where(
        ~df["target_category_selected"].map(review_model.is_no_category_required),
        "",
    )
    df["category_id"] = _normalize_text_series(
        df.get("target_category_id", pd.Series([""] * len(df), index=df.index))
    )
    missing_selected_ids = (~is_transfer) & upload_category.ne("") & df["category_id"].ne("")
    invalid_selected_ids = missing_selected_ids & ~df["category_id"].isin(set(category_ids.values()))
    if invalid_selected_ids.any():
        invalid_ids = sorted(df.loc[invalid_selected_ids, "category_id"].unique().tolist())
        raise ValueError(f"Unknown YNAB category ids in reviewed upload rows: {invalid_ids}")
    df.loc[df["category_id"] == "", "category_id"] = (
        upload_category.loc[df["category_id"] == ""].map(category_ids).astype("string").fillna("")
    )
    unresolved_category = (~is_transfer) & (df["category_id"] == "")
    if unresolved_category.any():
        aliases = upload_category.loc[unresolved_category].map(_category_alias)
        df.loc[unresolved_category, "category_id"] = (
            aliases.map(category_alias_ids).astype("string").fillna("")
        )
    unresolved_category = (~is_transfer) & (df["category_id"] == "")
    if unresolved_category.any() and uncategorized_category_id:
        df.loc[unresolved_category, "category_id"] = uncategorized_category_id
    missing_categories = sorted(
        upload_category.loc[~is_transfer & (df["category_id"] == "")]
        .unique()
        .tolist()
    )
    if missing_categories:
        raise ValueError(
            f"Missing YNAB category ids for target_category_selected values: {missing_categories}"
        )

    df["payee_id"] = ""
    df.loc[is_transfer, "payee_id"] = transfer_target_ids.loc[is_transfer]
    df["payee_name_upload"] = df["target_payee_selected"].where(~is_transfer, "")
    df.loc[is_transfer, "category_id"] = ""
    df["transfer_target_account_id"] = ""
    df.loc[is_transfer, "transfer_target_account_id"] = transfer_target_account_ids.loc[
        is_transfer
    ]
    df["parent_payee_id"] = ""
    df.loc[parent_is_transfer, "parent_payee_id"] = parent_transfer_target_ids.loc[parent_is_transfer]
    df["parent_payee_name_upload"] = _parent_target_payee_series(df).where(
        ~parent_is_transfer, ""
    )

    df["amount_milliunits"] = df.apply(_amount_milliunits, axis=1)
    df["bank_txn_id"] = (
        df.get("bank_txn_id", pd.Series([""] * len(df), index=df.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    missing_bank_ids = (
        df["bank_txn_id"].eq("")
        & _normalize_text_series(df.get("source", pd.Series([""] * len(df), index=df.index))).str.casefold().eq("bank")
    )
    if missing_bank_ids.any():
        df.loc[missing_bank_ids, "bank_txn_id"] = _normalize_text_series(
            df.get("source_transaction_id", pd.Series([""] * len(df), index=df.index))
        ).loc[missing_bank_ids]
    df["card_txn_id"] = (
        df.get("card_txn_id", pd.Series([""] * len(df), index=df.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    missing_card_ids = (
        df["card_txn_id"].eq("")
        & _normalize_text_series(df.get("source", pd.Series([""] * len(df), index=df.index))).str.casefold().eq("card")
    )
    if missing_card_ids.any():
        df.loc[missing_card_ids, "card_txn_id"] = _normalize_text_series(
            df.get("source_transaction_id", pd.Series([""] * len(df), index=df.index))
        ).loc[missing_card_ids]
    occurrence_order = (
        df[
            [
                "upload_row_position",
                "account_id",
                "date",
                "import_amount_milliunits",
                "transaction_id",
                "upload_transaction_id",
                "bank_txn_id",
                "card_txn_id",
            ]
        ]
        .drop_duplicates(subset=["upload_row_position"])
        .sort_values(
            [
                "account_id",
                "date",
                "import_amount_milliunits",
                "transaction_id",
                "upload_transaction_id",
                "upload_row_position",
            ]
        )
        .copy()
    )
    occurrence_order["import_occurrence"] = (
        occurrence_order.groupby(
            ["account_id", "date", "import_amount_milliunits"], dropna=False
        )
        .cumcount()
        .add(1)
    )
    occurrence_order["import_id"] = occurrence_order.apply(
        lambda row: _source_import_id(row)
        or f"YNAB:{int(row['import_amount_milliunits'])}:{row['date']}:{int(row['import_occurrence'])}",
        axis=1,
    )
    occurrence_map = occurrence_order.set_index("upload_row_position")["import_id"]
    df["import_id"] = (
        df["upload_row_position"].map(occurrence_map).astype("string").fillna("")
    )

    df["cleared"] = cleared
    df["approved"] = bool(approved)

    df["upload_kind"] = "regular"
    df.loc[is_transfer, "upload_kind"] = "transfer"
    columns = [
        "upload_transaction_id",
        "transaction_id",
        "decision_action",
        "account_name",
        "account_id",
        "date",
        "outflow_ils",
        "inflow_ils",
        "amount_milliunits",
        "memo",
        "parent_memo",
        "subtransaction_memo",
        "target_payee_selected",
        "payee_name_upload",
        "payee_id",
        "parent_payee_name_upload",
        "parent_payee_id",
        "transfer_target",
        "transfer_target_account_id",
        "target_category_selected",
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
        "workflow_type",
        "match_status",
        "max_sheet",
        "max_txn_type",
        "max_original_amount",
        "max_original_currency",
        "max_report_period",
        "max_report_scope",
    ]
    columns.extend([col for col in optional_columns if col in df.columns])
    return df[columns].copy()


def assemble_upload_transaction_units(prepared_df: pd.DataFrame) -> pd.DataFrame:
    if prepared_df.empty:
        return pd.DataFrame(columns=TRANSACTION_UNIT_COLUMNS)

    required = ["upload_kind", "account_id", "date", "amount_milliunits", "import_id"]
    _validate_columns(prepared_df, required)

    prepared = prepared_df.copy()
    if "upload_transaction_id" not in prepared.columns:
        prepared["upload_transaction_id"] = _normalize_text_series(
            prepared.get("transaction_id", pd.Series([""] * len(prepared), index=prepared.index))
        )
        blank_mask = prepared["upload_transaction_id"] == ""
        if blank_mask.any():
            generated = pd.Series(
                [f"upload_txn_{idx}" for idx in range(len(prepared))],
                index=prepared.index,
                dtype="string",
            )
            prepared.loc[blank_mask, "upload_transaction_id"] = generated.loc[blank_mask]
    if "account_name" not in prepared.columns:
        prepared["account_name"] = ""
    if "memo" not in prepared.columns:
        prepared["memo"] = ""
    if "parent_memo" not in prepared.columns:
        prepared["parent_memo"] = prepared["memo"]
    if "subtransaction_memo" not in prepared.columns:
        prepared["subtransaction_memo"] = prepared["memo"]
    if "cleared" not in prepared.columns:
        prepared["cleared"] = "cleared"
    if "approved" not in prepared.columns:
        prepared["approved"] = False
    if "payee_id" not in prepared.columns:
        prepared["payee_id"] = ""
    if "payee_name_upload" not in prepared.columns:
        prepared["payee_name_upload"] = ""
    if "parent_payee_id" not in prepared.columns:
        prepared["parent_payee_id"] = prepared["payee_id"]
    if "parent_payee_name_upload" not in prepared.columns:
        prepared["parent_payee_name_upload"] = prepared["payee_name_upload"]
    if "category_id" not in prepared.columns:
        prepared["category_id"] = ""
    if "target_category_selected" not in prepared.columns:
        prepared["target_category_selected"] = ""
    if "transfer_target_account_id" not in prepared.columns:
        prepared["transfer_target_account_id"] = ""
    prepared["upload_transaction_id"] = _normalize_text_series(prepared["upload_transaction_id"])
    prepared["upload_kind"] = _normalize_text_series(prepared["upload_kind"])
    prepared["account_id"] = _normalize_text_series(prepared["account_id"])
    prepared["account_name"] = _normalize_text_series(prepared["account_name"])
    prepared["date"] = _normalize_text_series(prepared["date"])
    prepared["memo"] = _normalize_text_series(prepared["memo"])
    prepared["parent_memo"] = _normalize_text_series(prepared["parent_memo"])
    prepared["subtransaction_memo"] = _normalize_text_series(prepared["subtransaction_memo"])
    prepared["cleared"] = _normalize_text_series(prepared["cleared"])
    prepared["import_id"] = _normalize_text_series(prepared["import_id"])
    prepared["payee_id"] = _normalize_text_series(prepared["payee_id"])
    prepared["payee_name_upload"] = _normalize_text_series(prepared["payee_name_upload"])
    prepared["parent_payee_id"] = _normalize_text_series(prepared["parent_payee_id"])
    prepared["parent_payee_name_upload"] = _normalize_text_series(prepared["parent_payee_name_upload"])
    prepared["category_id"] = _normalize_text_series(prepared["category_id"])
    prepared["target_category_selected"] = _normalize_text_series(
        prepared["target_category_selected"]
    )
    prepared["transfer_target_account_id"] = _normalize_text_series(
        prepared["transfer_target_account_id"]
    )
    prepared["approved"] = normalize_flag_series(prepared["approved"])
    prepared["amount_milliunits"] = (
        pd.to_numeric(prepared["amount_milliunits"], errors="coerce").fillna(0).astype(int)
    )

    unit_rows: list[dict[str, Any]] = []
    for upload_transaction_id, group in prepared.groupby(
        "upload_transaction_id", sort=False, dropna=False
    ):
        first = group.iloc[0]
        is_split = len(group) > 1
        unsupported_reason = ""
        unit_kind = _normalize_text(first.get("upload_kind", ""))
        parent_category_id = _normalize_text(first.get("category_id", ""))
        parent_target_category = _normalize_text(first.get("target_category_selected", ""))
        subtransactions: list[dict[str, Any]] = []

        if is_split:
            unit_kind = "split"
            if (
                group["upload_kind"].astype("string").fillna("").str.strip() == "transfer"
            ).any() or group["transfer_target_account_id"].astype("string").fillna("").str.strip().ne("").any() or group["parent_payee_id"].astype("string").fillna("").str.strip().ne("").any():
                unsupported_reason = "split_transfer_unsupported"
            elif group["category_id"].astype("string").fillna("").str.strip().eq("").any():
                unsupported_reason = "split_missing_category"
            parent_category_id = ""
            parent_target_category = ""
            for _, split_row in group.iterrows():
                split_payload: dict[str, Any] = {
                    "amount": int(split_row.get("amount_milliunits", 0) or 0),
                    "memo": _normalize_text(split_row.get("subtransaction_memo", "")) or None,
                    "category_id": _normalize_text(split_row.get("category_id", "")),
                }
                split_payee_id = _normalize_text(split_row.get("payee_id", ""))
                split_payee_name = _normalize_text(split_row.get("payee_name_upload", ""))
                if split_payee_id:
                    split_payload["payee_id"] = split_payee_id
                elif split_payee_name:
                    split_payload["payee_name"] = split_payee_name
                subtransactions.append(split_payload)

        unit_amount = int(group["amount_milliunits"].sum()) if is_split else int(first.get("amount_milliunits", 0) or 0)
        unit_rows.append(
            {
                "upload_transaction_id": _normalize_text(upload_transaction_id),
                "source_row_count": len(group),
                "upload_kind": unit_kind,
                "unsupported_reason": unsupported_reason,
                "account_id": _normalize_text(first.get("account_id", "")),
                "account_name": _normalize_text(first.get("account_name", "")),
                "date": _normalize_text(first.get("date", "")),
                "amount_milliunits": unit_amount,
                "memo": _normalize_text(first.get("parent_memo", first.get("memo", ""))),
                "cleared": _normalize_text(first.get("cleared", "")) or "cleared",
                "approved": bool(first.get("approved", False)),
                "import_id": _normalize_text(first.get("import_id", "")),
                "payee_id": _normalize_text(first.get("parent_payee_id", first.get("payee_id", ""))),
                "payee_name_upload": _normalize_text(
                    first.get("parent_payee_name_upload", first.get("payee_name_upload", ""))
                ),
                "category_id": parent_category_id,
                "target_category_selected": parent_target_category,
                "transfer_target_account_id": _normalize_text(
                    first.get("transfer_target_account_id", "")
                ),
                "subtransactions": subtransactions,
            }
        )

    units = pd.DataFrame(unit_rows, columns=TRANSACTION_UNIT_COLUMNS)
    duplicate_ids = units["upload_transaction_id"].duplicated(keep=False)
    if duplicate_ids.any():
        duplicates = sorted(units.loc[duplicate_ids, "upload_transaction_id"].unique().tolist())
        raise ValueError(f"Duplicate upload_transaction_id values: {duplicates}")
    return units


def upload_payload_records(prepared_df: pd.DataFrame) -> list[dict[str, Any]]:
    units = assemble_upload_transaction_units(prepared_df)
    records: list[dict[str, Any]] = []
    for _, row in units.iterrows():
        unsupported_reason = _normalize_text(row.get("unsupported_reason", ""))
        if unsupported_reason:
            raise ValueError(
                "Unsupported upload transaction unit "
                f"{_normalize_text(row.get('upload_transaction_id', ''))}: {unsupported_reason}"
            )
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
        upload_kind = _normalize_text(row.get("upload_kind", ""))
        if upload_kind == "split":
            subtransactions = row.get("subtransactions", []) or []
            if payee_id:
                payload["payee_id"] = payee_id
            elif payee_name:
                payload["payee_name"] = payee_name
            payload["category_id"] = None
            payload["subtransactions"] = subtransactions
        else:
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


def _response_transaction_lookup(
    transactions: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for txn in transactions or []:
        account_id = _normalize_text(txn.get("account_id", ""))
        import_id = _normalize_text(txn.get("import_id", ""))
        if not account_id or not import_id:
            continue
        lookup[(account_id, import_id)] = txn
    return lookup


def _normalize_split_line_for_compare(line: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "amount": int(line.get("amount", 0) or 0),
        "memo": _normalize_text(line.get("memo", "")),
        "category_id": _normalize_text(line.get("category_id", "")),
        "payee_id": _normalize_text(line.get("payee_id", "")),
        "payee_name": _normalize_text(line.get("payee_name", "")),
    }
    return payload


def upload_preflight(
    prepared_df: pd.DataFrame,
    existing_transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    prepared = assemble_upload_transaction_units(prepared_df)
    prepared["date_key"] = pd.to_datetime(prepared["date"], errors="coerce")

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
    split_mask = prepared["upload_kind"] == "split"
    unsupported_transaction_unit_ids = sorted(
        prepared.loc[
            prepared["unsupported_reason"].astype("string").fillna("").str.strip() != "",
            "upload_transaction_id",
        ].tolist()
    )

    return {
        "prepared_count": len(prepared),
        "transfer_count": int(is_transfer.sum()),
        "split_count": int(split_mask.sum()),
        "payload_duplicate_import_keys": payload_duplicate_keys,
        "existing_import_id_hits": existing_import_id_hits,
        "potential_match_import_ids": potential_match_import_ids,
        "transfer_payload_issue_ids": transfer_payload_issue_ids,
        "unsupported_transaction_unit_ids": unsupported_transaction_unit_ids,
    }


def summarize_upload_response(response: dict[str, Any]) -> dict[str, int]:
    transactions = response.get("transactions", []) or []
    saved_ids = response.get("transaction_ids", []) or []
    duplicate_ids = response.get("duplicate_import_ids", []) or []
    tx_df = _transactions_frame(transactions)

    matched_existing = 0
    transfer_saved = 0
    split_saved = 0
    if not tx_df.empty:
        matched_existing = int((tx_df["matched_transaction_id"] != "").sum())
        transfer_saved = int((tx_df["transfer_account_id"] != "").sum())
        split_saved = int(
            tx_df["category_name"].astype("string").fillna("").str.strip().eq("Split").sum()
        )

    return {
        "saved": len(saved_ids),
        "duplicate_import_ids": len(duplicate_ids),
        "matched_existing": matched_existing,
        "transfer_saved": transfer_saved,
        "split_saved": split_saved,
    }


def classify_upload_result(
    summary: dict[str, int], *, prepared_count: int
) -> dict[str, Any]:
    saved = int(summary.get("saved", 0))
    duplicate_import_ids = int(summary.get("duplicate_import_ids", 0))
    matched_existing = int(summary.get("matched_existing", 0))
    transfer_saved = int(summary.get("transfer_saved", 0))
    split_saved = int(summary.get("split_saved", 0))

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
        "split_saved": split_saved,
        "idempotent_rerun": idempotent_rerun,
        "verification_needed": verification_needed,
        "status": status,
    }


def verify_upload_response(
    prepared_df: pd.DataFrame,
    response: dict[str, Any],
) -> dict[str, Any]:
    prepared = assemble_upload_transaction_units(prepared_df)

    response_transactions = response.get("transactions", []) or []
    response_df = _transactions_frame(response_transactions)
    if response_df.empty:
        return {
            "checked": 0,
            "missing_saved_transactions": [],
            "amount_mismatches": [],
            "date_mismatches": [],
            "account_mismatches": [],
            "transfer_mismatches": [],
            "category_mismatches": [],
            "split_mismatches": [],
        }

    response_df = response_df[response_df["import_id"] != ""].copy()
    response_lookup = _response_transaction_lookup(response_transactions)
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
    split_mismatches: list[str] = []

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
        prepared_category_name = str(prepared_row.get("target_category_selected", "") or "")
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

        if str(prepared_row["upload_kind"]) == "split":
            response_txn = response_lookup.get((account_id, import_id), {})
            response_category_name = _normalize_text(response_txn.get("category_name", ""))
            response_subtransactions = response_txn.get("subtransactions", []) or []
            prepared_subtransactions = prepared_row.get("subtransactions", []) or []
            if response_category_name != "Split":
                split_mismatches.append(label)
                continue
            if len(prepared_subtransactions) != len(response_subtransactions):
                split_mismatches.append(label)
                continue
            prepared_lines = [
                _normalize_split_line_for_compare(line)
                for line in prepared_subtransactions
            ]
            response_lines = []
            for line in response_subtransactions:
                response_lines.append(
                    _normalize_split_line_for_compare(
                        {
                            "amount": line.get("amount", 0),
                            "memo": line.get("memo", ""),
                            "category_id": line.get("category_id", ""),
                            "payee_id": line.get("payee_id", ""),
                            "payee_name": line.get("payee_name", ""),
                        }
                    )
                )
            if prepared_lines != response_lines:
                split_mismatches.append(label)

    return {
        "checked": len(shared_keys),
        "missing_saved_transactions": missing_saved,
        "amount_mismatches": sorted(amount_mismatches),
        "date_mismatches": sorted(date_mismatches),
        "account_mismatches": sorted(account_mismatches),
        "transfer_mismatches": sorted(transfer_mismatches),
        "category_mismatches": sorted(category_mismatches),
        "split_mismatches": sorted(split_mismatches),
    }







