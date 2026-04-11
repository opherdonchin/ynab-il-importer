from __future__ import annotations

from collections import Counter
import math
from pathlib import Path
import re
from typing import Any, Mapping

import polars as pl

import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.working_schema as working_schema


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
_UPDATE_TARGET_ACTION = "update_target"
_UPLOAD_DECISION_ACTIONS = {_CREATE_TARGET_ACTION, _UPDATE_TARGET_ACTION}
_TRUE_VALUES = ("1", "true", "t", "yes", "y")
TRANSACTION_UNIT_COLUMNS = [
    "upload_transaction_id",
    "source_row_count",
    "decision_action",
    "existing_transaction_id",
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


def _text_expr(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()


def _text_expr_or_default(
    df: pl.DataFrame, name: str, default: str = ""
) -> pl.Expr:
    if name in df.columns:
        return _text_expr(name)
    return pl.lit(default, dtype=pl.String)


def _float_expr(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Float64, strict=False).fill_null(0.0)


def _flag_expr(name: str) -> pl.Expr:
    return (
        pl.col(name)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_lowercase()
        .is_in(_TRUE_VALUES)
    )


def _bool_expr_or_default(
    df: pl.DataFrame, name: str, default: bool = False
) -> pl.Expr:
    if name in df.columns:
        return _flag_expr(name)
    return pl.lit(default, dtype=pl.Boolean)


def _target_payee_expr(df: pl.DataFrame) -> pl.Expr:
    if "target_payee_selected" in df.columns:
        return _text_expr("target_payee_selected")
    return _text_expr_or_default(df, "payee_selected")


def _parent_target_payee_expr(df: pl.DataFrame) -> pl.Expr:
    if "parent_target_payee_selected" in df.columns:
        return _text_expr("parent_target_payee_selected")
    return _target_payee_expr(df)


def _target_category_expr(df: pl.DataFrame) -> pl.Expr:
    if "target_category_selected" in df.columns:
        source = _text_expr("target_category_selected")
    else:
        source = _text_expr_or_default(df, "category_selected")
    return source.map_elements(
        review_model.normalize_category_value,
        return_dtype=pl.String,
    )


def _combined_memo_expr(df: pl.DataFrame) -> pl.Expr:
    base = _text_expr_or_default(df, "memo")
    extra = _text_expr_or_default(df, "memo_append")
    return (
        pl.when(base.eq("") & extra.ne(""))
        .then(extra)
        .when(base.ne("") & extra.ne(""))
        .then(pl.concat_str([base, extra], separator="\n"))
        .otherwise(base)
    )


def _target_payee_series(df: pl.DataFrame) -> pl.Series:
    return df.select(_target_payee_expr(df).alias("target_payee_selected")).to_series()


def _parent_target_payee_series(df: pl.DataFrame) -> pl.Series:
    return df.select(
        _parent_target_payee_expr(df).alias("parent_target_payee_selected")
    ).to_series()


def _target_category_series(df: pl.DataFrame) -> pl.Series:
    return df.select(
        _target_category_expr(df).alias("target_category_selected")
    ).to_series()


def _is_selected_category(value: Any) -> bool:
    text = review_model.normalize_category_value(value)
    return bool(text) and not review_model.is_no_category_required(text)


def _numeric_value(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, float):
        return 0.0 if math.isnan(value) else float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        parsed = float(text)
    except ValueError:
        return 0.0
    return 0.0 if math.isnan(parsed) else parsed


def _amount_milliunits(row: Mapping[str, Any]) -> int:
    outflow = _numeric_value(row.get("outflow_ils", 0.0))
    inflow = _numeric_value(row.get("inflow_ils", 0.0))
    return int(round((inflow - outflow) * 1000))


def _amount_milliunits_from_values(*, inflow_ils: Any, outflow_ils: Any) -> int:
    outflow = _numeric_value(outflow_ils)
    inflow = _numeric_value(inflow_ils)
    return int(round((inflow - outflow) * 1000))


def _amount_milliunits_expr(*, inflow_col: str, outflow_col: str) -> pl.Expr:
    return ((_float_expr(inflow_col) - _float_expr(outflow_col)) * 1000).round(0).cast(
        pl.Int64
    )


def _nonzero_amount_mask(df: pl.DataFrame) -> pl.Series:
    return df.select(
        ((_float_expr("outflow_ils") != 0.0) | (_float_expr("inflow_ils") != 0.0)).alias(
            "nonzero_amount"
        )
    ).to_series()


def _decision_action_mask(df: pl.DataFrame) -> pl.Series:
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    return df.select(
        (
            _text_expr("decision_action").str.to_lowercase().is_in(_UPLOAD_DECISION_ACTIONS)
            & _flag_expr("reviewed")
        ).alias("decision_action_mask")
    ).to_series()

def _canonical_context_text(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = _normalize_text(row.get(name, ""))
        if value:
            return value
    return ""


def load_upload_working_frame(
    reviewed_artifact_path: str | Path | pl.DataFrame,
) -> pl.DataFrame:
    if isinstance(reviewed_artifact_path, pl.DataFrame):
        return working_schema.build_working_dataframe(reviewed_artifact_path)
    return review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(reviewed_artifact_path)
    )


def _normalize_split_records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        if isinstance(value, dict):
            return []
        try:
            value = list(value)
        except TypeError:
            return []
    return [line for line in value if isinstance(line, dict)]


def _explode_target_splits_for_upload(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.clone()

    rows: list[dict[str, Any]] = []
    for row_payload in df.rows(named=True):
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

    expanded = pl.from_dicts(rows, infer_schema_length=None)
    ordered_columns = list(
        dict.fromkeys(
            [
                *df.columns,
                "parent_target_payee_selected",
                "parent_memo",
                "subtransaction_memo",
                "upload_is_split",
            ]
        )
    )
    return expanded.select([col for col in ordered_columns if col in expanded.columns])


def _transfer_target(payee: str) -> str:
    if not review_model.is_transfer_payee(payee):
        return ""
    _, _, target = payee.partition(":")
    return target.strip()


def _source_import_id(row: Mapping[str, Any]) -> str:
    source_import_id = _normalize_text(row.get("source_import_id", ""))
    if source_import_id and bank_identity.is_bank_txn_id(source_import_id):
        return bank_identity.validate_bank_txn_id(source_import_id)
    if source_import_id and card_identity.is_card_txn_id(source_import_id):
        return card_identity.validate_card_txn_id(source_import_id)
    bank_txn_id = _normalize_text(row.get("bank_txn_id", ""))
    if not bank_txn_id:
        bank_txn_id = _normalize_text(row.get("source_bank_txn_id", ""))
    if not bank_txn_id:
        source_system = _normalize_text(
            row.get("source", row.get("source_source_system", ""))
        ).casefold()
        source_transaction_id = _normalize_text(
            row.get("source_transaction_id", row.get("transaction_id", ""))
        )
        if source_system == "bank" and source_transaction_id:
            bank_txn_id = source_transaction_id
    if bank_txn_id and bank_identity.is_bank_txn_id(bank_txn_id):
        return bank_identity.validate_bank_txn_id(bank_txn_id)
    if not bank_txn_id:
        card_txn_id = _normalize_text(row.get("card_txn_id", ""))
        if not card_txn_id:
            card_txn_id = _normalize_text(row.get("source_card_txn_id", ""))
        if not card_txn_id:
            source_system = _normalize_text(
                row.get("source", row.get("source_source_system", ""))
            ).casefold()
            source_transaction_id = _normalize_text(
                row.get("source_transaction_id", row.get("transaction_id", ""))
            )
            if source_system == "card" and source_transaction_id:
                card_txn_id = source_transaction_id
        if card_txn_id and card_identity.is_card_txn_id(card_txn_id):
            return card_identity.validate_card_txn_id(card_txn_id)
    return ""


def _validate_columns(df: pl.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _category_frame(categories_df: Any) -> pl.DataFrame:
    if isinstance(categories_df, pl.DataFrame):
        return categories_df
    if type(categories_df).__module__.startswith("pandas"):
        return pl.from_pandas(categories_df)
    raise TypeError("categories_df must be a polars DataFrame or pandas DataFrame")


def _visible_categories(categories_df: pl.DataFrame) -> pl.DataFrame:
    if "hidden" not in categories_df.columns:
        return categories_df
    hidden_mask = [
        bool(value)
        if isinstance(value, bool)
        else str(value or "").strip().casefold() in _TRUE_VALUES
        for value in categories_df["hidden"].to_list()
    ]
    return categories_df.filter(~pl.Series(hidden_mask, dtype=pl.Boolean))


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


def _account_on_budget_lookup(accounts: list[dict[str, Any]]) -> dict[str, bool]:
    return {
        _normalize_text(acc.get("name", "")): bool(acc.get("on_budget", False))
        for acc in accounts
        if not bool(acc.get("deleted", False)) and _normalize_text(acc.get("name", ""))
    }


def _lookup_optional_account_budget(
    account_budget_lookup: dict[str, bool],
    account_name: Any,
) -> bool | None:
    normalized = _normalize_text(account_name)
    if not normalized or normalized not in account_budget_lookup:
        return None
    return bool(account_budget_lookup[normalized])


def _category_required_expr(
    df: pl.DataFrame,
    *,
    payee_col: str,
    account_col: str,
    account_budget_lookup: dict[str, bool],
) -> pl.Expr:
    return pl.struct([payee_col, account_col]).map_elements(
        lambda row: review_model.category_required_for_payee(
            row.get(payee_col, ""),
            current_account_on_budget=_lookup_optional_account_budget(
                account_budget_lookup, row.get(account_col, "")
            ),
            transfer_target_on_budget=_lookup_optional_account_budget(
                account_budget_lookup,
                review_model.transfer_target_account_name(row.get(payee_col, "")),
            ),
        ),
        return_dtype=pl.Boolean,
    )


def uploadable_account_mask(
    working_df: pl.DataFrame, accounts: list[dict[str, Any]]
) -> pl.Series:
    df = working_schema.build_working_dataframe(working_df)
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    account_ids, _ = _account_lookup(accounts)
    account_names = df.select(_text_expr("account_name").alias("account_name")).to_series()
    upload_mask = _decision_action_mask(df)
    account_ok = account_names.is_in(list(account_ids.keys()))
    return (~upload_mask) | account_ok


def _category_lookup(categories_df: pl.DataFrame) -> dict[str, str]:
    active = _visible_categories(_category_frame(categories_df))

    names = active["category_name"].cast(pl.Utf8, strict=False).fill_null("").map_elements(
        _normalize_text,
        return_dtype=pl.Utf8,
    )
    name_list = names.to_list()
    name_counts = Counter(name_list)
    duplicates = sorted(name for name, count in name_counts.items() if name and count > 1)
    if duplicates:
        raise ValueError(f"Duplicate YNAB category names: {duplicates}")

    return {
        _normalize_text(row["category_name"]): _normalize_text(row["category_id"])
        for row in active.iter_rows(named=True)
        if _normalize_text(row["category_name"])
    }


def _category_alias(name: str) -> str:
    text = _normalize_text(name)
    text = _LEADING_SYMBOL_RE.sub("", text).strip()
    if text.casefold().startswith("inflow:"):
        _, _, text = text.partition(":")
        text = text.strip()
    return text.casefold()


def _category_alias_lookup(categories_df: pl.DataFrame) -> dict[str, str]:
    active = _visible_categories(_category_frame(categories_df))

    alias_to_id: dict[str, str] = {}
    duplicate_aliases: list[str] = []
    for row in active.iter_rows(named=True):
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


def _uncategorized_category_id(categories_df: pl.DataFrame) -> str:
    category_ids = _category_lookup(categories_df)
    if "Uncategorized" in category_ids:
        return category_ids["Uncategorized"]
    category_alias_ids = _category_alias_lookup(categories_df)
    return category_alias_ids.get("uncategorized", "")


def _upload_rows_for_validation(working_df: pl.DataFrame) -> pl.DataFrame:
    df = working_schema.build_working_dataframe(working_df).with_row_index(
        "_working_row_position"
    )
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    upload_df = df.filter(_decision_action_mask(df))
    if upload_df.is_empty():
        return upload_df
    upload_df = upload_df.with_row_index("upload_row_position").with_columns(
        _target_payee_expr(upload_df).alias("target_payee_selected"),
        _target_category_expr(upload_df).alias("target_category_selected"),
        _combined_memo_expr(upload_df).alias("memo"),
        _float_expr("outflow_ils").round(2).alias("outflow_ils"),
        _float_expr("inflow_ils").round(2).alias("inflow_ils"),
    )
    return _explode_target_splits_for_upload(upload_df)


def validate_ready_for_upload(
    working_df: pl.DataFrame,
    *,
    accounts: list[dict[str, Any]] | None = None,
) -> None:
    upload_df = _upload_rows_for_validation(working_df)
    if upload_df.is_empty():
        return

    account_budget_lookup = _account_on_budget_lookup(accounts or [])
    category_required_expr = _category_required_expr(
        upload_df,
        payee_col="target_payee_selected",
        account_col="account_name",
        account_budget_lookup=account_budget_lookup,
    )
    category_selected_expr = _target_category_expr(upload_df).map_elements(
        _is_selected_category,
        return_dtype=pl.Boolean,
    )
    nonzero_amount_expr = (_float_expr("outflow_ils") != 0.0) | (
        _float_expr("inflow_ils") != 0.0
    )

    missing_payee_count = upload_df.filter(_parent_target_payee_expr(upload_df) == "").height
    missing_category_count = upload_df.filter(
        (~category_selected_expr) & category_required_expr
    ).height
    zero_amount_count = upload_df.filter(~nonzero_amount_expr).height
    if missing_payee_count or missing_category_count or zero_amount_count:
        raise ValueError(
            "Rows selected for upload are not ready: "
            f"{missing_payee_count} rows missing payee, "
            f"{missing_category_count} rows missing category, "
            f"{zero_amount_count} rows with zero amount."
        )


def ready_mask(
    working_df: pl.DataFrame,
    *,
    accounts: list[dict[str, Any]] | None = None,
) -> pl.Series:
    df = working_schema.build_working_dataframe(working_df)
    _validate_columns(df, REQUIRED_REVIEW_COLUMNS)
    upload_mask = _decision_action_mask(df)
    if not upload_mask.any():
        return upload_mask

    upload_df = _upload_rows_for_validation(working_df)
    if upload_df.is_empty() or "_working_row_position" not in upload_df.columns:
        return pl.Series("ready_mask", [False] * len(df), dtype=pl.Boolean)

    account_budget_lookup = _account_on_budget_lookup(accounts or [])
    category_ready_expr = _target_category_expr(upload_df).map_elements(
        _is_selected_category,
        return_dtype=pl.Boolean,
    ) | (
        ~_category_required_expr(
            upload_df,
            payee_col="target_payee_selected",
            account_col="account_name",
            account_budget_lookup=account_budget_lookup,
        )
    )
    upload_row_ready = upload_df.select(
        "_working_row_position",
        (
            (_parent_target_payee_expr(upload_df) != "")
            & category_ready_expr
            & (((_float_expr("outflow_ils") != 0.0) | (_float_expr("inflow_ils") != 0.0)))
        ).alias("ready"),
    )
    row_readiness = dict(
        zip(
            upload_row_ready.get_column("_working_row_position").to_list(),
            upload_row_ready.get_column("ready").to_list(),
            strict=False,
        )
    )
    result = [
        bool(row_readiness.get(idx, False)) if upload_mask[idx] else False
        for idx in range(len(df))
    ]
    return pl.Series("ready_mask", result, dtype=pl.Boolean)


def prepare_upload_transactions(
    working_df: pl.DataFrame,
    *,
    accounts: list[dict[str, Any]],
    categories_df: pl.DataFrame,
    cleared: str = "cleared",
    approved: bool = False,
) -> pl.DataFrame:
    reviewed_df = working_schema.build_working_dataframe(working_df)
    validate_ready_for_upload(working_df, accounts=accounts)

    df = reviewed_df.filter(_decision_action_mask(reviewed_df))
    if df.is_empty():
        return pl.DataFrame()

    df = (
        df.with_row_index("upload_row_position")
        .with_columns(
            _text_expr("transaction_id").alias("transaction_id"),
            _text_expr("account_name").alias("account_name"),
            _text_expr("date").alias("date"),
            _target_payee_expr(df).alias("target_payee_selected"),
            _target_category_expr(df).alias("target_category_selected"),
            _text_expr("decision_action").alias("decision_action"),
            _combined_memo_expr(df).alias("memo"),
            _float_expr("outflow_ils").round(2).alias("outflow_ils"),
            _float_expr("inflow_ils").round(2).alias("inflow_ils"),
        )
        .with_columns(
            pl.when(pl.col("transaction_id") == "")
            .then(pl.format("upload_txn_{}", pl.col("upload_row_position")))
            .otherwise(pl.col("transaction_id"))
            .alias("upload_transaction_id"),
            pl.when(pl.col("decision_action").str.to_lowercase() == _UPDATE_TARGET_ACTION)
            .then(_text_expr_or_default(df, "target_row_id"))
            .otherwise(pl.lit(""))
            .alias("existing_transaction_id"),
            _amount_milliunits_expr(
                inflow_col="inflow_ils",
                outflow_col="outflow_ils",
            ).alias("import_amount_milliunits"),
        )
    )
    missing_existing_ids = sorted(
        set(
            df.filter(
                (pl.col("decision_action").str.to_lowercase() == _UPDATE_TARGET_ACTION)
                & (pl.col("existing_transaction_id") == "")
            )
            .get_column("upload_transaction_id")
            .to_list()
        )
    )
    if missing_existing_ids:
        raise ValueError(
            "Missing target_row_id values for update_target rows: "
            + ", ".join(missing_existing_ids)
        )

    df = _explode_target_splits_for_upload(df)

    account_ids, transfer_payees = _account_lookup(accounts)
    account_budget_lookup = _account_on_budget_lookup(accounts)
    category_ids = _category_lookup(categories_df)
    category_alias_ids = _category_alias_lookup(categories_df)
    uncategorized_category_id = _uncategorized_category_id(categories_df)
    valid_category_ids = list(set(category_ids.values()))
    parent_payee_expr = _parent_target_payee_expr(df)

    df = df.with_columns(
        _text_expr("account_name")
        .map_elements(lambda value: account_ids.get(value, ""), return_dtype=pl.String)
        .alias("account_id"),
        _text_expr("target_payee_selected")
        .map_elements(_transfer_target, return_dtype=pl.String)
        .alias("transfer_target"),
        _text_expr("target_payee_selected")
        .map_elements(review_model.is_transfer_payee, return_dtype=pl.Boolean)
        .alias("_is_transfer"),
        _category_required_expr(
            df,
            payee_col="target_payee_selected",
            account_col="account_name",
            account_budget_lookup=account_budget_lookup,
        ).alias("_category_required"),
        parent_payee_expr.map_elements(_transfer_target, return_dtype=pl.String).alias(
            "parent_transfer_target"
        ),
        parent_payee_expr.map_elements(
            review_model.is_transfer_payee,
            return_dtype=pl.Boolean,
        ).alias("_parent_is_transfer"),
        _text_expr_or_default(df, "target_category_id").alias("category_id"),
        _amount_milliunits_expr(
            inflow_col="inflow_ils",
            outflow_col="outflow_ils",
        ).alias("amount_milliunits"),
    )
    df = df.with_columns(
        pl.when(
            (~pl.col("_category_required"))
            & _text_expr("target_category_selected").map_elements(
                review_model.is_no_category_required,
                return_dtype=pl.Boolean,
            )
        )
        .then(pl.lit(""))
        .otherwise(_text_expr("target_category_selected"))
        .alias("_upload_category")
    )

    missing_accounts = sorted(
        set(
            df.filter(pl.col("account_id") == "")
            .get_column("account_name")
            .to_list()
        )
    )
    if missing_accounts:
        raise ValueError(
            f"Missing YNAB account ids for account_name values: {missing_accounts}"
        )

    df = df.with_columns(
        pl.col("transfer_target")
        .map_elements(lambda value: transfer_payees.get(value, ""), return_dtype=pl.String)
        .alias("_transfer_target_id"),
        pl.col("transfer_target")
        .map_elements(lambda value: account_ids.get(value, ""), return_dtype=pl.String)
        .alias("_transfer_target_account_id"),
        pl.col("parent_transfer_target")
        .map_elements(lambda value: transfer_payees.get(value, ""), return_dtype=pl.String)
        .alias("_parent_transfer_target_id"),
    )
    missing_transfer_targets = sorted(
        set(
            df.filter(pl.col("_is_transfer") & (pl.col("_transfer_target_id") == ""))
            .get_column("transfer_target")
            .to_list()
        )
    )
    if missing_transfer_targets:
        raise ValueError(
            f"Missing transfer payee ids for target accounts: {missing_transfer_targets}"
        )

    invalid_selected_ids = df.filter(
        pl.col("_category_required")
        & (pl.col("_upload_category") != "")
        & (pl.col("category_id") != "")
        & (~pl.col("category_id").is_in(valid_category_ids))
    )
    if not invalid_selected_ids.is_empty():
        invalid_ids = sorted(set(invalid_selected_ids.get_column("category_id").to_list()))
        raise ValueError(f"Unknown YNAB category ids in reviewed upload rows: {invalid_ids}")
    df = df.with_columns(
        pl.when(pl.col("_category_required") & (pl.col("category_id") == ""))
        .then(
            pl.col("_upload_category").map_elements(
                lambda value: category_ids.get(value, ""),
                return_dtype=pl.String,
            )
        )
        .otherwise(pl.col("category_id"))
        .alias("category_id")
    )
    df = df.with_columns(
        pl.when(pl.col("_category_required") & (pl.col("category_id") == ""))
        .then(
            pl.col("_upload_category").map_elements(
                lambda value: category_alias_ids.get(_category_alias(value), ""),
                return_dtype=pl.String,
            )
        )
        .otherwise(pl.col("category_id"))
        .alias("category_id")
    )
    if uncategorized_category_id:
        df = df.with_columns(
            pl.when(pl.col("_category_required") & (pl.col("category_id") == ""))
            .then(pl.lit(uncategorized_category_id))
            .otherwise(pl.col("category_id"))
            .alias("category_id")
        )
    missing_categories = sorted(
        set(
            df.filter(pl.col("_category_required") & (pl.col("category_id") == ""))
            .get_column("_upload_category")
            .to_list()
        )
    )
    if missing_categories:
        raise ValueError(
            f"Missing YNAB category ids for target_category_selected values: {missing_categories}"
        )

    df = df.with_columns(
        pl.when(pl.col("_is_transfer"))
        .then(pl.col("_transfer_target_id"))
        .otherwise(pl.lit(""))
        .alias("payee_id"),
        pl.when(~pl.col("_is_transfer"))
        .then(_text_expr("target_payee_selected"))
        .otherwise(pl.lit(""))
        .alias("payee_name_upload"),
        pl.when(pl.col("_is_transfer") & (~pl.col("_category_required")))
        .then(pl.lit(""))
        .otherwise(pl.col("category_id"))
        .alias("category_id"),
        pl.when(pl.col("_is_transfer"))
        .then(pl.col("_transfer_target_account_id"))
        .otherwise(pl.lit(""))
        .alias("transfer_target_account_id"),
        pl.when(pl.col("_parent_is_transfer"))
        .then(pl.col("_parent_transfer_target_id"))
        .otherwise(pl.lit(""))
        .alias("parent_payee_id"),
        pl.when(~pl.col("_parent_is_transfer"))
        .then(parent_payee_expr)
        .otherwise(pl.lit(""))
        .alias("parent_payee_name_upload"),
    )

    df = df.with_columns(
        _text_expr_or_default(df, "bank_txn_id").alias("bank_txn_id"),
        _text_expr_or_default(df, "card_txn_id").alias("card_txn_id"),
    )
    df = df.with_columns(
        pl.when(
            (pl.col("bank_txn_id") == "")
            & (_text_expr_or_default(df, "source_bank_txn_id") != "")
        )
        .then(_text_expr_or_default(df, "source_bank_txn_id"))
        .otherwise(pl.col("bank_txn_id"))
        .alias("bank_txn_id"),
        pl.when(
            (pl.col("card_txn_id") == "")
            & (_text_expr_or_default(df, "source_card_txn_id") != "")
        )
        .then(_text_expr_or_default(df, "source_card_txn_id"))
        .otherwise(pl.col("card_txn_id"))
        .alias("card_txn_id"),
    )
    df = df.with_columns(
        pl.when(
            (pl.col("bank_txn_id") == "")
            & (_text_expr_or_default(df, "source").str.to_lowercase() == "bank")
        )
        .then(_text_expr_or_default(df, "source_transaction_id"))
        .otherwise(pl.col("bank_txn_id"))
        .alias("bank_txn_id"),
        pl.when(
            (pl.col("card_txn_id") == "")
            & (_text_expr_or_default(df, "source").str.to_lowercase() == "card")
        )
        .then(_text_expr_or_default(df, "source_transaction_id"))
        .otherwise(pl.col("card_txn_id"))
        .alias("card_txn_id"),
    )
    occurrence_order = (
        df.unique(subset=["upload_row_position"], keep="first", maintain_order=True)
        .select(
            "upload_row_position",
            "account_id",
            "date",
            "import_amount_milliunits",
            "transaction_id",
            "upload_transaction_id",
            "bank_txn_id",
            "card_txn_id",
            _text_expr_or_default(df, "source_import_id").alias("source_import_id"),
            _text_expr_or_default(df, "source_bank_txn_id").alias("source_bank_txn_id"),
            _text_expr_or_default(df, "source_card_txn_id").alias("source_card_txn_id"),
            _text_expr_or_default(df, "source").alias("source"),
            _text_expr_or_default(df, "source_source_system").alias(
                "source_source_system"
            ),
            _text_expr_or_default(df, "source_transaction_id").alias(
                "source_transaction_id"
            ),
        )
        .sort(
            [
                "account_id",
                "date",
                "import_amount_milliunits",
                "transaction_id",
                "upload_transaction_id",
                "upload_row_position",
            ]
        )
        .with_columns(
            (
                pl.int_range(0, pl.len()).over(
                    ["account_id", "date", "import_amount_milliunits"]
                )
                + 1
            ).alias("import_occurrence")
        )
        .with_columns(
            pl.struct(
                [
                    "source_import_id",
                    "bank_txn_id",
                    "card_txn_id",
                    "source",
                    "source_source_system",
                    "source_transaction_id",
                    "transaction_id",
                    "import_amount_milliunits",
                    "date",
                    "import_occurrence",
                ]
            )
            .map_elements(
                lambda row: _source_import_id(row)
                or (
                    f"YNAB:{int(row['import_amount_milliunits'])}:"
                    f"{row['date']}:{int(row['import_occurrence'])}"
                ),
                return_dtype=pl.String,
            )
            .alias("import_id")
        )
        .select("upload_row_position", "import_id")
    )
    df = df.join(occurrence_order, on="upload_row_position", how="left")

    df = df.with_columns(
        pl.lit(cleared).alias("cleared"),
        pl.lit(bool(approved)).alias("approved"),
        pl.when(pl.col("_is_transfer"))
        .then(pl.lit("transfer"))
        .otherwise(pl.lit("regular"))
        .alias("upload_kind"),
    )
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
        "existing_transaction_id",
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
    return df.select(columns)


def _empty_transaction_units_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "upload_transaction_id": pl.String,
            "source_row_count": pl.Int64,
            "decision_action": pl.String,
            "existing_transaction_id": pl.String,
            "upload_kind": pl.String,
            "unsupported_reason": pl.String,
            "account_id": pl.String,
            "account_name": pl.String,
            "date": pl.String,
            "amount_milliunits": pl.Int64,
            "memo": pl.String,
            "cleared": pl.String,
            "approved": pl.Boolean,
            "import_id": pl.String,
            "payee_id": pl.String,
            "payee_name_upload": pl.String,
            "category_id": pl.String,
            "target_category_selected": pl.String,
            "transfer_target_account_id": pl.String,
            "subtransactions": pl.Object,
        }
    )


def assemble_upload_transaction_units(prepared_df: pl.DataFrame) -> pl.DataFrame:
    if prepared_df.is_empty():
        return _empty_transaction_units_frame()

    required = ["upload_kind", "account_id", "date", "amount_milliunits", "import_id"]
    _validate_columns(prepared_df, required)

    prepared = prepared_df.clone()
    if "upload_transaction_id" not in prepared.columns:
        prepared = (
            prepared.with_row_index("_row_index")
            .with_columns(
                _text_expr_or_default(prepared, "transaction_id").alias(
                    "upload_transaction_id"
                )
            )
            .with_columns(
                pl.when(pl.col("upload_transaction_id") == "")
                .then(pl.format("upload_txn_{}", pl.col("_row_index")))
                .otherwise(pl.col("upload_transaction_id"))
                .alias("upload_transaction_id")
            )
            .drop("_row_index")
        )
    defaults: dict[str, Any] = {
        "account_name": "",
        "memo": "",
        "cleared": "cleared",
        "approved": False,
        "decision_action": _CREATE_TARGET_ACTION,
        "existing_transaction_id": "",
        "payee_id": "",
        "payee_name_upload": "",
        "category_id": "",
        "target_category_selected": "",
        "transfer_target_account_id": "",
    }
    for name, default in defaults.items():
        if name not in prepared.columns:
            prepared = prepared.with_columns(pl.lit(default).alias(name))
    if "parent_memo" not in prepared.columns:
        prepared = prepared.with_columns(_text_expr("memo").alias("parent_memo"))
    if "subtransaction_memo" not in prepared.columns:
        prepared = prepared.with_columns(_text_expr("memo").alias("subtransaction_memo"))
    if "parent_payee_id" not in prepared.columns:
        prepared = prepared.with_columns(_text_expr("payee_id").alias("parent_payee_id"))
    if "parent_payee_name_upload" not in prepared.columns:
        prepared = prepared.with_columns(
            _text_expr("payee_name_upload").alias("parent_payee_name_upload")
        )
    prepared = prepared.with_columns(
        *[
            _text_expr(name).alias(name)
            for name in [
                "upload_transaction_id",
                "upload_kind",
                "decision_action",
                "existing_transaction_id",
                "account_id",
                "account_name",
                "date",
                "memo",
                "parent_memo",
                "subtransaction_memo",
                "cleared",
                "import_id",
                "payee_id",
                "payee_name_upload",
                "parent_payee_id",
                "parent_payee_name_upload",
                "category_id",
                "target_category_selected",
                "transfer_target_account_id",
            ]
        ],
        _bool_expr_or_default(prepared, "approved").alias("approved"),
        pl.col("amount_milliunits").cast(pl.Int64, strict=False).fill_null(0).alias(
            "amount_milliunits"
        ),
    )

    unit_rows: list[dict[str, Any]] = []
    for group_key, group in prepared.group_by("upload_transaction_id", maintain_order=True):
        upload_transaction_id = _normalize_text(
            group_key[0] if isinstance(group_key, tuple) else group_key
        )
        rows = group.rows(named=True)
        first = rows[0]
        is_split = len(rows) > 1
        unsupported_reason = ""
        unit_kind = _normalize_text(first.get("upload_kind", ""))
        decision_action = _normalize_text(first.get("decision_action", "")) or _CREATE_TARGET_ACTION
        existing_transaction_id = _normalize_text(first.get("existing_transaction_id", ""))
        parent_category_id = _normalize_text(first.get("category_id", ""))
        parent_target_category = _normalize_text(first.get("target_category_selected", ""))
        subtransactions: list[dict[str, Any]] = []

        if decision_action == _UPDATE_TARGET_ACTION and not existing_transaction_id:
            unsupported_reason = "missing_existing_transaction_id"

        if is_split:
            unit_kind = "split"
            if any(
                _normalize_text(row.get("upload_kind", "")) == "transfer"
                or _normalize_text(row.get("transfer_target_account_id", "")) != ""
                or _normalize_text(row.get("parent_payee_id", "")) != ""
                for row in rows
            ):
                unsupported_reason = "split_transfer_unsupported"
            elif any(_normalize_text(row.get("category_id", "")) == "" for row in rows):
                unsupported_reason = "split_missing_category"
            parent_category_id = ""
            parent_target_category = ""
            for split_row in rows:
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

        unit_amount = (
            int(group.get_column("amount_milliunits").sum())
            if is_split
            else int(first.get("amount_milliunits", 0) or 0)
        )
        unit_rows.append(
            {
                "upload_transaction_id": upload_transaction_id,
                "source_row_count": len(rows),
                "decision_action": decision_action,
                "existing_transaction_id": existing_transaction_id,
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

    units = pl.from_dicts(unit_rows, infer_schema_length=None).with_columns(
        pl.Series(
            "subtransactions",
            [row["subtransactions"] for row in unit_rows],
            dtype=pl.Object,
        )
    ).select(TRANSACTION_UNIT_COLUMNS)
    duplicate_ids = units.filter(pl.col("upload_transaction_id").is_duplicated())
    if not duplicate_ids.is_empty():
        duplicates = sorted(
            set(duplicate_ids.get_column("upload_transaction_id").to_list())
        )
        raise ValueError(f"Duplicate upload_transaction_id values: {duplicates}")
    return units


def upload_payload_records(
    prepared_df: pl.DataFrame,
    *,
    decision_action: str | None = None,
) -> list[dict[str, Any]]:
    units = assemble_upload_transaction_units(prepared_df)
    if decision_action:
        wanted_action = _normalize_text(decision_action).casefold()
        units = units.filter(
            pl.col("decision_action")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_lowercase()
            .eq(wanted_action)
        )
    records: list[dict[str, Any]] = []
    for row in units.rows(named=True):
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
        }
        mutation_action = _normalize_text(row.get("decision_action", "")) or _CREATE_TARGET_ACTION
        if mutation_action == _UPDATE_TARGET_ACTION:
            existing_transaction_id = _normalize_text(
                row.get("existing_transaction_id", "")
            )
            if not existing_transaction_id:
                raise ValueError(
                    "update_target payload requires existing_transaction_id"
                )
            payload["id"] = existing_transaction_id
        else:
            payload["import_id"] = _normalize_text(row.get("import_id", ""))
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


def _transactions_frame(transactions: list[dict[str, Any]]) -> pl.DataFrame:
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
    if not rows:
        return pl.DataFrame(
            schema={
                "id": pl.String,
                "account_id": pl.String,
                "date": pl.String,
                "amount_milliunits": pl.Int64,
                "memo": pl.String,
                "cleared": pl.String,
                "approved": pl.Boolean,
                "import_id": pl.String,
                "matched_transaction_id": pl.String,
                "transfer_account_id": pl.String,
                "deleted": pl.Boolean,
                "payee_name": pl.String,
                "category_name": pl.String,
                "category_id": pl.String,
                "date_key": pl.Date,
            }
        )
    return pl.from_dicts(rows).with_columns(
        _text_expr("date").str.to_date(strict=False).alias("date_key")
    )


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
    prepared_df: pl.DataFrame,
    existing_transactions: list[dict[str, Any]],
) -> dict[str, Any]:
    prepared = assemble_upload_transaction_units(prepared_df).with_columns(
        _text_expr("date").str.to_date(strict=False).alias("date_key")
    )

    payload_duplicates = prepared.group_by(["account_id", "import_id"]).agg(
        pl.len().alias("count")
    )
    payload_duplicate_keys = sorted(
        payload_duplicates.filter((pl.col("import_id") != "") & (pl.col("count") > 1))
        .select(["account_id", "import_id"])
        .rows()
    )

    existing_df = _transactions_frame(existing_transactions)
    existing_import_id_hits: list[tuple[str, str]] = []
    potential_match_import_ids: list[str] = []
    if not existing_df.is_empty():
        existing_with_import = existing_df.filter(pl.col("import_id") != "")
        if not existing_with_import.is_empty():
            existing_keys = set(
                existing_with_import.select(["account_id", "import_id"]).rows()
            )
            prepared_keys = {
                key
                for key in prepared.select(["account_id", "import_id"]).rows()
                if _normalize_text(key[1])
            }
            existing_import_id_hits = sorted(prepared_keys.intersection(existing_keys))

        candidates = existing_df.filter(pl.col("import_id") == "")
        if not candidates.is_empty():
            merged = (
                prepared.select(
                    "account_id",
                    "amount_milliunits",
                    pl.col("import_id").alias("import_id_prepared"),
                    pl.col("date_key").alias("date_key_prepared"),
                )
                .join(
                    candidates.select(
                        "account_id",
                        "amount_milliunits",
                        pl.col("date_key").alias("date_key_existing"),
                    ),
                    on=["account_id", "amount_milliunits"],
                    how="inner",
                )
                .with_columns(
                    (
                        (pl.col("date_key_prepared") - pl.col("date_key_existing"))
                        .dt.total_days()
                        .abs()
                    ).alias("date_gap_days")
                )
                .filter(pl.col("date_gap_days") <= 10)
            )
            if not merged.is_empty():
                potential_match_import_ids = sorted(
                    set(merged.get_column("import_id_prepared").to_list())
                    - {import_id for _, import_id in existing_import_id_hits}
                )

    transfer_payload_issue_ids = sorted(
        prepared.filter(
            (
                (pl.col("upload_kind") == "transfer")
                & (
                    (pl.col("payee_id") == "")
                    | (pl.col("category_id") != "")
                    | (pl.col("payee_name_upload") != "")
                )
            )
            | (
                (pl.col("upload_kind") != "transfer")
                & (pl.col("payee_id") != "")
            )
        )
        .get_column("import_id")
        .to_list()
    )
    unsupported_transaction_unit_ids = sorted(
        prepared.filter(_text_expr("unsupported_reason") != "")
        .get_column("upload_transaction_id")
        .to_list()
    )

    return {
        "prepared_count": len(prepared),
        "transfer_count": prepared.filter(pl.col("upload_kind") == "transfer").height,
        "split_count": prepared.filter(pl.col("upload_kind") == "split").height,
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
    if not tx_df.is_empty():
        matched_existing = tx_df.filter(pl.col("matched_transaction_id") != "").height
        transfer_saved = tx_df.filter(pl.col("transfer_account_id") != "").height
        split_saved = tx_df.filter(_text_expr("category_name") == "Split").height

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
    prepared_df: pl.DataFrame,
    response: dict[str, Any],
) -> dict[str, Any]:
    prepared = assemble_upload_transaction_units(prepared_df)

    response_transactions = response.get("transactions", []) or []
    response_df = _transactions_frame(response_transactions)
    if response_df.is_empty():
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

    response_df = response_df.filter(pl.col("import_id") != "")
    response_lookup = _response_transaction_lookup(response_transactions)
    prepared_indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for row in prepared.rows(named=True):
        key = (
            _normalize_text(row.get("account_id", "")),
            _normalize_text(row.get("import_id", "")),
        )
        if key[0] and key[1] and key not in prepared_indexed:
            prepared_indexed[key] = row

    response_indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for row in response_df.rows(named=True):
        key = (
            _normalize_text(row.get("account_id", "")),
            _normalize_text(row.get("import_id", "")),
        )
        if key[0] and key[1] and key not in response_indexed:
            response_indexed[key] = row

    missing_saved = sorted(
        set(response.get("transaction_ids", []) or [])
        - set(response_df.get_column("id").to_list())
    )

    shared_keys = sorted(
        set(prepared_indexed).intersection(set(response_indexed))
    )
    amount_mismatches: list[str] = []
    date_mismatches: list[str] = []
    account_mismatches: list[str] = []
    transfer_mismatches: list[str] = []
    category_mismatches: list[str] = []
    split_mismatches: list[str] = []

    for account_id, import_id in shared_keys:
        prepared_row = prepared_indexed[(account_id, import_id)]
        response_row = response_indexed[(account_id, import_id)]
        label = _account_import_label(account_id, import_id)

        if int(prepared_row["amount_milliunits"]) != int(response_row["amount_milliunits"]):
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
        if prepared_category_id and not category_matches:
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









def upload_payload_batches(prepared_df: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    return {
        "create_transactions": upload_payload_records(
            prepared_df,
            decision_action=_CREATE_TARGET_ACTION,
        ),
        "update_transactions": upload_payload_records(
            prepared_df,
            decision_action=_UPDATE_TARGET_ACTION,
        ),
    }
