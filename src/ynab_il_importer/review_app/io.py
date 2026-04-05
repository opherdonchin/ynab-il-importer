from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ynab_il_importer.artifacts.review_schema import (
    REVIEW_ARTIFACT_VERSION,
    REVIEW_CONTROL_FIELDS,
    REVIEW_SCHEMA,
    validate_review_table,
)
from ynab_il_importer.artifacts.transaction_schema import (
    SPLIT_LINE_STRUCT,
    TRANSACTION_ARTIFACT_VERSION,
    TRANSACTION_SCHEMA,
)
import ynab_il_importer.review_app.model as model
import ynab_il_importer.review_app.validation as validation
import ynab_il_importer.review_app.working_schema as working_schema


REQUIRED_COLUMNS = list(working_schema.WORKING_REQUIRED_COLUMNS)

LEGACY_INSTITUTIONAL_REQUIRED_COLUMNS = [
    "transaction_id",
    "source",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "fingerprint",
    "payee_options",
    "category_options",
    "payee_selected",
    "category_selected",
    "match_status",
    "reviewed",
]

REVIEW_FIELD_NAMES = [field.name for field in REVIEW_SCHEMA]
REVIEW_CONTROL_FIELD_NAMES = [field.name for field in REVIEW_CONTROL_FIELDS]
TRANSACTION_FIELD_NAMES = [field.name for field in TRANSACTION_SCHEMA]
SPLIT_FIELD_NAMES = [field.name for field in SPLIT_LINE_STRUCT]
SPLIT_COLUMNS = working_schema.SPLIT_COLUMNS
CURRENT_TRANSACTION_COLUMNS = working_schema.CURRENT_TRANSACTION_COLUMNS
ORIGINAL_TRANSACTION_COLUMNS = working_schema.ORIGINAL_TRANSACTION_COLUMNS
WORKING_COLUMNS = working_schema.WORKING_COLUMNS


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


def _text_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].astype("string").fillna("").str.strip()


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _normalize_bool(value: Any) -> bool:
    return bool(validation.normalize_flag_series(pd.Series([value])).iloc[0])


def _required_mapping_value(row: dict[str, Any], key: str) -> Any:
    if key not in row:
        raise ValueError(f"Review rows must include {key}")
    return row[key]


def _normalize_float(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _json_dump(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return ""


def _normalize_split_records(value: Any) -> list[dict[str, Any]] | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            return None
        value = decoded
    if not isinstance(value, list):
        if isinstance(value, dict):
            return None
        try:
            value = list(value)
        except TypeError:
            return None
    normalized: list[dict[str, Any]] = []
    for raw in value:
        if not isinstance(raw, dict):
            continue
        normalized.append(
            {
                name: (
                    _normalize_float(raw.get(name))
                    if name in {"inflow_ils", "outflow_ils"}
                    else _normalize_text(raw.get(name))
                )
                for name in SPLIT_FIELD_NAMES
            }
        )
    return normalized or None


def _empty_transaction_record() -> dict[str, Any]:
    record: dict[str, Any] = {}
    for field in TRANSACTION_SCHEMA:
        if pa.types.is_boolean(field.type):
            record[field.name] = False
        elif pa.types.is_floating(field.type):
            record[field.name] = 0.0
        elif pa.types.is_list(field.type):
            record[field.name] = None
        else:
            record[field.name] = ""
    record["artifact_kind"] = "transaction"
    record["artifact_version"] = TRANSACTION_ARTIFACT_VERSION
    return record


def _normalize_transaction_record(value: Any) -> dict[str, Any] | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            return None
        value = decoded
    if not isinstance(value, dict):
        return None

    record = _empty_transaction_record()
    for field in TRANSACTION_SCHEMA:
        raw = value.get(field.name)
        if raw is None or raw is pd.NA:
            continue
        if pa.types.is_boolean(field.type):
            record[field.name] = _normalize_bool(raw)
        elif pa.types.is_floating(field.type):
            record[field.name] = _normalize_float(raw)
        elif pa.types.is_list(field.type):
            record[field.name] = _normalize_split_records(raw)
        else:
            record[field.name] = _normalize_text(raw)

    record["artifact_kind"] = record.get("artifact_kind") or "transaction"
    record["artifact_version"] = record.get("artifact_version") or TRANSACTION_ARTIFACT_VERSION
    record["transaction_id"] = record.get("transaction_id") or record.get("ynab_id") or ""
    record["parent_transaction_id"] = (
        record.get("parent_transaction_id") or record.get("transaction_id") or ""
    )
    record["account_name"] = record.get("account_name") or record.get("source_account") or ""
    record["source_account"] = record.get("source_account") or record.get("account_name") or ""
    record["signed_amount_ils"] = _normalize_float(
        _normalize_float(record.get("inflow_ils")) - _normalize_float(record.get("outflow_ils"))
    )
    return record


def _legacy_source_row_ids(df: pd.DataFrame) -> pd.Series:
    source = _text_series(df, "source").str.casefold()
    bank_ids = _text_series(df, "bank_txn_id")
    card_ids = _text_series(df, "card_txn_id")
    row_ids = pd.Series([""] * len(df), index=df.index, dtype="string")
    row_ids = row_ids.mask(source.eq("bank"), bank_ids)
    row_ids = row_ids.mask(source.eq("card"), card_ids)
    return row_ids


def _legacy_update_maps(df: pd.DataFrame) -> pd.Series:
    if "update_map" not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    flagged = validation.normalize_flag_series(df["update_map"])
    return pd.Series(
        ["payee_add_fingerprint" if bool(value) else "" for value in flagged],
        index=df.index,
        dtype="string",
    )


def _legacy_institutional_mask(df: pd.DataFrame) -> bool:
    if _missing_columns(df, LEGACY_INSTITUTIONAL_REQUIRED_COLUMNS):
        return False
    sources = set(_text_series(df, "source").str.casefold().tolist()) - {""}
    return bool(sources) and sources <= {"bank", "card"}


def _translate_legacy_institutional_review(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["legacy_review_schema"] = "legacy_institutional_v0"
    out["legacy_match_status"] = _text_series(out, "match_status")
    out["match_status"] = "source_only"
    out["update_maps"] = _legacy_update_maps(out)
    out["decision_action"] = "create_target"
    out["workflow_type"] = "institutional"
    out["source_payee_selected"] = _text_series(out, "payee_selected")
    out["source_category_selected"] = _text_series(out, "category_selected")
    out["target_payee_selected"] = _text_series(out, "payee_selected")
    out["target_category_selected"] = _text_series(out, "category_selected")
    out["source_present"] = True
    out["target_present"] = False
    out["source_row_id"] = _legacy_source_row_ids(out)
    out["target_row_id"] = ""
    out["target_account"] = _text_series(out, "account_name")
    out["source_date"] = _text_series(out, "date")
    out["target_date"] = ""
    out["source_memo"] = _text_series(out, "memo")
    out["target_memo"] = ""
    out["source_fingerprint"] = _text_series(out, "fingerprint")
    out["target_fingerprint"] = ""
    out["source_splits"] = None
    out["target_splits"] = None
    out["changed"] = False
    return out


def detect_review_csv_format(df: pd.DataFrame) -> str:
    if not _missing_columns(df, REQUIRED_COLUMNS):
        return "unified_v1"
    if _legacy_institutional_mask(df):
        return "legacy_institutional_v0"
    return "unknown"


def translate_review_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    fmt = detect_review_csv_format(df)
    if fmt == "unified_v1":
        return df.copy()
    if fmt == "legacy_institutional_v0":
        return _translate_legacy_institutional_review(df)
    raise ValueError("Unsupported review CSV format for translation.")


def _input_to_pandas_dataframe(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
    *,
    label: str,
) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()
    if isinstance(source, pl.DataFrame):
        return source.to_pandas()
    if isinstance(source, pa.Table):
        return source.to_pandas()

    csv_path = Path(source)
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing {label} file: {csv_path}")
    return pd.read_csv(csv_path, dtype="string").fillna("")


def _is_review_artifact_table(table: pa.Table) -> bool:
    return {
        "review_transaction_id",
        "source_current",
        "target_current",
        "source_original",
        "target_original",
    }.issubset(set(table.column_names))


def _coerce_review_artifact_table(table: pa.Table) -> pa.Table:
    return pa.Table.from_arrays(
        [table[field.name].cast(field.type, safe=False) for field in REVIEW_SCHEMA],
        schema=REVIEW_SCHEMA,
    )


def _signed_amount(row: pd.Series) -> float:
    return _normalize_float(row.get("inflow_ils")) - _normalize_float(row.get("outflow_ils"))


def _value_from_row(row: pd.Series, *names: str) -> str:
    for name in names:
        if name and name in row.index:
            text = _normalize_text(row.get(name))
            if text:
                return text
    return ""


def _bool_from_row(row: pd.Series, *names: str) -> bool:
    for name in names:
        if name and name in row.index:
            return _normalize_bool(row.get(name))
    return False


def _side_present(row: pd.Series, side: str) -> bool:
    present_key = f"{side}_present"
    if present_key not in row.index:
        raise ValueError(f"Review rows must include {present_key}")
    return _normalize_bool(row.get(present_key))


def _has_current_side_data(row: pd.Series, side: str) -> bool:
    return _side_present(row, side)


def _category_id_for_current(
    *,
    row: pd.Series,
    side: str,
    category_value: str,
) -> str:
    current_value = _value_from_row(row, f"{side}_category_current")
    if category_value and category_value == current_value:
        return _value_from_row(row, f"{side}_category_id")
    return ""


def _transaction_from_flat_row(
    row: pd.Series,
    *,
    side: str,
    use_selected_values: bool,
    base_transaction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    txn = _empty_transaction_record()
    if base_transaction:
        txn.update(_normalize_transaction_record(base_transaction) or {})

    selected_payee = _value_from_row(
        row,
        f"{side}_payee_selected",
        "payee_selected" if side == "target" else "",
    )
    selected_category = model.normalize_category_value(
        _value_from_row(
            row,
            f"{side}_category_selected",
            "category_selected" if side == "target" else "",
        )
    )
    current_payee = _value_from_row(row, f"{side}_payee_current")
    current_category = model.normalize_category_value(
        _value_from_row(row, f"{side}_category_current")
    )
    payee_value = selected_payee if use_selected_values and selected_payee else current_payee
    category_value = (
        selected_category if use_selected_values and selected_category else current_category
    )

    txn["artifact_kind"] = txn.get("artifact_kind") or "transaction"
    txn["artifact_version"] = txn.get("artifact_version") or TRANSACTION_ARTIFACT_VERSION
    txn["source_system"] = _value_from_row(
        row,
        f"{side}_source_system",
        "source" if side == "source" else "",
    ) or _normalize_text(txn.get("source_system"))
    txn["transaction_id"] = (
        _value_from_row(row, f"{side}_transaction_id", f"{side}_row_id")
        or _normalize_text(txn.get("transaction_id"))
    )
    txn["ynab_id"] = _value_from_row(row, f"{side}_ynab_id") or _normalize_text(txn.get("ynab_id"))
    txn["import_id"] = _value_from_row(row, f"{side}_import_id") or _normalize_text(txn.get("import_id"))
    txn["parent_transaction_id"] = (
        _value_from_row(row, f"{side}_parent_transaction_id")
        or txn["transaction_id"]
        or _normalize_text(txn.get("parent_transaction_id"))
    )
    txn["account_id"] = (
        _value_from_row(row, f"{side}_account_id", "ynab_account_id" if side == "target" else "")
        or _normalize_text(txn.get("account_id"))
    )
    txn["account_name"] = (
        _value_from_row(row, f"{side}_account", "account_name")
        or _normalize_text(txn.get("account_name"))
    )
    txn["source_account"] = (
        _value_from_row(row, f"{side}_account", "account_name")
        or _normalize_text(txn.get("source_account"))
        or txn["account_name"]
    )
    txn["date"] = _value_from_row(row, f"{side}_date", "date") or _normalize_text(txn.get("date"))
    txn["secondary_date"] = (
        _value_from_row(row, f"{side}_secondary_date", "secondary_date")
        or _normalize_text(txn.get("secondary_date"))
    )
    txn["inflow_ils"] = _normalize_float(row.get("inflow_ils", txn.get("inflow_ils", 0.0)))
    txn["outflow_ils"] = _normalize_float(row.get("outflow_ils", txn.get("outflow_ils", 0.0)))
    txn["signed_amount_ils"] = _signed_amount(row)
    txn["payee_raw"] = payee_value or _normalize_text(txn.get("payee_raw"))
    txn["category_raw"] = category_value or _normalize_text(txn.get("category_raw"))
    txn["category_id"] = _category_id_for_current(
        row=row,
        side=side,
        category_value=txn["category_raw"],
    ) or _normalize_text(txn.get("category_id"))
    txn["memo"] = _value_from_row(row, f"{side}_memo", "memo") or _normalize_text(txn.get("memo"))
    txn["txn_kind"] = _normalize_text(txn.get("txn_kind"))
    txn["fingerprint"] = (
        _value_from_row(row, f"{side}_fingerprint", "fingerprint")
        or _normalize_text(txn.get("fingerprint"))
    )
    txn["description_raw"] = (
        _value_from_row(row, f"{side}_description_raw", "description_raw")
        or _normalize_text(txn.get("description_raw"))
    )
    txn["description_clean"] = (
        _value_from_row(row, f"{side}_description_clean", "description_clean")
        or _normalize_text(txn.get("description_clean"))
    )
    txn["description_clean_norm"] = (
        _value_from_row(row, f"{side}_description_clean_norm")
        or _normalize_text(txn.get("description_clean_norm"))
    )
    txn["merchant_raw"] = (
        _value_from_row(row, f"{side}_merchant_raw", "merchant_raw")
        or _normalize_text(txn.get("merchant_raw"))
    )
    txn["ref"] = _value_from_row(row, f"{side}_ref", "ref") or _normalize_text(txn.get("ref"))
    txn["matched_transaction_id"] = (
        _value_from_row(row, f"{side}_matched_transaction_id")
        or _normalize_text(txn.get("matched_transaction_id"))
    )
    txn["cleared"] = _value_from_row(row, f"{side}_cleared") or _normalize_text(txn.get("cleared"))
    txn["approved"] = _bool_from_row(row, f"{side}_approved") or bool(txn.get("approved", False))
    txn["is_subtransaction"] = _bool_from_row(row, f"{side}_is_subtransaction") or bool(
        txn.get("is_subtransaction", False)
    )
    txn["splits"] = _normalize_split_records(
        row.get(f"{side}_splits", txn.get("splits"))
    )
    return txn


def _original_transaction_from_row(row: pd.Series, *, side: str) -> dict[str, Any] | None:
    for key in [f"{side}_original", f"{side}_original_transaction", f"{side}_transaction"]:
        txn = _normalize_transaction_record(row.get(key))
        if txn:
            return txn
    return None


def _current_transaction_from_row(row: pd.Series, *, side: str) -> dict[str, Any] | None:
    explicit_current = False
    for key in [f"{side}_current", f"{side}_current_transaction"]:
        txn = _normalize_transaction_record(row.get(key))
        if txn:
            return txn
        if key in row.index:
            explicit_current = True
    original = _original_transaction_from_row(row, side=side)
    if not _side_present(row, side):
        return None
    if original is not None and not explicit_current:
        if not _normalize_bool(row.get("changed", False)):
            return original
    return _transaction_from_flat_row(
        row,
        side=side,
        use_selected_values=False,
        base_transaction=original,
    )


def _review_record_from_row(row: pd.Series) -> dict[str, Any]:
    source_current = _current_transaction_from_row(row, side="source")
    target_current = _current_transaction_from_row(row, side="target")
    source_original = _original_transaction_from_row(row, side="source")
    target_original = _original_transaction_from_row(row, side="target")
    if source_original is None and source_current is not None:
        source_original = dict(source_current)
    if target_original is None and target_current is not None:
        target_original = dict(target_current)
    return {
        "artifact_kind": "review_artifact",
        "artifact_version": REVIEW_ARTIFACT_VERSION,
        "review_transaction_id": _normalize_text(
            row.get("review_transaction_id", row.get("transaction_id", ""))
        ),
        "workflow_type": _normalize_text(row.get("workflow_type")),
        "relation_kind": _normalize_text(row.get("relation_kind")),
        "match_status": _normalize_text(row.get("match_status")),
        "match_method": _normalize_text(row.get("match_method")),
        "payee_options": _normalize_text(row.get("payee_options")),
        "category_options": _normalize_text(row.get("category_options")),
        "update_maps": validation.join_update_maps(
            validation.parse_update_maps(_normalize_text(row.get("update_maps", "")))
        ),
        "decision_action": validation.normalize_decision_action(row.get("decision_action")),
        "reviewed": _normalize_bool(row.get("reviewed", False)),
        "changed": _normalize_bool(row.get("changed", False)),
        "memo_append": _normalize_text(row.get("memo_append")),
        "source_present": _side_present(row, "source"),
        "target_present": _side_present(row, "target"),
        "source_row_id": _normalize_text(row.get("source_row_id")),
        "target_row_id": _normalize_text(row.get("target_row_id")),
        "target_account": _normalize_text(row.get("target_account")),
        "source_context_kind": _normalize_text(row.get("source_context_kind")),
        "source_context_category_id": _normalize_text(row.get("source_context_category_id")),
        "source_context_category_name": _normalize_text(row.get("source_context_category_name")),
        "source_context_matching_split_ids": _normalize_text(
            row.get("source_context_matching_split_ids")
        ),
        "source_payee_selected": _normalize_text(row.get("source_payee_selected")),
        "source_category_selected": model.normalize_category_value(
            row.get("source_category_selected")
        ),
        "target_context_kind": _normalize_text(row.get("target_context_kind")),
        "target_context_matching_split_ids": _normalize_text(
            row.get("target_context_matching_split_ids")
        ),
        "target_payee_selected": _normalize_text(row.get("target_payee_selected")),
        "target_category_selected": model.normalize_category_value(
            row.get("target_category_selected")
        ),
        "source_current": source_current,
        "target_current": target_current,
        "source_original": source_original,
        "target_original": target_original,
    }


def _review_table_from_dataframe(df: pd.DataFrame) -> pa.Table:
    review_df = translate_review_dataframe(df) if detect_review_csv_format(df) != "unknown" else df.copy()
    canonicalish_columns = {
        "source_current",
        "target_current",
        "source_original",
        "target_original",
        "source_current_transaction",
        "target_current_transaction",
        "source_transaction",
        "target_transaction",
        "source_original_transaction",
        "target_original_transaction",
    }
    if not canonicalish_columns.intersection(set(review_df.columns)):
        review_df = working_schema.build_working_dataframe(review_df)
    records = [_review_record_from_row(row) for _, row in review_df.iterrows()]
    if not records:
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in REVIEW_SCHEMA],
            schema=REVIEW_SCHEMA,
        )
    return pa.Table.from_pylist(records, schema=REVIEW_SCHEMA)


def load_review_artifact(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pa.Table:
    if isinstance(source, pa.Table):
        if _is_review_artifact_table(source):
            table = _coerce_review_artifact_table(source)
            validate_review_table(table)
            return table
        table = _review_table_from_dataframe(source.to_pandas())
        validate_review_table(table)
        return table
    if isinstance(source, pl.DataFrame):
        table = source.to_arrow()
        if _is_review_artifact_table(table):
            canonical = _coerce_review_artifact_table(table)
            validate_review_table(canonical)
            return canonical
        canonical = _review_table_from_dataframe(source.to_pandas())
        validate_review_table(canonical)
        return canonical
    if isinstance(source, pd.DataFrame):
        table = _review_table_from_dataframe(source)
        validate_review_table(table)
        return table

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Missing review artifact file: {path}")
    if path.suffix.lower() == ".parquet":
        table = pq.read_table(path)
        if _is_review_artifact_table(table):
            canonical = _coerce_review_artifact_table(table)
            validate_review_table(canonical)
            return canonical
        canonical = _review_table_from_dataframe(table.to_pandas())
        validate_review_table(canonical)
        return canonical

    df = pd.read_csv(path, dtype="string").fillna("")
    df = working_schema.decode_working_dataframe(df)
    detected_format = detect_review_csv_format(df)
    if detected_format != "unified_v1":
        if detected_format.startswith("legacy_"):
            raise ValueError(
                "proposed_transactions is in legacy review format "
                f"({detected_format}); run scripts/translate_review_csv.py first"
            )
    table = _review_table_from_dataframe(df)
    validate_review_table(table)
    return table


def load_review_artifact_polars(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pl.DataFrame:
    return pl.from_pandas(project_review_artifact_to_flat_dataframe(source))


def _preferred_summary_value(*values: Any) -> str:
    for value in values:
        text = _normalize_text(value)
        if text:
            return text
    return ""


def _preferred_summary_number(*values: Any) -> float:
    for value in values:
        try:
            if value is None or pd.isna(value):
                continue
        except TypeError:
            pass
        return _normalize_float(value)
    return 0.0


def _working_row_from_record(row: dict[str, Any]) -> dict[str, Any]:
    source_current = _normalize_transaction_record(row.get("source_current")) or _empty_transaction_record()
    target_current = _normalize_transaction_record(row.get("target_current")) or _empty_transaction_record()
    source_original = _normalize_transaction_record(row.get("source_original"))
    target_original = _normalize_transaction_record(row.get("target_original"))

    working: dict[str, Any] = {
        "transaction_id": _normalize_text(row.get("review_transaction_id")),
        "source": _preferred_summary_value(
            source_current.get("source_system"),
            source_original.get("source_system") if source_original else "",
            target_current.get("source_system"),
        ),
        "account_name": _preferred_summary_value(
            target_current.get("account_name"),
            source_current.get("account_name"),
        ),
        "date": _preferred_summary_value(source_current.get("date"), target_current.get("date")),
        "outflow_ils": _preferred_summary_number(
            source_current.get("outflow_ils"),
            target_current.get("outflow_ils"),
        ),
        "inflow_ils": _preferred_summary_number(
            source_current.get("inflow_ils"),
            target_current.get("inflow_ils"),
        ),
        "memo": _preferred_summary_value(source_current.get("memo"), target_current.get("memo")),
        "fingerprint": _preferred_summary_value(
            source_current.get("fingerprint"),
            target_current.get("fingerprint"),
        ),
        "workflow_type": _normalize_text(row.get("workflow_type")),
        "relation_kind": _normalize_text(row.get("relation_kind")),
        "match_status": _normalize_text(row.get("match_status")),
        "match_method": _normalize_text(row.get("match_method")),
        "payee_options": _normalize_text(row.get("payee_options")),
        "category_options": _normalize_text(row.get("category_options")),
        "update_maps": _normalize_text(row.get("update_maps")),
        "decision_action": _normalize_text(row.get("decision_action")),
        "reviewed": bool(row.get("reviewed", False)),
        "changed": bool(row.get("changed", False)),
        "memo_append": _normalize_text(row.get("memo_append")),
        "source_present": bool(_required_mapping_value(row, "source_present")),
        "target_present": bool(_required_mapping_value(row, "target_present")),
        "source_row_id": _normalize_text(row.get("source_row_id")),
        "target_row_id": _normalize_text(row.get("target_row_id")),
        "target_account": _preferred_summary_value(
            row.get("target_account"),
            target_current.get("account_name"),
            target_current.get("source_account"),
            row.get("account_name"),
        ),
        "source_context_kind": _normalize_text(row.get("source_context_kind")),
        "source_context_category_id": _normalize_text(row.get("source_context_category_id")),
        "source_context_category_name": _normalize_text(row.get("source_context_category_name")),
        "source_context_matching_split_ids": _normalize_text(
            row.get("source_context_matching_split_ids")
        ),
        "source_payee_selected": _normalize_text(row.get("source_payee_selected"))
        or _normalize_text(source_current.get("payee_raw")),
        "source_category_selected": model.normalize_category_value(
            row.get("source_category_selected")
        )
        or model.normalize_category_value(source_current.get("category_raw")),
        "target_context_kind": _normalize_text(row.get("target_context_kind")),
        "target_context_matching_split_ids": _normalize_text(
            row.get("target_context_matching_split_ids")
        ),
        "target_payee_selected": _normalize_text(row.get("target_payee_selected"))
        or _normalize_text(target_current.get("payee_raw")),
        "target_category_selected": model.normalize_category_value(
            row.get("target_category_selected")
        )
        or model.normalize_category_value(target_current.get("category_raw")),
        "source_current_transaction": source_current,
        "target_current_transaction": target_current,
        "source_original_transaction": source_original,
        "target_original_transaction": target_original,
    }

    for side, txn in [("source", source_current), ("target", target_current)]:
        working[f"{side}_source_system"] = _normalize_text(txn.get("source_system"))
        working[f"{side}_transaction_id"] = _normalize_text(txn.get("transaction_id"))
        working[f"{side}_ynab_id"] = _normalize_text(txn.get("ynab_id"))
        working[f"{side}_import_id"] = _normalize_text(txn.get("import_id"))
        working[f"{side}_parent_transaction_id"] = _normalize_text(txn.get("parent_transaction_id"))
        working[f"{side}_account_id"] = _normalize_text(txn.get("account_id"))
        working[f"{side}_account"] = _normalize_text(txn.get("account_name") or txn.get("source_account"))
        working[f"{side}_date"] = _normalize_text(txn.get("date"))
        working[f"{side}_secondary_date"] = _normalize_text(txn.get("secondary_date"))
        working[f"{side}_payee_current"] = _normalize_text(txn.get("payee_raw"))
        working[f"{side}_category_id"] = _normalize_text(txn.get("category_id"))
        working[f"{side}_category_current"] = model.normalize_category_value(txn.get("category_raw"))
        working[f"{side}_memo"] = _normalize_text(txn.get("memo"))
        working[f"{side}_fingerprint"] = _normalize_text(txn.get("fingerprint"))
        working[f"{side}_description_raw"] = _normalize_text(txn.get("description_raw"))
        working[f"{side}_description_clean"] = _normalize_text(txn.get("description_clean"))
        working[f"{side}_description_clean_norm"] = _normalize_text(
            txn.get("description_clean_norm")
        )
        working[f"{side}_merchant_raw"] = _normalize_text(txn.get("merchant_raw"))
        working[f"{side}_ref"] = _normalize_text(txn.get("ref"))
        working[f"{side}_matched_transaction_id"] = _normalize_text(
            txn.get("matched_transaction_id")
        )
        working[f"{side}_cleared"] = _normalize_text(txn.get("cleared"))
        working[f"{side}_approved"] = bool(txn.get("approved", False))
        working[f"{side}_is_subtransaction"] = bool(txn.get("is_subtransaction", False))
        working[f"{side}_splits"] = _normalize_split_records(txn.get("splits"))
    working["target_account"] = _preferred_summary_value(
        row.get("target_account"),
        target_current.get("account_name"),
        target_current.get("source_account"),
        row.get("account_name"),
    )
    working["payee_selected"] = working["target_payee_selected"]
    working["category_selected"] = working["target_category_selected"]
    return working


def project_review_artifact_to_flat_dataframe(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    canonicalish_columns = {
        "source_current",
        "target_current",
        "source_original",
        "target_original",
        "source_current_transaction",
        "target_current_transaction",
        "source_transaction",
        "target_transaction",
        "source_original_transaction",
        "target_original_transaction",
    }
    if isinstance(source, pd.DataFrame):
        if canonicalish_columns.intersection(set(source.columns)):
            rows = load_review_artifact(source).to_pylist()
        else:
            return working_schema.build_working_dataframe(source)
    elif isinstance(source, pl.DataFrame):
        if canonicalish_columns.intersection(set(source.columns)):
            rows = load_review_artifact(source).to_pylist()
        else:
            return working_schema.build_working_dataframe(source.to_pandas())
    elif isinstance(source, pa.Table):
        if _is_review_artifact_table(source):
            rows = load_review_artifact(source).to_pylist()
        else:
            return working_schema.build_working_dataframe(source.to_pandas())
    else:
        rows = load_review_artifact(source).to_pylist()
    if not rows:
        return pd.DataFrame(columns=WORKING_COLUMNS)
    df = pd.DataFrame([_working_row_from_record(row) for row in rows])
    return working_schema.build_working_dataframe(df)


def load_proposed_transactions(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    if isinstance(source, (pa.Table, pl.DataFrame, pd.DataFrame)):
        return project_review_artifact_to_flat_dataframe(source)

    if isinstance(source, (str, Path)) and Path(source).suffix.lower() == ".parquet":
        return project_review_artifact_to_flat_dataframe(source)

    df = _input_to_pandas_dataframe(source, label="proposed transactions")
    df = working_schema.decode_working_dataframe(df)
    detected_format = detect_review_csv_format(df)
    if detected_format != "unified_v1":
        if detected_format.startswith("legacy_"):
            raise ValueError(
                "proposed_transactions is in legacy review format "
                f"({detected_format}); run scripts/translate_review_csv.py first"
            )
        raise ValueError(
            f"proposed_transactions missing columns: {_missing_columns(df, REQUIRED_COLUMNS)}"
        )
    return project_review_artifact_to_flat_dataframe(df)


def save_review_artifact(
    data: pd.DataFrame | pl.DataFrame | pa.Table,
    path: str | Path,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = load_review_artifact(data)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    pq.write_table(table, tmp_path)
    tmp_path.replace(output_path)


def save_review_artifact_polars(
    data: pd.DataFrame | pl.DataFrame | pa.Table,
    path: str | Path,
) -> None:
    save_review_artifact(data, path)


def save_reviewed_transactions(
    df: pd.DataFrame | pl.DataFrame | pa.Table,
    path: str | Path,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix.lower() == ".parquet":
        save_review_artifact(df, output_path)
        return

    out = project_review_artifact_to_flat_dataframe(df).copy()
    out = out.drop(
        columns=[col for col in ["payee_selected", "category_selected"] if col in out.columns]
    )
    out["update_maps"] = validation.normalize_update_maps(out["update_maps"])
    out["reviewed"] = out["reviewed"].map(lambda value: "TRUE" if bool(value) else "")
    out["changed"] = out["changed"].map(lambda value: "TRUE" if bool(value) else "")
    for flag_col in [
        "source_present",
        "target_present",
        "source_approved",
        "source_is_subtransaction",
        "target_approved",
        "target_is_subtransaction",
    ]:
        if flag_col in out.columns:
            out[flag_col] = out[flag_col].map(lambda value: "TRUE" if bool(value) else "")
    for column in SPLIT_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_json_dump)
    for column in CURRENT_TRANSACTION_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_json_dump)
    for column in ORIGINAL_TRANSACTION_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_json_dump)

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    out.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(output_path)


def load_category_list(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    df = _input_to_pandas_dataframe(source, label="categories")
    if "category_name" not in df.columns:
        raise ValueError("Categories file must contain a category_name column.")
    if "category_group" not in df.columns:
        df["category_group"] = ""
    return df[["category_group", "category_name"]].copy()
