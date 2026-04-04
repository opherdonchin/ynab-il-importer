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
    REVIEW_SCHEMA,
    REVIEW_SIDE_SCALAR_FIELDS,
)
from ynab_il_importer.artifacts.transaction_schema import SPLIT_LINE_STRUCT
import ynab_il_importer.review_app.model as model
import ynab_il_importer.review_app.validation as validation


REQUIRED_COLUMNS = [
    "transaction_id",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "payee_options",
    "category_options",
    "match_status",
    "update_maps",
    "decision_action",
    "fingerprint",
    "workflow_type",
    "source_payee_selected",
    "source_category_selected",
    "target_payee_selected",
    "target_category_selected",
]

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
REVIEW_SIDE_SCALAR_FIELD_NAMES = [field.name for field in REVIEW_SIDE_SCALAR_FIELDS]
SPLIT_FIELD_NAMES = [field.name for field in SPLIT_LINE_STRUCT]
SPLIT_COLUMNS = ["source_splits", "target_splits"]


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


def _text_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].astype("string").fillna("").str.strip()


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bool(value: Any) -> bool:
    return bool(validation.normalize_flag_series(pd.Series([value])).iloc[0])


def _normalize_float(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


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
    out["source_payee_selected"] = ""
    out["source_category_selected"] = ""
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
        "source_row_id",
        "target_row_id",
        "source_splits",
        "target_splits",
    }.issubset(set(table.column_names))


def _coerce_review_artifact_table(table: pa.Table) -> pa.Table:
    return pa.Table.from_arrays(
        [table[field.name].cast(field.type, safe=False) for field in REVIEW_SCHEMA],
        schema=REVIEW_SCHEMA,
    )


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


def _flat_side_snapshot(row: pd.Series, *, side: str) -> dict[str, Any]:
    txn = row.get(f"{side}_transaction")
    txn_map = txn if isinstance(txn, dict) else {}

    def pick_row(*names: str, default: str = "") -> str:
        for name in names:
            if name in row.index:
                text = _normalize_text(row.get(name))
                if text:
                    return text
        return default

    def pick_txn(*names: str, default: str = "") -> str:
        for name in names:
            text = _normalize_text(txn_map.get(name))
            if text:
                return text
        return default

    return {
        f"{side}_source_system": pick_row(
            f"{side}_source_system",
            default=pick_txn("source_system", default="ynab" if side == "target" else ""),
        ),
        f"{side}_transaction_id": pick_row(
            f"{side}_transaction_id",
            f"{side}_row_id",
            default=pick_txn("transaction_id", default=_normalize_text(row.get("transaction_id"))),
        ),
        f"{side}_ynab_id": pick_row(f"{side}_ynab_id", default=pick_txn("ynab_id")),
        f"{side}_import_id": pick_row(f"{side}_import_id", default=pick_txn("import_id")),
        f"{side}_parent_transaction_id": pick_row(
            f"{side}_parent_transaction_id",
            default=pick_txn(
                "parent_transaction_id",
                default=pick_row(
                    f"{side}_transaction_id",
                    f"{side}_row_id",
                    default=_normalize_text(row.get("transaction_id")),
                ),
            ),
        ),
        f"{side}_account_id": pick_row(
            f"{side}_account_id",
            "ynab_account_id",
            default=pick_txn("account_id"),
        ),
        f"{side}_account": pick_row(
            f"{side}_account",
            "account_name",
            default=pick_txn("account_name", "source_account"),
        ),
        f"{side}_date": pick_row(f"{side}_date", "date", default=pick_txn("date")),
        f"{side}_secondary_date": pick_row(
            f"{side}_secondary_date",
            "secondary_date",
            default=pick_txn("secondary_date"),
        ),
        f"{side}_payee_current": pick_row(
            f"{side}_payee_current", default=pick_txn("payee_raw")
        ),
        f"{side}_category_id": pick_row(
            f"{side}_category_id", default=pick_txn("category_id")
        ),
        f"{side}_category_current": pick_row(
            f"{side}_category_current", default=pick_txn("category_raw")
        ),
        f"{side}_memo": pick_row(f"{side}_memo", "memo", default=pick_txn("memo")),
        f"{side}_fingerprint": pick_row(
            f"{side}_fingerprint", "fingerprint", default=pick_txn("fingerprint")
        ),
        f"{side}_description_raw": pick_row(
            f"{side}_description_raw", "description_raw", default=pick_txn("description_raw")
        ),
        f"{side}_description_clean": pick_row(
            f"{side}_description_clean",
            "description_clean",
            default=pick_txn("description_clean"),
        ),
        f"{side}_merchant_raw": pick_row(
            f"{side}_merchant_raw", "merchant_raw", default=pick_txn("merchant_raw")
        ),
        f"{side}_ref": pick_row(f"{side}_ref", "ref", default=pick_txn("ref")),
        f"{side}_cleared": pick_row(f"{side}_cleared", default=pick_txn("cleared")),
        f"{side}_approved": _normalize_bool(
            row.get(f"{side}_approved", txn_map.get("approved", False))
        ),
        f"{side}_is_subtransaction": _normalize_bool(
            row.get(f"{side}_is_subtransaction", txn_map.get("is_subtransaction", False))
        ),
        f"{side}_bank_txn_id": pick_row(f"{side}_bank_txn_id", "bank_txn_id"),
        f"{side}_card_txn_id": pick_row(f"{side}_card_txn_id", "card_txn_id"),
        f"{side}_card_suffix": pick_row(f"{side}_card_suffix", "card_suffix"),
        f"{side}_splits": _normalize_split_records(
            row.get(f"{side}_splits", txn_map.get("splits"))
        ),
    }


def _infer_source_present(row: pd.Series) -> bool:
    if "source_present" in row.index:
        return _normalize_bool(row.get("source_present", False))
    for name in [
        "source_row_id",
        "source_transaction",
        "source",
        "source_account",
        "source_date",
        "source_memo",
        "source_fingerprint",
        "source_payee_current",
        "source_category_current",
        "source_bank_txn_id",
        "source_card_txn_id",
        "bank_txn_id",
        "card_txn_id",
        "card_suffix",
        "transaction_id",
        "account_name",
        "date",
        "memo",
        "fingerprint",
    ]:
        if _normalize_text(row.get(name, "")):
            return True
    return False


def _infer_target_present(row: pd.Series) -> bool:
    if "target_present" in row.index:
        return _normalize_bool(row.get("target_present", False))
    for name in [
        "target_row_id",
        "target_transaction",
        "target_date",
        "target_memo",
        "target_fingerprint",
        "target_payee_current",
        "target_category_current",
        "target_splits",
    ]:
        value = row.get(name, "")
        if name == "target_splits":
            if _normalize_split_records(value):
                return True
            continue
        if _normalize_text(value):
            return True
    return False


def _review_record_from_row(row: pd.Series) -> dict[str, Any]:
    source_side = _flat_side_snapshot(row, side="source")
    target_side = _flat_side_snapshot(row, side="target")
    source_present = _infer_source_present(row)
    target_present = _infer_target_present(row)
    record: dict[str, Any] = {
        "artifact_kind": "review_artifact",
        "artifact_version": REVIEW_ARTIFACT_VERSION,
        "review_transaction_id": _normalize_text(
            row.get("review_transaction_id", row.get("transaction_id", ""))
        ),
        "source": _normalize_text(row.get("source")),
        "account_name": _normalize_text(row.get("account_name")),
        "date": _normalize_text(row.get("date")),
        "outflow_ils": _normalize_float(row.get("outflow_ils")),
        "inflow_ils": _normalize_float(row.get("inflow_ils")),
        "memo": _normalize_text(row.get("memo")),
        "fingerprint": _normalize_text(row.get("fingerprint")),
        "workflow_type": _normalize_text(row.get("workflow_type")),
        "relation_kind": _normalize_text(row.get("relation_kind")),
        "match_status": _normalize_text(row.get("match_status")),
        "match_method": _normalize_text(row.get("match_method")),
        "payee_options": _normalize_text(row.get("payee_options")),
        "category_options": _normalize_text(row.get("category_options")),
        "update_maps": validation.join_update_maps(
            validation.parse_update_maps(row.get("update_maps", ""))
        ),
        "decision_action": validation.normalize_decision_action(row.get("decision_action")),
        "reviewed": _normalize_bool(row.get("reviewed", False)),
        "memo_append": _normalize_text(row.get("memo_append")),
        "source_present": source_present,
        "target_present": target_present,
        "source_row_id": _normalize_text(row.get("source_row_id")),
        "target_row_id": _normalize_text(row.get("target_row_id")),
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
    }
    record.update(source_side)
    record.update(target_side)
    if not source_present:
        for name in REVIEW_SIDE_SCALAR_FIELD_NAMES:
            if name.startswith("source_"):
                if name.endswith("_approved") or name.endswith("_is_subtransaction"):
                    record[name] = False
                elif name == "source_splits":
                    record[name] = None
                else:
                    record[name] = ""
    if not target_present:
        for name in REVIEW_SIDE_SCALAR_FIELD_NAMES:
            if name.startswith("target_"):
                if name in {"target_account", "target_account_id"}:
                    continue
                if name.endswith("_approved") or name.endswith("_is_subtransaction"):
                    record[name] = False
                elif name == "target_splits":
                    record[name] = None
                else:
                    record[name] = ""
    return record


def _review_table_from_dataframe(df: pd.DataFrame) -> pa.Table:
    review_df = translate_review_dataframe(df) if detect_review_csv_format(df) != "unknown" else df.copy()
    if "target_payee_selected" not in review_df.columns and "payee_selected" in review_df.columns:
        review_df["target_payee_selected"] = _text_series(review_df, "payee_selected")
    if (
        "target_category_selected" not in review_df.columns
        and "category_selected" in review_df.columns
    ):
        review_df["target_category_selected"] = _text_series(review_df, "category_selected")
    records = [_review_record_from_row(row) for _, row in review_df.iterrows()]
    if not records:
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in REVIEW_SCHEMA],
            schema=REVIEW_SCHEMA,
        )
    return pa.Table.from_pylist(records, schema=REVIEW_SCHEMA)


def _decode_split_column_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in SPLIT_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(_normalize_split_records)
    return out


def load_review_artifact(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pa.Table:
    if isinstance(source, pa.Table):
        if _is_review_artifact_table(source):
            return _coerce_review_artifact_table(source)
        return _review_table_from_dataframe(source.to_pandas())
    if isinstance(source, pl.DataFrame):
        table = source.to_arrow()
        if _is_review_artifact_table(table):
            return _coerce_review_artifact_table(table)
        return _review_table_from_dataframe(source.to_pandas())
    if isinstance(source, pd.DataFrame):
        return _review_table_from_dataframe(source)

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Missing review artifact file: {path}")
    if path.suffix.lower() == ".parquet":
        table = pq.read_table(path)
        if not _is_review_artifact_table(table):
            raise ValueError(f"Parquet file is not a canonical review artifact: {path}")
        return _coerce_review_artifact_table(table)

    df = pd.read_csv(path, dtype="string").fillna("")
    df = _decode_split_column_if_needed(df)
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
    return _review_table_from_dataframe(df)


def load_review_artifact_polars(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pl.DataFrame:
    return pl.from_arrow(load_review_artifact(source))


def project_review_artifact_to_flat_dataframe(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    table = load_review_artifact(source)
    rows = table.to_pylist()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(
            columns=REVIEW_FIELD_NAMES + ["transaction_id", "payee_selected", "category_selected"]
        )
    for column in [
        "payee_options",
        "category_options",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_selected",
        "target_category_selected",
        "match_status",
        "fingerprint",
        "workflow_type",
        "memo_append",
        "source_context_kind",
        "source_context_category_id",
        "source_context_category_name",
        "source_context_matching_split_ids",
        "target_context_kind",
        "target_context_matching_split_ids",
    ]:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].astype("string").fillna("").str.strip()
    for column in [
        "reviewed",
        "source_present",
        "target_present",
        "source_approved",
        "source_is_subtransaction",
        "target_approved",
        "target_is_subtransaction",
    ]:
        if column in df.columns:
            df[column] = validation.normalize_flag_series(df[column])
    for column in ["source_category_selected", "target_category_selected"]:
        if column in df.columns:
            df[column] = df[column].map(model.normalize_category_value)
    df["update_maps"] = validation.normalize_update_maps(
        df.get("update_maps", pd.Series([""] * len(df), index=df.index))
    )
    df["decision_action"] = validation.normalize_decision_actions(
        df.get("decision_action", pd.Series([""] * len(df), index=df.index))
    )
    if "reviewed" not in df.columns:
        df["reviewed"] = False
    if "source_present" not in df.columns:
        df["source_present"] = False
    if "target_present" not in df.columns:
        df["target_present"] = False
    df["payee_selected"] = df.get("target_payee_selected", pd.Series([""] * len(df), index=df.index))
    df["category_selected"] = df.get(
        "target_category_selected",
        pd.Series([""] * len(df), index=df.index),
    )
    if "transaction_id" not in df.columns:
        df["transaction_id"] = df.get(
            "review_transaction_id", pd.Series([""] * len(df), index=df.index)
        )
    return df


def load_proposed_transactions(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    if isinstance(source, (pa.Table, pl.DataFrame, pd.DataFrame)):
        try:
            return project_review_artifact_to_flat_dataframe(source)
        except Exception:
            pass

    if isinstance(source, (str, Path)) and Path(source).suffix.lower() == ".parquet":
        return project_review_artifact_to_flat_dataframe(source)

    df = _input_to_pandas_dataframe(source, label="proposed transactions")
    df = _decode_split_column_if_needed(df)
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

    out = load_proposed_transactions(df)
    out = out.drop(
        columns=[col for col in ["payee_selected", "category_selected"] if col in out.columns]
    ).copy()
    out["update_maps"] = validation.normalize_update_maps(out["update_maps"])
    out["reviewed"] = out["reviewed"].map(lambda value: "TRUE" if bool(value) else "")
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
            out[column] = out[column].map(
                lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, list) else ""
            )

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
