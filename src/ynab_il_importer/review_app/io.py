from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ynab_il_importer.artifacts.review_schema import (
    REVIEW_ARTIFACT_VERSION,
    REVIEW_SCHEMA,
)
from ynab_il_importer.artifacts.transaction_schema import (
    TRANSACTION_ARTIFACT_VERSION,
    TRANSACTION_SCHEMA,
)
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
    return {"review_transaction_id", "source_transaction", "target_transaction"}.issubset(
        set(table.column_names)
    )


def _normalize_transaction_mapping(value: Any) -> dict[str, Any] | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, pd.Series):
        mapping = value.to_dict()
    elif isinstance(value, dict):
        mapping = dict(value)
    else:
        return None

    normalized: dict[str, Any] = {}
    for field in TRANSACTION_SCHEMA:
        raw = mapping.get(field.name)
        if raw is None or raw is pd.NA:
            normalized[field.name] = None
            continue
        if pa.types.is_boolean(field.type):
            normalized[field.name] = bool(raw)
        elif pa.types.is_floating(field.type):
            normalized[field.name] = float(pd.to_numeric(raw, errors="coerce") or 0.0)
        elif pa.types.is_list(field.type):
            normalized[field.name] = list(raw) if isinstance(raw, list) else None
        else:
            normalized[field.name] = str(raw).strip()
    return normalized


def _review_table_from_dataframe(df: pd.DataFrame) -> pa.Table:
    review_df = df.copy()
    if "target_payee_selected" not in review_df.columns and "payee_selected" in review_df.columns:
        review_df["target_payee_selected"] = _text_series(review_df, "payee_selected")
    if (
        "target_category_selected" not in review_df.columns
        and "category_selected" in review_df.columns
    ):
        review_df["target_category_selected"] = _text_series(review_df, "category_selected")
    for column in [
        "source_category_selected",
        "target_category_selected",
    ]:
        if column in review_df.columns:
            review_df[column] = review_df[column].map(model.normalize_category_value)

    row_count = len(review_df)

    def _text_column(name: str, default: str = "") -> pa.Array:
        if name in review_df.columns:
            return pa.array(
                review_df[name].astype("string").fillna(default).str.strip().tolist(),
                type=pa.string(),
            )
        return pa.array([default] * row_count, type=pa.string())

    def _bool_column(name: str, default: bool = False) -> pa.Array:
        if name in review_df.columns:
            values = validation.normalize_flag_series(review_df[name]).tolist()
            return pa.array([bool(value) for value in values], type=pa.bool_())
        return pa.array([default] * row_count, type=pa.bool_())

    def _transaction_array(name: str, side: str) -> pa.Array:
        values: list[dict[str, Any] | None] = []
        for _, row in review_df.iterrows():
            explicit = _normalize_transaction_mapping(row.get(name))
            if explicit is not None:
                values.append(explicit)
                continue
            present = bool(row.get(f"{side}_present", False))
            if not present:
                values.append(None)
                continue
            values.append(_transaction_from_flat_row(row, side=side))
        return pa.array(values, type=REVIEW_SCHEMA.field(name).type)

    arrays: list[pa.Array] = []
    for field in REVIEW_SCHEMA:
        if field.name == "artifact_kind":
            arrays.append(pa.array(["review_artifact"] * row_count, type=field.type))
        elif field.name == "artifact_version":
            arrays.append(pa.array([REVIEW_ARTIFACT_VERSION] * row_count, type=field.type))
        elif field.name == "review_transaction_id":
            arrays.append(_text_column("transaction_id"))
        elif field.name in {"reviewed", "source_present", "target_present"}:
            arrays.append(_bool_column(field.name))
        elif field.name == "source_transaction":
            arrays.append(_transaction_array(field.name, side="source"))
        elif field.name == "target_transaction":
            arrays.append(_transaction_array(field.name, side="target"))
        else:
            arrays.append(_text_column(field.name))
    return pa.Table.from_arrays(arrays, schema=REVIEW_SCHEMA)


def _transaction_from_flat_row(row: pd.Series, *, side: str) -> dict[str, Any]:
    source_system = _normalize_text(row.get("source")) if side == "source" else "ynab"
    row_id = _normalize_text(row.get(f"{side}_row_id"))
    account_name = _normalize_text(row.get(f"{side}_account")) or _normalize_text(
        row.get("account_name")
    )
    date = _normalize_text(row.get(f"{side}_date")) or _normalize_text(row.get("date"))
    memo = _normalize_text(row.get(f"{side}_memo")) or _normalize_text(row.get("memo"))
    fingerprint = _normalize_text(row.get(f"{side}_fingerprint")) or _normalize_text(
        row.get("fingerprint")
    )
    payee = _normalize_text(row.get(f"{side}_payee_current"))
    category = _normalize_text(row.get(f"{side}_category_current"))
    outflow = float(pd.to_numeric(row.get("outflow_ils", 0.0), errors="coerce") or 0.0)
    inflow = float(pd.to_numeric(row.get("inflow_ils", 0.0), errors="coerce") or 0.0)
    return {
        "artifact_kind": f"review_{side}_transaction",
        "artifact_version": TRANSACTION_ARTIFACT_VERSION,
        "source_system": source_system or side,
        "transaction_id": row_id or _normalize_text(row.get("transaction_id")),
        "ynab_id": row_id if side == "target" else "",
        "import_id": "",
        "parent_transaction_id": row_id or _normalize_text(row.get("transaction_id")),
        "account_id": _normalize_text(row.get("ynab_account_id")) if side == "target" else "",
        "account_name": account_name,
        "source_account": account_name,
        "date": date,
        "secondary_date": "",
        "inflow_ils": inflow,
        "outflow_ils": outflow,
        "signed_amount_ils": inflow - outflow,
        "payee_raw": payee,
        "category_id": "",
        "category_raw": category,
        "memo": memo,
        "txn_kind": "",
        "fingerprint": fingerprint,
        "description_raw": memo,
        "description_clean": memo,
        "description_clean_norm": "",
        "merchant_raw": payee,
        "ref": "",
        "matched_transaction_id": "",
        "cleared": "",
        "approved": False,
        "is_subtransaction": False,
        "splits": None,
    }


def load_review_artifact(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pa.Table:
    if isinstance(source, pa.Table):
        if _is_review_artifact_table(source):
            return pa.Table.from_arrays(
                [source[field.name].cast(field.type, safe=False) for field in REVIEW_SCHEMA],
                schema=REVIEW_SCHEMA,
            )
        return _review_table_from_dataframe(source.to_pandas())
    if isinstance(source, pl.DataFrame):
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
        return pa.Table.from_arrays(
            [table[field.name].cast(field.type, safe=False) for field in REVIEW_SCHEMA],
            schema=REVIEW_SCHEMA,
        )

    df = pd.read_csv(path, dtype="string").fillna("")
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


def project_review_artifact_to_flat_dataframe(
    source: str | Path | pd.DataFrame | pl.DataFrame | pa.Table,
) -> pd.DataFrame:
    table = load_review_artifact(source)
    rows: list[dict[str, Any]] = []
    for record in table.to_pylist():
        source_txn = record.get("source_transaction") or {}
        target_txn = record.get("target_transaction") or {}
        source_present = bool(record.get("source_present", False))
        target_present = bool(record.get("target_present", False))
        display_source = source_txn if source_present else {}
        display_target = target_txn if target_present else {}
        rows.append(
            {
                "transaction_id": _normalize_text(record.get("review_transaction_id")),
                "source": _normalize_text(
                    display_source.get("source_system")
                    or ("ynab" if not source_present and target_present else "")
                ),
                "account_name": _normalize_text(
                    display_target.get("account_name") or display_source.get("account_name")
                ),
                "date": _normalize_text(display_source.get("date") or display_target.get("date")),
                "outflow_ils": float(
                    pd.to_numeric(
                        display_source.get("outflow_ils", display_target.get("outflow_ils", 0.0)),
                        errors="coerce",
                    )
                    or 0.0
                ),
                "inflow_ils": float(
                    pd.to_numeric(
                        display_source.get("inflow_ils", display_target.get("inflow_ils", 0.0)),
                        errors="coerce",
                    )
                    or 0.0
                ),
                "memo": _normalize_text(display_source.get("memo") or display_target.get("memo")),
                "fingerprint": _normalize_text(
                    display_source.get("fingerprint") or display_target.get("fingerprint")
                ),
                "payee_options": _normalize_text(record.get("payee_options")),
                "category_options": _normalize_text(record.get("category_options")),
                "match_status": _normalize_text(record.get("match_status")),
                "update_maps": _normalize_text(record.get("update_maps")),
                "decision_action": _normalize_text(record.get("decision_action"))
                or validation.NO_DECISION,
                "reviewed": bool(record.get("reviewed", False)),
                "workflow_type": _normalize_text(record.get("workflow_type")),
                "relation_kind": _normalize_text(record.get("relation_kind")),
                "match_method": _normalize_text(record.get("match_method")),
                "source_present": source_present,
                "target_present": target_present,
                "source_row_id": _normalize_text(record.get("source_row_id")),
                "target_row_id": _normalize_text(record.get("target_row_id")),
                "source_account": _normalize_text(
                    source_txn.get("source_account") or source_txn.get("account_name")
                ),
                "target_account": _normalize_text(
                    target_txn.get("account_name") or target_txn.get("source_account")
                ),
                "source_date": _normalize_text(source_txn.get("date")),
                "target_date": _normalize_text(target_txn.get("date")),
                "source_payee_current": _normalize_text(source_txn.get("payee_raw")),
                "target_payee_current": _normalize_text(target_txn.get("payee_raw")),
                "source_category_current": model.normalize_category_value(
                    source_txn.get("category_raw")
                ),
                "target_category_current": model.normalize_category_value(
                    target_txn.get("category_raw")
                ),
                "source_memo": _normalize_text(source_txn.get("memo")),
                "target_memo": _normalize_text(target_txn.get("memo")),
                "source_fingerprint": _normalize_text(source_txn.get("fingerprint")),
                "target_fingerprint": _normalize_text(target_txn.get("fingerprint")),
                "source_payee_selected": _normalize_text(record.get("source_payee_selected")),
                "source_category_selected": model.normalize_category_value(
                    record.get("source_category_selected")
                ),
                "target_payee_selected": _normalize_text(record.get("target_payee_selected")),
                "target_category_selected": model.normalize_category_value(
                    record.get("target_category_selected")
                ),
                "memo_append": _normalize_text(record.get("memo_append")),
                "source_transaction": source_txn or None,
                "target_transaction": target_txn or None,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_COLUMNS + ["payee_selected", "category_selected"])
    for col in [
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
    ]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype("string").fillna("").str.strip()
    for col in ["source_category_selected", "target_category_selected"]:
        if col in df.columns:
            df[col] = df[col].map(model.normalize_category_value)
    if "update_maps" not in df.columns:
        df["update_maps"] = ""
    df["update_maps"] = validation.normalize_update_maps(df["update_maps"])
    if "reviewed" not in df.columns:
        df["reviewed"] = False
    else:
        df["reviewed"] = validation.normalize_flag_series(df["reviewed"])
    if "source_present" in df.columns:
        df["source_present"] = validation.normalize_flag_series(df["source_present"])
    if "target_present" in df.columns:
        df["target_present"] = validation.normalize_flag_series(df["target_present"])
    df["decision_action"] = validation.normalize_decision_actions(df["decision_action"])
    df["payee_selected"] = df["target_payee_selected"]
    df["category_selected"] = df["target_category_selected"]
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
        columns=[
            col
            for col in [
                "payee_selected",
                "category_selected",
                "source_transaction",
                "target_transaction",
            ]
            if col in out.columns
        ]
    )
    out["update_maps"] = validation.normalize_update_maps(out["update_maps"])
    out["reviewed"] = out["reviewed"].map(lambda v: "TRUE" if bool(v) else "")
    for flag_col in ["source_present", "target_present"]:
        if flag_col in out.columns:
            out[flag_col] = out[flag_col].map(lambda v: "TRUE" if bool(v) else "")

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
