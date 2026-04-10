from __future__ import annotations

import pyarrow as pa

from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_STRUCT


REVIEW_ARTIFACT_VERSION = "review_v4"

REVIEW_CONTROL_FIELDS: list[pa.Field] = [
    pa.field("artifact_kind", pa.string()),
    pa.field("artifact_version", pa.string()),
    pa.field("review_transaction_id", pa.string()),
    pa.field("workflow_type", pa.string()),
    pa.field("relation_kind", pa.string()),
    pa.field("match_status", pa.string()),
    pa.field("match_method", pa.string()),
    pa.field("payee_options", pa.string()),
    pa.field("category_options", pa.string()),
    pa.field("update_maps", pa.string()),
    pa.field("decision_action", pa.string()),
    pa.field("reviewed", pa.bool_()),
    pa.field("changed", pa.bool_()),
    pa.field("memo_append", pa.string()),
    pa.field("source_present", pa.bool_()),
    pa.field("target_present", pa.bool_()),
    pa.field("source_row_id", pa.string()),
    pa.field("target_row_id", pa.string()),
    pa.field("target_account", pa.string()),
    pa.field("source_context_kind", pa.string()),
    pa.field("source_context_category_id", pa.string()),
    pa.field("source_context_category_name", pa.string()),
    pa.field("source_context_matching_split_ids", pa.string()),
    pa.field("source_payee_selected", pa.string()),
    pa.field("source_category_selected", pa.string()),
    pa.field("target_context_kind", pa.string()),
    pa.field("target_context_matching_split_ids", pa.string()),
    pa.field("target_payee_selected", pa.string()),
    pa.field("target_category_selected", pa.string()),
]

REVIEW_SCHEMA = pa.schema(
    [
        *REVIEW_CONTROL_FIELDS,
        pa.field("source_current", TRANSACTION_STRUCT),
        pa.field("target_current", TRANSACTION_STRUCT),
        pa.field("source_original", TRANSACTION_STRUCT),
        pa.field("target_original", TRANSACTION_STRUCT),
    ]
)


def empty_review_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in REVIEW_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=REVIEW_SCHEMA)


def _transaction_amount(txn: dict | None) -> float:
    if not isinstance(txn, dict):
        return 0.0
    inflow = float(txn.get("inflow_ils", 0.0) or 0.0)
    outflow = float(txn.get("outflow_ils", 0.0) or 0.0)
    return inflow - outflow


def _split_amount(line: dict) -> float:
    inflow = float(line.get("inflow_ils", 0.0) or 0.0)
    outflow = float(line.get("outflow_ils", 0.0) or 0.0)
    return inflow - outflow


def validate_review_record(record: dict) -> list[str]:
    errors: list[str] = []
    changed = bool(record.get("changed", False))
    current_pairs = [
        ("source", record.get("source_current"), record.get("source_original")),
        ("target", record.get("target_current"), record.get("target_original")),
    ]

    for side, current_txn, original_txn in current_pairs:
        if not isinstance(current_txn, dict):
            continue
        signed_amount = _transaction_amount(current_txn)
        splits = current_txn.get("splits") or []
        if splits:
            split_total = sum(_split_amount(line) for line in splits if isinstance(line, dict))
            if abs(split_total - signed_amount) > 1e-9:
                errors.append(f"{side}_current split amounts do not sum to transaction amount")

        if isinstance(original_txn, dict):
            original_splits = original_txn.get("splits") or []
            if original_splits:
                split_total = sum(
                    _split_amount(line) for line in original_splits if isinstance(line, dict)
                )
                if abs(split_total - _transaction_amount(original_txn)) > 1e-9:
                    errors.append(
                        f"{side}_original split amounts do not sum to transaction amount"
                    )

        if not changed and current_txn != original_txn:
            errors.append(f"changed is FALSE but {side} current and original differ")

    return errors


def validate_review_table(table: pa.Table) -> None:
    all_errors: list[str] = []
    for idx, record in enumerate(table.to_pylist()):
        row_errors = validate_review_record(record)
        all_errors.extend(f"row {idx}: {message}" for message in row_errors)
    if all_errors:
        raise ValueError("Invalid review artifact:\n" + "\n".join(all_errors))
