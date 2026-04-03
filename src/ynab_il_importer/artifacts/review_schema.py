from __future__ import annotations

import pyarrow as pa

from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_SCHEMA


REVIEW_ARTIFACT_VERSION = "review_v2"
TRANSACTION_STRUCT = pa.struct(list(TRANSACTION_SCHEMA))

REVIEW_SCHEMA = pa.schema(
    [
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
        pa.field("memo_append", pa.string()),
        pa.field("source_present", pa.bool_()),
        pa.field("target_present", pa.bool_()),
        pa.field("source_row_id", pa.string()),
        pa.field("target_row_id", pa.string()),
        pa.field("source_account", pa.string()),
        pa.field("target_account", pa.string()),
        pa.field("source_date", pa.string()),
        pa.field("target_date", pa.string()),
        pa.field("source_memo", pa.string()),
        pa.field("target_memo", pa.string()),
        pa.field("source_fingerprint", pa.string()),
        pa.field("target_fingerprint", pa.string()),
        pa.field("source_bank_txn_id", pa.string()),
        pa.field("source_card_txn_id", pa.string()),
        pa.field("source_card_suffix", pa.string()),
        pa.field("source_secondary_date", pa.string()),
        pa.field("source_ref", pa.string()),
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
        pa.field("source_transaction", TRANSACTION_STRUCT),
        pa.field("target_transaction", TRANSACTION_STRUCT),
    ]
)


def empty_review_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in REVIEW_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=REVIEW_SCHEMA)
