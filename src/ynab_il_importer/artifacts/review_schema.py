from __future__ import annotations

import pyarrow as pa

from ynab_il_importer.artifacts.transaction_schema import SPLIT_LINE_STRUCT


REVIEW_ARTIFACT_VERSION = "review_v3"

REVIEW_SIDE_SCALAR_FIELDS: list[pa.Field] = [
    pa.field("source_source_system", pa.string()),
    pa.field("source_transaction_id", pa.string()),
    pa.field("source_ynab_id", pa.string()),
    pa.field("source_import_id", pa.string()),
    pa.field("source_parent_transaction_id", pa.string()),
    pa.field("source_account_id", pa.string()),
    pa.field("source_account", pa.string()),
    pa.field("source_date", pa.string()),
    pa.field("source_secondary_date", pa.string()),
    pa.field("source_payee_current", pa.string()),
    pa.field("source_category_id", pa.string()),
    pa.field("source_category_current", pa.string()),
    pa.field("source_memo", pa.string()),
    pa.field("source_fingerprint", pa.string()),
    pa.field("source_description_raw", pa.string()),
    pa.field("source_description_clean", pa.string()),
    pa.field("source_merchant_raw", pa.string()),
    pa.field("source_ref", pa.string()),
    pa.field("source_cleared", pa.string()),
    pa.field("source_approved", pa.bool_()),
    pa.field("source_is_subtransaction", pa.bool_()),
    pa.field("source_bank_txn_id", pa.string()),
    pa.field("source_card_txn_id", pa.string()),
    pa.field("source_card_suffix", pa.string()),
    pa.field("target_source_system", pa.string()),
    pa.field("target_transaction_id", pa.string()),
    pa.field("target_ynab_id", pa.string()),
    pa.field("target_import_id", pa.string()),
    pa.field("target_parent_transaction_id", pa.string()),
    pa.field("target_account_id", pa.string()),
    pa.field("target_account", pa.string()),
    pa.field("target_date", pa.string()),
    pa.field("target_secondary_date", pa.string()),
    pa.field("target_payee_current", pa.string()),
    pa.field("target_category_id", pa.string()),
    pa.field("target_category_current", pa.string()),
    pa.field("target_memo", pa.string()),
    pa.field("target_fingerprint", pa.string()),
    pa.field("target_description_raw", pa.string()),
    pa.field("target_description_clean", pa.string()),
    pa.field("target_merchant_raw", pa.string()),
    pa.field("target_ref", pa.string()),
    pa.field("target_cleared", pa.string()),
    pa.field("target_approved", pa.bool_()),
    pa.field("target_is_subtransaction", pa.bool_()),
]

REVIEW_SCHEMA = pa.schema(
    [
        pa.field("artifact_kind", pa.string()),
        pa.field("artifact_version", pa.string()),
        pa.field("review_transaction_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("account_name", pa.string()),
        pa.field("date", pa.string()),
        pa.field("outflow_ils", pa.float64()),
        pa.field("inflow_ils", pa.float64()),
        pa.field("memo", pa.string()),
        pa.field("fingerprint", pa.string()),
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
        *REVIEW_SIDE_SCALAR_FIELDS,
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
        pa.field("source_splits", pa.list_(SPLIT_LINE_STRUCT)),
        pa.field("target_splits", pa.list_(SPLIT_LINE_STRUCT)),
    ]
)


def empty_review_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in REVIEW_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=REVIEW_SCHEMA)
