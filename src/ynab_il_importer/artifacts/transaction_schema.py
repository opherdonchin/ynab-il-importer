from __future__ import annotations

import pyarrow as pa


TRANSACTION_ARTIFACT_VERSION = "transaction_v1"

SPLIT_LINE_STRUCT = pa.struct(
    [
        pa.field("split_id", pa.string()),
        pa.field("parent_transaction_id", pa.string()),
        pa.field("ynab_subtransaction_id", pa.string()),
        pa.field("payee_raw", pa.string()),
        pa.field("category_id", pa.string()),
        pa.field("category_raw", pa.string()),
        pa.field("memo", pa.string()),
        pa.field("inflow_ils", pa.float64()),
        pa.field("outflow_ils", pa.float64()),
        pa.field("import_id", pa.string()),
        pa.field("matched_transaction_id", pa.string()),
    ]
)

TRANSACTION_SCHEMA = pa.schema(
    [
        pa.field("artifact_kind", pa.string()),
        pa.field("artifact_version", pa.string()),
        pa.field("source_system", pa.string()),
        pa.field("transaction_id", pa.string()),
        pa.field("ynab_id", pa.string()),
        pa.field("import_id", pa.string()),
        pa.field("parent_transaction_id", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("account_name", pa.string()),
        pa.field("source_account", pa.string()),
        pa.field("date", pa.string()),
        pa.field("secondary_date", pa.string()),
        pa.field("inflow_ils", pa.float64()),
        pa.field("outflow_ils", pa.float64()),
        pa.field("signed_amount_ils", pa.float64()),
        pa.field("balance_ils", pa.float64()),
        pa.field("payee_raw", pa.string()),
        pa.field("category_id", pa.string()),
        pa.field("category_raw", pa.string()),
        pa.field("memo", pa.string()),
        pa.field("txn_kind", pa.string()),
        pa.field("fingerprint", pa.string()),
        pa.field("description_raw", pa.string()),
        pa.field("description_clean", pa.string()),
        pa.field("description_clean_norm", pa.string()),
        pa.field("merchant_raw", pa.string()),
        pa.field("max_sheet", pa.string()),
        pa.field("max_txn_type", pa.string()),
        pa.field("max_original_amount", pa.float64()),
        pa.field("max_original_currency", pa.string()),
        pa.field("ref", pa.string()),
        pa.field("matched_transaction_id", pa.string()),
        pa.field("cleared", pa.string()),
        pa.field("approved", pa.bool_()),
        pa.field("is_subtransaction", pa.bool_()),
        pa.field("splits", pa.list_(SPLIT_LINE_STRUCT)),
    ]
)

TRANSACTION_STRUCT = pa.struct(list(TRANSACTION_SCHEMA))


def empty_transaction_table() -> pa.Table:
    arrays = [pa.array([], type=field.type) for field in TRANSACTION_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=TRANSACTION_SCHEMA)
