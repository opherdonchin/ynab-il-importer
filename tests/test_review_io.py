from __future__ import annotations

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import ynab_il_importer.review_app.io as review_io


def _working_from_artifact_path(path) -> pd.DataFrame:
    return review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(path)
    ).to_pandas()


def _working_from_artifact_df(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    return review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(review_io.coerce_review_artifact_table(df))
    ).to_pandas()


def test_load_review_artifact_requires_parquet(tmp_path) -> None:
    path = tmp_path / "empty_review.csv"
    pd.DataFrame(columns=review_io.REQUIRED_COLUMNS).to_csv(
        path, index=False, encoding="utf-8-sig"
    )

    with pytest.raises(ValueError, match="must be parquet"):
        review_io.load_review_artifact(path)


def test_load_review_artifact_rejects_non_artifact_parquet(tmp_path) -> None:
    path = tmp_path / "not_review.parquet"
    pa.table({"transaction_id": ["t1"]}).combine_chunks().to_pandas().to_parquet(path)

    with pytest.raises(ValueError, match="not a canonical review artifact"):
        review_io.load_review_artifact(path)


def test_project_review_artifact_to_working_dataframe_accepts_polars_artifact_rows() -> (
    None
):
    df = pl.DataFrame(
        {
            "transaction_id": ["t1"],
            "account_name": ["Account 1"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10"],
            "inflow_ils": ["0"],
            "memo": ["memo"],
            "payee_options": ["Cafe"],
            "category_options": ["Food"],
            "match_status": ["source_only"],
            "update_maps": [""],
            "decision_action": ["create_target"],
            "fingerprint": ["fp1"],
            "workflow_type": ["institutional"],
            "source_payee_selected": [""],
            "source_category_selected": [""],
            "target_payee_selected": ["Cafe"],
            "target_category_selected": ["Food"],
            "source_present": [True],
            "target_present": [False],
        }
    )

    loaded = _working_from_artifact_df(df)

    assert isinstance(loaded, pd.DataFrame)
    assert loaded.loc[0, "target_payee_selected"] == "Cafe"
    assert bool(loaded.loc[0, "reviewed"]) is False


def test_project_review_artifact_to_working_dataframe_preserves_source_lineage_ids() -> (
    None
):
    table = review_io.coerce_review_artifact_table(
        pd.DataFrame(
            [
                {
                    "review_transaction_id": "row-1",
                    "workflow_type": "institutional",
                    "relation_kind": "source_only",
                    "match_status": "source_only",
                    "decision_action": "create_target",
                    "source_present": True,
                    "target_present": False,
                    "source_current": {
                        "artifact_kind": "review_source_transaction",
                        "artifact_version": "transaction_v1",
                        "source_system": "bank",
                        "transaction_id": "src_1",
                        "import_id": "BANK:V1:111111111111111111111111",
                        "parent_transaction_id": "src_1",
                        "account_name": "Bank Leumi",
                        "source_account": "123456",
                        "date": "2026-03-01",
                        "inflow_ils": 0.0,
                        "outflow_ils": 10.0,
                        "signed_amount_ils": -10.0,
                        "payee_raw": "Cafe",
                        "category_raw": "",
                        "memo": "memo",
                        "fingerprint": "fp-1",
                        "approved": False,
                        "is_subtransaction": False,
                    },
                    "source_original": {
                        "artifact_kind": "review_source_transaction",
                        "artifact_version": "transaction_v1",
                        "source_system": "bank",
                        "transaction_id": "src_1",
                        "import_id": "BANK:V1:111111111111111111111111",
                        "parent_transaction_id": "src_1",
                        "account_name": "Bank Leumi",
                        "source_account": "123456",
                        "date": "2026-03-01",
                        "inflow_ils": 0.0,
                        "outflow_ils": 10.0,
                        "signed_amount_ils": -10.0,
                        "payee_raw": "Cafe",
                        "category_raw": "",
                        "memo": "memo",
                        "fingerprint": "fp-1",
                        "approved": False,
                        "is_subtransaction": False,
                    },
                }
            ]
        )
    )

    loaded = review_io.project_review_artifact_to_working_dataframe(table).to_pandas()

    assert loaded.loc[0, "source_import_id"] == "BANK:V1:111111111111111111111111"
    assert loaded.loc[0, "source_bank_txn_id"] == "BANK:V1:111111111111111111111111"


def test_project_review_artifact_to_working_dataframe_preserves_target_only_values() -> (
    None
):
    table = review_io.coerce_review_artifact_table(
        pd.DataFrame(
            [
                {
                    "review_transaction_id": "row-target-only",
                    "workflow_type": "institutional",
                    "relation_kind": "target_only_manual",
                    "match_status": "target_only",
                    "decision_action": "ignore_row",
                    "source_present": False,
                    "target_present": True,
                    "target_account": "Leumi loan 64370054",
                    "target_current": {
                        "artifact_kind": "transaction",
                        "artifact_version": "transaction_v1",
                        "source_system": "ynab",
                        "transaction_id": "tgt-1",
                        "parent_transaction_id": "tgt-1",
                        "account_name": "Leumi loan 64370054",
                        "source_account": "Leumi loan 64370054",
                        "date": "2026-03-10",
                        "inflow_ils": 1663.12,
                        "outflow_ils": 0.0,
                        "signed_amount_ils": 1663.12,
                        "payee_raw": "Transfer : In Family",
                        "category_raw": "Leumi loan 64370054",
                        "memo": "",
                        "fingerprint": "transfer in family",
                        "approved": False,
                        "is_subtransaction": False,
                    },
                }
            ]
        )
    )

    loaded = review_io.project_review_artifact_to_working_dataframe(table).to_pandas()

    assert loaded.loc[0, "account_name"] == "Leumi loan 64370054"
    assert loaded.loc[0, "target_account"] == "Leumi loan 64370054"
    assert loaded.loc[0, "source_account"] == ""
    assert loaded.loc[0, "inflow_ils"] == pytest.approx(1663.12)
    assert loaded.loc[0, "outflow_ils"] == pytest.approx(0.0)
    assert loaded.loc[0, "target_payee_current"] == "Transfer : In Family"


def test_load_category_list_accepts_arrow_table() -> None:
    table = pa.table(
        {
            "category_group": ["Everyday"],
            "category_name": ["Groceries"],
        }
    )

    loaded = review_io.load_category_list(table)

    assert loaded.to_dict(orient="records") == [
        {"category_group": "Everyday", "category_name": "Groceries"}
    ]


def test_save_reviewed_transactions_accepts_polars_dataframe(tmp_path) -> None:
    path = tmp_path / "review.csv"
    df = pl.DataFrame(
        {
            "transaction_id": ["t1"],
            "account_name": ["Account 1"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10"],
            "inflow_ils": ["0"],
            "memo": ["memo"],
            "payee_options": ["Cafe"],
            "category_options": ["Food"],
            "match_status": ["source_only"],
            "update_maps": [""],
            "decision_action": ["create_target"],
            "fingerprint": ["fp1"],
            "workflow_type": ["institutional"],
            "source_payee_selected": [""],
            "source_category_selected": [""],
            "target_payee_selected": ["Cafe"],
            "target_category_selected": ["Food"],
            "reviewed": [True],
            "source_present": [True],
            "target_present": [False],
        }
    )

    review_io.save_reviewed_transactions(df, path)
    loaded = pd.read_csv(path, dtype="string").fillna("")

    assert bool(loaded.loc[0, "reviewed"]) is True
    assert loaded.loc[0, "target_category_selected"] == "Food"


def test_save_then_load_round_trip_preserves_review_fields(tmp_path) -> None:
    path = tmp_path / "review.csv"
    df = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "memo",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "update_maps": "fingerprint_add_source",
                "decision_action": "create_target",
                "fingerprint": "fp1",
                "workflow_type": "institutional",
                "source_payee_selected": "Cafe source",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe updated",
                "target_category_selected": "Dining updated",
                "payee_selected": "stale alias",
                "category_selected": "stale alias",
                "reviewed": True,
                "source_present": True,
                "target_present": False,
            }
        ]
    )

    review_io.save_reviewed_transactions(df, path)
    loaded = pd.read_csv(path, dtype="string").fillna("")

    assert loaded.loc[0, "target_payee_selected"] == "Cafe updated"
    assert loaded.loc[0, "target_category_selected"] == "Dining updated"
    assert bool(loaded.loc[0, "reviewed"]) is True
    assert bool(loaded.loc[0, "source_present"]) is True
    assert bool(loaded.loc[0, "target_present"]) is False
    assert "payee_selected" not in loaded.columns
    assert "category_selected" not in loaded.columns


def test_save_review_artifact_parquet_round_trip_preserves_flat_sides_and_splits(
    tmp_path,
) -> None:
    path = tmp_path / "review.parquet"
    df = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "memo",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "fingerprint": "fp1",
                "workflow_type": "institutional",
                "source_payee_selected": "Cafe source",
                "source_category_selected": "",
                "target_payee_selected": "Cafe target",
                "target_category_selected": "Food",
                "reviewed": True,
                "source_present": True,
                "target_present": False,
                "source_row_id": "src-1",
                "source_context_kind": "ynab_split_category_match",
                "source_context_category_id": "cat-food",
                "source_context_category_name": "Food",
                "source_context_matching_split_ids": "sub-1",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "source_transaction": {
                    "artifact_kind": "normalized_source",
                    "artifact_version": "transaction_v1",
                    "source_system": "bank",
                    "transaction_id": "src-1",
                    "parent_transaction_id": "src-1",
                    "account_name": "Account 1",
                    "source_account": "Account 1",
                    "date": "2026-03-01",
                    "inflow_ils": 0.0,
                    "outflow_ils": 10.0,
                    "signed_amount_ils": -10.0,
                    "payee_raw": "Cafe source",
                    "category_raw": "",
                    "memo": "memo",
                    "fingerprint": "fp1",
                    "approved": False,
                    "is_subtransaction": False,
                },
            }
        ]
    )

    review_io.save_reviewed_transactions(df, path)
    loaded = _working_from_artifact_path(path)

    assert loaded.loc[0, "target_payee_selected"] == "Cafe target"
    assert loaded.loc[0, "source_context_kind"] == "ynab_split_category_match"
    assert loaded.loc[0, "source_context_category_id"] == "cat-food"
    assert loaded.loc[0, "source_context_category_name"] == "Food"
    assert loaded.loc[0, "source_context_matching_split_ids"] == "sub-1"
    assert loaded.loc[0, "source_transaction_id"] == "src-1"
    assert loaded.loc[0, "source_payee_current"] == "Cafe source"
    assert loaded.loc[0, "source_splits"] is None


def test_project_review_artifact_to_working_dataframe_preserves_flat_sides_and_context(
    tmp_path,
) -> None:
    path = tmp_path / "review_polars.parquet"
    df = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "memo",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "fingerprint": "fp1",
                "workflow_type": "institutional",
                "source_present": True,
                "target_present": False,
                "source_context_kind": "ynab_split_category_match",
                "source_context_category_id": "cat-food",
                "source_context_category_name": "Food",
                "source_context_matching_split_ids": "sub-1",
                "source_payee_selected": "Cafe source",
                "source_category_selected": "",
                "target_payee_selected": "Cafe target",
                "target_category_selected": "Food",
                "source_transaction": {
                    "artifact_kind": "normalized_source",
                    "artifact_version": "transaction_v1",
                    "source_system": "bank",
                    "transaction_id": "src-1",
                    "parent_transaction_id": "src-1",
                    "account_name": "Account 1",
                    "source_account": "Account 1",
                    "date": "2026-03-01",
                    "inflow_ils": 0.0,
                    "outflow_ils": 10.0,
                    "signed_amount_ils": -10.0,
                    "payee_raw": "Cafe source",
                    "category_raw": "",
                    "memo": "memo",
                    "fingerprint": "fp1",
                    "approved": False,
                    "is_subtransaction": False,
                    "splits": [
                        {
                            "split_id": "sub-1",
                            "parent_transaction_id": "src-1",
                            "ynab_subtransaction_id": "sub-1",
                            "payee_raw": "Cafe split",
                            "category_id": "cat-food",
                            "category_raw": "Food",
                            "memo": "",
                            "inflow_ils": 0.0,
                            "outflow_ils": 10.0,
                            "import_id": "",
                            "matched_transaction_id": "",
                        }
                    ],
                },
            }
        ]
    )

    review_io.save_review_artifact(df, path)
    loaded = review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(path)
    )

    assert isinstance(loaded, pl.DataFrame)
    row = loaded.to_dicts()[0]
    assert row["source_context_kind"] == "ynab_split_category_match"
    assert row["source_context_matching_split_ids"] == "sub-1"
    assert row["source_transaction_id"] == "src-1"
    assert row["source_splits"][0]["split_id"] == "sub-1"


def _legacy_institutional_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "transaction_id": "legacy-1",
                "source": "bank",
                "account_name": "Bank Leumi",
                "date": "2026-03-20",
                "outflow_ils": "15",
                "inflow_ils": "0",
                "memo": "Cafe memo",
                "fingerprint": "cafe",
                "payee_options": "Cafe",
                "category_options": "Eating Out",
                "payee_selected": "Cafe Roma",
                "category_selected": "Eating Out",
                "match_status": "ambiguous",
                "update_map": "TRUE",
                "bank_txn_id": "BANK:1",
                "card_txn_id": "",
                "reviewed": "TRUE",
            }
        ]
    )


def test_load_review_artifact_rejects_legacy_review_csv(tmp_path) -> None:
    path = tmp_path / "legacy_review.csv"
    _legacy_institutional_df().to_csv(path, index=False, encoding="utf-8-sig")

    with pytest.raises(ValueError, match="must be parquet"):
        review_io.load_review_artifact(path)


def test_load_review_artifact_rejects_false_changed_when_current_differs() -> None:
    df = pd.DataFrame(
        [
            {
                "review_transaction_id": "row-1",
                "workflow_type": "institutional",
                "source_present": True,
                "target_present": False,
                "changed": False,
                "source_current": {
                    "artifact_kind": "transaction",
                    "artifact_version": "transaction_v1",
                    "transaction_id": "src-1",
                    "parent_transaction_id": "src-1",
                    "account_name": "Account 1",
                    "source_account": "Account 1",
                    "date": "2026-03-01",
                    "inflow_ils": 0.0,
                    "outflow_ils": 10.0,
                    "signed_amount_ils": -10.0,
                    "payee_raw": "Cafe",
                    "category_raw": "Food",
                    "memo": "",
                    "fingerprint": "fp-1",
                    "approved": False,
                    "is_subtransaction": False,
                },
                "source_original": {
                    "artifact_kind": "transaction",
                    "artifact_version": "transaction_v1",
                    "transaction_id": "src-1",
                    "parent_transaction_id": "src-1",
                    "account_name": "Account 1",
                    "source_account": "Account 1",
                    "date": "2026-03-01",
                    "inflow_ils": 0.0,
                    "outflow_ils": 10.0,
                    "signed_amount_ils": -10.0,
                    "payee_raw": "Cafe original",
                    "category_raw": "Food",
                    "memo": "",
                    "fingerprint": "fp-1",
                    "approved": False,
                    "is_subtransaction": False,
                },
            }
        ]
    )

    with pytest.raises(ValueError, match="changed is FALSE"):
        review_io.coerce_review_artifact_table(df)


def test_load_review_artifact_rejects_split_sum_mismatch() -> None:
    split_txn = {
        "artifact_kind": "transaction",
        "artifact_version": "transaction_v1",
        "transaction_id": "src-1",
        "parent_transaction_id": "src-1",
        "account_name": "Account 1",
        "source_account": "Account 1",
        "date": "2026-03-01",
        "inflow_ils": 0.0,
        "outflow_ils": 10.0,
        "signed_amount_ils": -10.0,
        "payee_raw": "Cafe",
        "category_raw": "Split",
        "memo": "",
        "fingerprint": "fp-1",
        "approved": False,
        "is_subtransaction": False,
        "splits": [
            {
                "split_id": "sub-1",
                "parent_transaction_id": "src-1",
                "ynab_subtransaction_id": "sub-1",
                "payee_raw": "Cafe",
                "category_id": "cat-a",
                "category_raw": "Food",
                "memo": "",
                "inflow_ils": 0.0,
                "outflow_ils": 7.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
            {
                "split_id": "sub-2",
                "parent_transaction_id": "src-1",
                "ynab_subtransaction_id": "sub-2",
                "payee_raw": "Cafe",
                "category_id": "cat-b",
                "category_raw": "Dining",
                "memo": "",
                "inflow_ils": 0.0,
                "outflow_ils": 2.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
        ],
    }
    df = pd.DataFrame(
        [
            {
                "review_transaction_id": "row-1",
                "workflow_type": "institutional",
                "source_present": True,
                "target_present": False,
                "changed": False,
                "source_current": split_txn,
                "source_original": split_txn,
            }
        ]
    )

    with pytest.raises(ValueError, match="split amounts do not sum"):
        review_io.coerce_review_artifact_table(df)


def test_load_review_artifact_accepts_split_single_category() -> None:
    split_txn = {
        "artifact_kind": "transaction",
        "artifact_version": "transaction_v1",
        "transaction_id": "src-1",
        "parent_transaction_id": "src-1",
        "account_name": "Account 1",
        "source_account": "Account 1",
        "date": "2026-03-01",
        "inflow_ils": 0.0,
        "outflow_ils": 10.0,
        "signed_amount_ils": -10.0,
        "payee_raw": "Cafe",
        "category_raw": "Split",
        "memo": "",
        "fingerprint": "fp-1",
        "approved": False,
        "is_subtransaction": False,
        "splits": [
            {
                "split_id": "sub-1",
                "parent_transaction_id": "src-1",
                "ynab_subtransaction_id": "sub-1",
                "payee_raw": "Cafe",
                "category_id": "cat-a",
                "category_raw": "Food",
                "memo": "",
                "inflow_ils": 0.0,
                "outflow_ils": 7.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
            {
                "split_id": "sub-2",
                "parent_transaction_id": "src-1",
                "ynab_subtransaction_id": "sub-2",
                "payee_raw": "Cafe",
                "category_id": "cat-a",
                "category_raw": "Food",
                "memo": "",
                "inflow_ils": 0.0,
                "outflow_ils": 3.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
        ],
    }
    df = pd.DataFrame(
        [
            {
                "review_transaction_id": "row-1",
                "workflow_type": "institutional",
                "source_present": True,
                "target_present": False,
                "changed": False,
                "source_current": split_txn,
                "source_original": split_txn,
            }
        ]
    )

    table = review_io.coerce_review_artifact_table(df)

    record = table.to_pylist()[0]
    assert len(record["source_current"]["splits"]) == 2
    assert {
        line["category_raw"] for line in record["source_current"]["splits"]
    } == {"Food"}
