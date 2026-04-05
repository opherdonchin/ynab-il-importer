from __future__ import annotations

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import ynab_il_importer.review_app.io as review_io


def _required_columns() -> list[str]:
    return list(review_io.REQUIRED_COLUMNS)


def test_load_proposed_transactions_accepts_empty_csv_with_headers(tmp_path) -> None:
    path = tmp_path / "empty_review.csv"
    pd.DataFrame(columns=_required_columns()).to_csv(path, index=False, encoding="utf-8-sig")

    loaded = review_io.load_proposed_transactions(path)

    assert loaded.empty
    assert "payee_selected" in loaded.columns
    assert "category_selected" in loaded.columns
    assert "reviewed" in loaded.columns


def test_load_proposed_transactions_raises_for_missing_columns(tmp_path) -> None:
    path = tmp_path / "missing_columns.csv"
    pd.DataFrame([{"transaction_id": "t1"}]).to_csv(path, index=False, encoding="utf-8-sig")

    with pytest.raises(ValueError, match="missing columns"):
        review_io.load_proposed_transactions(path)


def test_load_proposed_transactions_accepts_polars_dataframe() -> None:
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
        }
    )

    loaded = review_io.load_proposed_transactions(df)

    assert isinstance(loaded, pd.DataFrame)
    assert loaded.loc[0, "target_payee_selected"] == "Cafe"
    assert bool(loaded.loc[0, "reviewed"]) is False


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
        }
    )

    review_io.save_reviewed_transactions(df, path)
    loaded = review_io.load_proposed_transactions(path)

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
    loaded = review_io.load_proposed_transactions(path)

    assert loaded.loc[0, "target_payee_selected"] == "Cafe updated"
    assert loaded.loc[0, "target_category_selected"] == "Dining updated"
    assert loaded.loc[0, "payee_selected"] == "Cafe updated"
    assert loaded.loc[0, "category_selected"] == "Dining updated"
    assert bool(loaded.loc[0, "reviewed"]) is True
    assert bool(loaded.loc[0, "source_present"]) is True
    assert bool(loaded.loc[0, "target_present"]) is False


def test_save_review_artifact_parquet_round_trip_preserves_flat_sides_and_splits(tmp_path) -> None:
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
    loaded = review_io.load_proposed_transactions(path)

    assert loaded.loc[0, "target_payee_selected"] == "Cafe target"
    assert loaded.loc[0, "source_context_kind"] == "ynab_split_category_match"
    assert loaded.loc[0, "source_context_category_id"] == "cat-food"
    assert loaded.loc[0, "source_context_category_name"] == "Food"
    assert loaded.loc[0, "source_context_matching_split_ids"] == "sub-1"
    assert loaded.loc[0, "source_transaction_id"] == "src-1"
    assert loaded.loc[0, "source_payee_current"] == "Cafe source"
    assert loaded.loc[0, "source_splits"] is None


def test_load_review_artifact_polars_preserves_flat_sides_and_context(tmp_path) -> None:
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
    loaded = review_io.load_review_artifact_polars(path)

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


def test_load_proposed_transactions_rejects_legacy_review_csv_with_guidance(tmp_path) -> None:
    path = tmp_path / "legacy_review.csv"
    _legacy_institutional_df().to_csv(path, index=False, encoding="utf-8-sig")

    with pytest.raises(ValueError, match="run scripts/translate_review_csv.py first"):
        review_io.load_proposed_transactions(path)


def test_translate_review_dataframe_converts_legacy_institutional_review_csv() -> None:
    loaded = review_io.translate_review_dataframe(_legacy_institutional_df())

    assert review_io.detect_review_csv_format(loaded) == "unified_v1"
    assert loaded.loc[0, "legacy_review_schema"] == "legacy_institutional_v0"
    assert loaded.loc[0, "legacy_match_status"] == "ambiguous"
    assert loaded.loc[0, "match_status"] == "source_only"
    assert loaded.loc[0, "workflow_type"] == "institutional"
    assert loaded.loc[0, "decision_action"] == "create_target"
    assert loaded.loc[0, "update_maps"] == "payee_add_fingerprint"
    assert loaded.loc[0, "source_row_id"] == "BANK:1"
    assert loaded.loc[0, "target_payee_selected"] == "Cafe Roma"
    assert loaded.loc[0, "target_category_selected"] == "Eating Out"
    assert loaded.loc[0, "payee_selected"] == "Cafe Roma"
    assert loaded.loc[0, "category_selected"] == "Eating Out"
    assert bool(loaded.loc[0, "source_present"]) is True
    assert bool(loaded.loc[0, "target_present"]) is False
    assert bool(loaded.loc[0, "reviewed"]) is True


def test_load_review_artifact_rejects_false_changed_when_current_differs() -> None:
    df = pd.DataFrame(
        [
            {
                "review_transaction_id": "row-1",
                "workflow_type": "institutional",
                "source_present": True,
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
        review_io.load_review_artifact(df)


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
                "changed": False,
                "source_current": split_txn,
                "source_original": split_txn,
            }
        ]
    )

    with pytest.raises(ValueError, match="split amounts do not sum"):
        review_io.load_review_artifact(df)


def test_load_review_artifact_rejects_split_single_category() -> None:
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
                "changed": False,
                "source_current": split_txn,
                "source_original": split_txn,
            }
        ]
    )

    with pytest.raises(ValueError, match="split must span more than one category"):
        review_io.load_review_artifact(df)
