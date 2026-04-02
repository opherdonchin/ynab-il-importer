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
