from __future__ import annotations

import pandas as pd
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
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "payee_selected": "Cafe updated",
                "category_selected": "Dining updated",
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
