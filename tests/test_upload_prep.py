from __future__ import annotations

import pandas as pd
import pytest

import ynab_il_importer.upload_prep as upload_prep


def _accounts() -> list[dict[str, str]]:
    return [
        {"id": "acc-bank", "name": "Bank Leumi", "transfer_payee_id": "payee-bank"},
        {"id": "acc-cash", "name": "Cash", "transfer_payee_id": "payee-cash"},
    ]


def _categories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"category_group": "Flexible expenses", "category_name": "Groceries", "category_id": "cat-groceries", "hidden": False},
            {"category_group": "Internal Master Category", "category_name": "Uncategorized", "category_id": "cat-uncat", "hidden": False},
        ]
    )


def _api_like_categories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"category_group": "Flexible expenses", "category_name": "🛍️Groceries", "category_id": "cat-groceries", "hidden": False},
            {"category_group": "Internal Master Category", "category_name": "Inflow: Ready to Assign", "category_id": "cat-rta", "hidden": False},
        ]
    )


def test_prepare_upload_transactions_maps_regular_and_transfer_rows() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.50", "0"],
            "inflow_ils": ["0", "25.00"],
            "memo": ["groceries", "cash deposit"],
            "payee_selected": ["Superpharm", "Transfer : Cash"],
            "category_selected": ["Groceries", ""],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert prepared.loc[0, "account_id"] == "acc-bank"
    assert prepared.loc[0, "category_id"] == "cat-groceries"
    assert prepared.loc[0, "payee_name_upload"] == "Superpharm"
    assert prepared.loc[0, "payee_id"] == ""
    assert prepared.loc[0, "amount_milliunits"] == -10500
    assert prepared.loc[1, "payee_id"] == "payee-cash"
    assert prepared.loc[1, "category_id"] == ""
    assert prepared.loc[1, "upload_kind"] == "transfer"

    payload = upload_prep.upload_payload_records(prepared)
    assert payload[0]["payee_name"] == "Superpharm"
    assert payload[0]["category_id"] == "cat-groceries"
    assert payload[1]["payee_id"] == "payee-cash"
    assert "category_id" not in payload[1]


def test_prepare_upload_transactions_generates_stable_occurrence_import_ids() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-01"],
            "outflow_ils": ["10.00", "10.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["a", "b"],
            "payee_selected": ["Superpharm", "Superpharm"],
            "category_selected": ["Groceries", "Groceries"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert prepared["import_id"].tolist() == [
        "YNAB:-10000:2026-03-01:1",
        "YNAB:-10000:2026-03-01:2",
    ]


def test_prepare_upload_transactions_requires_category_for_non_transfer() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.00"],
            "inflow_ils": ["0"],
            "memo": [""],
            "payee_selected": ["Superpharm"],
            "category_selected": [""],
        }
    )

    with pytest.raises(ValueError, match="not ready for upload"):
        upload_prep.prepare_upload_transactions(
            reviewed,
            accounts=_accounts(),
            categories_df=_categories(),
        )


def test_ready_mask_treats_transfer_without_category_as_ready() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.00", "5.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["", ""],
            "payee_selected": ["Transfer : Cash", "Cafe"],
            "category_selected": ["", ""],
        }
    )

    assert upload_prep.ready_mask(reviewed).tolist() == [True, False]


def test_uploadable_account_mask_marks_unknown_accounts() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", ""],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.00", "5.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["", ""],
            "payee_selected": ["Superpharm", "Cafe"],
            "category_selected": ["Groceries", "Groceries"],
        }
    )

    assert upload_prep.uploadable_account_mask(reviewed, _accounts()).tolist() == [True, False]


def test_prepare_upload_transactions_resolves_simplified_category_aliases() -> None:
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.00", "0"],
            "inflow_ils": ["0", "5.00"],
            "memo": ["", ""],
            "payee_selected": ["Shop", "Employer"],
            "category_selected": ["Groceries", "Ready to Assign"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_api_like_categories(),
    )

    assert prepared["category_id"].tolist() == ["cat-groceries", "cat-rta"]
