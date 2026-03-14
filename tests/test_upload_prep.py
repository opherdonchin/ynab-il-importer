from __future__ import annotations

import pandas as pd
import pytest

import ynab_il_importer.bank_identity as bank_identity
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


def test_prepare_upload_transactions_uses_bank_txn_id_for_bank_rows() -> None:
    bank_txn_id = bank_identity.make_bank_txn_id(
        source="bank",
        source_account="123456",
        date="2026-03-01",
        secondary_date="2026-03-02",
        outflow_ils=10,
        inflow_ils=0,
        ref="0042",
        description_raw="groceries",
    )
    reviewed = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "source": ["bank", "card"],
            "account_name": ["Bank Leumi", "Cash"],
            "source_account": ["123456", ""],
            "card_suffix": ["7195", ""],
            "date": ["2026-03-01", "2026-03-01"],
            "secondary_date": ["2026-03-02", ""],
            "outflow_ils": ["10.00", "10.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["groceries", "card memo"],
            "ref": ["0042", ""],
            "balance_ils": ["100.00", ""],
            "ynab_account_id": ["acc-bank", ""],
            "bank_txn_id": [bank_txn_id, ""],
            "payee_selected": ["Shop", "Shop"],
            "category_selected": ["Groceries", "Groceries"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert prepared.loc[0, "import_id"] == bank_txn_id
    assert prepared.loc[1, "import_id"] == "YNAB:-10000:2026-03-01:1"
    assert prepared.loc[0, "bank_txn_id"] == bank_txn_id
    assert prepared.loc[0, "source_account"] == "123456"
    assert prepared.loc[0, "card_suffix"] == "7195"
    assert prepared.loc[0, "secondary_date"] == "2026-03-02"
    assert prepared.loc[0, "ref"] == "0042"


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


def test_upload_preflight_reports_duplicate_and_match_risks() -> None:
    prepared = pd.DataFrame(
        {
            "import_id": ["YNAB:-1000:2026-01-01:1", "YNAB:-1000:2026-01-01:1", "YNAB:-2000:2026-01-02:1"],
            "account_id": ["acc-bank", "acc-bank", "acc-bank"],
            "date": ["2026-01-01", "2026-01-01", "2026-01-02"],
            "amount_milliunits": [-1000, -1000, -2000],
            "upload_kind": ["regular", "regular", "transfer"],
            "payee_id": ["", "", "payee-cash"],
            "payee_name_upload": ["A", "B", ""],
            "category_id": ["cat-groceries", "cat-groceries", ""],
        }
    )
    existing = [
        {
            "id": "existing-1",
            "account_id": "acc-bank",
            "date": "2026-01-02",
            "amount": -2000,
            "import_id": "",
            "matched_transaction_id": "",
            "transfer_account_id": "",
        }
    ]

    preflight = upload_prep.upload_preflight(prepared, existing)

    assert preflight["payload_duplicate_import_keys"] == [("acc-bank", "YNAB:-1000:2026-01-01:1")]
    assert preflight["existing_import_id_hits"] == []
    assert preflight["potential_match_import_ids"] == ["YNAB:-2000:2026-01-02:1"]
    assert preflight["transfer_payload_issue_ids"] == []


def test_upload_preflight_allows_same_import_id_on_different_accounts() -> None:
    prepared = pd.DataFrame(
        {
            "import_id": ["YNAB:-1000:2026-01-01:1", "YNAB:-1000:2026-01-01:1"],
            "account_id": ["acc-bank", "acc-card"],
            "date": ["2026-01-01", "2026-01-01"],
            "amount_milliunits": [-1000, -1000],
            "upload_kind": ["regular", "regular"],
            "payee_id": ["", ""],
            "payee_name_upload": ["A", "B"],
            "category_id": ["cat-groceries", "cat-groceries"],
        }
    )

    preflight = upload_prep.upload_preflight(prepared, [])

    assert preflight["payload_duplicate_import_keys"] == []


def test_summarize_upload_response_counts_matches_and_transfers() -> None:
    response = {
        "transaction_ids": ["t1", "t2", "t3"],
        "duplicate_import_ids": ["dup-1"],
        "transactions": [
            {
                "id": "t1",
                "account_id": "acc-bank",
                "date": "2026-01-01",
                "amount": -1000,
                "matched_transaction_id": "",
                "transfer_account_id": "",
            },
            {
                "id": "t2",
                "account_id": "acc-bank",
                "date": "2026-01-02",
                "amount": -2000,
                "matched_transaction_id": "manual-1",
                "transfer_account_id": "",
            },
            {
                "id": "t3",
                "account_id": "acc-bank",
                "date": "2026-01-03",
                "amount": -3000,
                "matched_transaction_id": "",
                "transfer_account_id": "acc-cash",
            },
        ],
    }

    summary = upload_prep.summarize_upload_response(response)

    assert summary == {
        "saved": 3,
        "duplicate_import_ids": 1,
        "matched_existing": 1,
        "transfer_saved": 1,
    }


def test_classify_upload_result_detects_idempotent_rerun() -> None:
    outcome = upload_prep.classify_upload_result(
        {
            "saved": 0,
            "duplicate_import_ids": 561,
            "matched_existing": 0,
            "transfer_saved": 0,
        },
        prepared_count=561,
    )

    assert outcome["idempotent_rerun"] is True
    assert outcome["verification_needed"] is False
    assert outcome["status"] == "idempotent rerun confirmed"


def test_classify_upload_result_requires_verification_when_transactions_saved() -> None:
    outcome = upload_prep.classify_upload_result(
        {
            "saved": 7,
            "duplicate_import_ids": 0,
            "matched_existing": 2,
            "transfer_saved": 3,
        },
        prepared_count=10,
    )

    assert outcome["idempotent_rerun"] is False
    assert outcome["verification_needed"] is True
    assert outcome["status"] == "new transactions saved"


def test_verify_upload_response_checks_transfer_and_category_fields() -> None:
    prepared = pd.DataFrame(
        {
            "import_id": ["imp-regular", "imp-transfer"],
            "account_id": ["acc-bank", "acc-bank"],
            "date": ["2026-01-01", "2026-01-02"],
            "amount_milliunits": [-1000, -2000],
            "upload_kind": ["regular", "transfer"],
            "category_id": ["cat-groceries", ""],
            "transfer_target_account_id": ["", "acc-cash"],
        }
    )
    response = {
        "transaction_ids": ["t1", "t2"],
        "transactions": [
            {
                "id": "t1",
                "import_id": "imp-regular",
                "account_id": "acc-bank",
                "date": "2026-01-01",
                "amount": -1000,
                "category_id": "cat-groceries",
                "transfer_account_id": "",
            },
            {
                "id": "t2",
                "import_id": "imp-transfer",
                "account_id": "acc-bank",
                "date": "2026-01-02",
                "amount": -2000,
                "category_id": "",
                "transfer_account_id": "acc-cash",
            },
        ],
    }

    verification = upload_prep.verify_upload_response(prepared, response)

    assert verification == {
        "checked": 2,
        "missing_saved_transactions": [],
        "amount_mismatches": [],
        "date_mismatches": [],
        "account_mismatches": [],
        "transfer_mismatches": [],
        "category_mismatches": [],
    }


def test_verify_upload_response_allows_same_import_id_on_different_accounts() -> None:
    prepared = pd.DataFrame(
        {
            "import_id": ["imp-shared", "imp-shared"],
            "account_id": ["acc-bank", "acc-card"],
            "date": ["2026-01-01", "2026-01-01"],
            "amount_milliunits": [-1000, -1000],
            "upload_kind": ["regular", "regular"],
            "category_id": ["cat-groceries", "cat-groceries"],
            "transfer_target_account_id": ["", ""],
        }
    )
    response = {
        "transaction_ids": ["t1", "t2"],
        "transactions": [
            {
                "id": "t1",
                "import_id": "imp-shared",
                "account_id": "acc-bank",
                "date": "2026-01-01",
                "amount": -1000,
                "category_id": "cat-groceries",
                "transfer_account_id": "",
            },
            {
                "id": "t2",
                "import_id": "imp-shared",
                "account_id": "acc-card",
                "date": "2026-01-01",
                "amount": -1000,
                "category_id": "cat-groceries",
                "transfer_account_id": "",
            },
        ],
    }

    verification = upload_prep.verify_upload_response(prepared, response)

    assert verification == {
        "checked": 2,
        "missing_saved_transactions": [],
        "amount_mismatches": [],
        "date_mismatches": [],
        "account_mismatches": [],
        "transfer_mismatches": [],
        "category_mismatches": [],
    }
