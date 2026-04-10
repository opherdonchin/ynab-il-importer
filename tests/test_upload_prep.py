from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.upload_prep as upload_prep


def _accounts() -> list[dict[str, str]]:
    return [
        {
            "id": "acc-bank",
            "name": "Bank Leumi",
            "transfer_payee_id": "payee-bank",
            "on_budget": True,
        },
        {
            "id": "acc-cash",
            "name": "Cash",
            "transfer_payee_id": "payee-cash",
            "on_budget": True,
        },
        {
            "id": "acc-loan",
            "name": "Loan",
            "transfer_payee_id": "payee-loan",
            "on_budget": False,
        },
    ]


def _categories() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"category_group": "Flexible expenses", "category_name": "Groceries", "category_id": "cat-groceries", "hidden": False},
            {"category_group": "Fixed expenses", "category_name": "Loan Paydown", "category_id": "cat-loan", "hidden": False},
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


def _reviewed_df(columns: dict[str, list[object]]) -> pl.DataFrame:
    data = dict(columns)
    row_count = len(next(iter(data.values()))) if data else 0
    payee_selected = data.pop("payee_selected", [""] * row_count)
    category_selected = data.pop("category_selected", [""] * row_count)
    data.setdefault("target_payee_selected", payee_selected)
    data.setdefault("target_category_selected", category_selected)
    data.setdefault("decision_action", ["create_target"] * row_count)
    data.setdefault("reviewed", [True] * row_count)
    data.setdefault("source_present", [True] * row_count)
    data.setdefault("target_present", [False] * row_count)
    return pl.DataFrame(data)


def _row(df: pl.DataFrame, index: int = 0) -> dict[str, object]:
    return df.row(index, named=True)


def _txn(
    *,
    transaction_id: str,
    payee: str,
    category: str,
    category_id: str = "cat-groceries",
    amount: float = 10.0,
    memo: str = "memo",
) -> dict[str, object]:
    return {
        "artifact_kind": "transaction",
        "artifact_version": "transaction_v1",
        "source_system": "ynab",
        "transaction_id": transaction_id,
        "ynab_id": transaction_id,
        "import_id": "",
        "parent_transaction_id": transaction_id,
        "account_id": "acc-bank",
        "account_name": "Bank Leumi",
        "source_account": "Bank Leumi",
        "date": "2026-03-01",
        "secondary_date": "",
        "inflow_ils": 0.0,
        "outflow_ils": amount,
        "signed_amount_ils": -amount,
        "payee_raw": payee,
        "category_id": category_id,
        "category_raw": category,
        "memo": memo,
        "txn_kind": "",
        "fingerprint": f"fp-{transaction_id}",
        "description_raw": "",
        "description_clean": "",
        "description_clean_norm": "",
        "merchant_raw": "",
        "ref": "",
        "matched_transaction_id": "",
        "cleared": "uncleared",
        "approved": False,
        "is_subtransaction": False,
        "splits": None,
    }


def _split_edit_working_row(*, amount: float = 10.0) -> pl.DataFrame:
    source_txn = _txn(transaction_id="src-1", payee="Source Cafe", category="Groceries", amount=amount)
    target_original = _txn(transaction_id="tgt-1", payee="Target Cafe", category="Groceries", amount=amount)
    working = review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(
            review_io.coerce_review_artifact_table(
                pd.DataFrame(
                    [
                        {
                            "transaction_id": "review-1",
                            "account_name": "Bank Leumi",
                            "date": "2026-03-01",
                            "outflow_ils": amount,
                            "inflow_ils": 0.0,
                            "memo": "Parent memo",
                            "source": "bank",
                            "workflow_type": "cross_budget",
                            "decision_action": "create_target",
                            "reviewed": True,
                            "changed": True,
                            "source_present": True,
                            "target_present": True,
                            "source_row_id": "src-1",
                            "target_row_id": "tgt-1",
                            "target_account": "Bank Leumi",
                            "source_payee_selected": "Source Cafe",
                            "source_category_selected": "Groceries",
                            "target_payee_selected": "Parent Payee",
                            "target_category_selected": "Groceries",
                            "source_current": source_txn,
                            "source_original": source_txn,
                            "target_current": {**target_original, "payee_raw": "Parent Payee", "memo": "Parent memo"},
                            "target_original": target_original,
                        }
                    ]
                )
            )
        )
    )
    return working


def test_prepare_upload_transactions_maps_regular_and_transfer_rows() -> None:
    reviewed = _reviewed_df(
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

    assert _row(prepared, 0)["account_id"] == "acc-bank"
    assert _row(prepared, 0)["category_id"] == "cat-groceries"
    assert _row(prepared, 0)["payee_name_upload"] == "Superpharm"
    assert _row(prepared, 0)["payee_id"] == ""
    assert _row(prepared, 0)["amount_milliunits"] == -10500
    assert _row(prepared, 1)["payee_id"] == "payee-cash"
    assert _row(prepared, 1)["category_id"] == ""
    assert _row(prepared, 1)["upload_kind"] == "transfer"

    payload = upload_prep.upload_payload_records(prepared)
    assert payload[0]["payee_name"] == "Superpharm"
    assert payload[0]["category_id"] == "cat-groceries"
    assert payload[1]["payee_id"] == "payee-cash"
    assert "category_id" not in payload[1]


def test_prepare_upload_transactions_accepts_canonical_review_artifact(tmp_path) -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "source_account": ["Bank Leumi"],
            "target_account": ["Bank Leumi"],
            "source_date": ["2026-03-01"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.50"],
            "inflow_ils": ["0"],
            "source_memo": ["groceries"],
            "memo": ["groceries"],
            "source_fingerprint": ["shop"],
            "fingerprint": ["shop"],
            "source": ["bank"],
            "source_present": [True],
            "target_present": [False],
            "payee_selected": ["Superpharm"],
            "category_selected": ["Groceries"],
        }
    )
    artifact_path = tmp_path / "review.parquet"
    review_io.save_review_artifact(reviewed, artifact_path)

    prepared = upload_prep.prepare_upload_transactions(
        upload_prep.load_upload_working_frame(artifact_path),
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["account_id"] == "acc-bank"
    assert _row(prepared, 0)["category_id"] == "cat-groceries"


def test_prepare_upload_transactions_explodes_committed_target_splits() -> None:
    working = _split_edit_working_row(amount=10.0)
    split_working = review_state.apply_target_split_edit(
        working,
        0,
        lines=[
            {
                "split_id": "sub-1",
                "payee_raw": "",
                "category_raw": "Groceries",
                "amount_ils": -7.0,
                "memo": "Line one",
            },
            {
                "split_id": "sub-2",
                "payee_raw": "Line Payee",
                "category_raw": "Uncategorized",
                "amount_ils": -3.0,
                "memo": "Line two",
            },
        ],
    )

    prepared = upload_prep.prepare_upload_transactions(
        split_working,
        accounts=_accounts(),
        categories_df=_categories(),
    )
    units = upload_prep.assemble_upload_transaction_units(prepared)
    payload = upload_prep.upload_payload_records(prepared)

    assert prepared["upload_transaction_id"].to_list() == ["review-1", "review-1"]
    assert prepared["import_id"].to_list() == [
        "YNAB:-10000:2026-03-01:1",
        "YNAB:-10000:2026-03-01:1",
    ]
    assert prepared["amount_milliunits"].to_list() == [-7000, -3000]
    assert prepared["parent_payee_name_upload"].to_list() == ["Parent Payee", "Parent Payee"]
    assert prepared["payee_name_upload"].to_list() == ["", "Line Payee"]
    assert prepared["parent_memo"].to_list() == ["Parent memo", "Parent memo"]
    assert prepared["subtransaction_memo"].to_list() == ["Line one", "Line two"]
    assert prepared["category_id"].to_list() == ["cat-groceries", "cat-uncat"]

    assert len(units) == 1
    assert _row(units, 0)["upload_kind"] == "split"
    assert _row(units, 0)["memo"] == "Parent memo"
    assert _row(units, 0)["payee_name_upload"] == "Parent Payee"
    assert _row(units, 0)["amount_milliunits"] == -10000
    assert _row(units, 0)["subtransactions"] == [
        {"amount": -7000, "memo": "Line one", "category_id": "cat-groceries"},
        {
            "amount": -3000,
            "memo": "Line two",
            "category_id": "cat-uncat",
            "payee_name": "Line Payee",
        },
    ]

    assert payload == [
        {
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": -10000,
            "memo": "Parent memo",
            "cleared": "cleared",
            "approved": False,
            "import_id": "YNAB:-10000:2026-03-01:1",
            "payee_name": "Parent Payee",
            "category_id": None,
            "subtransactions": [
                {"amount": -7000, "memo": "Line one", "category_id": "cat-groceries"},
                {
                    "amount": -3000,
                    "memo": "Line two",
                    "category_id": "cat-uncat",
                    "payee_name": "Line Payee",
                },
            ],
        }
    ]


def test_prepare_upload_transactions_treats_one_line_collapsed_save_as_regular() -> None:
    working = _split_edit_working_row(amount=10.0)
    collapsed = review_state.apply_target_split_edit(
        working,
        0,
        lines=[
            {
                "split_id": "sub-1",
                "payee_raw": "Collapsed Payee",
                "category_raw": "Groceries",
                "amount_ils": -10.0,
                "memo": "Collapsed memo",
            }
        ],
    )

    prepared = upload_prep.prepare_upload_transactions(
        collapsed,
        accounts=_accounts(),
        categories_df=_categories(),
    )
    payload = upload_prep.upload_payload_records(prepared)

    assert len(prepared) == 1
    assert _row(prepared, 0)["upload_kind"] == "regular"
    assert _row(prepared, 0)["amount_milliunits"] == -10000
    assert _row(prepared, 0)["payee_name_upload"] == "Collapsed Payee"
    assert _row(prepared, 0)["category_id"] == "cat-groceries"
    assert payload[0]["payee_name"] == "Collapsed Payee"
    assert payload[0]["category_id"] == "cat-groceries"


def test_assemble_upload_transaction_units_preserves_regular_and_transfer_rows() -> None:
    reviewed = _reviewed_df(
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
    units = upload_prep.assemble_upload_transaction_units(prepared)

    assert units["upload_transaction_id"].to_list() == ["t1", "t2"]
    assert units["source_row_count"].to_list() == [1, 1]
    assert units["upload_kind"].to_list() == ["regular", "transfer"]
    assert units["payee_name_upload"].to_list() == ["Superpharm", ""]
    assert units["payee_id"].to_list() == ["", "payee-cash"]


def test_upload_payload_records_uses_transaction_units() -> None:
    prepared = pl.DataFrame(
        {
            "upload_transaction_id": ["u1"],
            "account_id": ["acc-bank"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "amount_milliunits": [-10500],
            "memo": ["groceries"],
            "cleared": ["cleared"],
            "approved": [False],
            "import_id": ["YNAB:-10500:2026-03-01:1"],
            "upload_kind": ["regular"],
            "payee_id": [""],
            "payee_name_upload": ["Superpharm"],
            "category_id": ["cat-groceries"],
            "transfer_target_account_id": [""],
        }
    )

    units = upload_prep.assemble_upload_transaction_units(prepared)
    payload = upload_prep.upload_payload_records(prepared)

    assert len(units) == 1
    assert _row(units, 0)["source_row_count"] == 1
    assert len(payload) == 1
    assert payload[0]["payee_name"] == "Superpharm"
    assert payload[0]["category_id"] == "cat-groceries"


def test_assemble_upload_transaction_units_builds_split_units_from_grouped_rows() -> None:
    prepared = pl.DataFrame(
        {
            "upload_transaction_id": ["split-1", "split-1"],
            "account_id": ["acc-bank", "acc-bank"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-01"],
            "amount_milliunits": [-8000, -4000],
            "memo": ["books", "gift"],
            "cleared": ["cleared", "cleared"],
            "approved": [False, False],
            "import_id": ["YNAB:-12000:2026-03-01:1", "YNAB:-12000:2026-03-01:1"],
            "upload_kind": ["regular", "regular"],
            "payee_id": ["", ""],
            "payee_name_upload": ["Tsomet Sfarim", "Tsomet Sfarim"],
            "category_id": ["cat-books", "cat-gifts"],
            "transfer_target_account_id": ["", ""],
        }
    )

    units = upload_prep.assemble_upload_transaction_units(prepared)
    payload = upload_prep.upload_payload_records(prepared)

    assert len(units) == 1
    assert _row(units, 0)["upload_kind"] == "split"
    assert _row(units, 0)["amount_milliunits"] == -12000
    assert _row(units, 0)["category_id"] == ""
    assert len(_row(units, 0)["subtransactions"]) == 2
    assert payload[0]["category_id"] is None
    assert len(payload[0]["subtransactions"]) == 2
    assert payload[0]["subtransactions"][0]["category_id"] == "cat-books"
    assert payload[0]["subtransactions"][1]["category_id"] == "cat-gifts"


def test_upload_payload_records_rejects_unsupported_split_transfer_units() -> None:
    prepared = pl.DataFrame(
        {
            "upload_transaction_id": ["split-transfer", "split-transfer"],
            "account_id": ["acc-bank", "acc-bank"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-01"],
            "amount_milliunits": [-8000, -4000],
            "memo": ["books", "gift"],
            "cleared": ["cleared", "cleared"],
            "approved": [False, False],
            "import_id": ["YNAB:-12000:2026-03-01:1", "YNAB:-12000:2026-03-01:1"],
            "upload_kind": ["regular", "transfer"],
            "payee_id": ["", "payee-cash"],
            "payee_name_upload": ["Tsomet Sfarim", ""],
            "category_id": ["cat-books", ""],
            "transfer_target_account_id": ["", "acc-cash"],
        }
    )

    units = upload_prep.assemble_upload_transaction_units(prepared)

    assert _row(units, 0)["unsupported_reason"] == "split_transfer_unsupported"
    with pytest.raises(ValueError, match="Unsupported upload transaction unit"):
        upload_prep.upload_payload_records(prepared)


def test_prepare_upload_transactions_generates_stable_occurrence_import_ids() -> None:
    reviewed = _reviewed_df(
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

    assert prepared["import_id"].to_list() == [
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
    reviewed = _reviewed_df(
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

    assert _row(prepared, 0)["import_id"] == bank_txn_id
    assert _row(prepared, 1)["import_id"] == "YNAB:-10000:2026-03-01:1"
    assert _row(prepared, 0)["bank_txn_id"] == bank_txn_id
    assert _row(prepared, 0)["source_account"] == "123456"
    assert _row(prepared, 0)["card_suffix"] == "7195"
    assert _row(prepared, 0)["secondary_date"] == "2026-03-02"
    assert _row(prepared, 0)["ref"] == "0042"


def test_prepare_upload_transactions_uses_card_txn_id_for_card_rows() -> None:
    card_txn_id = card_identity.make_card_txn_id(
        source="card",
        source_account="x9922",
        card_suffix="9922",
        date="2026-03-09",
        secondary_date="2026-04-10",
        outflow_ils=120,
        inflow_ils=0,
        description_raw="MERCHANT A",
        max_sheet="עסקאות במועד החיוב",
        max_txn_type="רגילה",
        max_original_amount=120,
        max_original_currency="ILS",
    )
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1", "t2"],
            "source": ["card", "card"],
            "account_name": ["Cash", "Cash"],
            "source_account": ["x9922", "x9922"],
            "card_suffix": ["9922", "9922"],
            "date": ["2026-03-09", "2026-03-09"],
            "secondary_date": ["2026-04-10", "2026-04-10"],
            "outflow_ils": ["120.00", "120.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["MERCHANT A", "MERCHANT B"],
            "card_txn_id": [card_txn_id, ""],
            "payee_selected": ["Shop", "Shop"],
            "category_selected": ["Groceries", "Groceries"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["import_id"] == card_txn_id
    assert _row(prepared, 0)["card_txn_id"] == card_txn_id
    assert _row(prepared, 1)["import_id"] == "YNAB:-120000:2026-03-09:2"


def test_prepare_upload_transactions_uses_source_import_id_for_bank_rows() -> None:
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
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "source": ["bank"],
            "account_name": ["Bank Leumi"],
            "source_account": ["123456"],
            "date": ["2026-03-01"],
            "secondary_date": ["2026-03-02"],
            "outflow_ils": ["10.00"],
            "inflow_ils": ["0"],
            "memo": ["groceries"],
            "source_import_id": [bank_txn_id],
            "source_bank_txn_id": [bank_txn_id],
            "payee_selected": ["Shop"],
            "category_selected": ["Groceries"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["import_id"] == bank_txn_id
    assert _row(prepared, 0)["bank_txn_id"] == bank_txn_id


def test_prepare_upload_transactions_requires_category_for_non_transfer() -> None:
    reviewed = _reviewed_df(
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
    reviewed = _reviewed_df(
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

    assert upload_prep.ready_mask(reviewed, accounts=_accounts()).to_list() == [True, False]


def test_ready_mask_treats_explicit_no_category_as_transfer_only() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.00", "5.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["", ""],
            "payee_selected": ["Transfer : Cash", "Cafe"],
            "category_selected": [review_model.NO_CATEGORY_REQUIRED, review_model.NO_CATEGORY_REQUIRED],
        }
    )

    assert upload_prep.ready_mask(reviewed, accounts=_accounts()).to_list() == [True, False]


def test_ready_mask_requires_category_for_off_budget_transfer() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.00"],
            "inflow_ils": ["0"],
            "memo": ["loan payment"],
            "payee_selected": ["Transfer : Loan"],
            "category_selected": [""],
        }
    )

    assert upload_prep.ready_mask(reviewed, accounts=_accounts()).to_list() == [False]


def test_ready_mask_allows_uncategorized_category() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10.00", "5.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["pending", "ready"],
            "payee_selected": ["Cafe", "Cafe"],
            "category_selected": ["Uncategorized", "Groceries"],
        }
    )

    assert upload_prep.ready_mask(reviewed).to_list() == [True, True]


def test_ready_mask_excludes_zero_amount_rows_even_if_other_fields_are_ready() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1", "t2"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["0", "10.00"],
            "inflow_ils": ["0", "0"],
            "memo": ["pending", "ready"],
            "payee_selected": ["Bit", "Cafe"],
            "category_selected": ["Uncategorized", "Groceries"],
        }
    )

    assert upload_prep.ready_mask(reviewed).to_list() == [False, True]


def test_prepare_upload_transactions_maps_uncategorized_category() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.00"],
            "inflow_ils": ["0"],
            "memo": ["pending"],
            "payee_selected": ["Bit"],
            "category_selected": ["Uncategorized"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["category_id"] == "cat-uncat"


def test_prepare_upload_transactions_maps_explicit_no_category_transfer_to_blank() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.50"],
            "inflow_ils": ["0"],
            "memo": ["cash move"],
            "payee_selected": ["Transfer : Cash"],
            "category_selected": [review_model.NO_CATEGORY_REQUIRED],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["category_id"] == ""
    assert _row(prepared, 0)["upload_kind"] == "transfer"


def test_prepare_upload_transactions_preserves_category_for_off_budget_transfer() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.50"],
            "inflow_ils": ["0"],
            "memo": ["loan payment"],
            "payee_selected": ["Transfer : Loan"],
            "category_selected": ["Loan Paydown"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )
    payload = upload_prep.upload_payload_records(prepared)

    assert _row(prepared, 0)["category_id"] == "cat-loan"
    assert _row(prepared, 0)["transfer_target_account_id"] == "acc-loan"
    assert _row(prepared, 0)["upload_kind"] == "transfer"
    assert payload[0]["payee_id"] == "payee-loan"
    assert payload[0]["category_id"] == "cat-loan"


def test_prepare_upload_transactions_falls_back_to_uncategorized_for_missing_category() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10.00"],
            "inflow_ils": ["0"],
            "memo": ["pending"],
            "payee_selected": ["Bit"],
            "category_selected": ["Hidden or Missing"],
        }
    )

    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=_accounts(),
        categories_df=_categories(),
    )

    assert _row(prepared, 0)["category_id"] == "cat-uncat"


def test_prepare_upload_transactions_rejects_zero_amount_rows() -> None:
    reviewed = _reviewed_df(
        {
            "transaction_id": ["t1"],
            "account_name": ["Bank Leumi"],
            "date": ["2026-03-01"],
            "outflow_ils": ["0"],
            "inflow_ils": ["0"],
            "memo": ["pending"],
            "payee_selected": ["Bit"],
            "category_selected": ["Uncategorized"],
        }
    )

    with pytest.raises(ValueError, match="zero amount"):
        upload_prep.prepare_upload_transactions(
            reviewed,
            accounts=_accounts(),
            categories_df=_categories(),
        )


def test_uploadable_account_mask_marks_unknown_accounts() -> None:
    reviewed = _reviewed_df(
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

    assert upload_prep.uploadable_account_mask(reviewed, _accounts()).to_list() == [True, False]


def test_prepare_upload_transactions_resolves_simplified_category_aliases() -> None:
    reviewed = _reviewed_df(
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

    assert prepared["category_id"].to_list() == ["cat-groceries", "cat-rta"]


def test_category_lookup_keeps_visible_categories_when_hidden_is_string_false() -> None:
    categories = pd.DataFrame(
        [
            {
                "category_group": "Flexible expenses",
                "category_name": "Groceries",
                "category_id": "cat-groceries",
                "hidden": "False",
            },
            {
                "category_group": "Internal Master Category",
                "category_name": "Uncategorized",
                "category_id": "cat-uncat",
                "hidden": "TRUE",
            },
        ]
    )

    category_lookup = upload_prep._category_lookup(categories)

    assert category_lookup == {"Groceries": "cat-groceries"}


def test_upload_preflight_reports_duplicate_and_match_risks() -> None:
    prepared = pl.DataFrame(
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
    prepared = pl.DataFrame(
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
                "category_name": "Split",
            },
        ],
    }

    summary = upload_prep.summarize_upload_response(response)

    assert summary == {
        "saved": 3,
        "duplicate_import_ids": 1,
        "matched_existing": 1,
        "transfer_saved": 1,
        "split_saved": 1,
    }


def test_classify_upload_result_detects_idempotent_rerun() -> None:
    outcome = upload_prep.classify_upload_result(
        {
            "saved": 0,
            "duplicate_import_ids": 561,
            "matched_existing": 0,
            "transfer_saved": 0,
            "split_saved": 0,
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
            "split_saved": 1,
        },
        prepared_count=10,
    )

    assert outcome["idempotent_rerun"] is False
    assert outcome["verification_needed"] is True
    assert outcome["status"] == "new transactions saved"


def test_verify_upload_response_checks_transfer_and_category_fields() -> None:
    prepared = pl.DataFrame(
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
        "split_mismatches": [],
    }


def test_verify_upload_response_allows_same_import_id_on_different_accounts() -> None:
    prepared = pl.DataFrame(
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
        "split_mismatches": [],
    }


def test_verify_upload_response_accepts_uncategorized_name_without_category_id() -> None:
    prepared = pl.DataFrame(
        {
            "import_id": ["imp-uncat"],
            "account_id": ["acc-bank"],
            "date": ["2026-01-01"],
            "amount_milliunits": [-1000],
            "upload_kind": ["regular"],
            "category_id": ["cat-uncat"],
            "target_category_selected": ["Uncategorized"],
            "transfer_target_account_id": [""],
        }
    )
    response = {
        "transaction_ids": ["t1"],
        "transactions": [
            {
                "id": "t1",
                "import_id": "imp-uncat",
                "account_id": "acc-bank",
                "date": "2026-01-01",
                "amount": -1000,
                "category_id": "",
                "category_name": "Uncategorized",
                "transfer_account_id": "",
            },
        ],
    }

    verification = upload_prep.verify_upload_response(prepared, response)

    assert verification == {
        "checked": 1,
        "missing_saved_transactions": [],
        "amount_mismatches": [],
        "date_mismatches": [],
        "account_mismatches": [],
        "transfer_mismatches": [],
        "category_mismatches": [],
        "split_mismatches": [],
    }


def test_verify_upload_response_checks_split_child_structure() -> None:
    prepared = pl.DataFrame(
        {
            "upload_transaction_id": ["split-1", "split-1"],
            "account_id": ["acc-bank", "acc-bank"],
            "account_name": ["Bank Leumi", "Bank Leumi"],
            "date": ["2026-03-01", "2026-03-01"],
            "amount_milliunits": [-8000, -4000],
            "memo": ["books", "gift"],
            "cleared": ["cleared", "cleared"],
            "approved": [False, False],
            "import_id": ["YNAB:-12000:2026-03-01:1", "YNAB:-12000:2026-03-01:1"],
            "upload_kind": ["regular", "regular"],
            "payee_id": ["", ""],
            "payee_name_upload": ["Tsomet Sfarim", "Tsomet Sfarim"],
            "category_id": ["cat-books", "cat-gifts"],
            "transfer_target_account_id": ["", ""],
        }
    )
    response = {
        "transaction_ids": ["t1"],
        "transactions": [
            {
                "id": "t1",
                "import_id": "YNAB:-12000:2026-03-01:1",
                "account_id": "acc-bank",
                "date": "2026-03-01",
                "amount": -12000,
                "category_id": "",
                "category_name": "Split",
                "transfer_account_id": "",
                "subtransactions": [
                    {
                        "amount": -8000,
                        "memo": "books",
                        "category_id": "cat-books",
                        "payee_name": "Tsomet Sfarim",
                    },
                    {
                        "amount": -4000,
                        "memo": "gift",
                        "category_id": "cat-gifts",
                        "payee_name": "Tsomet Sfarim",
                    },
                ],
            }
        ],
    }

    verification = upload_prep.verify_upload_response(prepared, response)

    assert verification == {
        "checked": 1,
        "missing_saved_transactions": [],
        "amount_mismatches": [],
        "date_mismatches": [],
        "account_mismatches": [],
        "transfer_mismatches": [],
        "category_mismatches": [],
        "split_mismatches": [],
    }
