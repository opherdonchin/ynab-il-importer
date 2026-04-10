from __future__ import annotations

import pandas as pd

import ynab_il_importer.ynab_api as ynab_api


def test_create_transactions_reads_save_transactions_response(monkeypatch) -> None:
    def fake_post(path: str, payload: dict) -> dict:
        assert path == "/plans/test-plan/transactions"
        assert "transactions" in payload
        return {
            "data": {
                "transaction_ids": ["t1", "t2"],
                "duplicate_import_ids": ["dup-1"],
                "server_knowledge": 123,
            }
        }

    monkeypatch.setattr(ynab_api, "_ynab_post", fake_post)

    response = ynab_api.create_transactions([{"import_id": "x"}], plan_id="test-plan")

    assert response["transaction_ids"] == ["t1", "t2"]
    assert response["duplicate_import_ids"] == ["dup-1"]
    assert response["server_knowledge"] == 123


def test_update_transactions_reads_bulk_patch_response(monkeypatch) -> None:
    def fake_patch(path: str, payload: dict) -> dict:
        assert path == "/plans/test-plan/transactions"
        assert payload == {"transactions": [{"id": "txn-1", "cleared": "reconciled"}]}
        return {
            "data": {
                "transactions": [{"id": "txn-1", "cleared": "reconciled"}],
                "server_knowledge": 456,
            }
        }

    monkeypatch.setattr(ynab_api, "_ynab_patch", fake_patch)

    response = ynab_api.update_transactions(
        [{"id": "txn-1", "cleared": "reconciled"}],
        plan_id="test-plan",
    )

    assert response["transactions"] == [{"id": "txn-1", "cleared": "reconciled"}]
    assert response["server_knowledge"] == 456


def test_delete_transaction_calls_item_delete_endpoint(monkeypatch) -> None:
    def fake_delete(path: str) -> dict:
        assert path == "/plans/test-plan/transactions/txn-1"
        return {"data": {"transaction": {"id": "txn-1", "deleted": True}}}

    monkeypatch.setattr(ynab_api, "_ynab_delete", fake_delete)

    response = ynab_api.delete_transaction("txn-1", plan_id="test-plan")

    assert response == {"transaction": {"id": "txn-1", "deleted": True}}


def test_transactions_to_dataframe_returns_canonical_rows() -> None:
    transactions = [
        {
            "id": "txn-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Merchant",
            "category_name": "Groceries",
            "amount": -12340,
            "memo": "memo text",
            "import_id": "YNAB:-12340:2026-03-01:1",
            "matched_transaction_id": "match-1",
            "cleared": "cleared",
            "approved": True,
        }
    ]
    accounts = [{"id": "acc-1", "name": "Bank Leumi"}]

    df = ynab_api.transactions_to_dataframe(transactions, accounts)

    assert df.loc[0, "transaction_id"] == "txn-1"
    assert df.loc[0, "ynab_id"] == "txn-1"
    assert df.loc[0, "account_id"] == "acc-1"
    assert df.loc[0, "source_system"] == "ynab"
    assert df.loc[0, "import_id"] == "YNAB:-12340:2026-03-01:1"
    assert df.loc[0, "matched_transaction_id"] == "match-1"
    assert df.loc[0, "cleared"] == "cleared"
    assert bool(df.loc[0, "approved"]) is True
    assert df.loc[0, "fingerprint"] == "merchant"
    assert pd.to_numeric(df.loc[0, "signed_amount_ils"]) == -12.34
    assert len(df.loc[0, "splits"]) == 0


def test_project_transactions_to_flat_dataframe_preserves_lineage_fields() -> None:
    transactions = [
        {
            "id": "txn-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Merchant",
            "category_name": "Groceries",
            "amount": -12340,
            "memo": "memo text",
            "import_id": "YNAB:-12340:2026-03-01:1",
            "matched_transaction_id": "match-1",
            "cleared": "cleared",
            "approved": True,
        }
    ]
    accounts = [{"id": "acc-1", "name": "Bank Leumi"}]

    canonical = ynab_api.transactions_to_dataframe(transactions, accounts)
    df = ynab_api.project_transactions_to_flat_dataframe(canonical)

    assert df.loc[0, "ynab_id"] == "txn-1"
    assert df.loc[0, "account_id"] == "acc-1"
    assert df.loc[0, "import_id"] == "YNAB:-12340:2026-03-01:1"
    assert df.loc[0, "matched_transaction_id"] == "match-1"
    assert df.loc[0, "cleared"] == "cleared"
    assert bool(df.loc[0, "approved"]) is True
    assert df.loc[0, "fingerprint"] == "merchant"
    assert pd.to_numeric(df.loc[0, "outflow_ils"]) == 12.34


def test_extract_category_transactions_preserves_parent_split_transaction() -> None:
    transactions = [
        {
            "id": "parent-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Salary Liya",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "",
            "import_id": "parent-import",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "sub-1",
                    "amount": -3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-pilates",
                    "category_name": "Pilates",
                    "deleted": False,
                },
                {
                    "id": "sub-2",
                    "amount": 3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-rta",
                    "category_name": "Inflow: Ready to Assign",
                    "deleted": False,
                },
            ],
        }
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    canonical = ynab_api.transactions_to_dataframe(transactions, accounts)
    df = ynab_api.extract_category_transactions(canonical, category_id="cat-pilates")

    assert df["transaction_id"].tolist() == ["parent-1"]
    assert df["category_raw"].tolist() == ["Split"]
    assert df["account_name"].tolist() == ["Family Leumi"]
    assert len(df.loc[0, "splits"]) == 2


def test_extract_category_transactions_includes_parent_and_split_matches() -> None:
    transactions = [
        {
            "id": "txn-parent-match",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Office Rent",
            "category_name": "Pilates",
            "category_id": "cat-pilates",
            "amount": -120000,
            "memo": "",
            "import_id": "import-parent",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
        },
        {
            "id": "txn-split-match",
            "account_id": "acc-1",
            "date": "2026-03-02",
            "payee_name": "Salary Liya",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "",
            "import_id": "import-split",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "sub-1",
                    "amount": -3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-pilates",
                    "category_name": "Pilates",
                    "deleted": False,
                },
                {
                    "id": "sub-2",
                    "amount": 3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-rta",
                    "category_name": "Inflow: Ready to Assign",
                    "deleted": False,
                },
            ],
        },
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    canonical = ynab_api.transactions_to_dataframe(transactions, accounts)
    df = ynab_api.extract_category_transactions(
        canonical,
        category_id="cat-pilates",
        category_name="Pilates",
    )

    assert set(df["transaction_id"].tolist()) == {"txn-parent-match", "txn-split-match"}
    assert set(df["category_raw"].tolist()) == {"Pilates", "Split"}


def test_project_category_transactions_to_source_rows_flattens_matches_for_bridge_workflows() -> None:
    transactions = [
        {
            "id": "parent-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Salary Liya",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "",
            "import_id": "parent-import",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "sub-1",
                    "amount": -3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-pilates",
                    "category_name": "Pilates",
                    "deleted": False,
                },
                {
                    "id": "sub-2",
                    "amount": 3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-rta",
                    "category_name": "Inflow: Ready to Assign",
                    "deleted": False,
                },
            ],
        }
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    canonical = ynab_api.transactions_to_dataframe(transactions, accounts)
    df = ynab_api.project_category_transactions_to_source_rows(
        canonical,
        category_id="cat-pilates",
        category_name="Pilates",
    )

    assert df["ynab_id"].tolist() == ["sub-1"]
    assert df["parent_ynab_id"].tolist() == ["parent-1"]
    assert df["is_subtransaction"].tolist() == [True]
    assert df["category_raw"].tolist() == ["Pilates"]
    assert df["payee_raw"].tolist() == ["Salary Liya"]
    assert df["account_name"].tolist() == ["Family Leumi"]
    assert pd.to_numeric(df.loc[0, "outflow_ils"]) == 3000.0


def test_transactions_to_canonical_table_preserves_nested_split_lines() -> None:
    transactions = [
        {
            "id": "parent-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "Salary Liya",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "parent memo",
            "import_id": "parent-import",
            "matched_transaction_id": "match-1",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "sub-1",
                    "amount": -3000000,
                    "memo": "",
                    "payee_name": "",
                    "category_id": "cat-pilates",
                    "category_name": "Pilates",
                    "deleted": False,
                },
                {
                    "id": "sub-2",
                    "amount": 3000000,
                    "memo": "child memo",
                    "payee_name": "Salary Liya",
                    "category_id": "cat-rta",
                    "category_name": "Inflow: Ready to Assign",
                    "deleted": False,
                },
            ],
        }
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    table = ynab_api.transactions_to_canonical_table(transactions, accounts)

    assert table["transaction_id"].to_pylist() == ["parent-1"]
    assert table["account_name"].to_pylist() == ["Family Leumi"]
    assert table["fingerprint"].to_pylist() == ["salary liya"]
    split_lines = table["splits"].to_pylist()[0]
    assert len(split_lines) == 2
    assert split_lines[0]["category_id"] == "cat-pilates"
    assert split_lines[0]["category_raw"] == "Pilates"
    assert split_lines[1]["memo"] == "child memo"


def test_transactions_to_canonical_table_uses_memo_when_payee_blank() -> None:
    transactions = [
        {
            "id": "txn-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "",
            "category_name": "Inflow: Ready to Assign",
            "category_id": "cat-rta",
            "amount": 2150000,
            "memo": "Bit from client",
            "import_id": "",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
        }
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    table = ynab_api.transactions_to_canonical_table(transactions, accounts)

    assert table["fingerprint"].to_pylist() == ["bit from client"]
    assert table["description_clean"].to_pylist() == ["Bit from client"]
    assert table["description_raw"].to_pylist() == ["Bit from client"]


def test_transactions_to_canonical_table_uses_split_identity_when_parent_blank() -> None:
    transactions = [
        {
            "id": "parent-1",
            "account_id": "acc-1",
            "date": "2026-03-01",
            "payee_name": "",
            "category_name": "Split",
            "category_id": "",
            "amount": 0,
            "memo": "",
            "import_id": "",
            "matched_transaction_id": "",
            "cleared": "cleared",
            "approved": True,
            "subtransactions": [
                {
                    "id": "sub-1",
                    "amount": -120000,
                    "memo": "snacks",
                    "payee_name": "Mega Store",
                    "category_id": "cat-food",
                    "category_name": "Food",
                    "deleted": False,
                },
                {
                    "id": "sub-2",
                    "amount": 120000,
                    "memo": "refund",
                    "payee_name": "Transfer : Family",
                    "category_id": "",
                    "category_name": "Uncategorized",
                    "deleted": False,
                },
            ],
        }
    ]
    accounts = [{"id": "acc-1", "name": "Family Leumi"}]

    table = ynab_api.transactions_to_canonical_table(transactions, accounts)

    assert table["fingerprint"].to_pylist() == [
        "mega store snacks transfer family refund"
    ]
    assert table["description_clean"].to_pylist() == [
        "Mega Store snacks Transfer : Family refund"
    ]
