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


def test_transactions_to_dataframe_preserves_lineage_fields() -> None:
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

    assert df.loc[0, "ynab_id"] == "txn-1"
    assert df.loc[0, "account_id"] == "acc-1"
    assert df.loc[0, "import_id"] == "YNAB:-12340:2026-03-01:1"
    assert df.loc[0, "matched_transaction_id"] == "match-1"
    assert df.loc[0, "cleared"] == "cleared"
    assert bool(df.loc[0, "approved"]) is True
    assert df.loc[0, "fingerprint"] == "merchant"
    assert pd.to_numeric(df.loc[0, "outflow_ils"]) == 12.34


def test_category_transactions_to_dataframe_explodes_subtransactions() -> None:
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

    df = ynab_api.category_transactions_to_dataframe(transactions, accounts)

    assert df["ynab_id"].tolist() == ["sub-1", "sub-2"]
    assert df["parent_ynab_id"].tolist() == ["parent-1", "parent-1"]
    assert df["is_subtransaction"].tolist() == [True, True]
    assert df["category_raw"].tolist() == ["Pilates", "Inflow: Ready to Assign"]
    assert df["payee_raw"].tolist() == ["Salary Liya", "Salary Liya"]
    assert df["account_name"].tolist() == ["Family Leumi", "Family Leumi"]
    assert pd.to_numeric(df.loc[0, "outflow_ils"]) == 3000.0
    assert pd.to_numeric(df.loc[1, "inflow_ils"]) == 3000.0


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
