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
