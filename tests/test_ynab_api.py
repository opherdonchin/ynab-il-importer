from __future__ import annotations

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
