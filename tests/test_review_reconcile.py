from __future__ import annotations

import pandas as pd

import ynab_il_importer.review_reconcile as review_reconcile


def test_reconcile_reviewed_transactions_matches_by_transaction_id() -> None:
    old = pd.DataFrame(
        {
            "transaction_id": ["t1"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10"],
            "inflow_ils": ["0"],
            "fingerprint": ["fp1"],
            "payee_selected": ["Payee A"],
            "category_selected": ["Cat A"],
            "update_map": ["TRUE"],
            "reviewed": ["TRUE"],
        }
    )
    new = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10", "12"],
            "inflow_ils": ["0", "0"],
            "fingerprint": ["fp1", "fp2"],
            "payee_selected": ["", ""],
            "category_selected": ["", ""],
            "update_map": ["", ""],
            "reviewed": ["", ""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(old, new)

    assert merged.loc[0, "payee_selected"] == "Payee A"
    assert merged.loc[0, "category_selected"] == "Cat A"
    assert bool(merged.loc[0, "reviewed"]) is True
    assert stats["direct_matches"] == 1
    assert stats["fallback_matches"] == 0


def test_reconcile_reviewed_transactions_falls_back_on_unique_key() -> None:
    old = pd.DataFrame(
        {
            "transaction_id": ["old-card"],
            "date": ["2026-03-01"],
            "outflow_ils": ["18.9"],
            "inflow_ils": ["0"],
            "fingerprint": ["park"],
            "payee_selected": ["Park Cafe"],
            "category_selected": ["Eating Out"],
            "update_map": [""],
            "reviewed": ["TRUE"],
        }
    )
    new = pd.DataFrame(
        {
            "transaction_id": ["new-bank"],
            "date": ["2026-03-01"],
            "outflow_ils": ["18.9"],
            "inflow_ils": ["0"],
            "fingerprint": ["park"],
            "payee_selected": [""],
            "category_selected": [""],
            "update_map": [""],
            "reviewed": [""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(old, new)

    assert merged.loc[0, "payee_selected"] == "Park Cafe"
    assert merged.loc[0, "category_selected"] == "Eating Out"
    assert bool(merged.loc[0, "reviewed"]) is True
    assert stats["direct_matches"] == 0
    assert stats["fallback_matches"] == 1
