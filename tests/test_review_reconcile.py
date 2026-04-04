from __future__ import annotations

import pandas as pd
import polars as pl

import ynab_il_importer.review_reconcile as review_reconcile


def test_reconcile_reviewed_transactions_matches_by_transaction_id() -> None:
    old = pd.DataFrame(
        {
            "transaction_id": ["t1"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10"],
            "inflow_ils": ["0"],
            "fingerprint": ["fp1"],
            "source_payee_selected": ["Source A"],
            "source_category_selected": ["Cat Source"],
            "target_payee_selected": ["Payee A"],
            "target_category_selected": ["Cat A"],
            "decision_action": ["keep_match"],
            "update_maps": ["fingerprint_add_source"],
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
            "source_payee_selected": ["", ""],
            "source_category_selected": ["", ""],
            "target_payee_selected": ["", ""],
            "target_category_selected": ["", ""],
            "decision_action": ["", ""],
            "update_maps": ["", ""],
            "reviewed": ["", ""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(
        pl.from_pandas(old),
        pl.from_pandas(new),
    )

    assert merged["source_payee_selected"].to_list()[0] == "Source A"
    assert merged["target_payee_selected"].to_list()[0] == "Payee A"
    assert merged["target_category_selected"].to_list()[0] == "Cat A"
    assert merged["decision_action"].to_list()[0] == "keep_match"
    assert merged["update_maps"].to_list()[0] == "fingerprint_add_source"
    assert bool(merged["reviewed"].to_list()[0]) is True
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
            "source_payee_selected": ["Source Cafe"],
            "source_category_selected": ["Eating Out"],
            "target_payee_selected": ["Park Cafe"],
            "target_category_selected": ["Eating Out"],
            "decision_action": ["create_target"],
            "update_maps": ["payee_add_fingerprint"],
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
            "source_payee_selected": [""],
            "source_category_selected": [""],
            "target_payee_selected": [""],
            "target_category_selected": [""],
            "decision_action": [""],
            "update_maps": [""],
            "reviewed": [""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(
        pl.from_pandas(old),
        pl.from_pandas(new),
    )

    assert merged["source_payee_selected"].to_list()[0] == "Source Cafe"
    assert merged["target_payee_selected"].to_list()[0] == "Park Cafe"
    assert merged["target_category_selected"].to_list()[0] == "Eating Out"
    assert merged["decision_action"].to_list()[0] == "create_target"
    assert merged["update_maps"].to_list()[0] == "payee_add_fingerprint"
    assert bool(merged["reviewed"].to_list()[0]) is True
    assert stats["direct_matches"] == 0
    assert stats["fallback_matches"] == 1


def test_reconcile_reviewed_transactions_preserves_new_auto_settled_rows() -> None:
    old = pd.DataFrame(
        {
            "transaction_id": ["t1"],
            "date": ["2026-01-07"],
            "outflow_ils": ["145"],
            "inflow_ils": ["0"],
            "fingerprint": ["gifts to us"],
            "source_payee_selected": [""],
            "source_category_selected": [""],
            "target_payee_selected": ["Subjects"],
            "target_category_selected": ["University"],
            "decision_action": ["create_source"],
            "update_maps": [""],
            "reviewed": [""],
        }
    )
    new = pd.DataFrame(
        {
            "transaction_id": ["t1"],
            "date": ["2026-01-07"],
            "outflow_ils": ["145"],
            "inflow_ils": ["0"],
            "fingerprint": ["gifts to us"],
            "source_payee_selected": [""],
            "source_category_selected": [""],
            "target_payee_selected": ["Gifts to us"],
            "target_category_selected": ["Unexplained expenses"],
            "decision_action": ["ignore_row"],
            "update_maps": [""],
            "reviewed": ["TRUE"],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(
        pl.from_pandas(old),
        pl.from_pandas(new),
    )

    assert merged["target_payee_selected"].to_list()[0] == "Gifts to us"
    assert merged["target_category_selected"].to_list()[0] == "Unexplained expenses"
    assert merged["decision_action"].to_list()[0] == "ignore_row"
    assert bool(merged["reviewed"].to_list()[0]) is True
    assert stats["direct_matches"] == 0


def test_reconcile_reviewed_transactions_matches_duplicate_transaction_ids_by_occurrence() -> None:
    old = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "date": ["2026-03-01", "2026-03-01"],
            "outflow_ils": ["10", "10"],
            "inflow_ils": ["0", "0"],
            "fingerprint": ["fp1", "fp1"],
            "source_payee_selected": ["", ""],
            "source_category_selected": ["", ""],
            "target_payee_selected": ["Payee A", "Payee B"],
            "target_category_selected": ["Cat A", "Cat B"],
            "decision_action": ["keep_match", "ignore_row"],
            "update_maps": ["", ""],
            "reviewed": ["TRUE", "TRUE"],
        }
    )
    new = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "date": ["2026-03-01", "2026-03-01"],
            "outflow_ils": ["10", "10"],
            "inflow_ils": ["0", "0"],
            "fingerprint": ["fp1", "fp1"],
            "source_payee_selected": ["", ""],
            "source_category_selected": ["", ""],
            "target_payee_selected": ["", ""],
            "target_category_selected": ["", ""],
            "decision_action": ["", ""],
            "update_maps": ["", ""],
            "reviewed": ["", ""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(
        pl.from_pandas(old),
        pl.from_pandas(new),
    )

    assert merged["target_payee_selected"].to_list() == ["Payee A", "Payee B"]
    assert merged["target_category_selected"].to_list() == ["Cat A", "Cat B"]
    assert merged["decision_action"].to_list() == ["keep_match", "ignore_row"]
    assert merged["reviewed"].to_list() == [True, True]
    assert stats["direct_matches"] == 2


def test_reconcile_reviewed_transactions_accepts_polars_frames() -> None:
    old = pl.DataFrame(
        {
            "transaction_id": ["t1"],
            "date": ["2026-03-01"],
            "outflow_ils": ["10"],
            "inflow_ils": ["0"],
            "fingerprint": ["fp1"],
            "source_payee_selected": ["Source A"],
            "source_category_selected": ["Cat Source"],
            "target_payee_selected": ["Payee A"],
            "target_category_selected": ["Cat A"],
            "decision_action": ["keep_match"],
            "update_maps": ["fingerprint_add_source"],
            "reviewed": ["TRUE"],
        }
    )
    new = pl.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "date": ["2026-03-01", "2026-03-02"],
            "outflow_ils": ["10", "12"],
            "inflow_ils": ["0", "0"],
            "fingerprint": ["fp1", "fp2"],
            "source_payee_selected": ["", ""],
            "source_category_selected": ["", ""],
            "target_payee_selected": ["", ""],
            "target_category_selected": ["", ""],
            "decision_action": ["", ""],
            "update_maps": ["", ""],
            "reviewed": ["", ""],
        }
    )

    merged, stats = review_reconcile.reconcile_reviewed_transactions(old, new)

    assert isinstance(merged, pl.DataFrame)
    assert merged["source_payee_selected"].to_list() == ["Source A", ""]
    assert merged["target_payee_selected"].to_list() == ["Payee A", ""]
    assert merged["decision_action"].to_list() == ["keep_match", ""]
    assert merged["reviewed"].to_list() == [True, False]
    assert stats["direct_matches"] == 1
