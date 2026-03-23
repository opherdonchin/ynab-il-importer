from __future__ import annotations

import pandas as pd

import ynab_il_importer.cross_budget_reconciliation as cross_budget_reconciliation


def _category_groups(*, balance: int = 150000, activity: int = -25000, budgeted: int = 0):
    return [
        {
            "name": "Business",
            "id": "group-1",
            "categories": [
                {
                    "id": "cat-1",
                    "name": "Pilates",
                    "balance": balance,
                    "activity": activity,
                    "budgeted": budgeted,
                    "deleted": False,
                }
            ],
        }
    ]


def _accounts(*, cleared_balance: int = 150000, uncleared_balance: int = 5000, balance: int = 155000):
    return [
        {
            "id": "acct-1",
            "name": "In Family",
            "cleared_balance": cleared_balance,
            "uncleared_balance": uncleared_balance,
            "balance": balance,
            "deleted": False,
        }
    ]


def _transactions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ynab_id": "r-1",
                "account_name": "In Family",
                "date": "2026-03-01",
                "payee_raw": "A",
                "category_raw": "Pilates",
                "memo": "",
                "outflow_ils": 0.0,
                "inflow_ils": 100.0,
                "cleared": "reconciled",
            },
            {
                "ynab_id": "c-1",
                "account_name": "In Family",
                "date": "2026-03-02",
                "payee_raw": "B",
                "category_raw": "Pilates",
                "memo": "",
                "outflow_ils": 0.0,
                "inflow_ils": 50.0,
                "cleared": "cleared",
            },
            {
                "ynab_id": "u-1",
                "account_name": "In Family",
                "date": "2026-03-03",
                "payee_raw": "C",
                "category_raw": "Pilates",
                "memo": "",
                "outflow_ils": 0.0,
                "inflow_ils": 5.0,
                "cleared": "uncleared",
            },
        ]
    )


def test_build_cross_budget_balance_report_balanced() -> None:
    result = cross_budget_reconciliation.build_cross_budget_balance_report(
        source_category_groups=_category_groups(),
        target_accounts=_accounts(),
        target_transactions_df=_transactions(),
        source_category_name="Pilates",
        target_account_name="In Family",
        source_profile="family",
        target_profile="pilates",
    )

    summary = result["summary"].iloc[0]
    assert summary["source_category_balance_ils"] == 150.0
    assert summary["target_account_cleared_balance_ils"] == 150.0
    assert summary["difference_ils"] == 0.0
    assert bool(summary["is_balanced"]) is True
    assert int(summary["updates_planned"]) == 1


def test_reconciliation_issues_reports_balance_mismatch() -> None:
    result = cross_budget_reconciliation.build_cross_budget_balance_report(
        source_category_groups=_category_groups(balance=130000),
        target_accounts=_accounts(),
        target_transactions_df=_transactions(),
        source_category_name="Pilates",
        target_account_name="In Family",
    )

    issues = cross_budget_reconciliation.reconciliation_issues(result["summary"])
    assert any("does not equal" in issue for issue in issues)


def test_planned_reconciliation_actions_only_include_cleared_rows() -> None:
    result = cross_budget_reconciliation.build_cross_budget_balance_report(
        source_category_groups=_category_groups(),
        target_accounts=_accounts(),
        target_transactions_df=_transactions(),
        source_category_name="Pilates",
        target_account_name="In Family",
    )

    target_report = result["target_report"]
    assert target_report["action"].tolist() == [
        "already_reconciled",
        "reconcile",
        "leave_uncleared",
    ]
    assert cross_budget_reconciliation.planned_reconciliation_actions(
        result["target_transactions"]
    ) == [{"id": "c-1", "cleared": "reconciled"}]
