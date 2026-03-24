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


def test_plan_cross_budget_reconciliation_reconciles_after_exact_anchor() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "ynab",
                "ynab_id": "src-1",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-01",
                "payee_raw": "A",
                "category_raw": "Pilates",
                "fingerprint": "a",
                "outflow_ils": 0.0,
                "inflow_ils": 10.0,
                "memo": "",
            },
            {
                "source": "ynab",
                "ynab_id": "src-2",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-02",
                "payee_raw": "B",
                "category_raw": "Pilates",
                "fingerprint": "b",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "memo": "",
            },
            {
                "source": "ynab",
                "ynab_id": "src-3",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "Pilates",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
            },
        ]
    )
    target_df = pd.DataFrame(
        [
            {
                "ynab_id": "t-1",
                "account_name": "In Family",
                "date": "2026-02-01",
                "payee_raw": "A",
                "category_raw": "",
                "fingerprint": "a",
                "outflow_ils": 0.0,
                "inflow_ils": 10.0,
                "memo": "",
                "cleared": "reconciled",
            },
            {
                "ynab_id": "t-2",
                "account_name": "In Family",
                "date": "2026-02-02",
                "payee_raw": "B",
                "category_raw": "",
                "fingerprint": "b",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "memo": "",
                "cleared": "reconciled",
            },
            {
                "ynab_id": "t-3",
                "account_name": "In Family",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
                "cleared": "cleared",
            },
        ]
    )
    result = cross_budget_reconciliation.plan_cross_budget_reconciliation(
        source_category_groups=_category_groups(balance=175000, activity=75000),
        source_month_details=[
            {
                "month": "2026-01-01",
                "categories": [
                    {
                        "name": "Pilates",
                        "category_group_name": "Business",
                        "balance": 0,
                        "activity": 0,
                        "budgeted": 0,
                        "deleted": False,
                    }
                ],
            }
        ],
        target_accounts=_accounts(cleared_balance=150000, uncleared_balance=25000, balance=175000),
        source_transactions_df=source_df,
        target_transactions_df=target_df,
        source_category_name="Pilates",
        target_account_name="In Family",
        since="2026-02-03",
        anchor_streak=2,
    )

    assert result["ok"] is True
    assert result["updates"] == [{"id": "t-3", "cleared": "reconciled"}]
    assert result["source_report"]["action"].tolist() == [
        "anchor_history",
        "anchor_history",
        "reconcile",
    ]


def test_plan_cross_budget_reconciliation_blocks_without_exact_anchor_month() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "ynab",
                "ynab_id": "src-1",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "Pilates",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
            }
        ]
    )
    target_df = pd.DataFrame(
        [
            {
                "ynab_id": "t-3",
                "account_name": "In Family",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
                "cleared": "cleared",
            }
        ]
    )
    result = cross_budget_reconciliation.plan_cross_budget_reconciliation(
        source_category_groups=_category_groups(balance=125000, activity=25000),
        source_month_details=[
            {
                "month": "2026-01-01",
                "categories": [
                    {
                        "name": "Pilates",
                        "category_group_name": "Business",
                        "balance": 90000,
                        "activity": 0,
                        "budgeted": 0,
                        "deleted": False,
                    }
                ],
            }
        ],
        target_accounts=_accounts(cleared_balance=100000, uncleared_balance=25000, balance=125000),
        source_transactions_df=source_df,
        target_transactions_df=target_df,
        source_category_name="Pilates",
        target_account_name="In Family",
        since="2026-02-03",
        anchor_streak=1,
    )

    assert result["ok"] is False
    assert "exact-balance anchor month" in str(result["reason"])


def test_plan_cross_budget_reconciliation_accepts_cached_source_month_report() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "ynab",
                "ynab_id": "src-1",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-01",
                "payee_raw": "A",
                "category_raw": "Pilates",
                "fingerprint": "a",
                "outflow_ils": 0.0,
                "inflow_ils": 10.0,
                "memo": "",
            },
            {
                "source": "ynab",
                "ynab_id": "src-2",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-02",
                "payee_raw": "B",
                "category_raw": "Pilates",
                "fingerprint": "b",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "memo": "",
            },
            {
                "source": "ynab",
                "ynab_id": "src-3",
                "account_name": "Family Leumi",
                "source_account": "Family Leumi",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "Pilates",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
            },
        ]
    )
    target_df = pd.DataFrame(
        [
            {
                "ynab_id": "t-1",
                "account_name": "In Family",
                "date": "2026-02-01",
                "payee_raw": "A",
                "category_raw": "",
                "fingerprint": "a",
                "outflow_ils": 0.0,
                "inflow_ils": 10.0,
                "memo": "",
                "cleared": "reconciled",
            },
            {
                "ynab_id": "t-2",
                "account_name": "In Family",
                "date": "2026-02-02",
                "payee_raw": "B",
                "category_raw": "",
                "fingerprint": "b",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "memo": "",
                "cleared": "reconciled",
            },
            {
                "ynab_id": "t-3",
                "account_name": "In Family",
                "date": "2026-02-03",
                "payee_raw": "C",
                "category_raw": "",
                "fingerprint": "c",
                "outflow_ils": 0.0,
                "inflow_ils": 25.0,
                "memo": "",
                "cleared": "cleared",
            },
        ]
    )
    source_month_report = pd.DataFrame(
        [
            {
                "month": "2026-01-01",
                "month_end": "2026-01-31",
                "source_category_group": "Business",
                "source_category_balance_ils": 0.0,
                "source_category_activity_ils": 0.0,
                "source_category_budgeted_ils": 0.0,
            }
        ]
    )

    result = cross_budget_reconciliation.plan_cross_budget_reconciliation(
        source_category_groups=_category_groups(balance=175000, activity=75000),
        source_month_details=[],
        source_month_report_df=source_month_report,
        target_accounts=_accounts(cleared_balance=150000, uncleared_balance=25000, balance=175000),
        source_transactions_df=source_df,
        target_transactions_df=target_df,
        source_category_name="Pilates",
        target_account_name="In Family",
        since="2026-02-03",
        anchor_streak=2,
    )

    assert result["ok"] is True
    assert result["updates"] == [{"id": "t-3", "cleared": "reconciled"}]
