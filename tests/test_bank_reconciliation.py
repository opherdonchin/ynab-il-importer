from __future__ import annotations

import pandas as pd
import pytest

import ynab_il_importer.bank_identity as bank_identity
import ynab_il_importer.bank_reconciliation as bank_reconciliation


def _accounts(*, last_reconciled_at: str = "2026-03-10T12:00:00Z") -> list[dict[str, str]]:
    return [
        {
            "id": "acc-bank",
            "name": "Bank Leumi",
            "transfer_payee_id": "payee-bank",
            "last_reconciled_at": last_reconciled_at,
        }
    ]


def _bank_row(
    *,
    date: str,
    amount_ils: float,
    balance_ils: float,
    description_raw: str,
    index: int,
) -> dict[str, object]:
    outflow_ils = abs(amount_ils) if amount_ils < 0 else 0.0
    inflow_ils = amount_ils if amount_ils > 0 else 0.0
    return {
        "account_name": "Bank Leumi",
        "source_account": "123456",
        "ynab_account_id": "acc-bank",
        "date": date,
        "secondary_date": date,
        "description_raw": description_raw,
        "ref": f"00{index}",
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "balance_ils": balance_ils,
        "bank_txn_id": bank_identity.make_bank_txn_id(
            source="bank",
            source_account="123456",
            date=date,
            secondary_date=date,
            outflow_ils=outflow_ils,
            inflow_ils=inflow_ils,
            ref=f"00{index}",
            description_raw=description_raw,
        ),
    }


def test_plan_bank_match_sync_stamps_and_clears_unique_memo_match() -> None:
    bank_df = pd.DataFrame(
        [
            _bank_row(
                date="2026-03-01",
                amount_ils=-10,
                balance_ils=90,
                description_raw="GROCERIES",
                index=1,
            )
        ]
    )
    bank_txn_id = bank_df.loc[0, "bank_txn_id"]
    ynab_transactions = [
        {
            "id": "txn-1",
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": -10000,
            "memo": "GROCERIES",
            "import_id": "",
            "cleared": "uncleared",
            "approved": True,
        }
    ]

    result = bank_reconciliation.plan_bank_match_sync(
        bank_df,
        _accounts(),
        ynab_transactions,
    )

    assert result["update_count"] == 1
    assert result["updates"] == [
        {
            "id": "txn-1",
            "memo": f"GROCERIES\n[ynab-il bank_txn_id={bank_txn_id}]",
            "cleared": "cleared",
        }
    ]
    assert result["report"].loc[0, "resolved_via"] == "memo_exact"
    assert result["report"].loc[0, "action"] == "stamp+clear"


def test_plan_bank_match_sync_refuses_weak_date_amount_only_match() -> None:
    bank_df = pd.DataFrame(
        [
            _bank_row(
                date="2026-03-01",
                amount_ils=-10,
                balance_ils=90,
                description_raw="GROCERIES",
                index=1,
            )
        ]
    )
    ynab_transactions = [
        {
            "id": "txn-1",
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": -10000,
            "memo": "SOMETHING ELSE",
            "import_id": "",
            "cleared": "uncleared",
            "approved": True,
        }
    ]

    result = bank_reconciliation.plan_bank_match_sync(
        bank_df,
        _accounts(),
        ynab_transactions,
    )

    assert result["update_count"] == 0
    assert result["report"].loc[0, "action"] == "unmatched"
    assert result["report"].loc[0, "reason"] == "no conservative memo match"


def test_plan_bank_statement_reconciliation_uses_exact_lineage_and_running_balance() -> None:
    amounts = [-10, -20, -30, -40, -50, -60, -70, -80]
    balances = [990, 970, 940, 900, 850, 790, 720, 640]
    bank_rows = [
        _bank_row(
            date=f"2026-03-0{i + 1}",
            amount_ils=amount,
            balance_ils=balance,
            description_raw=f"ROW {i + 1}",
            index=i + 1,
        )
        for i, (amount, balance) in enumerate(zip(amounts, balances))
    ]
    bank_df = pd.DataFrame(bank_rows)

    ynab_transactions: list[dict[str, object]] = []
    for i, bank_row in enumerate(bank_rows):
        cleared = "reconciled" if i < 7 else "uncleared"
        import_id = bank_row["bank_txn_id"] if i < 7 else ""
        memo = (
            f"ROW {i + 1}"
            if i < 7
            else f"ROW {i + 1}\n[ynab-il bank_txn_id={bank_row['bank_txn_id']}]"
        )
        ynab_transactions.append(
            {
                "id": f"txn-{i + 1}",
                "account_id": "acc-bank",
                "date": bank_row["date"],
                "amount": int(round(amounts[i] * 1000)),
                "memo": memo,
                "import_id": import_id,
                "cleared": cleared,
                "approved": True,
            }
        )

    result = bank_reconciliation.plan_bank_statement_reconciliation(
        bank_df,
        _accounts(),
        ynab_transactions,
    )

    assert result["anchor_type"] == "last_reconciled_at"
    assert result["anchor_balance_ils"] == 720.0
    assert result["update_count"] == 1
    assert result["updates"] == [{"id": "txn-8", "cleared": "reconciled"}]
    assert result["report"].iloc[-1]["resolved_via"] == "memo_marker"
    assert bool(result["report"].iloc[-1]["balance_match"]) is True


def test_plan_bank_statement_reconciliation_uses_starting_balance_when_last_reconciled_missing() -> None:
    bank_rows = [
        _bank_row(
            date="2026-03-01",
            amount_ils=-10,
            balance_ils=90,
            description_raw="ROW 1",
            index=1,
        ),
        _bank_row(
            date="2026-03-02",
            amount_ils=5,
            balance_ils=95,
            description_raw="ROW 2",
            index=2,
        ),
    ]
    bank_df = pd.DataFrame(bank_rows)
    ynab_transactions = [
        {
            "id": "starting-balance",
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": 100000,
            "memo": "Starting Balance",
            "import_id": "",
            "cleared": "reconciled",
            "approved": True,
        },
        {
            "id": "txn-1",
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": -10000,
            "memo": "ROW 1",
            "import_id": bank_rows[0]["bank_txn_id"],
            "cleared": "uncleared",
            "approved": True,
        },
        {
            "id": "txn-2",
            "account_id": "acc-bank",
            "date": "2026-03-02",
            "amount": 5000,
            "memo": "ROW 2",
            "import_id": bank_rows[1]["bank_txn_id"],
            "cleared": "cleared",
            "approved": True,
        },
    ]

    result = bank_reconciliation.plan_bank_statement_reconciliation(
        bank_df,
        _accounts(last_reconciled_at=""),
        ynab_transactions,
    )

    assert result["anchor_type"] == "starting_balance"
    assert result["anchor_transaction_id"] == "starting-balance"
    assert result["anchor_balance_ils"] == 100.0
    assert result["updates"] == [
        {"id": "txn-1", "cleared": "reconciled"},
        {"id": "txn-2", "cleared": "reconciled"},
    ]


def test_plan_bank_statement_reconciliation_requires_starting_balance_date_when_unset() -> None:
    bank_df = pd.DataFrame(
        [
            _bank_row(
                date="2026-03-02",
                amount_ils=-10,
                balance_ils=90,
                description_raw="ROW 1",
                index=1,
            )
        ]
    )
    ynab_transactions = [
        {
            "id": "starting-balance",
            "account_id": "acc-bank",
            "date": "2026-03-01",
            "amount": 100000,
            "memo": "Starting Balance",
            "import_id": "",
            "cleared": "reconciled",
            "approved": True,
        }
    ]

    with pytest.raises(ValueError, match="must start on the starting balance date"):
        bank_reconciliation.plan_bank_statement_reconciliation(
            bank_df,
            _accounts(last_reconciled_at=""),
            ynab_transactions,
        )
