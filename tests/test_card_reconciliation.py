from __future__ import annotations

from pathlib import Path

import pandas as pd

import ynab_il_importer.card_reconciliation as card_reconciliation


def _accounts() -> list[dict[str, str]]:
    return [
        {
            "id": "acc-card",
            "name": "Opher x9922",
            "transfer_payee_id": "payee-card",
        },
        {
            "id": "acc-bank",
            "name": "Bank Leumi",
            "transfer_payee_id": "payee-bank",
        },
    ]


def _base_columns() -> list[str]:
    return [
        "תאריך עסקה",
        "שם בית העסק",
        "קטגוריה",
        "4 ספרות אחרונות של כרטיס האשראי",
        "סוג עסקה",
        "סכום חיוב",
        "מטבע חיוב",
        "סכום עסקה מקורי",
        "מטבע עסקה מקורי",
        "תאריך חיוב",
        "הערות",
        "תיוגים",
        "מועדון הנחות",
        "מפתח דיסקונט",
        "אופן ביצוע ההעסקה",
        'שער המרה ממטבע מקור/התחשבנות לש"ח',
    ]


def _sheet_frame(rows: list[dict[str, str]], *, period: str) -> pd.DataFrame:
    raw = pd.DataFrame(rows, columns=_base_columns(), dtype="string").fillna("")
    width = max(len(raw.columns), 1)
    rows_out = [
        ["כל המשתמשים (2)"] + ["" for _ in range(width - 1)],
        ["9922-כרטיס UNIQ"] + ["" for _ in range(width - 1)],
        [period] + ["" for _ in range(width - 1)],
        list(raw.columns),
    ]
    rows_out.extend(raw.values.tolist())
    return pd.DataFrame(rows_out, dtype="string")


def _write_snapshot(
    path: Path,
    *,
    period: str,
    billed_rows: list[dict[str, str]],
    foreign_rows: list[dict[str, str]] | None = None,
    pending_rows: list[dict[str, str]] | None = None,
) -> None:
    foreign_rows = foreign_rows or []
    pending_rows = pending_rows or []
    with pd.ExcelWriter(path) as writer:
        _sheet_frame(billed_rows, period=period).to_excel(
            writer, sheet_name="עסקאות במועד החיוב", header=False, index=False
        )
        if foreign_rows:
            _sheet_frame(foreign_rows, period=period).to_excel(
                writer, sheet_name='עסקאות חו"ל ומט"ח', header=False, index=False
            )
        if pending_rows:
            _sheet_frame(pending_rows, period=period).to_excel(
                writer, sheet_name="עסקאות שאושרו וטרם נקלטו", header=False, index=False
            )


def _billed_row(
    *, date: str, merchant: str, amount: str, charge_date: str, txn_type: str = "רגילה"
) -> dict[str, str]:
    return {
        "תאריך עסקה": date,
        "שם בית העסק": merchant,
        "קטגוריה": "שונות",
        "4 ספרות אחרונות של כרטיס האשראי": "9922",
        "סוג עסקה": txn_type,
        "סכום חיוב": amount,
        "מטבע חיוב": "₪",
        "סכום עסקה מקורי": amount,
        "מטבע עסקה מקורי": "₪",
        "תאריך חיוב": charge_date,
        "הערות": "",
        "תיוגים": "",
        "מועדון הנחות": "",
        "מפתח דיסקונט": "",
        "אופן ביצוע ההעסקה": "אינטרנט",
        'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
    }


def _pending_row(*, date: str, merchant: str, amount: str) -> dict[str, str]:
    return {
        "תאריך עסקה": date,
        "שם בית העסק": merchant,
        "קטגוריה": "שונות",
        "4 ספרות אחרונות של כרטיס האשראי": "9922",
        "סוג עסקה": "רגילה",
        "סכום חיוב": "",
        "מטבע חיוב": "",
        "סכום עסקה מקורי": amount,
        "מטבע עסקה מקורי": "₪",
        "תאריך חיוב": "",
        "הערות": "",
        "תיוגים": "",
        "מועדון הנחות": "",
        "מפתח דיסקונט": "",
        "אופן ביצוע ההעסקה": "",
        'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
    }


def _txn_from_source(row: pd.Series, *, txn_id: str, cleared: str) -> dict[str, object]:
    signed = float(row["signed_ils"])
    return {
        "id": txn_id,
        "account_id": "acc-card",
        "date": str(row["date"]),
        "amount": int(round(signed * 1000)),
        "memo": row["description_raw"],
        "import_id": row["card_txn_id"],
        "cleared": cleared,
        "approved": True,
    }


def _txn_manual(
    *,
    txn_id: str,
    date: str,
    amount_ils: float,
    payee_name: str,
    cleared: str,
    memo: str = "",
    import_id: str = "",
) -> dict[str, object]:
    return {
        "id": txn_id,
        "account_id": "acc-card",
        "date": date,
        "amount": int(round(-amount_ils * 1000)),
        "memo": memo,
        "import_id": import_id,
        "cleared": cleared,
        "approved": True,
        "payee_name": payee_name,
    }


def _transfer_pair(
    *, transfer_id: str, date: str, amount_ils: float
) -> list[dict[str, object]]:
    milliunits = int(round(amount_ils * 1000))
    return [
        {
            "id": f"{transfer_id}-card",
            "account_id": "acc-card",
            "date": date,
            "amount": milliunits,
            "memo": "",
            "import_id": "",
            "cleared": "cleared",
            "approved": True,
            "transfer_account_id": "acc-bank",
            "transfer_transaction_id": f"{transfer_id}-bank",
            "payee_name": "Transfer : Bank Leumi",
        },
        {
            "id": f"{transfer_id}-bank",
            "account_id": "acc-bank",
            "date": date,
            "amount": -milliunits,
            "memo": "",
            "import_id": "",
            "cleared": "cleared",
            "approved": True,
            "transfer_account_id": "acc-card",
            "transfer_transaction_id": f"{transfer_id}-card",
            "payee_name": "Transfer : Opher x9922",
        },
    ]


def test_load_card_source_ignores_pending_rows(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            ),
        ],
        pending_rows=[
            _pending_row(date="13-03-2026", merchant="PAYPAL *FACEBOOK", amount="735"),
        ],
    )

    actual = card_reconciliation.load_card_source(source_path)

    assert len(actual) == 1
    assert actual.loc[0, "account_name"] == "Opher x9922"
    assert actual.loc[0, "card_txn_id"].startswith("CARD:V1:")


def test_source_only_blocks_when_older_cleared_rows_exist(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            ),
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        {
            "id": "older-open",
            "account_id": "acc-card",
            "date": "2026-03-01",
            "amount": -50000,
            "memo": "OLDER OPEN",
            "import_id": "",
            "cleared": "cleared",
            "approved": True,
        },
        _txn_from_source(source_rows.iloc[0], txn_id="txn-current", cleared="cleared"),
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert "Provide --previous" in result["reason"]


def test_source_only_clears_uncleared_current_rows(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            ),
            _billed_row(
                date="10-03-2026",
                merchant="MERCHANT B",
                amount="50",
                charge_date="10-04-2026",
            ),
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        _txn_from_source(source_rows.iloc[0], txn_id="txn-1", cleared="uncleared"),
        _txn_from_source(source_rows.iloc[1], txn_id="txn-2", cleared="cleared"),
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["update_count"] == 1
    assert result["updates"] == [{"id": "txn-1", "cleared": "cleared"}]
    assert set(result["report"]["action"]) == {"clear", "keep_cleared"}


def test_transition_reconciles_previous_and_keeps_current_open(tmp_path: Path) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            ),
            _billed_row(
                date="11-02-2026",
                merchant="MERCHANT B",
                amount="80",
                charge_date="10-03-2026",
            ),
        ],
        foreign_rows=[
            _billed_row(
                date="12-01-2026",
                merchant="PAYPAL *FACEBOOK",
                amount="60",
                charge_date="10-03-2026",
                txn_type="דחוי חודשיים",
            ),
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT C",
                amount="120",
                charge_date="10-04-2026",
            ),
            _billed_row(
                date="10-03-2026",
                merchant="MERCHANT D",
                amount="70",
                charge_date="10-04-2026",
            ),
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(previous_rows.iloc[1], txn_id="prev-2", cleared="uncleared"),
        _txn_from_source(previous_rows.iloc[2], txn_id="prev-3", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="uncleared"),
        _txn_from_source(current_rows.iloc[1], txn_id="curr-2", cleared="cleared"),
    ] + _transfer_pair(transfer_id="payment-mar", date="2026-03-10", amount_ils=240.0)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["previous_total_ils"] == -240.0
    assert result["source_total_ils"] == -190.0
    assert result["matched_previous_count"] == 3
    assert result["previous_row_count"] == 3
    assert result["matched_previous_total_ils"] == -240.0
    assert result["matched_source_count"] == 2
    assert result["source_row_count"] == 2
    assert result["matched_source_total_ils"] == -190.0
    assert result["payment_transfer_card_transaction_id"] == "payment-mar-card"
    assert result["payment_transfer_bank_transaction_id"] == "payment-mar-bank"
    assert result["payment_transfer_bank_account_name"] == "Bank Leumi"
    assert result["update_count"] == 5
    assert result["updates"] == [
        {"id": "prev-1", "cleared": "reconciled"},
        {"id": "prev-2", "cleared": "reconciled"},
        {"id": "prev-3", "cleared": "reconciled"},
        {"id": "curr-1", "cleared": "cleared"},
        {"id": "payment-mar-card", "cleared": "reconciled"},
    ]
    assert set(
        result["report"][result["report"]["snapshot_role"] == "previous"]["action"]
    ) == {"reconcile"}
    assert set(
        result["report"][result["report"]["snapshot_role"] == "source"]["action"]
    ) == {"clear", "keep_cleared"}


def test_transition_warns_when_previous_is_already_reconciled(tmp_path: Path) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="reconciled"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="uncleared"),
    ] + _transfer_pair(transfer_id="payment-mar", date="2026-03-10", amount_ils=100.0)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["warning"] == "All previous-file transactions are already reconciled."
    assert result["update_count"] == 2
    assert result["updates"] == [
        {"id": "curr-1", "cleared": "cleared"},
        {"id": "payment-mar-card", "cleared": "reconciled"},
    ]
    assert set(
        result["report"][result["report"]["snapshot_role"] == "previous"]["action"]
    ) == {"already_reconciled"}
    assert set(
        result["report"][result["report"]["snapshot_role"] == "source"]["action"]
    ) == {"clear"}


def test_transition_blocks_when_source_rows_are_already_reconciled(
    tmp_path: Path,
) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="reconciled"),
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert "already reconciled" in result["reason"]


def test_transition_blocks_when_payment_transfer_is_missing(tmp_path: Path) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert "No card payment transfer found" in result["reason"]


def test_transition_blocks_when_transfer_amount_does_not_match(tmp_path: Path) -> None:
    """Transfer exists in YNAB but its amount doesn't match the previous-cycle total."""
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # Transfer for 200 ILS, but previous total is only 100 ILS → no match
    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
    ] + _transfer_pair(transfer_id="payment-mar", date="2026-03-10", amount_ils=200.0)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert (
        "No card payment transfer found for previous total 100.00 ILS"
        in result["reason"]
    )


def test_transition_blocks_when_transfer_has_no_linked_bank_transaction(
    tmp_path: Path,
) -> None:
    """Transfer card-side exists with correct amount but has no transfer_transaction_id."""
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # Card-side transfer has the right amount but no transfer_transaction_id linking to bank
    unlinked_transfer = {
        "id": "payment-card",
        "account_id": "acc-card",
        "date": "2026-03-10",
        "amount": 100000,  # +100 ILS matches previous total
        "memo": "",
        "import_id": "",
        "cleared": "cleared",
        "approved": True,
        "transfer_account_id": "acc-bank",
        "transfer_transaction_id": "",  # no link
        "payee_name": "Transfer : Bank Leumi",
    }
    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
        unlinked_transfer,
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert "has no linked bank transfer transaction" in result["reason"]


def test_transition_blocks_when_linked_bank_transaction_is_missing(
    tmp_path: Path,
) -> None:
    """Transfer card-side has a bank link ID, but that bank transaction is absent from YNAB."""
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # Card-side transfer points to a bank transaction ID that doesn't exist in the list
    orphan_card_transfer = {
        "id": "payment-card",
        "account_id": "acc-card",
        "date": "2026-03-10",
        "amount": 100000,
        "memo": "",
        "import_id": "",
        "cleared": "cleared",
        "approved": True,
        "transfer_account_id": "acc-bank",
        "transfer_transaction_id": "ghost-bank-id",  # not in transactions list
        "payee_name": "Transfer : Bank Leumi",
    }
    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
        orphan_card_transfer,
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert "was not found in YNAB transactions" in result["reason"]


def test_transition_skips_transfer_update_when_already_reconciled(
    tmp_path: Path,
) -> None:
    """Payment transfer is already reconciled — no update emitted for it."""
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            )
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT B",
                amount="120",
                charge_date="10-04-2026",
            )
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # Build the transfer pair but override card-side to already be reconciled
    transfer_txns = _transfer_pair(
        transfer_id="payment-mar", date="2026-03-10", amount_ils=100.0
    )
    transfer_txns[0]["cleared"] = "reconciled"

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="cleared"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="uncleared"),
    ] + transfer_txns

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["payment_transfer_card_transaction_id"] == "payment-mar-card"
    # Transfer already reconciled — should not appear in updates
    update_ids = [u["id"] for u in result["updates"]]
    assert "payment-mar-card" not in update_ids
    # Previous charge and current uncleared should still be updated
    assert {"id": "prev-1", "cleared": "reconciled"} in result["updates"]
    assert {"id": "curr-1", "cleared": "cleared"} in result["updates"]


def test_plan_card_match_sync_stamps_unique_date_amount_and_clears_uncleared(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-03-09",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 1
    assert result["updates"] == [
        {
            "id": "txn-1",
            "memo": f"[ynab-il card_txn_id={source_rows.iloc[0]['card_txn_id']}]",
            "cleared": "cleared",
        }
    ]
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "date_amount_unique"
    assert report.loc[0, "action"] == "stamp+clear"


def test_plan_card_match_sync_stamps_same_date_memo_exact_candidate(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-03-09",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
            memo="MERCHANT A",
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 1
    assert result["updates"] == [
        {
            "id": "txn-1",
            "memo": f"MERCHANT A\n[ynab-il card_txn_id={source_rows.iloc[0]['card_txn_id']}]",
            "cleared": "cleared",
        }
    ]
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "date_amount_unique_memo_exact"
    assert report.loc[0, "action"] == "stamp+clear"


def test_plan_card_match_sync_stamps_secondary_date_amount_match_and_clears_uncleared(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-04-10",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 1
    assert result["updates"] == [
        {
            "id": "txn-1",
            "memo": f"[ynab-il card_txn_id={source_rows.iloc[0]['card_txn_id']}]",
            "cleared": "cleared",
        }
    ]
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "secondary_date_amount_unique"
    assert report.loc[0, "action"] == "stamp+clear"


def test_plan_card_match_sync_stamps_legacy_import_id_match(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")
    legacy_import_id = source_rows.iloc[0]["legacy_import_id"]

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-03-09",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
            import_id=legacy_import_id,
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 1
    assert result["updates"] == [
        {
            "id": "txn-1",
            "memo": f"[ynab-il card_txn_id={source_rows.iloc[0]['card_txn_id']}]",
            "cleared": "cleared",
        }
    ]
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "legacy_import_id"
    assert report.loc[0, "action"] == "stamp+clear"


def test_plan_card_match_sync_noops_on_exact_lineage(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)
    source_rows = card_reconciliation._target_source_rows(source_df, "Opher x9922")

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-03-09",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="cleared",
            import_id=source_rows.iloc[0]["card_txn_id"],
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 0
    assert result["updates"] == []
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "import_id"
    assert report.loc[0, "action"] == "noop"


def test_source_only_clears_secondary_date_memo_exact_match(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-04-10",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
            memo="MERCHANT A",
        )
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["update_count"] == 1
    assert result["updates"] == [{"id": "txn-1", "cleared": "cleared"}]
    report = result["report"]
    assert report.loc[0, "resolved_via"] == "secondary_date_memo_exact"
    assert report.loc[0, "action"] == "clear"


def test_plan_card_match_sync_refuses_conflicting_linked_candidate(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-04-2026",
            )
        ],
    )
    source_df = card_reconciliation.load_card_source(source_path)

    transactions = [
        _txn_manual(
            txn_id="txn-1",
            date="2026-03-09",
            amount_ils=100.0,
            payee_name="Canonical Payee",
            cleared="uncleared",
            import_id="CARD:V1:1234567890abcdef12345678",
        )
    ]

    result = card_reconciliation.plan_card_match_sync(
        account_name="Opher x9922",
        source_df=source_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["update_count"] == 0
    report = result["report"]
    assert report.loc[0, "action"] == "unmatched"
    assert report.loc[0, "candidate_status"] == "only_linked_date_amount_candidates"
    assert "already linked to a different card_txn_id" in report.loc[0, "reason"]


def test_transition_reconciles_separately_settled_rows(tmp_path: Path) -> None:
    """Previous file has rows with both a main billing date and earlier separately-settled dates.
    Only the main billing date rows are used to look up the payment transfer.
    All previous rows (main + separately settled) are marked reconciled.
    """
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            ),
            _billed_row(
                date="11-02-2026",
                merchant="MERCHANT B",
                amount="80",
                charge_date="10-03-2026",
            ),
        ],
        foreign_rows=[
            _billed_row(
                date="06-02-2026",
                merchant="NETFLIX",
                amount="69.90",
                charge_date="08-02-2026",
            ),
            _billed_row(
                date="01-02-2026",
                merchant="CHATGPT",
                amount="62.00",
                charge_date="03-02-2026",
            ),
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT C",
                amount="120",
                charge_date="10-04-2026",
            ),
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # 4 previous rows: 2 main (2026-03-10), 2 separately settled (2026-02-08, 2026-02-03)
    assert len(previous_rows) == 4

    # Payment transfer for the MAIN billing total only (100+80=180), NOT including sep rows
    transactions = [
        _txn_from_source(
            previous_rows.iloc[0], txn_id="prev-main-a", cleared="cleared"
        ),
        _txn_from_source(
            previous_rows.iloc[1], txn_id="prev-main-b", cleared="cleared"
        ),
        _txn_from_source(
            previous_rows.iloc[2], txn_id="prev-netflix", cleared="cleared"
        ),
        _txn_from_source(
            previous_rows.iloc[3], txn_id="prev-chatgpt", cleared="cleared"
        ),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
    ] + _transfer_pair(transfer_id="payment-mar", date="2026-03-10", amount_ils=180.0)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["separately_settled_count"] == 2
    assert result["separately_settled_dates"] == ["2026-02-03", "2026-02-08"]
    assert result["payment_transfer_card_transaction_id"] == "payment-mar-card"
    assert result["previous_total_ils"] == -311.9  # 100+80+69.90+62
    assert result["payment_transfer_card_amount_ils"] == 180.0

    prev_report = result["report"][result["report"]["snapshot_role"] == "previous"]
    main_actions = set(
        prev_report[prev_report["secondary_date"] == "2026-03-10"]["action"]
    )
    sep_actions = set(
        prev_report[prev_report["secondary_date"] != "2026-03-10"]["action"]
    )
    assert main_actions == {"reconcile"}
    assert sep_actions == {"reconcile_separate"}

    # All 4 previous rows + the card-side payment transfer should be reconciled
    update_ids = {u["id"] for u in result["updates"]}
    assert update_ids == {
        "prev-main-a",
        "prev-main-b",
        "prev-netflix",
        "prev-chatgpt",
        "payment-mar-card",
    }
    assert all(u["cleared"] == "reconciled" for u in result["updates"])


def test_transition_blocks_when_only_full_total_transfer_exists_for_sep_settled(
    tmp_path: Path,
) -> None:
    """If YNAB has a transfer for the full statement total (including sep-settled rows)
    but not for the main billing total alone, reconcile should block."""
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[
            _billed_row(
                date="10-02-2026",
                merchant="MERCHANT A",
                amount="100",
                charge_date="10-03-2026",
            ),
        ],
        foreign_rows=[
            _billed_row(
                date="06-02-2026",
                merchant="NETFLIX",
                amount="69.90",
                charge_date="08-02-2026",
            ),
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(
                date="09-03-2026",
                merchant="MERCHANT C",
                amount="120",
                charge_date="10-04-2026",
            ),
        ],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    # Transfer is for the FULL total (100+69.90=169.90) not the main-only total (100)
    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-main", cleared="cleared"),
        _txn_from_source(
            previous_rows.iloc[1], txn_id="prev-netflix", cleared="cleared"
        ),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
    ] + _transfer_pair(transfer_id="payment-mar", date="2026-03-10", amount_ils=169.90)

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is False
    assert (
        "No card payment transfer found for previous total 100.00 ILS"
        in result["reason"]
    )
