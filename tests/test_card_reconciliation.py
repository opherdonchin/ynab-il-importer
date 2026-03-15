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
        }
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


def _billed_row(*, date: str, merchant: str, amount: str, charge_date: str, txn_type: str = "רגילה") -> dict[str, str]:
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


def test_load_card_source_ignores_pending_rows(tmp_path: Path) -> None:
    source_path = tmp_path / "current.xlsx"
    _write_snapshot(
        source_path,
        period="04/2026",
        billed_rows=[
            _billed_row(date="09-03-2026", merchant="MERCHANT A", amount="100", charge_date="10-04-2026"),
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
            _billed_row(date="09-03-2026", merchant="MERCHANT A", amount="100", charge_date="10-04-2026"),
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
            _billed_row(date="09-03-2026", merchant="MERCHANT A", amount="100", charge_date="10-04-2026"),
            _billed_row(date="10-03-2026", merchant="MERCHANT B", amount="50", charge_date="10-04-2026"),
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
            _billed_row(date="10-02-2026", merchant="MERCHANT A", amount="100", charge_date="10-03-2026"),
            _billed_row(date="11-02-2026", merchant="MERCHANT B", amount="80", charge_date="10-03-2026"),
        ],
        foreign_rows=[
            _billed_row(date="12-01-2026", merchant="PAYPAL *FACEBOOK", amount="60", charge_date="10-03-2026", txn_type="דחוי חודשיים"),
        ],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[
            _billed_row(date="09-03-2026", merchant="MERCHANT C", amount="120", charge_date="10-04-2026"),
            _billed_row(date="10-03-2026", merchant="MERCHANT D", amount="70", charge_date="10-04-2026"),
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
    ]

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
    assert result["update_count"] == 4
    assert result["updates"] == [
        {"id": "prev-1", "cleared": "reconciled"},
        {"id": "prev-2", "cleared": "reconciled"},
        {"id": "prev-3", "cleared": "reconciled"},
        {"id": "curr-1", "cleared": "cleared"},
    ]
    assert set(result["report"][result["report"]["snapshot_role"] == "previous"]["action"]) == {"reconcile"}
    assert set(result["report"][result["report"]["snapshot_role"] == "source"]["action"]) == {"clear", "keep_cleared"}


def test_transition_warns_when_previous_is_already_reconciled(tmp_path: Path) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[_billed_row(date="10-02-2026", merchant="MERCHANT A", amount="100", charge_date="10-03-2026")],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[_billed_row(date="09-03-2026", merchant="MERCHANT B", amount="120", charge_date="10-04-2026")],
    )

    previous_df = card_reconciliation.load_card_source(previous_path)
    current_df = card_reconciliation.load_card_source(current_path)
    previous_rows = card_reconciliation._target_source_rows(previous_df, "Opher x9922")
    current_rows = card_reconciliation._target_source_rows(current_df, "Opher x9922")

    transactions = [
        _txn_from_source(previous_rows.iloc[0], txn_id="prev-1", cleared="reconciled"),
        _txn_from_source(current_rows.iloc[0], txn_id="curr-1", cleared="cleared"),
    ]

    result = card_reconciliation.plan_card_cycle_reconciliation(
        account_name="Opher x9922",
        source_df=current_df,
        previous_df=previous_df,
        accounts=_accounts(),
        transactions=transactions,
    )

    assert result["ok"] is True
    assert result["warning"] == "All previous-file transactions are already reconciled."
    assert result["update_count"] == 0


def test_transition_blocks_when_source_rows_are_already_reconciled(tmp_path: Path) -> None:
    previous_path = tmp_path / "previous.xlsx"
    current_path = tmp_path / "current.xlsx"
    _write_snapshot(
        previous_path,
        period="03/2026",
        billed_rows=[_billed_row(date="10-02-2026", merchant="MERCHANT A", amount="100", charge_date="10-03-2026")],
    )
    _write_snapshot(
        current_path,
        period="04/2026",
        billed_rows=[_billed_row(date="09-03-2026", merchant="MERCHANT B", amount="120", charge_date="10-04-2026")],
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
