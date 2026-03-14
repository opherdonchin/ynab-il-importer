import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.io_max as maxio


def _load_fixture_rows(input_csv: Path) -> pd.DataFrame:
    raw = pd.read_csv(input_csv, dtype="string").fillna("")
    return raw


def _mock_card_sheet_df(raw: pd.DataFrame, preface_rows: list[list[str]] | None = None) -> pd.DataFrame:
    width = max(len(raw.columns), 1)
    if preface_rows is None:
        preface_rows = [["" for _ in range(width)] for _ in range(3)]
    header_row = list(raw.columns)
    data_rows = raw.values.tolist()
    return pd.DataFrame(preface_rows + [header_row] + data_rows)


def test_read_card_emits_normalized_schema_from_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    input_csv = ROOT / "tests" / "fixtures" / "card" / "max_sample_input.csv"
    expected_csv = ROOT / "tests" / "fixtures" / "expected" / "card_max_sample_normalized.csv"
    raw = _load_fixture_rows(input_csv)
    workbook_df = _mock_card_sheet_df(raw)

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None, nrows=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return {"עסקאות": workbook_df}
        raise AssertionError(
            "Unexpected read_excel invocation: "
            f"path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}, nrows={nrows}"
        )

    monkeypatch.setattr("ynab_il_importer.io_max.pd.read_excel", _fake_read_excel)

    with pytest.warns(UserWarning):
        actual = maxio.read_raw("tests/fixtures/card/max_sample_input.xlsx")
    expected = pd.read_csv(expected_csv)

    actual_cmp = actual[expected.columns].copy()
    actual_cmp["date"] = actual_cmp["date"].astype("string")
    actual_cmp["secondary_date"] = actual_cmp["secondary_date"].astype("string")
    actual_cmp["outflow_ils"] = pd.to_numeric(actual_cmp["outflow_ils"], errors="coerce").round(2)
    actual_cmp["inflow_ils"] = pd.to_numeric(actual_cmp["inflow_ils"], errors="coerce").round(2)

    pd.testing.assert_frame_equal(actual_cmp.reset_index(drop=True), expected, check_dtype=False)


def test_read_card_drops_pure_empty_noise_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = pd.DataFrame(
        [
            {
                "תאריך עסקה": "01/02/2026",
                "תאריך חיוב": "10/02/2026",
                "שם בית העסק": "MERCHANT A",
                "הערות": "",
                "סכום עסקה": "10.00",
                "מטבע חיוב": "₪",
            },
            {
                "תאריך עסקה": "",
                "תאריך חיוב": "",
                "שם בית העסק": "",
                "הערות": "",
                "סכום עסקה": "",
                "מטבע חיוב": "",
            },
        ],
        dtype="string",
    ).fillna("")
    workbook_df = _mock_card_sheet_df(raw)

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None, nrows=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return {"עסקאות": workbook_df}
        raise AssertionError(
            "Unexpected read_excel invocation: "
            f"path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}, nrows={nrows}"
        )

    monkeypatch.setattr("ynab_il_importer.io_max.pd.read_excel", _fake_read_excel)
    actual = maxio.read_raw("tests/fixtures/card/noise_case.xlsx")

    assert len(actual) == 1
    assert actual.iloc[0]["currency"] == "ILS"


def test_read_card_combines_max_sections_and_preserves_source_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billed_raw = pd.DataFrame(
        [
            {
                "תאריך עסקה": "10-02-2026",
                "שם בית העסק": "הראל-ביטוח בריאות",
                "קטגוריה": "ביטוח",
                "4 ספרות אחרונות של כרטיס האשראי": "9922",
                "סוג עסקה": "רגילה",
                "סכום חיוב": "360.83",
                "מטבע חיוב": "₪",
                "סכום עסקה מקורי": "360.83",
                "מטבע עסקה מקורי": "₪",
                "תאריך חיוב": "10-03-2026",
                "הערות": "הוראת קבע",
                "תיוגים": "",
                "מועדון הנחות": "",
                "מפתח דיסקונט": "",
                "אופן ביצוע ההעסקה": "טלפוני",
                'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
            }
        ],
        dtype="string",
    ).fillna("")
    foreign_raw = pd.DataFrame(
        [
            {
                "תאריך עסקה": "16-12-2025",
                "שם בית העסק": "PAYPAL *FACEBOOK       35314369001   IE",
                "קטגוריה": "פנאי, בידור וספורט",
                "4 ספרות אחרונות של כרטיס האשראי": "9922",
                "סוג עסקה": "דחוי חודשיים",
                "סכום חיוב": "568",
                "מטבע חיוב": "₪",
                "סכום עסקה מקורי": "568",
                "מטבע עסקה מקורי": "₪",
                "תאריך חיוב": "10-03-2026",
                "הערות": 'חיוב עסקת חו"ל בש"ח ',
                "תיוגים": "",
                "מועדון הנחות": "",
                "מפתח דיסקונט": "",
                "אופן ביצוע ההעסקה": "אינטרנט",
                'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
            }
        ],
        dtype="string",
    ).fillna("")

    workbook = {
        "עסקאות במועד החיוב": _mock_card_sheet_df(
            billed_raw,
            preface_rows=[
                ["כל המשתמשים (2)"],
                ["9922-כרטיס UNIQ"],
                ["03/2026"],
            ],
        ),
        'עסקאות חו"ל ומט"ח': _mock_card_sheet_df(
            foreign_raw,
            preface_rows=[
                ["כל המשתמשים (2)"],
                ["9922-כרטיס UNIQ"],
                ["03/2026"],
            ],
        ),
    }

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None, nrows=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return workbook
        raise AssertionError(
            "Unexpected read_excel invocation: "
            f"path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}, nrows={nrows}"
        )

    monkeypatch.setattr("ynab_il_importer.io_max.pd.read_excel", _fake_read_excel)
    monkeypatch.setattr(
        "ynab_il_importer.io_max.account_map.apply_account_name_map",
        lambda df, source, account_map_path=None: df,
    )
    monkeypatch.setattr(
        "ynab_il_importer.io_max.fingerprint.apply_fingerprints",
        lambda df, use_fingerprint_map=True: df.assign(
            description_clean_norm=df["description_clean"].astype("string").fillna(""),
            fingerprint=df["description_clean"].astype("string").fillna(""),
        ),
    )

    actual = maxio.read_raw("tests/fixtures/card/max_sections.xlsx", use_fingerprint_map=False)

    assert len(actual) == 2
    assert set(actual["max_sheet"].tolist()) == {"עסקאות במועד החיוב", 'עסקאות חו"ל ומט"ח'}
    assert set(actual["max_txn_type"].tolist()) == {"רגילה", "דחוי חודשיים"}
    assert set(actual["max_report_scope"].tolist()) == {"9922-כרטיס UNIQ"}
    assert set(actual["max_report_period"].tolist()) == {"03/2026"}
    assert set(actual["card_suffix"].tolist()) == {"9922"}
    assert pd.to_numeric(actual["max_original_amount"], errors="coerce").round(2).tolist() == [
        360.83,
        568.00,
    ]


def test_read_card_flips_refund_sign_when_export_is_charge_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = pd.DataFrame(
        [
            {
                "תאריך עסקה": "01/02/2026",
                "שם בית העסק": "MERCHANT A",
                "קטגוריה": "שונות",
                "4 ספרות אחרונות של כרטיס האשראי": "9922",
                "סוג עסקה": "רגילה",
                "סכום חיוב": "100.00",
                "מטבע חיוב": "₪",
                "סכום עסקה מקורי": "100.00",
                "מטבע עסקה מקורי": "₪",
                "תאריך חיוב": "10/02/2026",
                "הערות": "",
                "תיוגים": "",
                "מועדון הנחות": "",
                "מפתח דיסקונט": "",
                "אופן ביצוע ההעסקה": "טלפוני",
                'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
            },
            {
                "תאריך עסקה": "02/02/2026",
                "שם בית העסק": "MERCHANT A",
                "קטגוריה": "שונות",
                "4 ספרות אחרונות של כרטיס האשראי": "9922",
                "סוג עסקה": "רגילה",
                "סכום חיוב": "-25.00",
                "מטבע חיוב": "₪",
                "סכום עסקה מקורי": "-25.00",
                "מטבע עסקה מקורי": "₪",
                "תאריך חיוב": "10/02/2026",
                "הערות": "ביטול עסקה",
                "תיוגים": "",
                "מועדון הנחות": "",
                "מפתח דיסקונט": "",
                "אופן ביצוע ההעסקה": "טלפוני",
                'שער המרה ממטבע מקור/התחשבנות לש"ח': "",
            },
        ],
        dtype="string",
    ).fillna("")
    workbook_df = _mock_card_sheet_df(raw)

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None, nrows=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return {"עסקאות": workbook_df}
        raise AssertionError(
            "Unexpected read_excel invocation: "
            f"path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}, nrows={nrows}"
        )

    monkeypatch.setattr("ynab_il_importer.io_max.pd.read_excel", _fake_read_excel)
    monkeypatch.setattr(
        "ynab_il_importer.io_max.account_map.apply_account_name_map",
        lambda df, source, account_map_path=None: df,
    )
    monkeypatch.setattr(
        "ynab_il_importer.io_max.fingerprint.apply_fingerprints",
        lambda df, use_fingerprint_map=True: df.assign(
            description_clean_norm=df["description_clean"].astype("string").fillna(""),
            fingerprint=df["description_clean"].astype("string").fillna(""),
        ),
    )

    actual = maxio.read_raw("tests/fixtures/card/refund_case.xlsx", use_fingerprint_map=False)

    assert actual["outflow_ils"].tolist() == [100.0, 0.0]
    assert actual["inflow_ils"].tolist() == [0.0, 25.0]
    assert actual["txn_kind"].tolist() == ["expense", "credit"]
