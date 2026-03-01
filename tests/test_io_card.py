import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.io_card import read_card


def _load_fixture_rows(input_csv: Path) -> pd.DataFrame:
    raw = pd.read_csv(input_csv, dtype="string").fillna("")
    return raw


def _mock_card_workbook_df(raw: pd.DataFrame) -> pd.DataFrame:
    width = max(len(raw.columns), 1)
    preface_rows = [["" for _ in range(width)] for _ in range(2)]
    header_row = list(raw.columns)
    data_rows = raw.values.tolist()
    return pd.DataFrame(preface_rows + [header_row] + data_rows)


def test_read_card_emits_normalized_schema_from_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    input_csv = ROOT / "tests" / "fixtures" / "card" / "max_sample_input.csv"
    expected_csv = ROOT / "tests" / "fixtures" / "expected" / "card_max_sample_normalized.csv"
    raw = _load_fixture_rows(input_csv)
    workbook_df = _mock_card_workbook_df(raw)

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return {"עסקאות": workbook_df}
        if sheet_name == "עסקאות" and header == 2:
            return raw
        raise AssertionError(
            f"Unexpected read_excel invocation: path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}"
        )

    monkeypatch.setattr("ynab_il_importer.io_card.pd.read_excel", _fake_read_excel)

    with pytest.warns(UserWarning):
        actual = read_card("tests/fixtures/card/max_sample_input.xlsx")
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
    workbook_df = _mock_card_workbook_df(raw)

    def _fake_read_excel(path: Path, sheet_name=None, header=None, dtype=None):  # noqa: ANN001
        if sheet_name is None and header is None:
            return {"עסקאות": workbook_df}
        if sheet_name == "עסקאות" and header == 2:
            return raw
        raise AssertionError(
            f"Unexpected read_excel invocation: path={path}, sheet_name={sheet_name}, header={header}, dtype={dtype}"
        )

    monkeypatch.setattr("ynab_il_importer.io_card.pd.read_excel", _fake_read_excel)
    actual = read_card("tests/fixtures/card/noise_case.xlsx")

    assert len(actual) == 1
    assert actual.iloc[0]["currency"] == "ILS"
