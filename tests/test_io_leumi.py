import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.io_leumi as leumi
import ynab_il_importer.io_leumi_xls as leumi_xls


def _stub_apply_fingerprints(df: pd.DataFrame, use_fingerprint_map: bool = True) -> pd.DataFrame:
    _ = use_fingerprint_map
    out = df.copy()
    text = out.get("description_clean", "").astype("string").fillna("")
    out["description_clean_norm"] = text
    out["fingerprint"] = text
    return out


def test_read_bankin_dat_forwards_running_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "ynab_il_importer.io_leumi.account_map.apply_account_name_map",
        lambda df, source, account_map_path=None: df,
    )
    monkeypatch.setattr(
        "ynab_il_importer.io_leumi.fingerprint.apply_fingerprints",
        _stub_apply_fingerprints,
    )

    with tempfile.NamedTemporaryFile(dir=ROOT, suffix=".dat", delete=False) as tmp:
        raw_path = Path(tmp.name)
        tmp.write(
            "\n".join(
                [
                    "0001,021125,ACME STORE,-000000010.50,+000000100.00,0039,67833011333622",
                    "0002,031125,PAYROLL,+000001000.00,+000001100.25,0039,67833011333622",
                ]
            ).encode("cp862")
        )

    try:
        actual = leumi.read_raw(raw_path, use_fingerprint_map=False)
    finally:
        raw_path.unlink(missing_ok=True)

    assert "balance_ils" in actual.columns
    assert pd.to_numeric(actual["balance_ils"], errors="coerce").round(2).tolist() == [
        100.00,
        1100.25,
    ]


def test_read_bank_xls_forwards_balance_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "ynab_il_importer.io_leumi_xls.account_map.apply_account_name_map",
        lambda df, source, account_map_path=None: df,
    )
    monkeypatch.setattr(
        "ynab_il_importer.io_leumi_xls.fingerprint.apply_fingerprints",
        _stub_apply_fingerprints,
    )

    raw = pd.DataFrame(
        [
            {
                "תאריך": "02/11/2025",
                "תאריך ערך": "02/11/2025",
                "תיאור": "שכר",
                "אסמכתא": "0001",
                "בחובה": "",
                "בזכות": "1,000.00",
                "יתרה": "10,550.75",
                "מספר חשבון": "67833011333622",
            },
            {
                "תאריך": "03/11/2025",
                "תאריך ערך": "03/11/2025",
                "תיאור": "קניות",
                "אסמכתא": "0002",
                "בחובה": "25.50",
                "בזכות": "",
                "יתרה": "10,525.25",
                "מספר חשבון": "67833011333622",
            },
        ],
        dtype="string",
    ).fillna("")

    monkeypatch.setattr("ynab_il_importer.io_leumi_xls._read_bank_table", lambda _: raw)

    actual = leumi_xls.read_raw("tests/fixtures/bank/leumi_sample.xls", use_fingerprint_map=False)

    assert "balance_ils" in actual.columns
    assert pd.to_numeric(actual["balance_ils"], errors="coerce").round(2).tolist() == [
        10550.75,
        10525.25,
    ]
