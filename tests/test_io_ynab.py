from __future__ import annotations

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

import ynab_il_importer.io_ynab as io_ynab


def test_io_ynab_reads_register_zip_and_applies_fingerprints(monkeypatch, tmp_path) -> None:
    zip_path = tmp_path / "pilates_export.zip"
    csv_text = "\n".join(
        [
            "Account,Date,Payee,Category,Memo,Outflow,Inflow,Cleared",
            "Credit card 0602,01/03/2026,Merchant,Groceries,memo,12.34,0.00,Cleared",
        ]
    )
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("Pilates - Register.csv", csv_text.encode("utf-8-sig"))
    zip_path.write_bytes(buffer.getvalue())

    captured: dict[str, object] = {}

    def fake_apply_fingerprints(
        df: pd.DataFrame,
        *,
        use_fingerprint_map: bool,
        fingerprint_map_path: Path,
        log_path: Path,
    ) -> pd.DataFrame:
        captured["use_fingerprint_map"] = use_fingerprint_map
        captured["fingerprint_map_path"] = fingerprint_map_path
        captured["log_path"] = log_path
        out = df.copy()
        out["description_clean_norm"] = "merchant"
        out["fingerprint"] = "merchant"
        return out

    monkeypatch.setattr(io_ynab.fingerprint, "apply_fingerprints", fake_apply_fingerprints)

    assert io_ynab.is_proper_format(zip_path) is True

    out = io_ynab.read_raw(
        zip_path,
        use_fingerprint_map=False,
        fingerprint_map_path=tmp_path / "fingerprint_map.csv",
        fingerprint_log_path=tmp_path / "fingerprint_log.csv",
    )

    assert out.loc[0, "account_name"] == "Credit card 0602"
    assert out.loc[0, "source_account"] == "Credit card 0602"
    assert str(out.loc[0, "date"]) == "2026-03-01"
    assert out.loc[0, "merchant_raw"] == "Merchant"
    assert out.loc[0, "description_clean"] == "Merchant"
    assert out.loc[0, "description_raw"] == "memo"
    assert out.loc[0, "description_clean_norm"] == "merchant"
    assert out.loc[0, "fingerprint"] == "merchant"
    assert float(out.loc[0, "outflow_ils"]) == 12.34
    assert out.loc[0, "payee_raw"] == "Merchant"
    assert out.loc[0, "cleared"] == "Cleared"
    assert captured == {
        "use_fingerprint_map": False,
        "fingerprint_map_path": tmp_path / "fingerprint_map.csv",
        "log_path": tmp_path / "fingerprint_log.csv",
    }
