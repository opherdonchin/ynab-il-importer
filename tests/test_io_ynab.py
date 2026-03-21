from __future__ import annotations

from io import BytesIO
from zipfile import ZipFile

import ynab_il_importer.io_ynab as io_ynab


def test_io_ynab_reads_register_zip(tmp_path) -> None:
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

    assert io_ynab.is_proper_format(zip_path) is True

    out = io_ynab.read_raw(zip_path)

    assert out.loc[0, "account_name"] == "Credit card 0602"
    assert str(out.loc[0, "date"]) == "2026-03-01"
    assert float(out.loc[0, "outflow_ils"]) == 12.34
    assert out.loc[0, "payee_raw"] == "Merchant"
