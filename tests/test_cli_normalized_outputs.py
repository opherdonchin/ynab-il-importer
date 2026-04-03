from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest


def test_parse_leumi_command_writes_canonical_parquet(monkeypatch, tmp_path) -> None:
    cli_testing = pytest.importorskip("typer.testing")
    cli_runner = cli_testing.CliRunner()
    import ynab_il_importer.cli as cli

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        cli.leumi,
        "read_raw",
        lambda path: pd.DataFrame(
            [
                {
                    "source": "bank",
                    "account_name": "Family Leumi",
                    "source_account": "Family Leumi",
                    "date": "2026-03-01",
                    "txn_kind": "expense",
                    "merchant_raw": "Clalit",
                    "description_clean": "Clalit",
                    "description_raw": "Clalit HMO",
                    "description_clean_norm": "clalit",
                    "fingerprint": "clalit",
                    "outflow_ils": 54.0,
                    "inflow_ils": 0.0,
                    "bank_txn_id": "BANK:1",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        cli.leumi,
        "read_canonical",
        lambda path: pa.table(
            {
                "artifact_kind": ["normalized_source_transaction"],
                "artifact_version": ["transaction_v1"],
                "source_system": ["bank"],
                "transaction_id": ["BANK:1"],
                "parent_transaction_id": ["BANK:1"],
                "account_name": ["Family Leumi"],
                "source_account": ["Family Leumi"],
                "date": ["2026-03-01"],
                "inflow_ils": [0.0],
                "outflow_ils": [54.0],
                "signed_amount_ils": [-54.0],
                "payee_raw": ["Clalit"],
                "memo": ["Clalit HMO"],
                "merchant_raw": ["Clalit"],
                "description_clean": ["Clalit"],
                "description_raw": ["Clalit HMO"],
                "description_clean_norm": ["clalit"],
                "fingerprint": ["clalit"],
                "approved": [False],
                "is_subtransaction": [False],
                "splits": [None],
            }
        ),
    )
    monkeypatch.setattr(
        cli,
        "write_canonical_transaction_artifacts",
        lambda table, path, **kwargs: (
            captured.update(
                {
                    "path": path,
                    "csv_projection_rows": len(kwargs["csv_projection"]),
                    "transaction_id": table["transaction_id"].to_pylist()[0],
                }
            )
            or (path, path.with_suffix(".parquet"))
        ),
    )

    out_path = tmp_path / "leumi_norm.csv"
    in_path = tmp_path / "input.dat"
    in_path.write_text("placeholder", encoding="utf-8")
    result = cli_runner.invoke(
        cli.app, ["parse-leumi", "--in", str(in_path), "--out", str(out_path)]
    )

    assert result.exit_code == 0
    assert captured["path"] == out_path
    assert captured["csv_projection_rows"] == 1
    assert captured["transaction_id"] == "BANK:1"
