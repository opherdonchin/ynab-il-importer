from __future__ import annotations

import pandas as pd
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
        cli,
        "write_flat_transaction_artifacts",
        lambda df, path, **kwargs: (
            captured.update(
                {
                    "csv_path": path,
                    "rows": len(df),
                    "artifact_kind": kwargs["artifact_kind"],
                    "source_system": kwargs["source_system"],
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
    assert captured["csv_path"] == out_path
    assert captured["rows"] == 1
    assert captured["artifact_kind"] == "normalized_source_transaction"
    assert captured["source_system"] == "bank"
