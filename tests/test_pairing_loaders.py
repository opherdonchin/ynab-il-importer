from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import polars as pl

from ynab_il_importer.artifacts.transaction_io import write_flat_transaction_artifacts


ROOT = Path(__file__).resolve().parents[1]

BOOTSTRAP_PAIRS_PATH = ROOT / "scripts" / "bootstrap_pairs.py"
BOOTSTRAP_PAIRS_SPEC = importlib.util.spec_from_file_location(
    "bootstrap_pairs_script",
    BOOTSTRAP_PAIRS_PATH,
)
assert BOOTSTRAP_PAIRS_SPEC is not None and BOOTSTRAP_PAIRS_SPEC.loader is not None
bootstrap_pairs = importlib.util.module_from_spec(BOOTSTRAP_PAIRS_SPEC)
sys.modules["bootstrap_pairs_script"] = bootstrap_pairs
BOOTSTRAP_PAIRS_SPEC.loader.exec_module(bootstrap_pairs)


def test_bootstrap_pairs_prefers_sidecar_parquet(monkeypatch, tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    ynab_path = tmp_path / "ynab.csv"

    write_flat_transaction_artifacts(
        pl.DataFrame(
            {
                "source": ["bank"],
                "account_name": ["Family Leumi"],
                "source_account": ["Family Leumi"],
                "date": ["2026-03-01"],
                "txn_kind": ["expense"],
                "merchant_raw": ["Mega Pet"],
                "description_clean": ["Mega Pet"],
                "description_raw": ["Mega Pet Pet Food"],
                "description_clean_norm": ["mega pet"],
                "fingerprint": ["source-parquet"],
                "outflow_ils": [90.0],
                "inflow_ils": [0.0],
                "bank_txn_id": ["BANK:1"],
            }
        ),
        source_path,
        artifact_kind="normalized_source_transaction",
        source_system="bank",
    )
    write_flat_transaction_artifacts(
        pl.DataFrame(
            {
                "source": ["ynab"],
                "ynab_id": ["txn-1"],
                "account_name": ["Family Leumi"],
                "date": ["2026-03-01"],
                "payee_raw": ["Mega Pet"],
                "category_raw": ["Pets"],
                "fingerprint": ["target-parquet"],
                "outflow_ils": [90.0],
                "inflow_ils": [0.0],
            }
        ),
        ynab_path,
        artifact_kind="ynab_transaction",
        source_system="ynab",
    )
    source_path.write_text(
        "source,account_name,fingerprint\nbank,Family Leumi,source-csv\n",
        encoding="utf-8",
    )
    ynab_path.write_text(
        "source,account_name,fingerprint\nynab,Family Leumi,target-csv\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        bootstrap_pairs.pairing,
        "match_pairs",
        lambda source_df, ynab_df: (
            captured.update(
                {
                    "source_fingerprint": source_df[0, "fingerprint"],
                    "ynab_fingerprint": ynab_df[0, "fingerprint"],
                }
            )
            or pl.DataFrame()
        ),
    )
    monkeypatch.setattr(
        bootstrap_pairs.export, "write_dataframe", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        bootstrap_pairs.export, "wrote_message", lambda *_args, **_kwargs: ""
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bootstrap_pairs.py",
            "--source",
            str(source_path),
            "--ynab",
            str(ynab_path),
            "--out",
            str(tmp_path / "pairs.csv"),
        ],
    )

    bootstrap_pairs.main()

    assert captured["source_fingerprint"] == "source-parquet"
    assert captured["ynab_fingerprint"] == "target-parquet"
