from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "normalize_file.py"
SPEC = importlib.util.spec_from_file_location("normalize_file_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
normalize_file = importlib.util.module_from_spec(SPEC)
sys.modules["normalize_file_script"] = normalize_file
SPEC.loader.exec_module(normalize_file)


def test_normalize_one_writes_canonical_parquet(monkeypatch, tmp_path) -> None:
    in_path = tmp_path / "input.txt"
    in_path.write_text("placeholder", encoding="utf-8")
    out_path = tmp_path / "normalized.csv"
    captured: dict[str, object] = {}

    module = type(
        "FakeModule",
        (),
        {
            "is_proper_format": staticmethod(lambda path: path == in_path),
            "read_raw": staticmethod(
                lambda *_args, **_kwargs: pd.DataFrame(
                    [
                        {
                            "source": "bank",
                            "account_name": "Family Leumi",
                            "source_account": "Family Leumi",
                            "date": "2026-03-01",
                            "txn_kind": "expense",
                            "merchant_raw": "Mega Pet",
                            "description_clean": "Mega Pet",
                            "description_raw": "Mega Pet Pet Food",
                            "description_clean_norm": "mega pet",
                            "fingerprint": "mega pet",
                            "outflow_ils": 90.0,
                            "inflow_ils": 0.0,
                            "bank_txn_id": "BANK:1",
                        }
                    ]
                )
            ),
        },
    )

    monkeypatch.setitem(normalize_file.FORMAT_MODULES, "fake", module)
    monkeypatch.setattr(
        normalize_file,
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
    monkeypatch.setattr(normalize_file.export, "wrote_message", lambda *_args, **_kwargs: "")

    normalize_file._normalize_one(
        in_path,
        "fake",
        out_path,
        True,
        Path("mappings/account_name_map.csv"),
        Path("mappings/fingerprint_map.csv"),
        Path("outputs/fingerprint_log.csv"),
    )

    assert captured["csv_path"] == out_path
    assert captured["rows"] == 1
    assert captured["artifact_kind"] == "normalized_source_transaction"
    assert captured["source_system"] == "bank"
