from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa


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
    out_path = tmp_path / "normalized.parquet"
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
            "read_canonical": staticmethod(
                lambda *_args, **_kwargs: pa.table(
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
                        "outflow_ils": [90.0],
                        "signed_amount_ils": [-90.0],
                        "payee_raw": ["Mega Pet"],
                        "memo": ["Mega Pet Pet Food"],
                        "merchant_raw": ["Mega Pet"],
                        "description_clean": ["Mega Pet"],
                        "description_raw": ["Mega Pet Pet Food"],
                        "description_clean_norm": ["mega pet"],
                        "fingerprint": ["mega pet"],
                        "approved": [False],
                        "is_subtransaction": [False],
                        "splits": [None],
                    }
                )
            ),
        },
    )

    monkeypatch.setitem(normalize_file.normalize_runner.FORMAT_MODULES, "fake", module)
    monkeypatch.setattr(
        normalize_file.normalize_runner,
        "write_canonical_transaction_artifacts",
        lambda table, path, **_kwargs: (
            captured.update(
                {
                    "path": path,
                    "transaction_id": table["transaction_id"].to_pylist()[0],
                }
            )
            or (None, path)
        ),
    )
    monkeypatch.setattr(
        normalize_file.normalize_runner.export, "wrote_message", lambda *_args, **_kwargs: ""
    )

    normalize_file.normalize_runner.normalize_one(
        in_path,
        "fake",
        out_path,
        use_fingerprint_map=True,
        account_map_path=Path("mappings/account_name_map.csv"),
        fingerprint_map_path=Path("mappings/fingerprint_map.csv"),
        fingerprint_log_path=Path("outputs/fingerprint_log.csv"),
    )

    assert captured["path"] == out_path
    assert captured["transaction_id"] == "BANK:1"
