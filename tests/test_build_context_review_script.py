from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import polars as pl

import ynab_il_importer.context_config as context_config
from ynab_il_importer.artifacts.transaction_io import write_transactions_parquet

ROOT = Path(__file__).resolve().parents[1]
BUILD_PROPOSED_PATH = ROOT / "scripts" / "build_proposed_transactions.py"
BUILD_PROPOSED_SPEC = importlib.util.spec_from_file_location(
    "build_proposed_transactions",
    BUILD_PROPOSED_PATH,
)
assert BUILD_PROPOSED_SPEC is not None and BUILD_PROPOSED_SPEC.loader is not None
build_proposed_transactions = importlib.util.module_from_spec(BUILD_PROPOSED_SPEC)
sys.modules["build_proposed_transactions"] = build_proposed_transactions
BUILD_PROPOSED_SPEC.loader.exec_module(build_proposed_transactions)

SCRIPT_PATH = ROOT / "scripts" / "build_context_review.py"
SPEC = importlib.util.spec_from_file_location("build_context_review_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
build_context_review = importlib.util.module_from_spec(SPEC)
sys.modules["build_context_review_script"] = build_context_review
SPEC.loader.exec_module(build_context_review)


def _write_transactions(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_transactions_parquet(pl.DataFrame(rows), path)


def test_resolve_review_source_paths_carries_forward_only_unreconciled_previous_statements(
    tmp_path: Path,
) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("family")
    run_paths = context_config.resolve_run_paths(defaults, run_tag="2026_04_14")
    run_paths.derived_dir.mkdir(parents=True, exist_ok=True)

    _write_transactions(
        run_paths.derived_dir / "family_leumi_norm.parquet",
        [
            {
                "artifact_kind": "normalized_source_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "bank",
                "transaction_id": "bank-active-1",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-04-14",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -10.0,
                "description_raw": "Bank row",
                "fingerprint": "bank row",
            }
        ],
    )
    _write_transactions(
        run_paths.derived_dir / "family_max_norm.parquet",
        [
            {
                "artifact_kind": "normalized_source_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "card",
                "transaction_id": "card-active-1",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-04-14",
                "outflow_ils": 11.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -11.0,
                "description_raw": "Active row",
                "fingerprint": "active row",
            }
        ],
    )
    _write_transactions(
        run_paths.derived_dir / "family_ynab_api_norm.parquet",
        [
            {
                "artifact_kind": "ynab_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "ynab",
                "transaction_id": "ynab-liya-2026-03",
                "ynab_id": "ynab-liya-2026-03",
                "account_id": "acct-liya",
                "account_name": "Liya X7195",
                "date": "2026-03-05",
                "outflow_ils": 50.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -50.0,
                "memo": "March charge",
                "payee_raw": "March charge",
                "import_id": "",
                "cleared": "reconciled",
                "approved": True,
            },
            {
                "artifact_kind": "ynab_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "ynab",
                "transaction_id": "ynab-opher-2026-04",
                "ynab_id": "ynab-opher-2026-04",
                "account_id": "acct-opher",
                "account_name": "Opher X5898",
                "date": "2026-04-03",
                "outflow_ils": 70.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -70.0,
                "memo": "Opher reconciled charge",
                "payee_raw": "Opher reconciled charge",
                "import_id": "",
                "cleared": "reconciled",
                "approved": True,
            },
        ],
    )
    _write_transactions(
        defaults.derived_root / "previous_max" / "x7195" / "2026_03_max_norm.parquet",
        [
            {
                "artifact_kind": "normalized_source_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "card",
                "transaction_id": "card-liya-2026-03",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-03-05",
                "outflow_ils": 50.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -50.0,
                "description_raw": "March charge",
                "fingerprint": "march charge",
            }
        ],
    )
    _write_transactions(
        defaults.derived_root / "previous_max" / "x7195" / "2026_04_max_norm.parquet",
        [
            {
                "artifact_kind": "normalized_source_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "card",
                "transaction_id": "card-liya-2026-04",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-04-03",
                "outflow_ils": 60.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -60.0,
                "description_raw": "April missing charge",
                "fingerprint": "april missing charge",
            }
        ],
    )
    _write_transactions(
        defaults.derived_root / "previous_max" / "x5898" / "2026_04_max_norm.parquet",
        [
            {
                "artifact_kind": "normalized_source_transaction",
                "artifact_version": "transaction_v1",
                "source_system": "card",
                "transaction_id": "card-opher-2026-04",
                "account_name": "Opher X5898",
                "source_account": "x5898",
                "date": "2026-04-03",
                "outflow_ils": 70.0,
                "inflow_ils": 0.0,
                "signed_amount_ils": -70.0,
                "description_raw": "Opher reconciled charge",
                "fingerprint": "opher reconciled charge",
            }
        ],
    )

    source_paths = build_context_review._resolve_review_source_paths(
        context,
        defaults,
        run_paths,
        run_tag="2026_04_14",
    )

    assert source_paths == [
        run_paths.derived_dir / "family_leumi_norm.parquet",
        run_paths.derived_dir / "family_max_norm.parquet",
        (defaults.derived_root / "previous_max" / "x7195" / "2026_04_max_norm.parquet").resolve(),
    ]
