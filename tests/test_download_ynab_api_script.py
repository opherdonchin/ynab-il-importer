from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa

import ynab_il_importer.workflow_profiles as workflow_profiles


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "download_ynab_api.py"
SPEC = importlib.util.spec_from_file_location("download_ynab_api_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
download_ynab_api = importlib.util.module_from_spec(SPEC)
sys.modules["download_ynab_api_script"] = download_ynab_api
SPEC.loader.exec_module(download_ynab_api)


def test_main_writes_csv_and_canonical_parquet(monkeypatch) -> None:
    profile = workflow_profiles.WorkflowProfile(
        name="family",
        account_map_path=Path("mappings/account_name_map.csv"),
        fingerprint_map_path=Path("mappings/family/fingerprint_map.csv"),
        payee_map_path=Path("mappings/payee_map.csv"),
        categories_path=Path("outputs/ynab_categories.csv"),
        budget_id="family-budget",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        download_ynab_api.workflow_profiles,
        "resolve_profile",
        lambda *_args, **_kwargs: profile,
    )
    monkeypatch.setattr(
        download_ynab_api.workflow_profiles,
        "resolve_budget_id",
        lambda **kwargs: kwargs.get("budget_id") or "family-budget",
    )
    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_accounts",
        lambda plan_id=None: [{"id": "acc-1", "name": "Family Leumi"}],
    )
    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_transactions",
        lambda plan_id=None, since_date=None: [
            {
                "id": "txn-1",
                "account_id": "acc-1",
                "date": "2026-03-01",
                "payee_name": "Merchant",
                "category_name": "Groceries",
                "category_id": "cat-groceries",
                "amount": -12340,
                "memo": "memo text",
                "import_id": "YNAB:-12340:2026-03-01:1",
                "matched_transaction_id": "match-1",
                "cleared": "cleared",
                "approved": True,
            }
        ],
    )
    monkeypatch.setattr(
        download_ynab_api.export,
        "write_dataframe",
        lambda df, path: captured.update(
            {
                "csv_path": path,
                "csv_rows": len(df),
                "csv_columns": df.columns.tolist(),
            }
        ),
    )
    monkeypatch.setattr(
        download_ynab_api,
        "write_transactions_parquet",
        lambda table, path: captured.update(
            {
                "parquet_path": path,
                "parquet_rows": table.num_rows,
            }
        ),
    )
    monkeypatch.setattr(download_ynab_api.export, "wrote_message", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        sys,
        "argv",
        ["download_ynab_api.py", "--profile", "family"],
    )

    download_ynab_api.main()

    assert captured["csv_path"] == Path("data/derived/family/ynab_api_norm.csv")
    assert captured["csv_rows"] == 1
    assert captured["parquet_path"] == Path("data/derived/family/ynab_api_norm.parquet")
    assert captured["parquet_rows"] == 1
    assert "fingerprint" in captured["csv_columns"]


def test_filter_canonical_by_date_filters_string_dates() -> None:
    table = pa.table(
        {
            "artifact_kind": ["ynab_transaction", "ynab_transaction"],
            "artifact_version": ["transaction_v1", "transaction_v1"],
            "source_system": ["ynab", "ynab"],
            "transaction_id": ["txn-1", "txn-2"],
            "ynab_id": ["txn-1", "txn-2"],
            "import_id": ["", ""],
            "parent_transaction_id": ["txn-1", "txn-2"],
            "account_id": ["acc-1", "acc-1"],
            "account_name": ["Family", "Family"],
            "source_account": ["Family", "Family"],
            "date": ["2026-03-01", "2026-03-03"],
            "secondary_date": ["", ""],
            "inflow_ils": [0.0, 0.0],
            "outflow_ils": [10.0, 20.0],
            "signed_amount_ils": [-10.0, -20.0],
            "payee_raw": ["A", "B"],
            "category_id": ["cat-1", "cat-1"],
            "category_raw": ["Groceries", "Groceries"],
            "memo": ["", ""],
            "txn_kind": ["expense", "expense"],
            "fingerprint": ["a", "b"],
            "description_raw": ["A", "B"],
            "description_clean": ["A", "B"],
            "description_clean_norm": ["a", "b"],
            "merchant_raw": ["A", "B"],
            "ref": ["", ""],
            "matched_transaction_id": ["", ""],
            "cleared": ["cleared", "cleared"],
            "approved": [True, True],
            "is_subtransaction": [False, False],
            "splits": [None, None],
        }
    )

    filtered = download_ynab_api._filter_canonical_by_date(
        table,
        "2026-03-02",
        "2026-03-31",
    )

    assert filtered["transaction_id"].to_pylist() == ["txn-2"]
