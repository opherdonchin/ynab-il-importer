from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "download_ynab_api.py"
SPEC = importlib.util.spec_from_file_location("download_ynab_api_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
download_ynab_api = importlib.util.module_from_spec(SPEC)
sys.modules["download_ynab_api_script"] = download_ynab_api
SPEC.loader.exec_module(download_ynab_api)

import ynab_il_importer.context_config as context_config  # noqa: E402


def test_main_writes_context_parquet(monkeypatch, tmp_path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("family")
    captured: dict[str, object] = {}

    monkeypatch.setattr(download_ynab_api.context_config, "load_defaults", lambda *_args, **_kwargs: defaults)
    monkeypatch.setattr(download_ynab_api.context_config, "load_context", lambda *_args, **_kwargs: context)
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_ynab_dependencies",
        lambda current, **_kwargs: [current],
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_budget_id",
        lambda *_args, **_kwargs: "family-budget",
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
        download_ynab_api,
        "write_transactions_parquet",
        lambda table, path: captured.update(
            {
                "path": path,
                "rows": table.num_rows,
                "transaction_id": table["transaction_id"].to_pylist()[0],
            }
        ),
    )
    monkeypatch.setattr(download_ynab_api.export, "wrote_message", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        sys,
        "argv",
        ["download_ynab_api.py", "family", "2026_04_01"],
    )

    download_ynab_api.main()

    assert captured["path"] == tmp_path / "derived" / "2026_04_01" / "family_ynab_api_norm.parquet"
    assert captured["rows"] == 1
    assert captured["transaction_id"] == "txn-1"


def test_main_downloads_upstream_ynab_dependencies(monkeypatch, tmp_path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    family = context_config.load_context("family")
    pilates = context_config.load_context("pilates")
    writes: list[tuple[str, Path]] = []
    fetch_calls: list[str] = []

    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_context",
        lambda *_args, **_kwargs: pilates,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_ynab_dependencies",
        lambda current, **_kwargs: [family, current],
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_budget_id",
        lambda current, **_kwargs: f"{current.name}-budget",
    )
    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_accounts",
        lambda plan_id=None: [{"id": f"{plan_id}-acc", "name": plan_id}],
    )

    def fake_fetch_transactions(plan_id=None, since_date=None):
        fetch_calls.append(str(plan_id))
        return [
            {
                "id": f"{plan_id}-txn",
                "account_id": f"{plan_id}-acc",
                "date": "2026-04-01",
                "payee_name": "Merchant",
                "category_name": "Category",
                "category_id": "cat-1",
                "amount": -1000,
                "memo": "",
                "import_id": f"{plan_id}-import",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            }
        ]

    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_transactions",
        fake_fetch_transactions,
    )
    monkeypatch.setattr(
        download_ynab_api,
        "write_transactions_parquet",
        lambda table, path: writes.append(
            (table["transaction_id"].to_pylist()[0], path)
        ),
    )
    monkeypatch.setattr(
        download_ynab_api.export,
        "wrote_message",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["download_ynab_api.py", "pilates", "2026_04_14"],
    )

    download_ynab_api.main()

    assert fetch_calls == ["family-budget", "pilates-budget"]
    assert writes == [
        (
            "family-budget-txn",
            tmp_path / "derived" / "2026_04_14" / "family_ynab_api_norm.parquet",
        ),
        (
            "pilates-budget-txn",
            tmp_path / "derived" / "2026_04_14" / "pilates_ynab_api_norm.parquet",
        ),
    ]


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
