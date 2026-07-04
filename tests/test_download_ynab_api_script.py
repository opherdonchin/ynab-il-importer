from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "download_ynab_api.py"
SPEC = importlib.util.spec_from_file_location("download_ynab_api_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
download_ynab_api = importlib.util.module_from_spec(SPEC)
sys.modules["download_ynab_api_script"] = download_ynab_api
SPEC.loader.exec_module(download_ynab_api)

import ynab_il_importer.context_config as context_config  # noqa: E402
from ynab_il_importer.artifacts.transaction_io import write_transactions_parquet  # noqa: E402


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


def test_main_lists_budgets(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_budgets",
        lambda: [
            {
                "id": "budget-family",
                "name": "Family",
                "last_modified_on": "2026-05-07T12:34:56Z",
            },
            {
                "id": "budget-pilates",
                "name": "Pilates",
                "last_modified_on": "",
            },
        ],
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["download_ynab_api.py", "--list-budgets"],
    )

    download_ynab_api.main()

    out = capsys.readouterr().out
    assert "YNAB budgets available to this token:" in out
    assert "Family\tbudget-family last_modified=2026-05-07T12:34:56Z" in out
    assert "Pilates\tbudget-pilates" in out


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


def test_main_can_infer_download_window_from_normalized_sources(
    monkeypatch, tmp_path
) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("family")
    run_dir = tmp_path / "derived" / "2026_04_01"
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["bank-1", "bank-2"],
                "date": ["2026-03-01", "2026-03-10"],
            }
        ),
        run_dir / "family_leumi_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["card-1", "card-2"],
                "date": ["2026-03-05", "2026-04-05"],
            }
        ),
        run_dir / "family_max_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["previous-card-1"],
                "date": ["2026-01-26"],
                "account_name": ["Liya X7195"],
                "source_account": ["x7195"],
            }
        ),
        tmp_path / "derived" / "previous_max" / "x7195" / "2026_04_max_norm.parquet",
    )
    captured: dict[str, object] = {"fetch_since_dates": []}

    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_context",
        lambda *_args, **_kwargs: context,
    )
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

    def fake_fetch_transactions(plan_id=None, since_date=None):
        captured["fetch_since_dates"].append(since_date)
        return [
            {
                "id": "before-window",
                "account_id": "acc-1",
                "date": "2026-02-20",
                "payee_name": "Early",
                "category_name": "Category",
                "category_id": "cat-1",
                "amount": -1000,
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            },
            {
                "id": "inside-window",
                "account_id": "acc-1",
                "date": "2026-04-10",
                "payee_name": "Inside",
                "category_name": "Category",
                "category_id": "cat-1",
                "amount": -1000,
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            },
            {
                "id": "after-window",
                "account_id": "acc-1",
                "date": "2026-04-20",
                "payee_name": "Late",
                "category_name": "Category",
                "category_id": "cat-1",
                "amount": -1000,
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            },
        ]

    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_transactions",
        fake_fetch_transactions,
    )
    monkeypatch.setattr(
        download_ynab_api,
        "write_transactions_parquet",
        lambda table, path: captured.update(
            {
                "path": path,
                "transaction_ids": table["transaction_id"].to_pylist(),
            }
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
        [
            "download_ynab_api.py",
            "family",
            "2026_04_01",
            "--source-window",
            "--source-window-padding-days",
            "7",
        ],
    )

    download_ynab_api.main()

    assert captured["fetch_since_dates"] == ["2026-01-19"]
    assert captured["transaction_ids"] == ["before-window", "inside-window"]
    assert captured["path"] == run_dir / "family_ynab_api_norm.parquet"


def test_main_source_window_requires_latest_previous_snapshot_normalized(
    monkeypatch, tmp_path
) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("family")
    run_dir = tmp_path / "derived" / "2026_04_01"
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["bank-1"],
                "date": ["2026-03-01"],
            }
        ),
        run_dir / "family_leumi_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["card-1"],
                "date": ["2026-03-05"],
            }
        ),
        run_dir / "family_max_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["previous-card-1"],
                "date": ["2026-01-26"],
                "account_name": ["Liya X7195"],
                "source_account": ["x7195"],
            }
        ),
        tmp_path / "derived" / "previous_max" / "x7195" / "2026_03_max_norm.parquet",
    )
    latest_raw_path = tmp_path / "raw" / "previous_max" / "x7195" / "2026_04.xlsx"
    latest_raw_path.parent.mkdir(parents=True, exist_ok=True)
    latest_raw_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_context",
        lambda *_args, **_kwargs: context,
    )
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
        sys,
        "argv",
        [
            "download_ynab_api.py",
            "family",
            "2026_04_01",
            "--source-window",
        ],
    )

    with pytest.raises(FileNotFoundError, match="2026_04_max_norm\\.parquet"):
        download_ynab_api.main()


def test_main_source_window_refreshes_ynab_category_sources_before_inferring_window(
    monkeypatch, tmp_path
) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("pilates")
    family_context = context_config.load_context("family")
    run_dir = tmp_path / "derived" / "2026_05_16"
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["bank-1"],
                "date": ["2026-04-20"],
            }
        ),
        run_dir / "pilates_leumi_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["card-1"],
                "date": ["2026-05-01"],
            }
        ),
        run_dir / "pilates_leumi_card_html_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "transaction_id": ["stale-category-1"],
                "date": ["2026-04-10"],
                "account_name": ["In Family"],
                "source_account": ["Bank Leumi"],
                "payee_raw": ["Loan Pilates"],
                "memo": ["פרעון הלוואה"],
                "description_raw": ["פרעון הלוואה"],
                "category_raw": ["Pilates"],
            }
        ),
        run_dir / "pilates_family_category_norm.parquet",
    )
    write_transactions_parquet(
        pl.DataFrame(
            {
                "artifact_kind": ["ynab_transaction"],
                "artifact_version": ["transaction_v1"],
                "source_system": ["ynab"],
                "transaction_id": ["family-pilates-1"],
                "ynab_id": ["family-pilates-1"],
                "import_id": ["BANK:V1:family-1"],
                "parent_transaction_id": ["family-pilates-1"],
                "account_id": ["acc-family"],
                "account_name": ["Bank Leumi"],
                "source_account": ["Bank Leumi"],
                "date": ["2025-12-10"],
                "secondary_date": [""],
                "inflow_ils": [0.0],
                "outflow_ils": [1836.8],
                "signed_amount_ils": [-1836.8],
                "balance_ils": [0.0],
                "payee_raw": ["Loan Pilates"],
                "category_id": ["cat-pilates"],
                "category_raw": ["Pilates"],
                "memo": ["פרעון הלוואה"],
                "txn_kind": ["expense"],
                "fingerprint": ["loan pilates"],
                "description_raw": ["פרעון הלוואה"],
                "description_clean": ["Loan Pilates"],
                "description_clean_norm": ["loan pilates"],
                "merchant_raw": ["Loan Pilates"],
                "max_sheet": [""],
                "max_txn_type": [""],
                "max_original_amount": [0.0],
                "max_original_currency": [""],
                "ref": ["family-pilates-1"],
                "matched_transaction_id": [""],
                "cleared": ["reconciled"],
                "approved": [True],
                "is_subtransaction": [False],
                "splits": [None],
            }
        ),
        run_dir / "family_ynab_api_norm.parquet",
    )
    captured: dict[str, object] = {"fetch_since_dates": []}

    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "load_context",
        lambda name, **_kwargs: family_context if str(name).strip().lower() == "family" else context,
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_ynab_dependencies",
        lambda current, **_kwargs: [current],
    )
    monkeypatch.setattr(
        download_ynab_api.context_config,
        "resolve_context_budget_id",
        lambda *_args, **_kwargs: "pilates-budget",
    )
    monkeypatch.setattr(
        download_ynab_api.ynab_api,
        "fetch_accounts",
        lambda plan_id=None: [{"id": "acc-1", "name": "Pilates"}],
    )

    def fake_fetch_transactions(plan_id=None, since_date=None):
        captured["fetch_since_dates"].append(since_date)
        return [
            {
                "id": "inside-window",
                "account_id": "acc-1",
                "date": "2026-04-10",
                "payee_name": "Inside",
                "category_name": "Category",
                "category_id": "cat-1",
                "amount": -1000,
                "memo": "",
                "import_id": "",
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
        download_ynab_api.export,
        "wrote_message",
        lambda *_args, **_kwargs: "",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "download_ynab_api.py",
            "pilates",
            "2026_05_16",
            "--source-window",
        ],
    )

    download_ynab_api.main()

    assert captured["fetch_since_dates"] == ["2025-11-26"]
    refreshed_category = pl.read_parquet(run_dir / "pilates_family_category_norm.parquet")
    assert refreshed_category.select(pl.col("date").min()).item() == "2025-12-10"


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
