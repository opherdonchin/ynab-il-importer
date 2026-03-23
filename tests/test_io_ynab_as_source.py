from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

import ynab_il_importer.workflow_profiles as workflow_profiles


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "io_ynab_as_source.py"
SPEC = importlib.util.spec_from_file_location("io_ynab_as_source_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
io_ynab_as_source = importlib.util.module_from_spec(SPEC)
sys.modules["io_ynab_as_source_script"] = io_ynab_as_source
SPEC.loader.exec_module(io_ynab_as_source)


def test_default_out_path_uses_profile_directory() -> None:
    actual = io_ynab_as_source._default_out_path(
        "pilates",
        "Pilates",
        "2025-11-01",
        "2025-11-05",
    )

    assert actual == Path("data/derived/pilates/ynab_category_pilates-20251101-20251105.csv")


def test_main_resolves_profile_budget_and_profile_paths(
    monkeypatch,
) -> None:
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
        io_ynab_as_source.workflow_profiles,
        "resolve_profile",
        lambda *_args, **_kwargs: profile,
    )
    monkeypatch.setattr(
        io_ynab_as_source.workflow_profiles,
        "resolve_budget_id",
        lambda **kwargs: kwargs.get("budget_id") or "family-budget",
    )
    def fake_fetch_categories(plan_id=None):
        captured["categories_plan_id"] = plan_id
        return [
            {
                "name": "Business",
                "categories": [{"id": "cat-pilates", "name": "Pilates", "deleted": False}],
            }
        ]

    def fake_fetch_transactions(plan_id=None, since_date=None):
        captured["transactions_call"] = (plan_id, since_date)
        return [
            {
                "id": "txn-1",
                "category_id": "cat-pilates",
                "account_id": "acc-1",
                "date": "2025-11-01",
                "payee_name": "Client Payment",
                "memo": "November",
                "amount": 100_000,
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            }
        ]

    def fake_fetch_accounts(plan_id=None):
        captured["accounts_plan_id"] = plan_id
        return [{"id": "acc-1", "name": "Family Leumi"}]

    monkeypatch.setattr(io_ynab_as_source.ynab_api, "fetch_categories", fake_fetch_categories)
    monkeypatch.setattr(io_ynab_as_source.ynab_api, "fetch_transactions", fake_fetch_transactions)
    monkeypatch.setattr(io_ynab_as_source.ynab_api, "fetch_accounts", fake_fetch_accounts)

    def fake_apply_fingerprints(
        df: pd.DataFrame,
        *,
        use_fingerprint_map: bool,
        fingerprint_map_path: Path,
        log_path: Path,
    ) -> pd.DataFrame:
        captured["fingerprint_call"] = {
            "use_fingerprint_map": use_fingerprint_map,
            "fingerprint_map_path": fingerprint_map_path,
            "log_path": log_path,
        }
        out = df.copy()
        out["description_clean_norm"] = "client payment"
        out["fingerprint"] = "client payment"
        return out

    monkeypatch.setattr(
        io_ynab_as_source.fingerprint_mod,
        "apply_fingerprints",
        fake_apply_fingerprints,
    )

    def fake_write_dataframe(df: pd.DataFrame, path: Path) -> None:
        captured["written_path"] = path
        captured["written_rows"] = len(df)
        captured["written_columns"] = df.columns.tolist()

    monkeypatch.setattr(io_ynab_as_source.export, "write_dataframe", fake_write_dataframe)
    monkeypatch.setattr(io_ynab_as_source.export, "wrote_message", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(
        sys,
        "argv",
        ["io_ynab_as_source.py", "--profile", "family", "--category", "Pilates"],
    )

    io_ynab_as_source.main()

    assert captured["categories_plan_id"] == "family-budget"
    assert captured["transactions_call"] == ("family-budget", None)
    assert captured["accounts_plan_id"] == "family-budget"
    assert captured["fingerprint_call"] == {
        "use_fingerprint_map": True,
        "fingerprint_map_path": Path("mappings/family/fingerprint_map.csv"),
        "log_path": Path("outputs/family/fingerprint_log.csv"),
    }
    assert captured["written_path"] == Path("data/derived/family/ynab_category_business_pilates.csv")
    assert captured["written_rows"] == 1
    assert "source_account" in captured["written_columns"]


def test_main_explicit_budget_id_overrides_profile_budget(monkeypatch) -> None:
    profile = workflow_profiles.WorkflowProfile(
        name="family",
        account_map_path=Path("mappings/account_name_map.csv"),
        fingerprint_map_path=Path("mappings/family/fingerprint_map.csv"),
        payee_map_path=Path("mappings/payee_map.csv"),
        categories_path=Path("outputs/ynab_categories.csv"),
        budget_id="family-budget",
    )
    seen_plan_ids: list[str | None] = []

    monkeypatch.setattr(
        io_ynab_as_source.workflow_profiles,
        "resolve_profile",
        lambda *_args, **_kwargs: profile,
    )
    monkeypatch.setattr(
        io_ynab_as_source.workflow_profiles,
        "resolve_budget_id",
        lambda **kwargs: kwargs.get("budget_id") or "family-budget",
    )
    monkeypatch.setattr(
        io_ynab_as_source.ynab_api,
        "fetch_categories",
        lambda plan_id=None: seen_plan_ids.append(plan_id) or [],
    )
    monkeypatch.setattr(sys, "argv", ["io_ynab_as_source.py", "--profile", "family", "--budget-id", "override-budget", "--category", "Pilates"])

    try:
        io_ynab_as_source.main()
    except ValueError:
        pass

    assert seen_plan_ids == ["override-budget"]
