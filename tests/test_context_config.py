from pathlib import Path

import pytest

import ynab_il_importer.context_config as context_config


def test_load_family_context_resolves_map_paths() -> None:
    loaded = context_config.load_context("family")

    assert loaded.name == "family"
    assert loaded.budget_id_env == "YNAB_FAMILY_BUDGET_ID"
    assert loaded.account_map_path.name == "account_name_map.csv"
    assert loaded.fingerprint_map_path.name == "fingerprint_map.csv"
    assert loaded.payee_map_path.name == "payee_map.csv"
    assert len(loaded.config.sources) == 2


def test_resolve_context_sources_supports_exact_and_regex(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "Bankin family.dat").write_text("bank", encoding="utf-8")
    (raw_dir / "transaction-details_export_1775044561886.xlsx").write_text(
        "card",
        encoding="utf-8",
    )

    loaded = context_config.load_context("family")
    sources = context_config.resolve_context_sources(loaded, raw_dir)

    assert [source.id for source in sources] == ["family_bank", "family_card"]
    assert [source.raw_path.name for source in sources] == [
        "Bankin family.dat",
        "transaction-details_export_1775044561886.xlsx",
    ]


def test_resolve_context_sources_requires_unique_regex_match(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "Bankin family.dat").write_text("bank", encoding="utf-8")
    (raw_dir / "transaction-details_export_1.xlsx").write_text("card1", encoding="utf-8")
    (raw_dir / "transaction-details_export_2.xlsx").write_text("card2", encoding="utf-8")

    loaded = context_config.load_context("family")

    with pytest.raises(ValueError, match="matched 2 files"):
        context_config.resolve_context_sources(loaded, raw_dir)
