from __future__ import annotations

from pathlib import Path

import ynab_il_importer.workflow_profiles as workflow_profiles


def test_resolve_family_profile_uses_legacy_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "ynab.local.toml"
    config_path.write_text('budget_id = "family-budget"\n', encoding="utf-8")

    profile = workflow_profiles.resolve_profile("family", config_path=config_path)

    assert profile.name == "family"
    assert profile.budget_id == "family-budget"
    assert profile.account_map_path == Path("mappings/account_name_map.csv")
    assert profile.fingerprint_map_path == Path("mappings/fingerprint_map.csv")
    assert profile.payee_map_path == Path("mappings/payee_map.csv")
    assert profile.categories_path == Path("outputs/ynab_categories.csv")


def test_resolve_named_profile_uses_conventional_paths_and_profile_budget(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "ynab.local.toml"
    config_path.write_text(
        '\n'.join(
            [
                'default_profile = "pilates"',
                "",
                "[profiles.pilates]",
                'budget_id = "pilates-budget"',
            ]
        ),
        encoding="utf-8",
    )

    profile = workflow_profiles.resolve_profile(None, config_path=config_path)

    assert profile.name == "pilates"
    assert profile.budget_id == "pilates-budget"
    assert profile.account_map_path == Path("mappings/pilates/account_name_map.csv")
    assert profile.fingerprint_map_path == Path("mappings/pilates/fingerprint_map.csv")
    assert profile.payee_map_path == Path("mappings/pilates/payee_map.csv")
    assert profile.categories_path == Path("outputs/pilates/ynab_categories.csv")
