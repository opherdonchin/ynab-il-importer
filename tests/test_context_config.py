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
    assert loaded.ynab_normalized_name == "family_ynab_api_norm.parquet"
    assert len(loaded.config.sources) == 2
    assert loaded.config.sources[0].target_account_names == ["Bank Leumi"]
    assert loaded.config.sources[1].target_account_names == [
        "Bank Leumi",
        "Liya X7195",
        "Opher X5898",
        "Opher x9922",
    ]
    assert loaded.config.sources[1].closeout_account_names == [
        "Liya X7195",
        "Opher X5898",
        "Opher x9922",
    ]


def test_load_aikido_context_with_ynab_category_source() -> None:
    loaded = context_config.load_context("aikido")

    assert loaded.name == "aikido"
    assert loaded.budget_id_env == "YNAB_AIKIDO_BUDGET_ID"
    assert loaded.ynab_normalized_name == "aikido_ynab_api_norm.parquet"
    assert [source.id for source in loaded.config.sources] == ["aikido_family_category"]
    source = loaded.config.sources[0]
    assert source.kind == "ynab_category"
    assert source.from_context == "family"
    assert source.category_name == "Aikido"
    assert source.target_account_name == "Personal In Leumi"


def test_load_pilates_context_with_family_category_source() -> None:
    loaded = context_config.load_context("pilates")

    assert loaded.name == "pilates"
    assert loaded.budget_id_env == "YNAB_PILATES_BUDGET_ID"
    assert loaded.ynab_normalized_name == "pilates_ynab_api_norm.parquet"
    assert [source.id for source in loaded.config.sources] == [
        "pilates_bank",
        "pilates_card",
        "pilates_family_category",
    ]
    source = loaded.config.sources[2]
    assert source.kind == "ynab_category"
    assert source.from_context == "family"
    assert source.category_name == "Pilates"
    assert source.target_account_name == "In Family"
    assert source.target_account_id == "6fbef967-60b8-4897-b8da-d14202907584"


def test_resolve_context_sources_supports_ynab_category_without_raw_dir(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "missing-raw"
    loaded = context_config.load_context("aikido")

    sources = context_config.resolve_context_sources(loaded, raw_dir)

    assert len(sources) == 1
    assert sources[0].kind == "ynab_category"
    assert sources[0].raw_path is None
    assert sources[0].from_context == "family"
    assert sources[0].category_name == "Aikido"
    assert sources[0].target_account_name == "Personal In Leumi"


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
    assert sources[0].target_account_names == ("Bank Leumi",)
    assert sources[1].target_account_names == (
        "Bank Leumi",
        "Liya X7195",
        "Opher X5898",
        "Opher x9922",
    )
    assert sources[1].closeout_account_names == (
        "Liya X7195",
        "Opher X5898",
        "Opher x9922",
    )


def test_ynab_category_source_validation_requires_context_and_single_category() -> None:
    with pytest.raises(ValueError, match="must define from_context"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "ynab_category",
                "category_name": "Aikido",
                "target_account_name": "Personal In Leumi",
            }
        )

    with pytest.raises(ValueError, match="exactly one of category_name or category_id"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "ynab_category",
                "from_context": "family",
                "category_name": "Aikido",
                "category_id": "cat-1",
                "target_account_name": "Personal In Leumi",
            }
        )

    with pytest.raises(ValueError, match="cannot define raw_file or raw_match"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "ynab_category",
                "from_context": "family",
                "category_name": "Aikido",
                "target_account_name": "Personal In Leumi",
                "raw_file": "unexpected.csv",
            }
        )


def test_raw_backed_source_validation_requires_target_account_scope() -> None:
    with pytest.raises(ValueError, match="target_account_names"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "leumi",
                "raw_file": "bank.dat",
            }
        )


def test_ynab_category_source_rejects_target_account_names() -> None:
    with pytest.raises(ValueError, match="cannot define target_account_names"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "ynab_category",
                "from_context": "family",
                "category_name": "Aikido",
                "target_account_name": "Personal In Leumi",
                "target_account_names": ["Personal In Leumi"],
            }
        )


def test_raw_backed_source_rejects_closeout_accounts_outside_target_scope() -> None:
    with pytest.raises(ValueError, match="closeout_account_names"):
        context_config.ContextSourceConfig.model_validate(
            {
                "id": "source-1",
                "kind": "max",
                "raw_file": "card.xlsx",
                "target_account_names": ["Liya X7195"],
                "closeout_account_names": ["Bank Leumi"],
            }
        )


def test_resolve_context_sources_requires_unique_regex_match(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "Bankin family.dat").write_text("bank", encoding="utf-8")
    (raw_dir / "transaction-details_export_1.xlsx").write_text(
        "card1", encoding="utf-8"
    )
    (raw_dir / "transaction-details_export_2.xlsx").write_text(
        "card2", encoding="utf-8"
    )

    loaded = context_config.load_context("family")

    with pytest.raises(ValueError, match="matched 2 files"):
        context_config.resolve_context_sources(loaded, raw_dir)


def test_resolve_run_paths_and_context_artifacts(tmp_path: Path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    run_paths = context_config.resolve_run_paths(defaults, run_tag="2026_04_01")
    context = context_config.load_context("family")

    run_paths.derived_dir.mkdir(parents=True)
    for name in [
        "family_leumi_norm.parquet",
        "family_max_norm.parquet",
        "family_ynab_api_norm.parquet",
    ]:
        (run_paths.derived_dir / name).write_text("x", encoding="utf-8")

    source_paths = context_config.resolve_context_normalized_source_paths(
        context, run_paths
    )
    ynab_path = context_config.resolve_context_ynab_path(context, run_paths)

    assert [path.name for path in source_paths] == [
        "family_leumi_norm.parquet",
        "family_max_norm.parquet",
    ]
    assert ynab_path.name == "family_ynab_api_norm.parquet"
    assert (
        run_paths.proposal_review_path(defaults, "family").name
        == "family_proposed_transactions.parquet"
    )
    assert (
        run_paths.matched_pairs_path(defaults, "family").name
        == "family_matched_pairs.parquet"
    )
    assert (
        run_paths.category_account_reconcile_report_path(
            defaults, "aikido", "aikido_family_category"
        ).name
        == "aikido_aikido_family_category_category_account_reconcile_report.csv"
    )


def test_resolve_context_ynab_path_requires_file(tmp_path: Path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    run_paths = context_config.resolve_run_paths(defaults, run_tag="2026_04_01")
    run_paths.derived_dir.mkdir(parents=True)
    context = context_config.load_context("family")

    with pytest.raises(FileNotFoundError, match="Missing normalized YNAB artifact"):
        context_config.resolve_context_ynab_path(context, run_paths)


def test_resolve_context_normalized_source_path_requires_single_selected_source(
    tmp_path: Path,
) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    run_paths = context_config.resolve_run_paths(defaults, run_tag="2026_04_01")
    run_paths.derived_dir.mkdir(parents=True)
    (run_paths.derived_dir / "family_leumi_norm.parquet").write_text(
        "x", encoding="utf-8"
    )
    (run_paths.derived_dir / "family_max_norm.parquet").write_text(
        "x", encoding="utf-8"
    )
    context = context_config.load_context("family")

    with pytest.raises(ValueError, match="exactly one source"):
        context_config.resolve_context_normalized_source_path(context, run_paths)

    resolved = context_config.resolve_context_normalized_source_path(
        context,
        run_paths,
        source_id="family_bank",
    )
    assert resolved.name == "family_leumi_norm.parquet"


def test_resolve_context_budget_id_falls_back_to_local_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("YNAB_FAMILY_BUDGET_ID", raising=False)
    context = context_config.load_context("family")

    resolved = context_config.resolve_context_budget_id(context)

    assert resolved == "15662d89-1e9a-4b67-b83c-38359bcea8a7"


def test_resolve_context_target_account_names_unions_source_scopes() -> None:
    loaded = context_config.load_context("family")

    accounts = context_config.resolve_context_target_account_names(loaded)

    assert accounts == ["Bank Leumi", "Liya X7195", "Opher X5898", "Opher x9922"]


def test_resolve_context_target_account_names_includes_ynab_category_targets() -> None:
    loaded = context_config.load_context("pilates")

    accounts = context_config.resolve_context_target_account_names(loaded)

    assert accounts == ["Bank Leumi 225237", "Credit card 0602", "In Family"]


def test_resolve_context_ynab_dependencies_orders_upstream_first() -> None:
    loaded = context_config.load_context("pilates")

    dependencies = context_config.resolve_context_ynab_dependencies(loaded)

    assert [context.name for context in dependencies] == ["family", "pilates"]
