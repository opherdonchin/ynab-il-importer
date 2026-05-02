from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl

import ynab_il_importer.context_config as context_config
import ynab_il_importer.context_run_status as context_run_status


def _write_defaults(path: Path, root: Path) -> None:
    path.write_text(
        "\n".join(
            [
                f'raw_root = "{(root / "data/raw").as_posix()}"',
                f'derived_root = "{(root / "data/derived").as_posix()}"',
                f'paired_root = "{(root / "data/paired").as_posix()}"',
                f'outputs_root = "{(root / "outputs").as_posix()}"',
                "",
                "[files]",
                'proposed_review = "{context}_proposed_transactions.parquet"',
                'reviewed_review = "{context}_proposed_transactions_reviewed.parquet"',
                'matched_pairs = "{context}_matched_pairs.parquet"',
                'upload_csv = "{context}_upload.csv"',
                'upload_json = "{context}_upload.json"',
                'bank_sync_report = "{context}_{source_id}_bank_sync_report.csv"',
                'bank_uncleared_report = "{context}_{source_id}_bank_uncleared_ynab_report.csv"',
                'bank_reconcile_report = "{context}_{source_id}_bank_reconcile_report.csv"',
                'card_sync_report = "{context}_{source_id}_{account_key}_card_sync_report.csv"',
                'card_reconcile_report = "{context}_{source_id}_{account_key}_card_reconcile_report.csv"',
                'category_account_reconcile_report = "{context}_{source_id}_category_account_reconcile_report.csv"',
            ]
        ),
        encoding="utf-8",
    )


def _write_bank_context(contexts_root: Path) -> None:
    context_dir = contexts_root / "demo"
    context_dir.mkdir(parents=True)
    (context_dir / "context.toml").write_text(
        "\n".join(
            [
                'name = "demo"',
                'budget_id_env = "YNAB_DEMO_BUDGET_ID"',
                "",
                "[maps]",
                'account_map = "../../mappings/account_name_map.csv"',
                'fingerprint_map = "../../mappings/fingerprint_map.csv"',
                'payee_map = "../../mappings/payee_map.csv"',
                "",
                "[ynab]",
                'normalized_name = "demo_ynab_api_norm.parquet"',
                "",
                "[[sources]]",
                'id = "demo_bank"',
                'kind = "leumi"',
                'raw_file = "demo-bank.dat"',
                'normalized_name = "demo_bank_norm.parquet"',
                'target_account_names = ["Bank Demo"]',
            ]
        ),
        encoding="utf-8",
    )


def test_collect_context_run_status_summarizes_artifacts_and_reports(
    tmp_path: Path,
) -> None:
    defaults_path = tmp_path / "defaults.toml"
    contexts_root = tmp_path / "contexts"
    _write_defaults(defaults_path, tmp_path)
    _write_bank_context(contexts_root)

    raw_dir = tmp_path / "data/raw/2026_04_28"
    derived_dir = tmp_path / "data/derived/2026_04_28"
    paired_dir = tmp_path / "data/paired/2026_04_28"
    raw_dir.mkdir(parents=True)
    derived_dir.mkdir(parents=True)
    paired_dir.mkdir(parents=True)

    (raw_dir / "demo-bank.dat").write_text("demo", encoding="utf-8")
    pl.DataFrame({"value": [1, 2]}).write_parquet(derived_dir / "demo_bank_norm.parquet")
    pl.DataFrame({"value": [1]}).write_parquet(derived_dir / "demo_ynab_api_norm.parquet")
    pl.DataFrame({"value": [1]}).write_parquet(paired_dir / "demo_matched_pairs.parquet")
    pl.DataFrame({"value": [1]}).write_parquet(paired_dir / "demo_proposed_transactions.parquet")
    pl.DataFrame({"value": [1]}).write_csv(paired_dir / "demo_upload.csv")
    (paired_dir / "demo_upload.json").write_text(
        '{"create_transactions": [{"id": 1}], "update_transactions": []}',
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"action": "matched_preview"},
            {"action": "unmatched"},
        ]
    ).to_csv(paired_dir / "demo_demo_bank_bank_sync_report.csv", index=False)
    pd.DataFrame(
        [
            {"triage": "recent_pending"},
            {"triage": "recent_pending"},
        ]
    ).to_csv(
        paired_dir / "demo_demo_bank_bank_uncleared_ynab_report.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"action": "anchor_history"},
            {"action": "reconcile"},
        ]
    ).to_csv(paired_dir / "demo_demo_bank_bank_reconcile_report.csv", index=False)

    status = context_run_status.collect_context_run_status(
        context_name="demo",
        run_tag="2026_04_28",
        defaults_path=defaults_path,
        contexts_root=contexts_root,
    )

    artifact_by_name = {item["name"]: item for item in status["artifact_checks"]}
    report_by_name = {item["name"]: item for item in status["report_checks"]}

    assert artifact_by_name["raw source demo_bank"]["status"] == "present"
    assert artifact_by_name["normalized source demo_bank"]["detail"] == "rows=2"
    assert artifact_by_name["reviewed artifact"]["status"] == "missing"
    assert artifact_by_name["upload payload json"]["detail"] == "create=1 | update=0"

    assert report_by_name["bank sync report demo_bank"]["status"] == "attention"
    assert report_by_name["bank uncleared report demo_bank"]["status"] == "pending"
    assert report_by_name["bank reconcile report demo_bank"]["status"] == "pending"

    rendered = context_run_status.render_context_run_status(status)
    assert "Context: demo" in rendered
    assert "bank sync report demo_bank" in rendered


def test_infer_previous_card_snapshot_path_uses_latest_cycle_not_after_run_month(
    tmp_path: Path,
) -> None:
    defaults = context_config.DefaultsConfig.model_validate(
        {
            "raw_root": str(tmp_path / "data/raw"),
            "derived_root": str(tmp_path / "data/derived"),
            "paired_root": str(tmp_path / "data/paired"),
            "outputs_root": str(tmp_path / "outputs"),
        }
    )
    previous_dir = tmp_path / "data/derived/previous_leumi_card/x0602"
    previous_dir.mkdir(parents=True)
    for cycle in ("2026_03", "2026_04", "2026_05"):
        pl.DataFrame({"value": [1]}).write_parquet(
            previous_dir / f"{cycle}_leumi_card_html_norm.parquet"
        )

    resolved = context_run_status.infer_previous_card_snapshot_path(
        defaults=defaults,
        run_tag="2026_04_28",
        source_kind="leumi_card_html",
        account_name="Credit card 0602",
    )

    assert resolved is not None
    assert resolved.name == "2026_04_leumi_card_html_norm.parquet"


def test_collect_context_run_status_can_run_live_bank_checks_with_stubs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    defaults_path = tmp_path / "defaults.toml"
    contexts_root = tmp_path / "contexts"
    _write_defaults(defaults_path, tmp_path)
    _write_bank_context(contexts_root)

    raw_dir = tmp_path / "data/raw/2026_04_28"
    derived_dir = tmp_path / "data/derived/2026_04_28"
    paired_dir = tmp_path / "data/paired/2026_04_28"
    raw_dir.mkdir(parents=True)
    derived_dir.mkdir(parents=True)
    paired_dir.mkdir(parents=True)

    (raw_dir / "demo-bank.dat").write_text("demo", encoding="utf-8")
    pl.DataFrame({"value": [1]}).write_parquet(derived_dir / "demo_bank_norm.parquet")
    pl.DataFrame({"value": [1]}).write_parquet(derived_dir / "demo_ynab_api_norm.parquet")

    monkeypatch.setattr(
        context_run_status.ynab_api,
        "fetch_accounts",
        lambda plan_id=None: [{"id": "acc-1", "name": "Bank Demo"}],
    )
    monkeypatch.setattr(
        context_run_status.ynab_api,
        "fetch_transactions",
        lambda plan_id=None: [{"id": "txn-1"}],
    )
    monkeypatch.setattr(
        context_run_status.bank_reconciliation,
        "load_bank_transactions",
        lambda _path: pd.DataFrame([{"amount": 1}]),
    )
    monkeypatch.setattr(
        context_run_status.bank_reconciliation,
        "plan_bank_match_sync",
        lambda *_args, **_kwargs: {
            "report": pd.DataFrame([{"action": "matched"}]),
            "matched_count": 1,
            "update_count": 0,
        },
    )
    monkeypatch.setattr(
        context_run_status.bank_reconciliation,
        "plan_uncleared_ynab_triage",
        lambda *_args, **_kwargs: {
            "report": pd.DataFrame([]),
            "recent_pending_count": 0,
            "candidate_source_match_count": 0,
            "stale_orphan_count": 0,
        },
    )
    monkeypatch.setattr(
        context_run_status.bank_reconciliation,
        "plan_bank_statement_reconciliation",
        lambda *_args, **_kwargs: {
            "report": pd.DataFrame([{"action": "anchor_history"}]),
            "ok": True,
            "update_count": 0,
            "reason": "",
        },
    )

    status = context_run_status.collect_context_run_status(
        context_name="demo",
        run_tag="2026_04_28",
        defaults_path=defaults_path,
        contexts_root=contexts_root,
        budget_id="budget-1",
        verify_live=True,
    )

    live_by_name = {item["name"]: item for item in status["live_checks"]}
    assert live_by_name["live bank sync demo_bank"]["status"] == "clean"
    assert live_by_name["live bank uncleared demo_bank"]["status"] == "clean"
    assert live_by_name["live bank reconcile demo_bank"]["status"] == "clean"
