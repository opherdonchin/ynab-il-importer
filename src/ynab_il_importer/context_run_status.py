from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import re
from typing import Any

import polars as pl

import ynab_il_importer.bank_reconciliation as bank_reconciliation
import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.context_config as context_config
import ynab_il_importer.upload_prep as upload_prep
import ynab_il_importer.ynab_api as ynab_api
import ynab_il_importer.ynab_category_reconciliation as category_reconciliation


BANK_SOURCE_KINDS = {"leumi", "leumi_xls"}
CARD_SOURCE_KINDS = {"max", "leumi_card_html"}
CATEGORY_SOURCE_KINDS = {"ynab_category"}

STATUS_SEVERITY = {
    "error": 5,
    "missing": 4,
    "blocked": 3,
    "attention": 2,
    "pending": 1,
    "clean": 0,
    "present": 0,
}

RUN_TAG_PATTERN = re.compile(r"^(?P<year>\d{4})_(?P<month>\d{2})_\d{2}$")
PREVIOUS_CYCLE_PATTERN = re.compile(r"^(?P<cycle>\d{4}_\d{2})")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _account_key(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    return text.strip("_") or "account"


def _run_month(run_tag: str) -> str:
    match = RUN_TAG_PATTERN.fullmatch(_normalize_text(run_tag))
    if not match:
        raise ValueError(f"Run tag must look like YYYY_MM_DD, got {_normalize_text(run_tag)!r}.")
    return f"{match.group('year')}_{match.group('month')}"


def infer_previous_card_snapshot_path(
    *,
    defaults: context_config.DefaultsConfig,
    run_tag: str,
    source_kind: str,
    account_name: str,
) -> Path | None:
    suffix_match = re.search(r"(\d{4})", _normalize_text(account_name))
    if suffix_match is None:
        return None

    source_root_name = {
        "max": "previous_max",
        "leumi_card_html": "previous_leumi_card",
    }.get(_normalize_text(source_kind))
    if source_root_name is None:
        return None

    run_month = _run_month(run_tag)
    previous_dir = defaults.derived_root / source_root_name / f"x{suffix_match.group(1)}"
    if not previous_dir.exists() or not previous_dir.is_dir():
        return None

    candidates: list[tuple[str, Path]] = []
    for path in sorted(previous_dir.glob("*.parquet")):
        match = PREVIOUS_CYCLE_PATTERN.match(path.stem)
        if match is None:
            continue
        cycle = match.group("cycle")
        if cycle <= run_month:
            candidates.append((cycle, path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _worst_status(checks: list[dict[str, Any]]) -> str:
    if not checks:
        return "clean"
    return max(checks, key=lambda item: STATUS_SEVERITY.get(item["status"], 0))["status"]


def _check(
    *,
    name: str,
    status: str,
    detail: str,
    path: Path | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": name,
        "status": status,
        "detail": detail,
    }
    if path is not None:
        payload["path"] = str(path.resolve())
    if data:
        payload["data"] = data
    return payload


def _parquet_row_count(path: Path) -> int:
    return pl.read_parquet(path).height


def _csv_frame(path: Path) -> pl.DataFrame:
    return pl.read_csv(path)


def _csv_row_count(path: Path) -> int:
    return _csv_frame(path).height


def _truthy_count(frame: pl.DataFrame, column: str) -> int:
    if column not in frame.columns:
        return 0
    return int(
        frame.select(
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_lowercase()
            .is_in(["1", "true", "t", "yes", "y"])
            .sum()
            .alias("count")
        ).item()
    )


def _value_counts(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns or frame.is_empty():
        return {}
    rows = (
        frame.select(pl.col(column).cast(pl.Utf8, strict=False).fill_null("").alias(column))
        .group_by(column)
        .len()
        .sort(column)
        .iter_rows(named=True)
    )
    return {(_normalize_text(row[column]) or "<blank>"): int(row["len"]) for row in rows}


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return " | ".join(f"{key}={value}" for key, value in counts.items())


def _resolve_raw_source_check(
    source: context_config.ContextSourceConfig,
    *,
    raw_dir: Path,
) -> dict[str, Any]:
    name = f"raw source {source.id}"
    if source.kind == "ynab_category":
        detail = f"from_context={source.from_context} -> {source.normalized_name}"
        return _check(name=name, status="present", detail=detail)

    if not raw_dir.exists():
        return _check(
            name=name,
            status="missing",
            detail="raw run directory is missing",
            path=raw_dir,
        )
    if source.raw_file:
        raw_path = raw_dir / source.raw_file
        if not raw_path.exists():
            return _check(
                name=name,
                status="missing",
                detail=f"expected {source.raw_file}",
                path=raw_path,
            )
        detail = f"{raw_path.name} ({raw_path.stat().st_size} bytes)"
        return _check(name=name, status="present", detail=detail, path=raw_path)

    pattern = re.compile(_normalize_text(source.raw_match))
    matches = [
        path
        for path in sorted(raw_dir.iterdir())
        if path.is_file() and pattern.fullmatch(path.name)
    ]
    if len(matches) != 1:
        status = "missing" if not matches else "attention"
        detail = f"pattern {source.raw_match!r} matched {[path.name for path in matches]}"
        return _check(name=name, status=status, detail=detail, path=raw_dir)
    raw_path = matches[0]
    detail = f"{raw_path.name} ({raw_path.stat().st_size} bytes)"
    return _check(name=name, status="present", detail=detail, path=raw_path)


def _artifact_file_check(
    *,
    name: str,
    path: Path,
    row_counter: callable | None = None,
    detail_prefix: str = "",
) -> dict[str, Any]:
    if not path.exists():
        return _check(name=name, status="missing", detail="not found", path=path)
    try:
        if row_counter is None:
            detail = detail_prefix or "present"
        else:
            rows = int(row_counter(path))
            detail = f"{detail_prefix}rows={rows}" if detail_prefix else f"rows={rows}"
        return _check(name=name, status="present", detail=detail, path=path)
    except Exception as exc:  # pragma: no cover - defensive summary path
        return _check(name=name, status="error", detail=str(exc), path=path)


def _review_artifact_check(
    *,
    reviewed_path: Path,
) -> dict[str, Any]:
    name = "reviewed artifact"
    if not reviewed_path.exists():
        return _check(name=name, status="missing", detail="not found", path=reviewed_path)
    try:
        working = upload_prep.load_upload_working_frame(reviewed_path)
        action_counts = _value_counts(working, "decision_action")
        reviewed_true = _truthy_count(working, "reviewed")
        upload_decisions = sum(
            count
            for key, count in action_counts.items()
            if key.lower() in {"create_target", "update_target"}
        )
        detail = (
            f"rows={working.height} | reviewed={reviewed_true} | "
            f"upload_decisions={upload_decisions} | actions: {_format_counts(action_counts)}"
        )
        return _check(
            name=name,
            status="present",
            detail=detail,
            path=reviewed_path,
            data={
                "row_count": working.height,
                "reviewed_true_count": reviewed_true,
                "upload_decision_count": upload_decisions,
                "decision_action_counts": action_counts,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive summary path
        return _check(name=name, status="error", detail=str(exc), path=reviewed_path)


def _upload_json_check(path: Path) -> dict[str, Any]:
    name = "upload payload json"
    if not path.exists():
        return _check(name=name, status="missing", detail="not found", path=path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        create_count = len(payload.get("create_transactions", []) or [])
        update_count = len(payload.get("update_transactions", []) or [])
        detail = f"create={create_count} | update={update_count}"
        return _check(
            name=name,
            status="present",
            detail=detail,
            path=path,
            data={
                "create_count": create_count,
                "update_count": update_count,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive summary path
        return _check(name=name, status="error", detail=str(exc), path=path)


def _report_file_check(
    *,
    name: str,
    path: Path,
    action_column: str = "action",
    pending_action_names: set[str] | None = None,
    extra_detail_columns: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not path.exists():
        return _check(name=name, status="missing", detail="report not found", path=path)
    try:
        frame = _csv_frame(path)
        counts = _value_counts(frame, action_column)
        if counts:
            blocked = counts.get("blocked", 0)
            unmatched = counts.get("unmatched", 0)
            pending = sum(counts.get(action, 0) for action in pending_action_names or set())
            if blocked:
                status = "blocked"
            elif unmatched:
                status = "attention"
            elif pending:
                status = "pending"
            else:
                status = "clean"
            detail = _format_counts(counts)
        else:
            extra_parts = []
            for column in extra_detail_columns:
                if column not in frame.columns:
                    continue
                column_counts = _value_counts(frame, column)
                if column_counts:
                    extra_parts.append(f"{column}: {_format_counts(column_counts)}")
            status = "clean"
            detail = " | ".join(extra_parts) if extra_parts else f"rows={frame.height}"
        return _check(
            name=name,
            status=status,
            detail=detail,
            path=path,
            data={
                "row_count": frame.height,
                "counts": counts,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive summary path
        return _check(name=name, status="error", detail=str(exc), path=path)


def _bank_uncleared_report_check(name: str, path: Path) -> dict[str, Any]:
    if not path.exists():
        return _check(name=name, status="missing", detail="report not found", path=path)
    try:
        frame = _csv_frame(path)
        triage_counts = _value_counts(frame, "triage")
        stale = triage_counts.get("stale_orphan", 0)
        candidate = triage_counts.get("candidate_source_match", 0)
        recent = triage_counts.get("recent_pending", 0)
        if stale or candidate:
            status = "attention"
        elif recent:
            status = "pending"
        else:
            status = "clean"
        detail = _format_counts(triage_counts)
        return _check(
            name=name,
            status=status,
            detail=detail,
            path=path,
            data={
                "row_count": frame.height,
                "triage_counts": triage_counts,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive summary path
        return _check(name=name, status="error", detail=str(exc), path=path)


def _budget_accounts(plan_id: str, cache: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    if plan_id not in cache:
        cache[plan_id] = ynab_api.fetch_accounts(plan_id=plan_id or None)
    return cache[plan_id]


def _budget_transactions(
    plan_id: str,
    cache: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    if plan_id not in cache:
        cache[plan_id] = ynab_api.fetch_transactions(plan_id=plan_id or None)
    return cache[plan_id]


def _budget_categories(
    plan_id: str,
    cache: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    if plan_id not in cache:
        groups = ynab_api.fetch_categories(plan_id=plan_id or None)
        categories = ynab_api.categories_to_dataframe(groups)
        if categories.is_empty():
            categories = ynab_api.categories_from_transactions_to_dataframe(
                ynab_api.fetch_transactions(plan_id=plan_id or None)
            )
        cache[plan_id] = categories
    return cache[plan_id]


def _live_bank_checks(
    *,
    source: context_config.ContextSourceConfig,
    normalized_path: Path,
    plan_id: str,
    account_cache: dict[str, list[dict[str, Any]]],
    transaction_cache: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    accounts = _budget_accounts(plan_id, account_cache)
    transactions = _budget_transactions(plan_id, transaction_cache)
    bank_df = bank_reconciliation.load_bank_transactions(normalized_path)

    sync_result = bank_reconciliation.plan_bank_match_sync(bank_df, accounts, transactions)
    sync_unmatched = int((sync_result["report"]["action"] == "unmatched").sum())
    sync_blocked = int((sync_result["report"]["action"] == "blocked").sum())
    sync_status = (
        "blocked"
        if sync_blocked
        else (
            "attention"
            if sync_unmatched
            else ("pending" if sync_result["update_count"] else "clean")
        )
    )

    uncleared_result = bank_reconciliation.plan_uncleared_ynab_triage(
        bank_df,
        accounts,
        transactions,
    )
    uncleared_status = (
        "attention"
        if uncleared_result["stale_orphan_count"]
        or uncleared_result["candidate_source_match_count"]
        else ("pending" if uncleared_result["recent_pending_count"] else "clean")
    )

    reconcile_result = bank_reconciliation.plan_bank_statement_reconciliation(
        bank_df,
        accounts,
        transactions,
    )
    reconcile_unmatched = int((reconcile_result["report"]["action"] == "unmatched").sum())
    reconcile_status = (
        "blocked"
        if not reconcile_result["ok"]
        else (
            "attention"
            if reconcile_unmatched
            else ("pending" if reconcile_result["update_count"] else "clean")
        )
    )

    return [
        _check(
            name=f"live bank sync {source.id}",
            status=sync_status,
            detail=(
                f"matched={sync_result['matched_count']} | updates={sync_result['update_count']} | "
                f"unmatched={sync_unmatched} | blocked={sync_blocked}"
            ),
            path=normalized_path,
        ),
        _check(
            name=f"live bank uncleared {source.id}",
            status=uncleared_status,
            detail=(
                f"recent_pending={uncleared_result['recent_pending_count']} | "
                f"candidate_source_match={uncleared_result['candidate_source_match_count']} | "
                f"stale_orphan={uncleared_result['stale_orphan_count']}"
            ),
            path=normalized_path,
        ),
        _check(
            name=f"live bank reconcile {source.id}",
            status=reconcile_status,
            detail=(
                f"ok={bool(reconcile_result['ok'])} | updates={reconcile_result['update_count']} | "
                f"unmatched={reconcile_unmatched} | reason={_normalize_text(reconcile_result.get('reason')) or '<none>'}"
            ),
            path=normalized_path,
        ),
    ]


def _live_card_checks(
    *,
    defaults: context_config.DefaultsConfig,
    source: context_config.ContextSourceConfig,
    normalized_path: Path,
    run_tag: str,
    plan_id: str,
    account_cache: dict[str, list[dict[str, Any]]],
    transaction_cache: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    accounts = _budget_accounts(plan_id, account_cache)
    transactions = _budget_transactions(plan_id, transaction_cache)
    source_df = card_reconciliation.load_card_source(normalized_path)
    checks: list[dict[str, Any]] = []

    for account_name in source.target_account_names:
        sync_result = card_reconciliation.plan_card_match_sync(
            account_name=account_name,
            source_df=source_df,
            accounts=accounts,
            transactions=transactions,
        )
        sync_unmatched = int((sync_result["report"]["action"] == "unmatched").sum())
        sync_blocked = int((sync_result["report"]["action"] == "blocked").sum())
        sync_status = (
            "blocked"
            if sync_blocked
            else (
                "attention"
                if sync_unmatched
                else ("pending" if sync_result["update_count"] else "clean")
            )
        )
        checks.append(
            _check(
                name=f"live card sync {source.id} / {account_name}",
                status=sync_status,
                detail=(
                    f"matched={sync_result['matched_count']} | updates={sync_result['update_count']} | "
                    f"unmatched={sync_unmatched} | blocked={sync_blocked}"
                ),
                path=normalized_path,
            )
        )

        previous_path = infer_previous_card_snapshot_path(
            defaults=defaults,
            run_tag=run_tag,
            source_kind=source.kind,
            account_name=account_name,
        )
        if previous_path is None:
            checks.append(
                _check(
                    name=f"live card reconcile {source.id} / {account_name}",
                    status="missing",
                    detail="no previous snapshot inferred",
                    path=normalized_path,
                )
            )
            continue

        previous_df = card_reconciliation.load_card_source(previous_path)
        reconcile_result = card_reconciliation.plan_card_cycle_reconciliation(
            account_name=account_name,
            source_df=source_df,
            previous_df=previous_df,
            accounts=accounts,
            transactions=transactions,
            allow_reconciled_source=source.allow_reconciled_source,
        )
        reconcile_status = (
            "blocked"
            if not reconcile_result["ok"]
            else ("pending" if reconcile_result["update_count"] else "clean")
        )
        checks.append(
            _check(
                name=f"live card reconcile {source.id} / {account_name}",
                status=reconcile_status,
                detail=(
                    f"mode={reconcile_result['mode']} | updates={reconcile_result['update_count']} | "
                    f"warning={_normalize_text(reconcile_result.get('warning')) or '<none>'} | "
                    f"reason={_normalize_text(reconcile_result.get('reason')) or '<none>'} | "
                    f"previous={previous_path.name}"
                ),
                path=previous_path,
            )
        )
    return checks


def _live_category_checks(
    *,
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    source: context_config.ContextSourceConfig,
    run_tag: str,
    reviewed_path: Path,
    target_plan_id: str,
    account_cache: dict[str, list[dict[str, Any]]],
    transaction_cache: dict[str, list[dict[str, Any]]],
    category_cache: dict[str, pl.DataFrame],
    month_cache: dict[tuple[str, str], dict[str, Any]],
    contexts_root: Path,
) -> list[dict[str, Any]]:
    if not reviewed_path.exists():
        return [
            _check(
                name=f"live category reconcile {source.id}",
                status="missing",
                detail="reviewed artifact not found",
                path=reviewed_path,
            )
        ]

    target_accounts = _budget_accounts(target_plan_id, account_cache)
    target_transactions = _budget_transactions(target_plan_id, transaction_cache)
    target_categories = _budget_categories(target_plan_id, category_cache)
    reviewed = upload_prep.load_upload_working_frame(reviewed_path)
    target_account = category_reconciliation.resolve_live_account(
        target_accounts,
        account_id=source.target_account_id,
        account_name=source.target_account_name,
    )
    relevant_review = category_reconciliation.select_review_rows_for_source(
        reviewed,
        source=category_reconciliation.CategoryReconcileSource(
            category_id=source.category_id,
            category_name=source.category_name,
            target_account_id=_normalize_text(target_account.get("id", "")),
            target_account_name=_normalize_text(target_account.get("name", "")),
        ),
    )
    prepared = upload_prep.prepare_upload_transactions(
        reviewed,
        accounts=target_accounts,
        categories_df=target_categories,
    )
    prepared_units = upload_prep.assemble_upload_transaction_units(prepared)
    source_context = context_config.load_context(source.from_context, contexts_root=contexts_root)
    source_plan_id = context_config.resolve_context_budget_id(source_context)
    run_month = category_reconciliation.run_month_from_tag(run_tag)
    month_key = (source_plan_id, run_month)
    if month_key not in month_cache:
        month_cache[month_key] = ynab_api.fetch_month_detail(
            run_month, plan_id=source_plan_id or None
        )
    source_month = month_cache[month_key]
    source_category = category_reconciliation.resolve_month_category(
        source_month,
        category_id=source.category_id,
        category_name=source.category_name,
    )
    result = category_reconciliation.plan_category_account_reconciliation(
        relevant_review,
        prepared_units,
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )
    status = "blocked" if not result["ok"] else ("pending" if result["update_count"] else "clean")
    return [
        _check(
            name=f"live category reconcile {source.id}",
            status=status,
            detail=(
                f"reviewed={result['reviewed_row_count']} | resolved={result['resolved_count']} | "
                f"updates={result['update_count']} | blocked={result['blocked_count']} | "
                f"already_reconciled={result['already_reconciled_count']} | "
                f"reason={_normalize_text(result.get('reason')) or '<none>'}"
            ),
            path=reviewed_path,
        )
    ]


def collect_context_run_status(
    *,
    context_name: str,
    run_tag: str,
    defaults_path: Path = context_config.DEFAULTS_PATH,
    contexts_root: Path = context_config.CONTEXTS_ROOT,
    budget_id: str = "",
    verify_live: bool = False,
) -> dict[str, Any]:
    defaults = context_config.load_defaults(defaults_path)
    context = context_config.load_context(context_name, contexts_root=contexts_root)
    run_paths = context_config.resolve_run_paths(defaults, run_tag=run_tag)

    artifact_checks: list[dict[str, Any]] = []
    report_checks: list[dict[str, Any]] = []
    live_checks: list[dict[str, Any]] = []

    artifact_checks.append(
        _check(
            name="raw run directory",
            status="present" if run_paths.raw_dir.exists() else "missing",
            detail="present" if run_paths.raw_dir.exists() else "missing",
            path=run_paths.raw_dir,
        )
    )
    artifact_checks.append(
        _check(
            name="derived run directory",
            status="present" if run_paths.derived_dir.exists() else "missing",
            detail="present" if run_paths.derived_dir.exists() else "missing",
            path=run_paths.derived_dir,
        )
    )
    artifact_checks.append(
        _check(
            name="paired run directory",
            status="present" if run_paths.paired_dir.exists() else "missing",
            detail="present" if run_paths.paired_dir.exists() else "missing",
            path=run_paths.paired_dir,
        )
    )

    dependency_contexts = context_config.resolve_context_ynab_dependencies(
        context,
        contexts_root=contexts_root,
    )[:-1]
    for dependency in dependency_contexts:
        dependency_path = run_paths.derived_dir / dependency.ynab_normalized_name
        artifact_checks.append(
            _artifact_file_check(
                name=f"dependency ynab snapshot {dependency.name}",
                path=dependency_path,
                row_counter=_parquet_row_count,
            )
        )

    reviewed_path = run_paths.reviewed_review_path(defaults, context.name)
    proposal_path = run_paths.proposal_review_path(defaults, context.name)
    matched_pairs_path = run_paths.matched_pairs_path(defaults, context.name)
    upload_csv_path = run_paths.upload_csv_path(defaults, context.name)
    upload_json_path = run_paths.upload_json_path(defaults, context.name)
    ynab_path = run_paths.derived_dir / context.ynab_normalized_name

    for source in context.config.sources:
        artifact_checks.append(_resolve_raw_source_check(source, raw_dir=run_paths.raw_dir))
        normalized_path = run_paths.derived_dir / source.normalized_name
        artifact_checks.append(
            _artifact_file_check(
                name=f"normalized source {source.id}",
                path=normalized_path,
                row_counter=_parquet_row_count,
            )
        )

    artifact_checks.append(
        _artifact_file_check(
            name="context ynab snapshot",
            path=ynab_path,
            row_counter=_parquet_row_count,
        )
    )
    artifact_checks.append(
        _artifact_file_check(
            name="matched pairs artifact",
            path=matched_pairs_path,
            row_counter=_parquet_row_count,
        )
    )
    artifact_checks.append(
        _artifact_file_check(
            name="proposal artifact",
            path=proposal_path,
            row_counter=_parquet_row_count,
        )
    )
    artifact_checks.append(_review_artifact_check(reviewed_path=reviewed_path))
    artifact_checks.append(
        _artifact_file_check(
            name="upload csv",
            path=upload_csv_path,
            row_counter=_csv_row_count,
        )
    )
    artifact_checks.append(_upload_json_check(upload_json_path))

    for source in context.config.sources:
        if source.kind in BANK_SOURCE_KINDS:
            report_checks.append(
                _report_file_check(
                    name=f"bank sync report {source.id}",
                    path=run_paths.bank_sync_report_path(defaults, context.name, source.id),
                )
            )
            report_checks.append(
                _bank_uncleared_report_check(
                    f"bank uncleared report {source.id}",
                    run_paths.bank_uncleared_report_path(defaults, context.name, source.id),
                )
            )
            report_checks.append(
                _report_file_check(
                    name=f"bank reconcile report {source.id}",
                    path=run_paths.bank_reconcile_report_path(defaults, context.name, source.id),
                    pending_action_names={"reconcile"},
                )
            )
            continue

        if source.kind in CARD_SOURCE_KINDS:
            for account_name in source.target_account_names:
                account_key = _account_key(account_name)
                report_checks.append(
                    _report_file_check(
                        name=f"card sync report {source.id} / {account_name}",
                        path=run_paths.card_sync_report_path(
                            defaults,
                            context.name,
                            source.id,
                            account_key,
                        ),
                    )
                )
                report_checks.append(
                    _report_file_check(
                        name=f"card reconcile report {source.id} / {account_name}",
                        path=run_paths.card_reconcile_report_path(
                            defaults,
                            context.name,
                            source.id,
                            account_key,
                        ),
                        pending_action_names={"reconcile"},
                    )
                )
            continue

        if source.kind in CATEGORY_SOURCE_KINDS:
            report_checks.append(
                _report_file_check(
                    name=f"category reconcile report {source.id}",
                    path=run_paths.category_account_reconcile_report_path(
                        defaults,
                        context.name,
                        source.id,
                    ),
                    pending_action_names={"reconcile"},
                )
            )

    if verify_live:
        target_plan_id = context_config.resolve_context_budget_id(context, budget_id=budget_id)
        account_cache: dict[str, list[dict[str, Any]]] = {}
        transaction_cache: dict[str, list[dict[str, Any]]] = {}
        category_cache: dict[str, pl.DataFrame] = {}
        month_cache: dict[tuple[str, str], dict[str, Any]] = {}
        for source in context.config.sources:
            normalized_path = run_paths.derived_dir / source.normalized_name
            if not normalized_path.exists() and source.kind != "ynab_category":
                live_checks.append(
                    _check(
                        name=f"live verify input {source.id}",
                        status="missing",
                        detail="normalized source artifact not found",
                        path=normalized_path,
                    )
                )
                continue
            if source.kind in BANK_SOURCE_KINDS:
                live_checks.extend(
                    _live_bank_checks(
                        source=source,
                        normalized_path=normalized_path,
                        plan_id=target_plan_id,
                        account_cache=account_cache,
                        transaction_cache=transaction_cache,
                    )
                )
            elif source.kind in CARD_SOURCE_KINDS:
                live_checks.extend(
                    _live_card_checks(
                        defaults=defaults,
                        source=source,
                        normalized_path=normalized_path,
                        run_tag=run_tag,
                        plan_id=target_plan_id,
                        account_cache=account_cache,
                        transaction_cache=transaction_cache,
                    )
                )
            elif source.kind in CATEGORY_SOURCE_KINDS:
                live_checks.extend(
                    _live_category_checks(
                        context=context,
                        defaults=defaults,
                        source=source,
                        run_tag=run_tag,
                        reviewed_path=reviewed_path,
                        target_plan_id=target_plan_id,
                        account_cache=account_cache,
                        transaction_cache=transaction_cache,
                        category_cache=category_cache,
                        month_cache=month_cache,
                        contexts_root=contexts_root,
                    )
                )

    artifact_summary = Counter(check["status"] for check in artifact_checks)
    report_summary = Counter(check["status"] for check in report_checks)
    live_summary = Counter(check["status"] for check in live_checks)

    return {
        "context": context.name,
        "run_tag": run_tag,
        "verify_live": verify_live,
        "artifact_checks": artifact_checks,
        "report_checks": report_checks,
        "live_checks": live_checks,
        "summary": {
            "artifacts": {
                "overall_status": _worst_status(artifact_checks),
                "status_counts": dict(artifact_summary),
            },
            "reports": {
                "overall_status": _worst_status(report_checks),
                "status_counts": dict(report_summary),
            },
            "live": {
                "overall_status": _worst_status(live_checks),
                "status_counts": dict(live_summary),
            },
        },
    }


def render_context_run_status(status: dict[str, Any]) -> str:
    lines = [
        f"Context: {status['context']}",
        f"Run tag: {status['run_tag']}",
        f"Live verification: {'enabled' if status['verify_live'] else 'disabled'}",
        "",
        "Summary",
    ]
    for section_name in ("artifacts", "reports", "live"):
        section = status["summary"][section_name]
        lines.append(
            f"  {section_name:<10} {section['overall_status']:<9} "
            f"{_format_counts(section['status_counts'])}"
        )

    def append_checks(title: str, checks: list[dict[str, Any]]) -> None:
        lines.append("")
        lines.append(title)
        if not checks:
            lines.append("  <none>")
            return
        for check in checks:
            lines.append(f"  {check['status']:<9} {check['name']:<45} {check['detail']}")

    append_checks("Artifact Checks", status["artifact_checks"])
    append_checks("Report Checks", status["report_checks"])
    append_checks("Live Checks", status["live_checks"])
    return "\n".join(lines)
