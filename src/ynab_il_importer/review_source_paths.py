from __future__ import annotations

import csv
from datetime import date, timedelta
import re
from pathlib import Path

import polars as pl

import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.context_config as context_config
from ynab_il_importer.artifacts.transaction_io import read_transactions_polars

CARD_SOURCE_KINDS = {"max", "leumi_card_html"}
PREVIOUS_CARD_ROOTS = {
    "max": "previous_max",
    "leumi_card_html": "previous_leumi_card",
}
PREVIOUS_CARD_EXTENSIONS = {
    "max": ".xlsx",
    "leumi_card_html": ".html",
}
RUN_CYCLE_RE = re.compile(r"^(?P<cycle>\d{4}_\d{2})(?:_\d{2})?$")
PREVIOUS_ARTIFACT_RE = re.compile(
    r"^(?P<cycle>\d{4}_\d{2})_(?P<kind>[a-z_]+)_norm\.parquet$"
)
ACCOUNT_SUFFIX_RE = re.compile(r"^(?:x)?(?P<digits>\d{4})$", re.IGNORECASE)


def _resolve_run_cycle(run_tag: str) -> str:
    text = str(run_tag or "").strip()
    match = RUN_CYCLE_RE.fullmatch(text)
    if not match:
        raise ValueError(
            f"run_tag must start with YYYY_MM and optionally include _DD, got {run_tag!r}."
        )
    return str(match.group("cycle"))


def _normalize_previous_account_suffix(value: str) -> str:
    text = str(value or "").strip()
    match = ACCOUNT_SUFFIX_RE.fullmatch(text)
    if not match:
        return ""
    return f"x{match.group('digits')}"


def _load_card_account_mappings(
    context: context_config.LoadedContext,
    source: context_config.ContextSourceConfig,
) -> list[tuple[str, str]]:
    map_path = context.account_map_path
    if not map_path.exists():
        raise FileNotFoundError(
            f"Missing account map required for card carryforward: {map_path}"
        )
    with open(map_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    required_cols = {"source", "source_account", "ynab_account_name"}
    columns = set(reader.fieldnames or [])
    if not required_cols.issubset(columns):
        raise ValueError(
            f"Account map {map_path} must include columns {sorted(required_cols)} "
            "for card carryforward."
        )

    target_names = set(context_config.resolve_source_closeout_account_names(source))
    ordered: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if str(row.get("source", "") or "").strip().lower() != "card":
            continue
        account_name = str(row.get("ynab_account_name", "") or "").strip()
        if account_name not in target_names:
            continue
        suffix = _normalize_previous_account_suffix(
            str(row.get("source_account", "") or "")
        )
        mapping = (suffix, account_name)
        if not suffix or mapping in seen:
            continue
        ordered.append(mapping)
        seen.add(mapping)
    return ordered


def _statement_cycle_from_path(path: Path, *, kind: str) -> str:
    match = PREVIOUS_ARTIFACT_RE.fullmatch(path.name)
    if not match or str(match.group("kind")).strip().lower() != kind:
        raise ValueError(f"Unexpected previous statement artifact name: {path.name}")
    return str(match.group("cycle")).strip()


def _list_previous_card_snapshot_candidates(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    *,
    run_tag: str,
) -> list[Path]:
    run_cycle = _resolve_run_cycle(run_tag)
    ordered: list[Path] = []
    seen: set[Path] = set()
    for source in context.config.sources:
        if source.kind not in CARD_SOURCE_KINDS:
            continue
        account_mappings = _load_card_account_mappings(context, source)
        previous_root_name = PREVIOUS_CARD_ROOTS[source.kind]
        for suffix, _account_name in account_mappings:
            account_dir = (defaults.derived_root / previous_root_name / suffix).resolve()
            if not account_dir.exists():
                continue
            candidates = [
                path.resolve()
                for path in sorted(account_dir.glob(f"*_{source.kind}_norm.parquet"))
                if _statement_cycle_from_path(path, kind=source.kind) <= run_cycle
            ]
            for path in candidates:
                if path in seen:
                    continue
                ordered.append(path)
                seen.add(path)
    return ordered


def _list_previous_card_raw_cycles(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    *,
    run_tag: str,
) -> list[tuple[str, str, str, str]]:
    run_cycle = _resolve_run_cycle(run_tag)
    ordered: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for source in context.config.sources:
        if source.kind not in CARD_SOURCE_KINDS:
            continue
        account_mappings = _load_card_account_mappings(context, source)
        previous_root_name = PREVIOUS_CARD_ROOTS[source.kind]
        source_extension = PREVIOUS_CARD_EXTENSIONS[source.kind]
        for suffix, account_name in account_mappings:
            account_dir = (defaults.raw_root / previous_root_name / suffix).resolve()
            if not account_dir.exists():
                continue
            candidates = sorted(account_dir.glob(f"*{source_extension}"))
            for path in candidates:
                cycle_match = RUN_CYCLE_RE.fullmatch(path.stem)
                if not cycle_match:
                    continue
                cycle = str(cycle_match.group("cycle"))
                if cycle > run_cycle:
                    continue
                row = (source.kind, suffix, account_name, cycle)
                if row in seen:
                    continue
                ordered.append(row)
                seen.add(row)
    return ordered


def _validate_latest_previous_snapshots_prepared(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    *,
    run_tag: str,
) -> None:
    latest_raw_by_account: dict[tuple[str, str, str], str] = {}
    for kind, suffix, account_name, cycle in _list_previous_card_raw_cycles(
        context,
        defaults,
        run_tag=run_tag,
    ):
        key = (kind, suffix, account_name)
        current = latest_raw_by_account.get(key, "")
        if cycle > current:
            latest_raw_by_account[key] = cycle

    if not latest_raw_by_account:
        return

    missing: list[str] = []
    for (kind, suffix, account_name), cycle in sorted(latest_raw_by_account.items()):
        previous_root_name = PREVIOUS_CARD_ROOTS[kind]
        expected_path = (
            defaults.derived_root
            / previous_root_name
            / suffix
            / f"{cycle}_{kind}_norm.parquet"
        ).resolve()
        if expected_path.exists():
            continue
        missing.append(
            "- "
            f"{account_name} ({suffix}, {kind}) is missing {expected_path.as_posix()} "
            f"for latest raw cycle {cycle}. Run: pixi run normalize-previous-max -- "
            f"{context.name} {suffix} --cycle {cycle}"
        )

    if missing:
        detail = "\n".join(missing)
        raise FileNotFoundError(
            "Missing normalized latest previous-card snapshots required for "
            f"source-window inference for context {context.name!r}:\n{detail}"
        )


def _build_card_ynab_rows_for_account(
    ynab_df: object,
    *,
    account_name: str,
) -> list[dict[str, object]]:
    work = read_transactions_polars(ynab_df) if isinstance(ynab_df, (str, Path)) else ynab_df
    if work.is_empty():
        return []
    filtered = (
        work.filter(
            pl.col("account_name")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            == account_name
        )
        .select(
            pl.coalesce(
                [
                    pl.col("ynab_id").cast(pl.Utf8, strict=False).fill_null(""),
                    pl.col("transaction_id").cast(pl.Utf8, strict=False).fill_null(""),
                ]
            ).alias("id"),
            pl.col("account_id").cast(pl.Utf8, strict=False).fill_null(""),
            pl.col("account_name").cast(pl.Utf8, strict=False).fill_null(""),
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("signed_amount_ils")
            .cast(pl.Float64, strict=False)
            .fill_null(0.0)
            .round(2)
            .alias("signed_ils"),
            pl.col("memo").cast(pl.Utf8, strict=False).fill_null(""),
            pl.col("import_id").cast(pl.Utf8, strict=False).fill_null(""),
            pl.col("cleared").cast(pl.Utf8, strict=False).fill_null(""),
            pl.col("approved").cast(pl.Boolean, strict=False).fill_null(False),
            pl.col("payee_raw").cast(pl.Utf8, strict=False).fill_null("").alias(
                "payee_name"
            ),
        )
        .with_columns(
            pl.col("memo")
            .map_elements(
                card_reconciliation._normalize_match_text,
                return_dtype=pl.String,
            )
            .alias("memo_match"),
            pl.col("memo")
            .map_elements(
                card_identity.extract_card_txn_id_from_memo,
                return_dtype=pl.String,
            )
            .alias("card_txn_id_marker"),
        )
    )
    return filtered.to_dicts()


def _statement_is_fully_reconciled(
    statement_path: Path,
    *,
    account_name: str,
    ynab_rows: list[dict[str, object]],
) -> bool:
    previous_df = card_reconciliation.load_card_source(statement_path)
    previous_rows = card_reconciliation._build_card_source_frame(previous_df, account_name)
    report_rows = card_reconciliation._evaluate_snapshot_rows(
        previous_rows,
        ynab_rows,
        snapshot_role="previous",
    )
    return bool(report_rows) and all(
        str(row.get("prior_cleared", "") or "").strip().lower() == "reconciled"
        for row in report_rows
    )


def _resolve_previous_card_snapshot_paths(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    *,
    run_tag: str,
    ynab_path: Path,
) -> list[Path]:
    run_cycle = _resolve_run_cycle(run_tag)
    ynab_df = read_transactions_polars(ynab_path)
    ordered: list[Path] = []
    seen: set[Path] = set()
    for source in context.config.sources:
        if source.kind not in CARD_SOURCE_KINDS:
            continue
        account_mappings = _load_card_account_mappings(context, source)
        previous_root_name = PREVIOUS_CARD_ROOTS[source.kind]
        for suffix, account_name in account_mappings:
            account_dir = (defaults.derived_root / previous_root_name / suffix).resolve()
            if not account_dir.exists():
                continue
            candidates = [
                path.resolve()
                for path in sorted(account_dir.glob(f"*_{source.kind}_norm.parquet"))
                if _statement_cycle_from_path(path, kind=source.kind) <= run_cycle
            ]
            if not candidates:
                continue
            ynab_rows = _build_card_ynab_rows_for_account(ynab_df, account_name=account_name)
            selected: list[Path] = []
            for path in sorted(
                candidates,
                key=lambda candidate: _statement_cycle_from_path(candidate, kind=source.kind),
                reverse=True,
            ):
                if _statement_is_fully_reconciled(
                    path,
                    account_name=account_name,
                    ynab_rows=ynab_rows,
                ):
                    break
                selected.append(path)
            for path in reversed(selected):
                if path in seen:
                    continue
                ordered.append(path)
                seen.add(path)
    return ordered


def _merge_active_and_extra_paths(
    active_paths: list[Path],
    extra_paths: list[Path],
) -> list[Path]:
    active_resolved = {path.resolve() for path in active_paths}
    return active_paths + [
        path for path in extra_paths if path.resolve() not in active_resolved
    ]


def resolve_source_window_paths(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_paths: context_config.ContextRunPaths,
    *,
    run_tag: str,
) -> list[Path]:
    active_paths = context_config.resolve_context_normalized_source_paths(context, run_paths)
    previous_paths = _list_previous_card_snapshot_candidates(
        context,
        defaults,
        run_tag=run_tag,
    )
    return _merge_active_and_extra_paths(active_paths, previous_paths)


def resolve_review_source_paths(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_paths: context_config.ContextRunPaths,
    *,
    run_tag: str,
    ynab_path: Path,
) -> list[Path]:
    active_paths = context_config.resolve_context_normalized_source_paths(context, run_paths)
    carryforward_paths = _resolve_previous_card_snapshot_paths(
        context,
        defaults,
        run_tag=run_tag,
        ynab_path=ynab_path,
    )
    return _merge_active_and_extra_paths(active_paths, carryforward_paths)


def infer_source_window(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_paths: context_config.ContextRunPaths,
    *,
    run_tag: str,
    padding_days: int,
) -> tuple[str, str] | None:
    if padding_days < 0:
        raise ValueError("--source-window-padding-days cannot be negative.")

    _validate_latest_previous_snapshots_prepared(
        context,
        defaults,
        run_tag=run_tag,
    )

    min_dates: list[date] = []
    max_dates: list[date] = []
    source_paths = resolve_source_window_paths(
        context,
        defaults,
        run_paths,
        run_tag=run_tag,
    )
    for source_path in source_paths:
        frame = read_transactions_polars(source_path)
        if "date" not in frame.columns:
            raise ValueError(f"{source_path} is missing required date column.")
        dates = (
            frame.select(
                pl.col("date")
                .cast(pl.String, strict=False)
                .fill_null("")
                .str.strip_chars()
                .alias("date")
            )
            .filter(pl.col("date") != "")
        )
        if dates.is_empty():
            continue
        stats = dates.select(
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        ).row(0, named=True)
        min_dates.append(date.fromisoformat(str(stats["min_date"])))
        max_dates.append(date.fromisoformat(str(stats["max_date"])))

    if not min_dates:
        return None

    since = min(min_dates) - timedelta(days=padding_days)
    until = max(max_dates) + timedelta(days=padding_days)
    return since.isoformat(), until.isoformat()
