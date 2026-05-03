# ruff: noqa: E402

import argparse
import csv
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import build_proposed_transactions
import ynab_il_importer.card_identity as card_identity
import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.context_config as context_config
from ynab_il_importer.artifacts.transaction_io import read_transactions_polars

CARD_SOURCE_KINDS = {"max", "leumi_card_html"}
PREVIOUS_CARD_ROOTS = {
    "max": "previous_max",
    "leumi_card_html": "previous_leumi_card",
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


def _resolve_review_source_paths(
    context: context_config.LoadedContext,
    defaults: context_config.DefaultsConfig,
    run_paths: context_config.ContextRunPaths,
    *,
    run_tag: str,
) -> list[Path]:
    active_paths = context_config.resolve_context_normalized_source_paths(
        context, run_paths
    )
    carryforward_paths = _resolve_previous_card_snapshot_paths(
        context,
        defaults,
        run_tag=run_tag,
        ynab_path=context_config.resolve_context_ynab_path(context, run_paths),
    )
    active_resolved = {path.resolve() for path in active_paths}
    return active_paths + [
        path for path in carryforward_paths if path.resolve() not in active_resolved
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build institutional review rows for one context/run-tag pair."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_01")
    parser.add_argument(
        "--defaults",
        dest="defaults_path",
        type=Path,
        default=context_config.DEFAULTS_PATH,
        help="Defaults TOML path.",
    )
    parser.add_argument(
        "--contexts-root",
        dest="contexts_root",
        type=Path,
        default=context_config.CONTEXTS_ROOT,
        help="Contexts root directory.",
    )
    parser.add_argument(
        "--include-reconciled-ynab",
        action="store_true",
        help=(
            "Include already reconciled YNAB transactions in the review artifact "
            "instead of skipping them by default."
        ),
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    run_paths = context_config.resolve_run_paths(defaults, run_tag=args.run_tag)
    run_paths.paired_dir.mkdir(parents=True, exist_ok=True)

    source_paths = _resolve_review_source_paths(
        context,
        defaults,
        run_paths,
        run_tag=args.run_tag,
    )
    target_account_names = context_config.resolve_context_target_account_names(context)
    ynab_path = context_config.resolve_context_ynab_path(context, run_paths)
    out_path = run_paths.proposal_review_path(defaults, context.name)
    pairs_out = run_paths.matched_pairs_path(defaults, context.name)

    build_proposed_transactions.run_build(
        source_paths=source_paths,
        ynab_path=ynab_path,
        map_path=context.payee_map_path,
        fingerprint_map_path=context.fingerprint_map_path,
        out_path=out_path,
        pairs_out=str(pairs_out),
        allowed_target_accounts=target_account_names,
        include_reconciled_ynab=args.include_reconciled_ynab,
    )


if __name__ == "__main__":
    main()
