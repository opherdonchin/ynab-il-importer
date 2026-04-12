# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.context_config as context_config
import ynab_il_importer.normalize_runner as normalize_runner
from ynab_il_importer.artifacts.transaction_io import write_canonical_transaction_artifacts
import ynab_il_importer.ynab_category_source as ynab_category_source


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize the declared raw sources for one context/run-tag pair."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name under data/raw, for example: 2026_04_01")
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
        "--no-fingerprint-map",
        dest="no_fingerprint_map",
        action="store_true",
        help="Skip fingerprint_map.csv when generating fingerprints.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    run_tag = str(args.run_tag or "").strip()
    if not run_tag:
        raise ValueError("run_tag cannot be empty.")

    raw_dir = defaults.raw_root / run_tag
    out_dir = defaults.derived_root / run_tag
    out_dir.mkdir(parents=True, exist_ok=True)

    fingerprint_log_path = (
        defaults.outputs_root / context.name / defaults.files.fingerprint_log
    ).resolve()
    fingerprint_log_path.parent.mkdir(parents=True, exist_ok=True)

    resolved_sources = context_config.resolve_context_sources(context, raw_dir)
    for source in resolved_sources:
        if not source.normalized_name:
            if source.raw_path is None:
                raise ValueError(
                    f"Context source {source.id!r} must declare normalized_name."
                )
            normalized_name = f"{source.raw_path.stem}_{source.kind}_norm.parquet"
        else:
            normalized_name = source.normalized_name
        out_path = out_dir / normalized_name
        if source.kind == "ynab_category":
            from_context = context_config.load_context(
                source.from_context,
                contexts_root=args.contexts_root,
            )
            from_ynab_path = context_config.resolve_context_ynab_path(
                from_context,
                context_config.resolve_run_paths(defaults, run_tag=run_tag),
            )
            canonical = ynab_category_source.build_category_source_canonical(
                from_ynab_path,
                category_name=source.category_name,
                category_id=source.category_id,
                target_account_name=source.target_account_name,
                target_account_id=source.target_account_id,
                use_fingerprint_map=not args.no_fingerprint_map,
                fingerprint_map_path=context.fingerprint_map_path,
                fingerprint_log_path=fingerprint_log_path,
            )
            _, parquet_path = write_canonical_transaction_artifacts(canonical, out_path)
            print(f"Wrote {parquet_path} ({canonical.num_rows} rows)")
            continue

        if source.raw_path is None:
            raise ValueError(f"Context source {source.id!r} is missing raw_path.")
        normalize_runner.normalize_one(
            source.raw_path,
            source.kind,
            out_path,
            use_fingerprint_map=not args.no_fingerprint_map,
            account_map_path=context.account_map_path,
            fingerprint_map_path=context.fingerprint_map_path,
            fingerprint_log_path=fingerprint_log_path,
        )


if __name__ == "__main__":
    main()
