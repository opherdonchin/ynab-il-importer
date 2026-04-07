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
        normalized_name = source.normalized_name or f"{source.raw_path.stem}_{source.kind}_norm.parquet"
        out_path = out_dir / normalized_name.replace(".parquet", ".csv")
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
