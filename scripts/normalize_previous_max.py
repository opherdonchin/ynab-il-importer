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
        description="Normalize explicit previous_max snapshots into canonical parquet artifacts."
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument(
        "account_suffix",
        help="Account suffix folder under data/raw/previous_max, for example: x9922",
    )
    parser.add_argument(
        "--cycle",
        default="",
        help="Optional cycle stem to normalize, for example: 2026_03",
    )
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
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)

    raw_dir = (defaults.raw_root / "previous_max" / args.account_suffix).resolve()
    out_dir = (defaults.derived_root / "previous_max" / args.account_suffix).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    fingerprint_log_path = (
        defaults.outputs_root / context.name / defaults.files.fingerprint_log
    ).resolve()
    fingerprint_log_path.parent.mkdir(parents=True, exist_ok=True)

    if args.cycle:
        candidates = [raw_dir / f"{args.cycle}.xlsx"]
    else:
        candidates = sorted(raw_dir.glob("*.xlsx"))

    if not candidates:
        raise FileNotFoundError(f"No previous_max snapshots found in {raw_dir}")

    for source_path in candidates:
        if not source_path.exists():
            raise FileNotFoundError(f"Missing previous_max snapshot: {source_path}")
        out_path = out_dir / f"{source_path.stem}_max_norm.csv"
        normalize_runner.normalize_one(
            source_path,
            "max",
            out_path,
            use_fingerprint_map=True,
            account_map_path=context.account_map_path,
            fingerprint_map_path=context.fingerprint_map_path,
            fingerprint_log_path=fingerprint_log_path,
        )


if __name__ == "__main__":
    main()
