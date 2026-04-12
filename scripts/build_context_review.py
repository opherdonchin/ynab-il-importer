# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import build_proposed_transactions
import ynab_il_importer.context_config as context_config


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

    source_paths = context_config.resolve_context_normalized_source_paths(context, run_paths)
    ynab_path = context_config.resolve_context_ynab_path(context, run_paths)
    out_path = run_paths.proposal_review_path(defaults, context.name)
    pairs_out = run_paths.matched_pairs_path(defaults, context.name)

    build_proposed_transactions.run_build(
        source_paths=source_paths,
        ynab_path=ynab_path,
        map_path=context.payee_map_path,
        out_path=out_path,
        pairs_out=str(pairs_out),
        include_reconciled_ynab=args.include_reconciled_ynab,
    )


if __name__ == "__main__":
    main()
