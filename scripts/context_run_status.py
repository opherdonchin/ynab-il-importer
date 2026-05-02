# ruff: noqa: E402

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.context_config as context_config
import ynab_il_importer.context_run_status as context_run_status


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect one context/run-tag workflow end to end from the canonical "
            "artifacts, existing reports, and optional live dry-run checks."
        )
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument("run_tag", help="Run folder name, for example: 2026_04_28")
    parser.add_argument(
        "--verify-live",
        action="store_true",
        help=(
            "Run live YNAB dry-run verification for closeout checks using the same "
            "reconciliation logic as the workflow scripts."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of the human summary.",
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
    parser.add_argument(
        "--budget-id",
        dest="budget_id",
        default="",
        help=(
            "Override the target context budget id instead of resolving it from "
            "the context env binding."
        ),
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    status = context_run_status.collect_context_run_status(
        context_name=args.context,
        run_tag=args.run_tag,
        defaults_path=args.defaults_path,
        contexts_root=args.contexts_root,
        budget_id=args.budget_id,
        verify_live=args.verify_live,
    )
    if args.json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return
    print(context_run_status.render_context_run_status(status))


if __name__ == "__main__":
    main()
