# ruff: noqa: E402

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.context_config as context_config
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_reconcile as review_reconcile


def _rebase_reviewed_artifact(reviewed_path: Path, proposal_path: Path) -> None:
    old_reviewed = review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(reviewed_path)
    )
    new_proposed = review_io.project_review_artifact_to_working_dataframe(
        review_io.load_review_artifact(proposal_path)
    )
    merged, _stats = review_reconcile.reconcile_reviewed_transactions(
        old_reviewed,
        new_proposed,
    )
    review_io.save_reviewed_transactions(merged, reviewed_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch the review app for one context/run-tag pair."
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
        "--resume",
        dest="resume_path",
        type=Path,
        default=None,
        help="Optional reviewed artifact to resume from. Defaults to the standard reviewed artifact path.",
    )
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Keep the review app wrapper attached instead of returning immediately.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Preferred Streamlit port.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)
    run_paths = context_config.resolve_run_paths(defaults, run_tag=args.run_tag)

    proposal_path = run_paths.proposal_review_path(defaults, context.name)
    if not proposal_path.exists():
        raise FileNotFoundError(f"Missing proposal review artifact: {proposal_path}")

    reviewed_path = args.resume_path or run_paths.reviewed_review_path(defaults, context.name)
    if reviewed_path.exists():
        _rebase_reviewed_artifact(reviewed_path, proposal_path)
    command = [
        sys.executable,
        str(ROOT / "scripts" / "review_app.py"),
        "--profile",
        context.name,
        "--in",
        str(proposal_path),
    ]
    if reviewed_path.exists():
        command.extend(["--resume", str(reviewed_path)])
    if args.foreground:
        command.append("--foreground")
    if args.port:
        command.extend(["--port", str(args.port)])

    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
