# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.normalize_runner as normalize_runner
import ynab_il_importer.workflow_profiles as workflow_profiles


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize input files")
    parser.add_argument("--profile", default="", help="Workflow profile (for default paths).")

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--leumi", dest="leumi_path", type=Path)
    group.add_argument("--leumi-card-html", dest="leumi_card_html_path", type=Path)
    group.add_argument("--leumi-xls", dest="leumi_xls_path", type=Path)
    group.add_argument("--max", dest="max_path", type=Path)
    group.add_argument("--ynab", dest="ynab_path", type=Path)
    group.add_argument("--dir", dest="dir_path", type=Path)

    parser.add_argument("--out", dest="out_path", type=Path, required=False)
    parser.add_argument("--out-dir", dest="out_dir", type=Path, default=None)
    parser.add_argument("--account-map", dest="account_map_path", type=Path, default=None)
    parser.add_argument(
        "--fingerprint-map",
        dest="fingerprint_map_path",
        type=Path,
        default=None,
        help="Override fingerprint_map.csv path.",
    )
    parser.add_argument(
        "--fingerprint-log",
        dest="fingerprint_log_path",
        type=Path,
        default=None,
        help="Override fingerprint log output path.",
    )
    parser.add_argument(
        "--no-fingerprint-map",
        dest="no_fingerprint_map",
        action="store_true",
        help="Skip fingerprint_map.csv when generating fingerprints.",
    )

    args = parser.parse_args()
    use_fingerprint_map = not args.no_fingerprint_map
    profile = workflow_profiles.resolve_profile(args.profile or None)
    out_dir = args.out_dir or (Path("data/derived") / profile.name)
    account_map_path = args.account_map_path or profile.account_map_path
    fingerprint_map_path = args.fingerprint_map_path or profile.fingerprint_map_path
    fingerprint_log_path = args.fingerprint_log_path or (
        Path("outputs") / profile.name / "fingerprint_log.csv"
    )

    if args.dir_path is not None:
        if args.out_path is not None:
            raise ValueError("--out is only valid for single-file normalization.")
        normalize_runner.normalize_dir(
            args.dir_path,
            out_dir,
            use_fingerprint_map=use_fingerprint_map,
            account_map_path=account_map_path,
            fingerprint_map_path=fingerprint_map_path,
            fingerprint_log_path=fingerprint_log_path,
        )
        return

    single_inputs = {
        "leumi": args.leumi_path,
        "leumi_card_html": args.leumi_card_html_path,
        "leumi_xls": args.leumi_xls_path,
        "max": args.max_path,
        "ynab": args.ynab_path,
    }
    provided = {fmt: path for fmt, path in single_inputs.items() if path is not None}
    if len(provided) != 1:
        raise ValueError(
            "Provide exactly one of --leumi, --leumi-xls, --max, --ynab, or use --dir."
        )

    fmt, in_path = next(iter(provided.items()))
    if not in_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {in_path}")

    out_path = args.out_path or normalize_runner.default_out_path(in_path, fmt, out_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    normalize_runner.normalize_one(
        in_path,
        fmt,
        out_path,
        use_fingerprint_map=use_fingerprint_map,
        account_map_path=account_map_path,
        fingerprint_map_path=fingerprint_map_path,
        fingerprint_log_path=fingerprint_log_path,
    )


if __name__ == "__main__":
    main()
