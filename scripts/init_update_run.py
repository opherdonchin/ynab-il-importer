import argparse
from pathlib import Path


def _ensure(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    print(path.as_posix())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create standard folder layout for a dated update run."
    )
    parser.add_argument(
        "--run-tag",
        required=True,
        help="Run folder name (for example: 2026_03_24).",
    )
    parser.add_argument(
        "--derived-root",
        default="data/derived",
        help="Base folder for normalized/output-derived files.",
    )
    parser.add_argument(
        "--paired-root",
        default="data/paired",
        help="Base folder for matched/review/reconcile artifacts.",
    )
    parser.add_argument(
        "--raw-root",
        default="data/raw",
        help="Base folder for raw downloaded files.",
    )
    parser.add_argument(
        "--create-raw",
        action="store_true",
        help="Also create a raw run folder under --raw-root.",
    )
    args = parser.parse_args()

    run_tag = args.run_tag.strip()
    if not run_tag:
        raise ValueError("--run-tag cannot be empty.")

    print("Created/confirmed folders:")
    _ensure(Path(args.derived_root) / run_tag)
    _ensure(Path(args.paired_root) / run_tag)
    if args.create_raw:
        _ensure(Path(args.raw_root) / run_tag)


if __name__ == "__main__":
    main()
