import argparse
import sys
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.io_leumi as leumi
import ynab_il_importer.io_leumi_xls as leumi_xls
import ynab_il_importer.io_max as maxio
import ynab_il_importer.io_ynab as ynab


FORMAT_MODULES = {
    "leumi": leumi,
    "leumi_xls": leumi_xls,
    "max": maxio,
    "ynab": ynab,
}

DETECT_ORDER = [
    ("leumi", leumi),
    ("leumi_xls", leumi_xls),
    ("max", maxio),
    ("ynab", ynab),
]


def _default_out_path(in_path: Path, fmt: str, out_dir: Path) -> Path:
    stem = in_path.stem
    return out_dir / f"{stem}_{fmt}_norm.csv"


def _normalize_one(
    in_path: Path,
    fmt: str,
    out_path: Path,
    use_fingerprint_map: bool,
) -> None:
    module = FORMAT_MODULES[fmt]
    if not module.is_proper_format(in_path):
        raise ValueError(f"{in_path} does not look like a valid {fmt} file.")
    df = module.read_raw(in_path, use_fingerprint_map=use_fingerprint_map)
    export.write_dataframe(df, out_path)
    print(export.wrote_message(out_path, len(df)))


def _normalize_dir(
    dir_path: Path,
    out_dir: Path,
    use_fingerprint_map: bool,
) -> None:
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory does not exist: {dir_path}")
    out_dir.mkdir(parents=True, exist_ok=True)
    for path in sorted(dir_path.iterdir()):
        if not path.is_file():
            continue
        matched = None
        for fmt, module in DETECT_ORDER:
            try:
                if module.is_proper_format(path):
                    matched = (fmt, module)
                    break
            except Exception:
                continue
        if matched is None:
            warnings.warn(f"Skipping {path} (no format match).", UserWarning)
            continue

        fmt, module = matched
        out_path = _default_out_path(path, fmt, out_dir)
        try:
            df = module.read_raw(path, use_fingerprint_map=use_fingerprint_map)
        except Exception as exc:
            warnings.warn(f"Failed to parse {path} as {fmt}: {exc}", UserWarning)
            continue
        export.write_dataframe(df, out_path)
        print(export.wrote_message(out_path, len(df)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize input files")

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--leumi", dest="leumi_path", type=Path)
    group.add_argument("--leumi-xls", dest="leumi_xls_path", type=Path)
    group.add_argument("--max", dest="max_path", type=Path)
    group.add_argument("--ynab", dest="ynab_path", type=Path)
    group.add_argument("--dir", dest="dir_path", type=Path)

    parser.add_argument("--out", dest="out_path", type=Path, required=False)
    parser.add_argument("--out-dir", dest="out_dir", type=Path, default=Path("data/derived"))
    parser.add_argument(
        "--no-fingerprint-map",
        dest="no_fingerprint_map",
        action="store_true",
        help="Skip fingerprint_map.csv when generating fingerprints.",
    )

    args = parser.parse_args()
    use_fingerprint_map = not args.no_fingerprint_map

    if args.dir_path is not None:
        if args.out_path is not None:
            raise ValueError("--out is only valid for single-file normalization.")
        _normalize_dir(args.dir_path, args.out_dir, use_fingerprint_map)
        return

    single_inputs = {
        "leumi": args.leumi_path,
        "leumi_xls": args.leumi_xls_path,
        "max": args.max_path,
        "ynab": args.ynab_path,
    }
    provided = {fmt: path for fmt, path in single_inputs.items() if path is not None}
    if len(provided) != 1:
        raise ValueError("Provide exactly one of --leumi, --leumi-xls, --max, --ynab, or use --dir.")

    fmt, in_path = next(iter(provided.items()))
    if not in_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {in_path}")

    out_path = args.out_path or _default_out_path(in_path, fmt, args.out_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _normalize_one(in_path, fmt, out_path, use_fingerprint_map)


if __name__ == "__main__":
    main()
