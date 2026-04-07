from __future__ import annotations

from pathlib import Path
import warnings

import pandas as pd

from ynab_il_importer.artifacts.transaction_io import (
    write_canonical_transaction_artifacts,
    write_flat_transaction_artifacts,
)
import ynab_il_importer.export as export
import ynab_il_importer.io_leumi as leumi
import ynab_il_importer.io_leumi_card_html as leumi_card_html
import ynab_il_importer.io_leumi_xls as leumi_xls
import ynab_il_importer.io_max as maxio
import ynab_il_importer.io_ynab as ynab


FORMAT_MODULES = {
    "leumi": leumi,
    "leumi_card_html": leumi_card_html,
    "leumi_xls": leumi_xls,
    "max": maxio,
    "ynab": ynab,
}

DETECT_ORDER = [
    ("leumi", leumi),
    ("leumi_card_html", leumi_card_html),
    ("leumi_xls", leumi_xls),
    ("max", maxio),
    ("ynab", ynab),
]


def default_out_path(in_path: Path, fmt: str, out_dir: Path) -> Path:
    stem = in_path.stem
    return out_dir / f"{stem}_{fmt}_norm.csv"


def normalize_one(
    in_path: Path,
    fmt: str,
    out_path: Path,
    *,
    use_fingerprint_map: bool,
    account_map_path: Path,
    fingerprint_map_path: Path,
    fingerprint_log_path: Path,
) -> None:
    module = FORMAT_MODULES[fmt]
    if not module.is_proper_format(in_path):
        raise ValueError(f"{in_path} does not look like a valid {fmt} file.")
    df = module.read_raw(
        in_path,
        use_fingerprint_map=use_fingerprint_map,
        account_map_path=account_map_path,
        fingerprint_map_path=fingerprint_map_path,
        fingerprint_log_path=fingerprint_log_path,
    )
    if hasattr(module, "read_canonical"):
        canonical = module.read_canonical(
            in_path,
            use_fingerprint_map=use_fingerprint_map,
            account_map_path=account_map_path,
            fingerprint_map_path=fingerprint_map_path,
            fingerprint_log_path=fingerprint_log_path,
        )
        _, parquet_path = write_canonical_transaction_artifacts(
            canonical,
            out_path,
            csv_projection=df,
        )
    else:
        _, parquet_path = write_flat_transaction_artifacts(
            df,
            out_path,
            artifact_kind="normalized_source_transaction",
            source_system=str(
                df.get("source", pd.Series([""])).astype("string").fillna("").iloc[0] or fmt
            ),
        )
    print(f"Wrote canonical parquet to {parquet_path}")
    print(export.wrote_message(out_path, len(df)))


def normalize_dir(
    dir_path: Path,
    out_dir: Path,
    *,
    use_fingerprint_map: bool,
    account_map_path: Path,
    fingerprint_map_path: Path,
    fingerprint_log_path: Path,
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

        fmt, _ = matched
        out_path = default_out_path(path, fmt, out_dir)
        try:
            normalize_one(
                path,
                fmt,
                out_path,
                use_fingerprint_map=use_fingerprint_map,
                account_map_path=account_map_path,
                fingerprint_map_path=fingerprint_map_path,
                fingerprint_log_path=fingerprint_log_path,
            )
        except Exception as exc:
            warnings.warn(f"Failed to parse {path} as {fmt}: {exc}", UserWarning)
