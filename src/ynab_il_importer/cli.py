import argparse
from pathlib import Path

import pandas as pd
import polars as pl
from ynab_il_importer.artifacts.transaction_io import (
    write_canonical_transaction_artifacts,
    write_flat_transaction_artifacts,
)
import ynab_il_importer.export as export
import ynab_il_importer.io_leumi as leumi
import ynab_il_importer.io_leumi_xls as leumi_xls
import ynab_il_importer.io_max as maxio
import ynab_il_importer.io_ynab as ynab

try:
    import typer
except ModuleNotFoundError:  # pragma: no cover - fallback for bare Python envs
    typer = None


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _print_wrote(path: Path, row_count: int) -> None:
    print(export.wrote_message(path, row_count))


def _write_normalized_with_parquet(
    df: pd.DataFrame, out_path: Path, *, fmt: str
) -> None:
    if "source" in df.columns and len(df) > 0:
        v = df["source"].iloc[0]
        source_system = str(v).strip() if v is not None and str(v).strip() else fmt
    else:
        source_system = fmt
    _, parquet_path = write_flat_transaction_artifacts(
        pl.from_pandas(df),
        out_path,
        artifact_kind="normalized_source_transaction",
        source_system=source_system,
    )
    print(f"Wrote canonical parquet to {parquet_path}")
    _print_wrote(out_path, len(df))


def _write_normalized_from_module(
    module, in_path: Path, out_path: Path, *, fmt: str
) -> None:
    df = module.read_raw(in_path)
    if hasattr(module, "read_canonical"):
        canonical = module.read_canonical(in_path)
        _, parquet_path = write_canonical_transaction_artifacts(
            canonical,
            out_path,
            csv_projection=pl.from_pandas(df),
        )
        print(f"Wrote canonical parquet to {parquet_path}")
        _print_wrote(out_path, len(df))
        return
    _write_normalized_with_parquet(df, out_path, fmt=fmt)


def _fill_and_validate_ynab_account(
    df: pd.DataFrame, fallback_account_name: str
) -> pd.DataFrame:
    out = df.copy()
    if "account_name" not in out.columns:
        out["account_name"] = ""
    out["account_name"] = out["account_name"].astype("string").fillna("").str.strip()
    fallback = str(fallback_account_name).strip()
    if fallback:
        out.loc[out["account_name"] == "", "account_name"] = fallback
    if (out["account_name"] == "").any():
        raise ValueError(
            "YNAB data has empty account_name. Provide --account-name or fix source data."
        )
    return out


if typer is not None:
    app = typer.Typer(help="YNAB IL Importer CLI")

    @app.command("parse-leumi-xls")
    def parse_leumi_xls(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        _ensure_parent(out_path)
        _write_normalized_from_module(leumi_xls, in_path, out_path, fmt="leumi_xls")

    @app.command("parse-max")
    def parse_max(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        _ensure_parent(out_path)
        _write_normalized_from_module(maxio, in_path, out_path, fmt="max")

    @app.command("parse-ynab")
    def parse_ynab(
        in_path: Path = typer.Option(..., "--in"),
        account_name: str = typer.Option("", "--account-name"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = _fill_and_validate_ynab_account(ynab.read_raw(in_path), account_name)
        _ensure_parent(out_path)
        if hasattr(ynab, "read_canonical"):
            canonical = ynab.read_canonical(in_path)
            canonical_pl = pl.from_arrow(canonical)
            if "account_name" not in canonical_pl.columns:
                canonical_pl = canonical_pl.with_columns(pl.lit("").alias("account_name"))
            canonical_pl = canonical_pl.with_columns(
                pl.col("account_name").cast(pl.Utf8).fill_null("").str.strip_chars()
            )
            if account_name:
                canonical_pl = canonical_pl.with_columns(
                    pl.when(pl.col("account_name") == "")
                    .then(pl.lit(account_name))
                    .otherwise(pl.col("account_name"))
                    .alias("account_name")
                )
            canonical = canonical_pl.to_arrow()
            _, parquet_path = write_canonical_transaction_artifacts(
                canonical,
                out_path,
                csv_projection=pl.from_pandas(df),
            )
            print(f"Wrote canonical parquet to {parquet_path}")
            _print_wrote(out_path, len(df))
        else:
            _write_normalized_with_parquet(df, out_path, fmt="ynab")

    @app.command("parse-leumi")
    def parse_leumi(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        _ensure_parent(out_path)
        _write_normalized_from_module(leumi, in_path, out_path, fmt="leumi")

else:
    app = None


def _fallback_main() -> None:
    parser = argparse.ArgumentParser(prog="ynab-il", description="YNAB IL Importer CLI")
    subparsers = parser.add_subparsers(dest="command")

    parse_leumi_xls_parser = subparsers.add_parser("parse-leumi-xls")
    parse_leumi_xls_parser.add_argument("--in", dest="in_path", required=True)
    parse_leumi_xls_parser.add_argument("--out", dest="out_path", required=True)

    parse_max_parser = subparsers.add_parser("parse-max")
    parse_max_parser.add_argument("--in", dest="in_path", required=True)
    parse_max_parser.add_argument("--out", dest="out_path", required=True)

    parse_ynab_parser = subparsers.add_parser("parse-ynab")
    parse_ynab_parser.add_argument("--in", dest="in_path", required=True)
    parse_ynab_parser.add_argument("--account-name", dest="account_name", default="")
    parse_ynab_parser.add_argument("--out", dest="out_path", required=True)

    parse_leumi_parser = subparsers.add_parser("parse-leumi")
    parse_leumi_parser.add_argument("--in", dest="in_path", required=True)
    parse_leumi_parser.add_argument("--out", dest="out_path", required=True)

    args = parser.parse_args()
    if args.command == "parse-leumi-xls":
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_from_module(
            leumi_xls, Path(args.in_path), out_path, fmt="leumi_xls"
        )
    elif args.command == "parse-max":
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_from_module(maxio, Path(args.in_path), out_path, fmt="max")
    elif args.command == "parse-ynab":
        df = _fill_and_validate_ynab_account(
            ynab.read_raw(args.in_path), args.account_name
        )
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        if hasattr(ynab, "read_canonical"):
            canonical = ynab.read_canonical(args.in_path)
            canonical_pl = pl.from_arrow(canonical)
            if "account_name" not in canonical_pl.columns:
                canonical_pl = canonical_pl.with_columns(pl.lit("").alias("account_name"))
            canonical_pl = canonical_pl.with_columns(
                pl.col("account_name").cast(pl.Utf8).fill_null("").str.strip_chars()
            )
            if args.account_name:
                canonical_pl = canonical_pl.with_columns(
                    pl.when(pl.col("account_name") == "")
                    .then(pl.lit(args.account_name))
                    .otherwise(pl.col("account_name"))
                    .alias("account_name")
                )
            canonical = canonical_pl.to_arrow()
            _, parquet_path = write_canonical_transaction_artifacts(
                canonical,
                out_path,
                csv_projection=pl.from_pandas(df),
            )
            print(f"Wrote canonical parquet to {parquet_path}")
            _print_wrote(out_path, len(df))
        else:
            _write_normalized_with_parquet(df, out_path, fmt="ynab")
    elif args.command == "parse-leumi":
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_from_module(leumi, Path(args.in_path), out_path, fmt="leumi")


def main() -> None:
    if typer is not None:
        app()
    else:
        _fallback_main()


if __name__ == "__main__":
    main()
