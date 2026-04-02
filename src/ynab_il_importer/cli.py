import argparse
from pathlib import Path

import pandas as pd
from ynab_il_importer.artifacts.transaction_io import write_flat_transaction_artifacts
import ynab_il_importer.export as export
import ynab_il_importer.io_leumi as leumi
import ynab_il_importer.io_leumi_xls as leumi_xls
import ynab_il_importer.io_max as maxio
import ynab_il_importer.io_ynab as ynab
import ynab_il_importer.pairing as pairing
import ynab_il_importer.rules as rules

try:
    import typer
except ModuleNotFoundError:  # pragma: no cover - fallback for bare Python envs
    typer = None


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _print_wrote(path: Path, row_count: int) -> None:
    print(export.wrote_message(path, row_count))


def _write_normalized_with_parquet(df: pd.DataFrame, out_path: Path, *, fmt: str) -> None:
    source_series = (
        df["source"].astype("string").fillna("").str.strip()
        if "source" in df.columns
        else pd.Series([""] * len(df), index=df.index, dtype="string")
    )
    source_system = str(source_series.iloc[0] if not source_series.empty else "").strip() or fmt
    _, parquet_path = write_flat_transaction_artifacts(
        df,
        out_path,
        artifact_kind="normalized_source_transaction",
        source_system=source_system,
    )
    print(f"Wrote canonical parquet to {parquet_path}")
    _print_wrote(out_path, len(df))


def _fill_and_validate_ynab_account(df: pd.DataFrame, fallback_account_name: str) -> pd.DataFrame:
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


def _load_many_csvs(paths: list[Path], label: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        df = pd.read_csv(path)
        if "account_name" not in df.columns:
            raise ValueError(f"{label} file missing account_name column: {path}")
        df["account_name"] = df["account_name"].astype("string").fillna("").str.strip()
        if (df["account_name"] == "").any():
            raise ValueError(f"{label} file has empty account_name rows: {path}")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _build_groups_df(pairs: pd.DataFrame) -> pd.DataFrame:
    if "fingerprint" not in pairs.columns:
        raise ValueError("Input pairs file must include fingerprint column")

    def _most_common_text(series: pd.Series) -> str:
        clean = series.astype("string").fillna("").str.strip()
        clean = clean[clean != ""]
        if clean.empty:
            return ""
        return str(clean.value_counts().index[0])

    def _top_counts(series: pd.Series, limit: int = 3) -> str:
        clean = series.astype("string").fillna("").str.strip()
        clean = clean[clean != ""]
        if clean.empty:
            return ""
        top = clean.value_counts().head(limit)
        return "; ".join(f"{name} ({count})" for name, count in top.items())

    grouped = (
        pairs.groupby(["fingerprint"], dropna=False)
        .agg(
            count=("fingerprint", "size"),
            example_raw_text=("raw_text", _most_common_text),
            top_ynab_payees=("ynab_payee_raw", _top_counts),
            top_ynab_categories=("ynab_category_raw", _top_counts),
            canonical_payee=("ynab_payee_raw", _unique_values),
        )
        .reset_index()
    )
    return grouped


def _unique_values(series: pd.Series) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    ordered = clean.value_counts().index.tolist()
    return "; ".join(str(value) for value in ordered)


def _resolve_account_column(df: pd.DataFrame) -> pd.Series:
    if "account_name" in df.columns:
        return df["account_name"]
    for col in df.columns:
        if str(col).strip().lower() == "account":
            return df[col]
    raise ValueError("No account column found. Expected 'account_name' or 'Account'.")


def _load_csv_paths(paths: list[Path], label: str) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"{label} file does not exist: {path}")
        frames.append(pd.read_csv(path))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _top_examples(series: pd.Series, limit: int = 2) -> str:
    clean = series.astype("string").fillna("").str.strip()
    examples: list[str] = []
    for value in clean.tolist():
        if not value or value in examples:
            continue
        examples.append(value)
        if len(examples) == limit:
            break
    return " | ".join(examples)


def _top_counts(series: pd.Series, limit: int = 3) -> str:
    clean = series.astype("string").fillna("").str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return ""
    counts = clean.value_counts().head(limit)
    return "; ".join(f"{name} ({count})" for name, count in counts.items())


def _count_unique_rule_ids(series: pd.Series) -> int:
    clean = series.astype("string").fillna("").str.strip()
    rule_ids: set[str] = set()
    for value in clean.tolist():
        if not value:
            continue
        for token in value.split(";"):
            token_norm = token.strip()
            if token_norm:
                rule_ids.add(token_norm)
    return len(rule_ids)


def _derive_candidate_status(series: pd.Series) -> str:
    statuses = {s for s in series.astype("string").fillna("").str.strip().tolist() if s}
    if not statuses or statuses == {"none"}:
        return "unmatched"
    if "ambiguous" in statuses or "none" in statuses:
        return "ambiguous"
    return "matched_uniquely"


def _build_hint_distributions(matched_pairs: pd.DataFrame) -> pd.DataFrame:
    if matched_pairs.empty:
        return pd.DataFrame(
            columns=[
                "fingerprint",
                "suggested_payee_distribution",
                "suggested_category_distribution",
            ]
        )

    prepared = rules.prepare_transactions_for_rules(matched_pairs)
    prepared["ynab_payee_raw"] = (
        matched_pairs["ynab_payee_raw"].astype("string").fillna("").str.strip()
        if "ynab_payee_raw" in matched_pairs.columns
        else ""
    )
    prepared["ynab_category_raw"] = (
        matched_pairs["ynab_category_raw"].astype("string").fillna("").str.strip()
        if "ynab_category_raw" in matched_pairs.columns
        else ""
    )

    hints = (
        prepared.groupby("fingerprint", dropna=False)
        .agg(
            suggested_payee_distribution=("ynab_payee_raw", _top_counts),
            suggested_category_distribution=("ynab_category_raw", _top_counts),
        )
        .reset_index()
    )
    return hints


def _run_build_payee_map(
    parsed_paths: list[Path],
    matched_pairs_paths: list[Path],
    out_dir: Path,
    map_path: Path = Path("mappings/payee_map.csv"),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed_raw = _load_csv_paths(parsed_paths, "parsed")
    if parsed_raw.empty:
        raise ValueError("No rows found in --parsed inputs.")

    parsed_prepared = rules.prepare_transactions_for_rules(parsed_raw)
    rules_df = rules.load_payee_map(map_path)
    applied = rules.apply_payee_map_rules(parsed_prepared, rules_df)
    preview = parsed_prepared.join(applied)

    matched_pairs = _load_csv_paths(matched_pairs_paths, "matched-pairs")
    hints = _build_hint_distributions(matched_pairs)

    candidate_group_keys = ["txn_kind", "fingerprint", "description_clean_norm"]
    candidates = (
        preview.groupby(candidate_group_keys, dropna=False)
        .agg(
            count_in_period=("fingerprint", "size"),
            examples=("example_text", _top_examples),
            existing_rules_hit_count=("match_candidate_rule_ids", _count_unique_rule_ids),
            status=("match_status", _derive_candidate_status),
        )
        .reset_index()
    )

    if hints.empty:
        candidates["suggested_payee_distribution"] = ""
        candidates["suggested_category_distribution"] = ""
    else:
        candidates = candidates.merge(hints, on="fingerprint", how="left")
        candidates["suggested_payee_distribution"] = (
            candidates["suggested_payee_distribution"].astype("string").fillna("")
        )
        candidates["suggested_category_distribution"] = (
            candidates["suggested_category_distribution"].astype("string").fillna("")
        )

    candidates = candidates[
        [
            "txn_kind",
            "fingerprint",
            "description_clean_norm",
            "count_in_period",
            "examples",
            "suggested_payee_distribution",
            "suggested_category_distribution",
            "existing_rules_hit_count",
            "status",
        ]
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    candidates_out = out_dir / "payee_map_candidates.csv"
    preview_out = out_dir / "payee_map_applied_preview.csv"
    candidates.to_csv(candidates_out, index=False, encoding="utf-8-sig")
    preview.to_csv(preview_out, index=False, encoding="utf-8-sig")
    _print_wrote(candidates_out, len(candidates))
    _print_wrote(preview_out, len(preview))
    return candidates, preview


if typer is not None:
    app = typer.Typer(help="YNAB IL Importer CLI")

    @app.command("parse-leumi-xls")
    def parse_leumi_xls(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = leumi_xls.read_raw(in_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="leumi_xls")

    @app.command("parse-max")
    def parse_max(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = maxio.read_raw(in_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="max")

    @app.command("parse-ynab")
    def parse_ynab(
        in_path: Path = typer.Option(..., "--in"),
        account_name: str = typer.Option("", "--account-name"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = _fill_and_validate_ynab_account(ynab.read_raw(in_path), account_name)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="ynab")

    @app.command("parse-leumi")
    def parse_leumi(
        in_path: Path = typer.Option(..., "--in"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        df = leumi.read_raw(in_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="leumi")

    @app.command("match-pairs")
    def match_pairs(
        source_paths: list[Path] = typer.Option(None, "--source"),
        ynab_path: Path = typer.Option(..., "--ynab"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        sources = source_paths or []
        if not sources:
            raise ValueError("Provide at least one --source input.")

        source_df = _load_many_csvs(sources, "source")
        if "source" not in source_df.columns:
            raise ValueError("source inputs must include a 'source' column.")
        ynab_df = pd.read_csv(ynab_path)
        if "account_name" not in ynab_df.columns:
            raise ValueError(f"ynab file missing account_name column: {ynab_path}")
        ynab_df["account_name"] = ynab_df["account_name"].astype("string").fillna("").str.strip()
        if (ynab_df["account_name"] == "").any():
            raise ValueError("ynab file has empty account_name rows.")

        pairs_df = pairing.match_pairs(source_df, ynab_df)
        _ensure_parent(out_path)
        pairs_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        _print_wrote(out_path, len(pairs_df))

    @app.command("build-groups")
    def build_groups(
        pairs_path: Path = typer.Option(..., "--pairs"),
        out_path: Path = typer.Option(..., "--out"),
    ) -> None:
        pairs = pd.read_csv(pairs_path)
        grouped = _build_groups_df(pairs)
        _ensure_parent(out_path)
        grouped.to_csv(out_path, index=False, encoding="utf-8-sig")
        _print_wrote(out_path, len(grouped))

    @app.command("build-payee-map")
    def build_payee_map(
        parsed_paths: list[Path] = typer.Option(None, "--parsed"),
        matched_pairs_paths: list[Path] = typer.Option(None, "--matched-pairs"),
        out_dir: Path = typer.Option(..., "--out-dir"),
        map_path: Path = typer.Option(Path("mappings/payee_map.csv"), "--map-path"),
    ) -> None:
        parsed = parsed_paths or []
        matched = matched_pairs_paths or []
        if not parsed:
            raise ValueError("Provide at least one --parsed input.")
        _run_build_payee_map(parsed, matched, out_dir, map_path)

    @app.command("list-accounts")
    def list_accounts(
        in_path: Path = typer.Option(..., "--in"),
    ) -> None:
        df = pd.read_csv(in_path)
        accounts = _resolve_account_column(df).astype("string").fillna("").str.strip()
        unique_accounts = sorted({value for value in accounts.tolist() if value}, key=str.casefold)
        for account in unique_accounts:
            print(account)

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

    match_pairs_parser = subparsers.add_parser("match-pairs")
    match_pairs_parser.add_argument("--source", action="append", default=[])
    match_pairs_parser.add_argument("--ynab", required=True)
    match_pairs_parser.add_argument("--out", required=True)

    build_groups_parser = subparsers.add_parser("build-groups")
    build_groups_parser.add_argument("--pairs", required=True)
    build_groups_parser.add_argument("--out", required=True)

    build_payee_map_parser = subparsers.add_parser("build-payee-map")
    build_payee_map_parser.add_argument("--parsed", action="append", required=True)
    build_payee_map_parser.add_argument("--matched-pairs", action="append", default=[])
    build_payee_map_parser.add_argument("--out-dir", required=True)
    build_payee_map_parser.add_argument("--map-path", default=str(Path("mappings/payee_map.csv")))

    list_accounts_parser = subparsers.add_parser("list-accounts")
    list_accounts_parser.add_argument("--in", dest="in_path", required=True)

    args = parser.parse_args()
    if args.command == "parse-leumi-xls":
        df = leumi_xls.read_raw(args.in_path)
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="leumi_xls")
    elif args.command == "parse-max":
        df = maxio.read_raw(args.in_path)
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="max")
    elif args.command == "parse-ynab":
        df = _fill_and_validate_ynab_account(ynab.read_raw(args.in_path), args.account_name)
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="ynab")
    elif args.command == "parse-leumi":
        df = leumi.read_raw(args.in_path)
        out_path = Path(args.out_path)
        _ensure_parent(out_path)
        _write_normalized_with_parquet(df, out_path, fmt="leumi")
    elif args.command == "match-pairs":
        if not args.source:
            raise ValueError("Provide at least one --source input.")
        source_df = _load_many_csvs([Path(p) for p in args.source], "source")
        if "source" not in source_df.columns:
            raise ValueError("source inputs must include a 'source' column.")
        ynab_df = pd.read_csv(Path(args.ynab))
        if "account_name" not in ynab_df.columns:
            raise ValueError(f"ynab file missing account_name column: {args.ynab}")
        ynab_df["account_name"] = ynab_df["account_name"].astype("string").fillna("").str.strip()
        if (ynab_df["account_name"] == "").any():
            raise ValueError("ynab file has empty account_name rows.")
        pairs_df = pairing.match_pairs(source_df, ynab_df)
        out_path = Path(args.out)
        _ensure_parent(out_path)
        pairs_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        _print_wrote(out_path, len(pairs_df))
    elif args.command == "build-groups":
        pairs = pd.read_csv(Path(args.pairs))
        grouped = _build_groups_df(pairs)
        out_path = Path(args.out)
        _ensure_parent(out_path)
        grouped.to_csv(out_path, index=False, encoding="utf-8-sig")
        _print_wrote(out_path, len(grouped))
    elif args.command == "build-payee-map":
        parsed = [Path(p) for p in args.parsed]
        matched = [Path(p) for p in args.matched_pairs]
        _run_build_payee_map(parsed, matched, Path(args.out_dir), Path(args.map_path))
    elif args.command == "list-accounts":
        df = pd.read_csv(Path(args.in_path))
        accounts = _resolve_account_column(df).astype("string").fillna("").str.strip()
        unique_accounts = sorted({value for value in accounts.tolist() if value}, key=str.casefold)
        for account in unique_accounts:
            print(account)


def main() -> None:
    if typer is not None:
        app()
    else:
        _fallback_main()


if __name__ == "__main__":
    main()
