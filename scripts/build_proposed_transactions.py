import argparse
import hashlib
import sys
import warnings
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.pairing as pairing
import ynab_il_importer.proposed_defaults as proposed_defaults
import ynab_il_importer.rules as rules_mod


def _load_csvs(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    skipped: list[str] = []
    for path in paths:
        df = pd.read_csv(path)
        if df.empty:
            warnings.warn(f"Skipping {path} (no rows).", UserWarning)
            continue
        if "fingerprint" not in df.columns:
            warnings.warn(f"Skipping {path} (missing fingerprint column).", UserWarning)
            skipped.append(str(path))
            continue
        fp = df["fingerprint"].astype("string").fillna("").str.strip()
        if (fp == "").all():
            warnings.warn(f"Skipping {path} (all fingerprint values empty).", UserWarning)
            skipped.append(str(path))
            continue
        frames.append(df)

    if not frames:
        detail = f" Skipped: {', '.join(skipped)}" if skipped else ""
        raise ValueError(
            "No usable source rows found. Ensure normalized source files with fingerprint."
            + detail
        )

    return pd.concat(frames, ignore_index=True)


def _expand_source_paths(files: list[Path], dirs: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for file_path in files:
        if not file_path.exists():
            raise FileNotFoundError(f"Source file does not exist: {file_path}")
        paths.append(file_path)

    for dir_path in dirs:
        if not dir_path.exists():
            raise FileNotFoundError(f"Source directory does not exist: {dir_path}")
        if not dir_path.is_dir():
            raise ValueError(f"Source path is not a directory: {dir_path}")
        csv_paths = sorted(dir_path.glob("*.csv"))
        if not csv_paths:
            raise ValueError(f"No CSV files found in source directory: {dir_path}")
        paths.extend(csv_paths)

    return paths


def _dedupe_source_overlaps(source_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty or "source" not in source_df.columns:
        return source_df.reset_index(drop=True)

    source_norm = source_df["source"].astype("string").fillna("").str.strip().str.lower()
    if not (source_norm == "bank").any() or not (source_norm == "card").any():
        return source_df.reset_index(drop=True)

    work = source_df.copy()
    work["_source_norm"] = source_norm
    work["_date_key"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["_outflow_key"] = pd.to_numeric(work["outflow_ils"], errors="coerce").fillna(0.0).round(2)
    work["_inflow_key"] = pd.to_numeric(work["inflow_ils"], errors="coerce").fillna(0.0).round(2)
    work["_fingerprint_key"] = work["fingerprint"].astype("string").fillna("").str.strip()

    key_cols = ["_date_key", "_outflow_key", "_inflow_key", "_fingerprint_key"]
    valid = work["_date_key"].notna() & (work["_fingerprint_key"] != "")

    bank = work.loc[(work["_source_norm"] == "bank") & valid, key_cols].copy()
    bank["_dup_rank"] = bank.groupby(key_cols, dropna=False).cumcount()

    card = work.loc[(work["_source_norm"] == "card") & valid, key_cols].copy()
    card["_dup_rank"] = card.groupby(key_cols, dropna=False).cumcount()
    card["_row_index"] = card.index

    matched_cards = card.merge(
        bank.assign(_matched=True),
        on=key_cols + ["_dup_rank"],
        how="left",
    )
    drop_index = matched_cards.loc[matched_cards["_matched"].eq(True), "_row_index"]
    if drop_index.empty:
        return source_df.reset_index(drop=True)

    warnings.warn(
        f"Dropping {len(drop_index)} bank/card overlap rows matched on date+amount+fingerprint.",
        UserWarning,
    )
    return source_df.drop(index=drop_index.to_list()).reset_index(drop=True).copy()


def _dedupe_sources(source_df: pd.DataFrame, ynab_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pairs = pairing.match_pairs(source_df, ynab_df)
    if pairs.empty:
        return source_df.copy(), pairs

    key_cols = ["account_name", "date", "outflow_ils", "inflow_ils"]
    keys = pairs[key_cols].drop_duplicates().copy()
    keys["date"] = pd.to_datetime(keys["date"], errors="coerce").dt.date
    keys["outflow_ils"] = pd.to_numeric(keys["outflow_ils"], errors="coerce").fillna(0.0)
    keys["inflow_ils"] = pd.to_numeric(keys["inflow_ils"], errors="coerce").fillna(0.0)

    source_clean = source_df.copy()
    source_clean["date"] = pd.to_datetime(source_clean["date"], errors="coerce").dt.date
    source_clean["outflow_ils"] = pd.to_numeric(
        source_clean["outflow_ils"], errors="coerce"
    ).fillna(0.0)
    source_clean["inflow_ils"] = pd.to_numeric(
        source_clean["inflow_ils"], errors="coerce"
    ).fillna(0.0)

    merged = source_clean.merge(keys, on=key_cols, how="left", indicator=True)
    is_dup = merged["_merge"] == "both"
    deduped = source_df.reset_index(drop=True).loc[~is_dup.to_numpy()].copy()
    return deduped, pairs


def _build_options(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    tx = rules_mod.prepare_transactions_for_rules(transactions)
    active_rules = rules[rules["is_active"]]
    payee_options: list[str] = []
    category_options: list[str] = []

    for _, txn in tx.iterrows():
        matched = [
            rule
            for _, rule in active_rules.iterrows()
            if rules_mod._rule_matches(rule, txn)
        ]
        payees = []
        categories = []
        for rule in matched:
            payee = str(rule.get("payee_canonical") or "").strip()
            category = str(rule.get("category_target") or "").strip()
            if payee and payee not in payees:
                payees.append(payee)
            if category and category not in categories:
                categories.append(category)
        payee_options.append("; ".join(payees))
        category_options.append("; ".join(categories))

    return pd.DataFrame({"payee_options": payee_options, "category_options": category_options}, index=tx.index)


def _rules_are_simple(rules: pd.DataFrame) -> bool:
    non_fingerprint_cols = [
        col for col in rules_mod.RULE_KEY_COLUMNS if col != "fingerprint"
    ]
    has_other_keys = rules[non_fingerprint_cols].notna() & (rules[non_fingerprint_cols] != "")
    return not has_other_keys.any().any()


def _fast_apply_rules(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    rules = rules.copy()
    rules["payee_canonical"] = rules["payee_canonical"].astype("string").fillna("")
    rules["category_target"] = rules["category_target"].astype("string").fillna("")

    grouped = (
        rules.groupby("fingerprint", dropna=False)
        .agg(
            payee_options=("payee_canonical", lambda s: "; ".join([p for p in s if p])),
            category_options=("category_target", lambda s: "; ".join([c for c in s if c])),
            rule_count=("rule_id", "size"),
            payee_single=("payee_canonical", lambda s: next((p for p in s if p), "")),
            category_single=("category_target", lambda s: next((c for c in s if c), "")),
        )
        .reset_index()
    )

    merged = transactions.merge(grouped, on="fingerprint", how="left")
    merged["rule_count"] = merged["rule_count"].fillna(0).astype(int)
    merged["match_status"] = merged["rule_count"].map(
        lambda n: "none" if n == 0 else ("unique" if n == 1 else "ambiguous")
    )
    merged["payee_selected"] = merged["payee_single"].where(merged["match_status"] == "unique", "")
    merged["category_selected"] = merged["category_single"].where(
        merged["match_status"] == "unique", ""
    )
    return merged


def _make_transaction_id(row: pd.Series) -> str:
    parts = [
        str(row.get("account_name", "")),
        str(row.get("date", "")),
        str(row.get("outflow_ils", "")),
        str(row.get("inflow_ils", "")),
        str(row.get("fingerprint", "")),
        str(row.get("raw_text", row.get("description_raw", ""))),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"txn_{digest}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build proposed_transactions.csv")
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--source-dir", action="append", default=[])
    parser.add_argument("--ynab", required=True)
    parser.add_argument("--map", dest="map_path", default=Path("mappings/payee_map.csv"))
    parser.add_argument("--out", dest="out_path", default="outputs/proposed_transactions.csv")
    parser.add_argument("--pairs-out", dest="pairs_out", default="")
    args = parser.parse_args()

    source_paths = _expand_source_paths(
        [Path(p) for p in args.source],
        [Path(p) for p in args.source_dir],
    )
    if not source_paths:
        raise ValueError("Provide at least one --source or --source-dir input.")

    source_df = _load_csvs(source_paths)
    source_df = _dedupe_source_overlaps(source_df)
    ynab_df = pd.read_csv(Path(args.ynab))
    if ynab_df.empty:
        raise ValueError("No rows found in YNAB input.")

    deduped, pairs = _dedupe_sources(source_df, ynab_df)
    if args.pairs_out:
        export.write_dataframe(pairs, args.pairs_out)
        print(export.wrote_message(args.pairs_out, len(pairs)))

    rules = rules_mod.load_payee_map(args.map_path)
    out = deduped.copy()
    out["transaction_id"] = out.apply(_make_transaction_id, axis=1)
    out["memo"] = out.get("raw_text", out.get("description_raw", ""))
    if _rules_are_simple(rules):
        out = _fast_apply_rules(out, rules)
    else:
        applied = rules_mod.apply_payee_map_rules(out, rules)
        options = _build_options(out, rules)
        out = out.join(options)
        out = out.join(applied)
        out["payee_selected"] = out["payee_canonical_suggested"].where(
            out["match_status"] == "unique", ""
        )
        out["category_selected"] = out["category_target_suggested"].where(
            out["match_status"] == "unique", ""
        )
    out = proposed_defaults.apply_default_selections(out, only_unreviewed=False)
    out["update_map"] = ""

    columns = [
        "transaction_id",
        "source",
        "account_name",
        "date",
        "outflow_ils",
        "inflow_ils",
        "memo",
        "fingerprint",
        "payee_options",
        "category_options",
        "payee_selected",
        "category_selected",
        "match_status",
        "update_map",
    ]
    optional_columns = [
        "source_account",
        "card_suffix",
        "secondary_date",
        "ref",
        "balance_ils",
        "ynab_account_id",
        "bank_txn_id",
    ]
    columns.extend([col for col in optional_columns if col in out.columns])
    out = out[columns]

    export.write_dataframe(out, args.out_path)
    print(export.wrote_message(args.out_path, len(out)))


if __name__ == "__main__":
    main()
