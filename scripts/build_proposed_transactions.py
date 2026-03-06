import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.export import write_dataframe
from ynab_il_importer.pairing import match_pairs
from ynab_il_importer.rules import (
    RULE_KEY_COLUMNS,
    _rule_matches,
    apply_payee_map_rules,
    load_payee_map,
    prepare_transactions_for_rules,
)


def _load_csvs(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        df = pd.read_csv(path)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _dedupe_sources(source_df: pd.DataFrame, ynab_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    pairs = match_pairs(source_df, ynab_df)
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
    deduped = source_df.loc[~is_dup].copy()
    return deduped, pairs


def _build_options(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    tx = prepare_transactions_for_rules(transactions)
    active_rules = rules[rules["is_active"]]
    payee_options: list[str] = []
    category_options: list[str] = []

    for _, txn in tx.iterrows():
        matched = [rule for _, rule in active_rules.iterrows() if _rule_matches(rule, txn)]
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
    non_fingerprint_cols = [col for col in RULE_KEY_COLUMNS if col != "fingerprint"]
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
    parser.add_argument("--source", action="append", required=True)
    parser.add_argument("--ynab", required=True)
    parser.add_argument("--map", dest="map_path", default="mappings/payee_map.csv")
    parser.add_argument("--out", dest="out_path", default="outputs/proposed_transactions.csv")
    parser.add_argument("--pairs-out", dest="pairs_out", default="")
    args = parser.parse_args()

    source_df = _load_csvs([Path(p) for p in args.source])
    if source_df.empty:
        raise ValueError("No rows found in source inputs.")
    ynab_df = pd.read_csv(Path(args.ynab))
    if ynab_df.empty:
        raise ValueError("No rows found in YNAB input.")

    deduped, pairs = _dedupe_sources(source_df, ynab_df)
    if args.pairs_out:
        write_dataframe(pairs, args.pairs_out)

    rules = load_payee_map(args.map_path)
    out = deduped.copy()
    out["transaction_id"] = out.apply(_make_transaction_id, axis=1)
    out["memo"] = out.get("raw_text", out.get("description_raw", ""))
    if _rules_are_simple(rules):
        out = _fast_apply_rules(out, rules)
    else:
        applied = apply_payee_map_rules(out, rules)
        options = _build_options(out, rules)
        out = out.join(options)
        out = out.join(applied)
        out["payee_selected"] = out["payee_canonical_suggested"].where(
            out["match_status"] == "unique", ""
        )
        out["category_selected"] = out["category_target_suggested"].where(
            out["match_status"] == "unique", ""
        )
    out["update_map"] = ""

    out = out[
        [
            "transaction_id",
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
    ]

    write_dataframe(out, args.out_path)
    print(f"Wrote {args.out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
