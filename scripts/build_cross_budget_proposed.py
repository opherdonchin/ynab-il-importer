import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing
import ynab_il_importer.export as export
import ynab_il_importer.proposed_defaults as proposed_defaults
import ynab_il_importer.rules as rules_mod
import ynab_il_importer.workflow_profiles as workflow_profiles


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _filter_by_date(df: pd.DataFrame, since: str | None, until: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    date_series = pd.to_datetime(out["date"], errors="coerce")
    if since:
        out = out.loc[date_series >= pd.to_datetime(since, errors="coerce")].copy()
        date_series = pd.to_datetime(out["date"], errors="coerce")
    if until:
        out = out.loc[date_series <= pd.to_datetime(until, errors="coerce")].copy()
    return out


def _default_artifact_root(target_profile: str, phase: str) -> Path:
    profile_name = str(target_profile or "").strip().lower()
    if profile_name:
        return Path("data/paired") / f"{profile_name}_cross_budget_{phase}"
    return Path("data/paired") / f"cross_budget_{phase}"


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
        payees: list[str] = []
        categories: list[str] = []
        for rule in matched:
            payee = str(rule.get("payee_canonical") or "").strip()
            category = str(rule.get("category_target") or "").strip()
            if payee and payee not in payees:
                payees.append(payee)
            if category and category not in categories:
                categories.append(category)
        payee_options.append("; ".join(payees))
        category_options.append("; ".join(categories))

    return pd.DataFrame(
        {"payee_options": payee_options, "category_options": category_options},
        index=transactions.index,
    )


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
        str(row.get("raw_text", row.get("memo", ""))),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"txn_{digest}"


def build_proposed_output(
    transactions: pd.DataFrame,
    *,
    map_path: Path,
) -> pd.DataFrame:
    base_columns = [
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

    if transactions.empty:
        return pd.DataFrame(columns=base_columns + ["source_account", "source_row_id", "row_kind", "signed_amount"])

    rules = rules_mod.load_payee_map(map_path)
    out = transactions.copy()
    out["transaction_id"] = out.apply(_make_transaction_id, axis=1)
    existing_memo = out.get("memo", pd.Series([""] * len(out), index=out.index))
    out["memo"] = (
        existing_memo.astype("string").fillna("").where(
            existing_memo.astype("string").fillna("").str.strip() != "",
            out.get("raw_text", pd.Series([""] * len(out), index=out.index)),
        )
    )

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

    optional_columns = [
        "source_account",
        "source_row_id",
        "row_kind",
        "signed_amount",
        "status",
        "raw_text",
    ]
    columns = base_columns + [col for col in optional_columns if col in out.columns]
    return out[columns].copy()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build proposed transactions for a generic cross-budget source-to-target account sync."
    )
    parser.add_argument("--source", required=True, help="Source CSV path.")
    parser.add_argument("--ynab", required=True, help="Target YNAB snapshot CSV path.")
    parser.add_argument("--source-profile", default="", help="Source workflow profile (for metadata only).")
    parser.add_argument("--source-category", default="", help="Optional source category filter.")
    parser.add_argument("--target-profile", default="", help="Target workflow profile (for payee-map/output defaults).")
    parser.add_argument("--target-account", required=True, help="Target account name in the target budget.")
    parser.add_argument("--since", default="", help="Start date YYYY-MM-DD.")
    parser.add_argument("--until", default="", help="End date YYYY-MM-DD.")
    parser.add_argument(
        "--date-tolerance-days",
        type=int,
        default=0,
        help="Allow date-window matching within this many days after exact matching fails.",
    )
    parser.add_argument(
        "--map",
        dest="map_path",
        type=Path,
        default=None,
        help="Override payee_map.csv path. Defaults to the target profile payee map.",
    )
    parser.add_argument("--out", dest="out_path", default="", help="Proposed-transactions output CSV.")
    parser.add_argument("--pairs-out", default="", help="Matched-pairs output CSV.")
    parser.add_argument("--unmatched-source-out", default="", help="Unmatched source rows output CSV.")
    parser.add_argument("--unmatched-target-out", default="", help="Unmatched target rows output CSV.")
    parser.add_argument("--ambiguous-out", default="", help="Ambiguous-match output CSV.")
    args = parser.parse_args()

    target_profile = workflow_profiles.resolve_profile(args.target_profile or None)
    map_path = args.map_path or target_profile.payee_map_path

    source_path = Path(args.source)
    target_path = Path(args.ynab)
    source_df = _read_csv_or_empty(source_path)
    target_df = _read_csv_or_empty(target_path)
    source_df["source_file"] = source_path.name
    target_df["target_file"] = target_path.name

    source_df = _filter_by_date(source_df, args.since or None, args.until or None)
    target_df = _filter_by_date(target_df, args.since or None, args.until or None)

    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account=args.target_account,
        source_category=args.source_category or None,
        date_tolerance_days=int(args.date_tolerance_days),
    )

    candidates = result.unmatched_source_df.copy()
    if not candidates.empty:
        original_account = candidates.get(
            "source_account",
            candidates.get("account_name", pd.Series([""] * len(candidates), index=candidates.index)),
        )
        candidates["source_account"] = (
            original_account.astype("string").fillna("").str.strip()
        )
        candidates["account_name"] = str(args.target_account).strip()

    proposed = build_proposed_output(
        candidates,
        map_path=map_path,
    )

    artifact_root = _default_artifact_root(target_profile.name, "live")
    out_path = Path(args.out_path) if args.out_path else artifact_root / "proposed_transactions.csv"
    pairs_out = Path(args.pairs_out) if args.pairs_out else artifact_root / "matched_pairs.csv"
    unmatched_source_out = (
        Path(args.unmatched_source_out)
        if args.unmatched_source_out
        else artifact_root / "unmatched_source.csv"
    )
    unmatched_target_out = (
        Path(args.unmatched_target_out)
        if args.unmatched_target_out
        else artifact_root / "unmatched_target.csv"
    )
    ambiguous_out = (
        Path(args.ambiguous_out)
        if args.ambiguous_out
        else artifact_root / "ambiguous_matches.csv"
    )

    export.write_dataframe(proposed, out_path)
    export.write_dataframe(result.matched_pairs_df, pairs_out)
    export.write_dataframe(result.unmatched_source_df, unmatched_source_out)
    export.write_dataframe(result.unmatched_target_df, unmatched_target_out)
    export.write_dataframe(result.ambiguous_matches_df, ambiguous_out)

    print(export.wrote_message(out_path, len(proposed)))
    print(export.wrote_message(pairs_out, len(result.matched_pairs_df)))
    print(export.wrote_message(unmatched_source_out, len(result.unmatched_source_df)))
    print(export.wrote_message(unmatched_target_out, len(result.unmatched_target_df)))
    print(export.wrote_message(ambiguous_out, len(result.ambiguous_matches_df)))
    print(
        "Summary: "
        f"proposed={len(proposed)} "
        f"matched_existing={len(result.matched_pairs_df)} "
        f"unmatched_target={len(result.unmatched_target_df)} "
        f"ambiguous={len(result.ambiguous_matches_df)}"
    )


if __name__ == "__main__":
    main()
