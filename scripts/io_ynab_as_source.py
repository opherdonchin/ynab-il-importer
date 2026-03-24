"""Download YNAB transactions for a given category and date range.

Produces a CSV in the normalised source format used by the rest of the
pipeline (same columns as bank/card derived files) so the output can be
used as a source file for analysis, mapping, and reconciliation.

Extra columns compared to a plain YNAB export:
  merchant_raw         – alias of payee_raw
  description_raw      – memo when present, otherwise payee_raw
  description_clean    – same as merchant_raw (payee name is already clean)
  description_clean_norm / fingerprint – derived via fingerprint pipeline
  source_account       – account_name repeated for schema compatibility
  category_balance     – running balance within the filtered category,
                         sorted ascending by (date, ynab_id)

Usage examples
--------------
  # All transactions in "Groceries" between two dates:
  python scripts/io_ynab_as_source.py --category Groceries \\
      --since 2026-01-01 --until 2026-03-16

  # Match by partial category name (case-insensitive):
  python scripts/io_ynab_as_source.py --category "vet" \\
      --since 2026-01-01 --out data/derived/ynab_vet_2026.csv

  # Match by exact YNAB category id:
  python scripts/io_ynab_as_source.py --category-id <uuid> \\
      --since 2026-01-01
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.fingerprint as fingerprint_mod
import ynab_il_importer.normalize as normalize
import ynab_il_importer.workflow_profiles as workflow_profiles
import ynab_il_importer.ynab_api as ynab_api
from ynab_il_importer.io_ynab import _infer_txn_kind


# ---------------------------------------------------------------------------
# Category resolution
# ---------------------------------------------------------------------------


def _resolve_category(
    groups: list[dict[str, Any]],
    name_pattern: str | None,
    category_id: str | None,
) -> tuple[str, str]:
    """Return (category_id, display_name) for the first matching category.

    *name_pattern* is matched case-insensitively as a substring of the
    full ``Group: Sub`` name.  *category_id* takes precedence when given.
    Raises ValueError if no unique match is found.
    """
    candidates: list[tuple[str, str]] = []  # (id, display_name)
    for group in groups:
        group_name = group.get("name", "") or ""
        for cat in group.get("categories", []) or []:
            if cat.get("deleted"):
                continue
            cid = cat.get("id", "") or ""
            cname = cat.get("name", "") or ""
            full_name = f"{group_name}: {cname}" if group_name else cname
            candidates.append((cid, full_name))

    if category_id:
        matches = [(cid, name) for cid, name in candidates if cid == category_id]
        if not matches:
            raise ValueError(f"No category found with id={category_id!r}")
        return matches[0]

    if name_pattern:
        pattern_lower = name_pattern.lower()
        matches = [
            (cid, name) for cid, name in candidates if pattern_lower in name.lower()
        ]
        if not matches:
            raise ValueError(
                f"No category matched pattern={name_pattern!r}. "
                f"Use scripts/download_ynab_categories.py to list all categories."
            )
        if len(matches) > 1:
            names = [name for _, name in matches]
            raise ValueError(
                f"Pattern {name_pattern!r} matched {len(matches)} categories: {names}. "
                f"Use a more specific pattern or --category-id."
            )
        return matches[0]

    raise ValueError("Either --category or --category-id must be provided.")


# ---------------------------------------------------------------------------
# DataFrame building
# ---------------------------------------------------------------------------


def _build_source_dataframe(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]],
    category_id: str,
    category_display_name: str,
    since: str | None,
    until: str | None,
    *,
    fingerprint_map_path: Path,
    fingerprint_log_path: Path,
) -> pd.DataFrame:
    category_name = category_display_name.split(":")[-1].strip()
    df = ynab_api.category_transactions_to_dataframe(transactions, accounts)
    if df.empty:
        return df
    if "category_id" not in df.columns:
        raise ValueError("Category-source dataframe is missing category_id.")

    df = df.loc[
        df["category_id"].astype("string").fillna("").str.strip() == str(category_id).strip()
    ].copy()
    if df.empty:
        return df

    df["source_account"] = df["account_name"].astype("string").fillna("").str.strip()
    df["secondary_date"] = pd.NaT
    df["merchant_raw"] = df["payee_raw"].astype("string").fillna("")
    memo_series = df["memo"].astype("string").fillna("")
    payee_series = df["payee_raw"].astype("string").fillna("")
    df["description_raw"] = memo_series.where(memo_series.str.strip() != "", payee_series)
    df["description_clean"] = payee_series
    df["description_clean_norm"] = ""
    df["fingerprint"] = ""
    df["category_raw"] = category_name
    df["category_display_name"] = category_display_name
    df["category_id"] = str(category_id).strip()

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Date filtering
    if since:
        since_date = pd.to_datetime(since, errors="coerce").date()
        df = df[df["date"] >= since_date]
    if until:
        until_date = pd.to_datetime(until, errors="coerce").date()
        df = df[df["date"] <= until_date]

    if df.empty:
        return df

    df["txn_kind"] = _infer_txn_kind(
        df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
    )

    # Apply fingerprint pipeline (description_clean_norm + fingerprint)
    df = fingerprint_mod.apply_fingerprints(
        df,
        use_fingerprint_map=True,
        fingerprint_map_path=fingerprint_map_path,
        log_path=fingerprint_log_path,
    )

    # Running category balance: sort ascending by date then ynab_id for stability
    df = df.sort_values(["date", "ynab_id"], kind="stable").reset_index(drop=True)
    net = df["inflow_ils"] - df["outflow_ils"]
    df["category_balance"] = net.cumsum().round(2)

    return df[
        [
            "source",
            "ynab_id",
            "account_id",
            "account_name",
            "source_account",
            "date",
            "secondary_date",
            "payee_raw",
            "category_raw",
            "category_display_name",
            "category_id",
            "txn_kind",
            "merchant_raw",
            "description_raw",
            "description_clean",
            "description_clean_norm",
            "fingerprint",
            "outflow_ils",
            "inflow_ils",
            "category_balance",
            "currency",
            "amount_bucket",
            "memo",
            "import_id",
            "matched_transaction_id",
            "cleared",
            "approved",
        ]
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _default_out_path(
    profile_name: str,
    category_display_name: str,
    since: str | None,
    until: str | None,
) -> Path:
    slug = re.sub(r"[^\w]+", "_", category_display_name).strip("_").lower()
    parts = [slug]
    if since:
        parts.append(since.replace("-", ""))
    if until:
        parts.append(until.replace("-", ""))
    return (
        Path("data/derived")
        / profile_name
        / f"ynab_category_{'-'.join(parts)}.csv"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download YNAB transactions for a category as a normalised source CSV."
    )
    parser.add_argument(
        "--profile",
        default="",
        help="Workflow profile used for default budget and fingerprint paths.",
    )
    parser.add_argument(
        "--budget-id",
        dest="budget_id",
        default="",
        help="Override YNAB budget/plan id.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--category",
        default="",
        metavar="NAME",
        help="Category name or substring (case-insensitive).",
    )
    group.add_argument(
        "--category-id",
        default="",
        metavar="UUID",
        help="Exact YNAB category UUID (takes precedence over --category).",
    )
    parser.add_argument(
        "--since", default="", metavar="YYYY-MM-DD", help="Start date (inclusive)."
    )
    parser.add_argument(
        "--until", default="", metavar="YYYY-MM-DD", help="End date (inclusive)."
    )
    parser.add_argument(
        "--out",
        dest="out_path",
        type=Path,
        default=None,
        help=(
            "Output CSV path. Defaults to "
            "data/derived/ynab_category_<slug>[-since][-until].csv"
        ),
    )
    parser.add_argument(
        "--fingerprint-map",
        dest="fingerprint_map_path",
        type=Path,
        default=None,
        help="Override fingerprint map path used when deriving source fingerprints.",
    )
    parser.add_argument(
        "--fingerprint-log",
        dest="fingerprint_log_path",
        type=Path,
        default=None,
        help="Override fingerprint log output path.",
    )
    args = parser.parse_args()

    if not args.category and not args.category_id:
        parser.error("Either --category or --category-id is required.")

    profile = workflow_profiles.resolve_profile(args.profile or None)
    plan_id = workflow_profiles.resolve_budget_id(
        profile=profile.name,
        budget_id=args.budget_id,
    )
    fingerprint_map_path = args.fingerprint_map_path or profile.fingerprint_map_path
    fingerprint_log_path = args.fingerprint_log_path or (
        Path("outputs") / profile.name / "fingerprint_log.csv"
    )

    print("Fetching categories…")
    groups = ynab_api.fetch_categories(plan_id=plan_id or None)

    category_id, category_display_name = _resolve_category(
        groups,
        name_pattern=args.category or None,
        category_id=args.category_id or None,
    )
    print(f"Category: {category_display_name!r}  (id={category_id})")

    print("Fetching transactions…")
    # Pass since_date to reduce API payload when possible
    txns = ynab_api.fetch_transactions(
        plan_id=plan_id or None,
        since_date=args.since or None,
    )
    print(f"  {len(txns)} transactions returned from API")

    print("Fetching accounts…")
    accounts = ynab_api.fetch_accounts(plan_id=plan_id or None)

    df = _build_source_dataframe(
        txns,
        accounts,
        category_id=category_id,
        category_display_name=category_display_name,
        since=args.since or None,
        until=args.until or None,
        fingerprint_map_path=fingerprint_map_path,
        fingerprint_log_path=fingerprint_log_path,
    )

    if df.empty:
        print("No transactions matched the given category and date range.")
        return

    out_path = args.out_path or _default_out_path(
        profile.name,
        category_display_name,
        args.since or None,
        args.until or None,
    )
    export.write_dataframe(df, out_path)
    print(export.wrote_message(out_path, len(df)))

    if not df.empty:
        final_balance = df["category_balance"].iloc[-1]
        print(f"Category balance at end of range: {final_balance:,.2f} ILS")


if __name__ == "__main__":
    main()
