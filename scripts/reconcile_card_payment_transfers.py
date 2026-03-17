"""Ensure card-side payment transfer transactions are reconciled for every
billing cycle that has been reconciled.

The trigger is the existence of a card reconcile report in
data/paired/previous_max/<account>/: if we have a report for cycle C, the
previous-cycle charges listed in that report have been settled and the card
company has been paid. The payment transfer for those charges (the inflow to
the card account whose amount equals the previous-row total) must therefore
be reconciled.

For each (account, cycle) combination, this script scans ALL report variants
(e.g. _after_cleanup, _filtered_previous) and selects the last-alphabetically
file that has previous rows with action in {reconcile, already_reconciled},
since those variants represent the actually-executed reconciliation.

Dry-run by default; pass --execute to apply.
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.card_reconciliation as card_reconciliation
import ynab_il_importer.ynab_api as ynab_api

REPORTS_ROOT = ROOT / "data" / "paired" / "previous_max"
# Suffix in folder name -> account name substring to match in YNAB
ACCOUNT_SUFFIXES = ("x5898", "x7195", "x9922")

# Previous-row action values that signal the billing cycle was settled
VALID_PREVIOUS_ACTIONS = {"reconcile", "reconcile_separate", "already_reconciled"}
# Only rows with these actions map to the main monthly payment transfer
MAIN_BILLING_ACTIONS = {"reconcile", "already_reconciled"}
CYCLE_RE = re.compile(r"^(\d{4}_\d{2})_card_reconcile_report")

execute = "--execute" in sys.argv

print("Fetching accounts and transactions…")
accounts = ynab_api.fetch_accounts()
transactions = ynab_api.fetch_transactions()
account_names = {acc["id"]: acc["name"] for acc in accounts}
all_ynab_df = card_reconciliation._all_ynab_transactions_frame(
    transactions, account_names=account_names
)

# Resolve card account IDs
card_account_ids: dict[str, str] = {}
for acc in accounts:
    for suffix in ACCOUNT_SUFFIXES:
        if suffix in acc["name"].lower() and not acc.get("deleted"):
            card_account_ids[suffix] = acc["id"]
print(f"Card accounts: {card_account_ids}")

updates: list[dict] = []
issues: list[str] = []

for suffix in ACCOUNT_SUFFIXES:
    folder = REPORTS_ROOT / suffix
    if not folder.exists():
        print(f"\n{suffix}: no folder found, skipping")
        continue

    card_acc_id = card_account_ids.get(suffix)
    if not card_acc_id:
        print(f"\n{suffix}: no YNAB account found, skipping")
        continue

    # Group all report variants by cycle prefix (e.g. "2026_01")
    cycle_variants: dict[str, list[Path]] = defaultdict(list)
    for f in folder.glob("*_card_reconcile_report*.csv"):
        m = CYCLE_RE.match(f.name)
        if m:
            cycle_variants[m.group(1)].append(f)

    print(f"\n{suffix}: {len(cycle_variants)} cycle(s) found")

    for cycle in sorted(cycle_variants.keys()):
        # Among all variants for this cycle, pick the last (alphabetically)
        # that has at least one previous row with a settled action.
        settled_files = []
        for f in cycle_variants[cycle]:
            df = pd.read_csv(f)
            prev = df[df["snapshot_role"] == "previous"]
            if not prev[prev["action"].isin(VALID_PREVIOUS_ACTIONS)].empty:
                settled_files.append(f)

        if not settled_files:
            print(f"  {cycle}: no report with settled previous rows, skipping")
            continue

        report_path = sorted(settled_files)[-1]
        report = pd.read_csv(report_path)
        previous_rows = report[report["snapshot_role"] == "previous"]
        settled_rows = previous_rows[
            previous_rows["action"].isin(VALID_PREVIOUS_ACTIONS)
        ]

        # Separately-settled rows have no bundled payment transfer; only include
        # the main billing rows when looking for the monthly payment transfer.
        main_rows = settled_rows[settled_rows["action"].isin(MAIN_BILLING_ACTIONS)]
        if main_rows.empty:
            sep_count = (settled_rows["action"] == "reconcile_separate").sum()
            print(
                f"  {cycle} [{report_path.name}]: {sep_count} separately-settled row(s) only, "
                f"no payment transfer to check"
            )
            continue

        outflow = pd.to_numeric(main_rows["outflow_ils"], errors="coerce").fillna(0.0)
        inflow = pd.to_numeric(main_rows["inflow_ils"], errors="coerce").fillna(0.0)
        previous_total = round(float((inflow - outflow).sum()), 2)

        # Find the card-side payment transfer with the matching amount
        match = card_reconciliation._validate_payment_transfer(
            previous_rows=pd.DataFrame({"signed_ils": [previous_total]}),
            all_ynab_df=all_ynab_df,
            card_account_id=card_acc_id,
            account_names=account_names,
        )

        if not match.ok:
            issues.append(f"{suffix}/{cycle} ({report_path.name}): {match.reason}")
            print(
                f"  {cycle} [{report_path.name}]: previous total {abs(previous_total):.2f} ILS — NOT FOUND: {match.reason}"
            )
            continue

        card_id = match.card_transaction_id
        # Look up current cleared state
        row = all_ynab_df[all_ynab_df["id"] == card_id]
        current_cleared = row.iloc[0]["cleared"] if not row.empty else "MISSING"

        status = (
            "already reconciled"
            if current_cleared == "reconciled"
            else "→ needs reconcile"
        )
        print(
            f"  {cycle} [{report_path.name}]: previous total {abs(previous_total):.2f} ILS  "
            f"transfer {match.card_date}  cleared={current_cleared}  {status}"
        )

        if current_cleared != "reconciled":
            updates.append({"id": card_id, "cleared": "reconciled"})

if issues:
    print("\nWarnings:")
    for issue in issues:
        print(f"  {issue}")

print(f"\nTransactions needing reconcile: {len(updates)}")
if not updates:
    print("Nothing to patch.")
elif execute:
    resp = ynab_api.update_transactions(updates)
    print(f"Patched: {len(resp.get('transactions', []))} transactions")
else:
    print("Dry run — pass --execute to apply.")
