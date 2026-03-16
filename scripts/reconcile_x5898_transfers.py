"""Patch the two remaining outstanding payment transfers on x5898 to reconciled.
Nov 10 (+758.35) = Oct billing cycle payment (2025_10 as previous)
Dec 10 (+791.85) = Nov billing cycle payment (2025_11 as previous)
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.ynab_api as ynab_api

ACCOUNT_ID = "05c8a478-b0df-4f5d-ac31-6da5d17bf86e"

TARGETS = [
    ("2025-11-10", 758350),  # Nov billing cycle payment
    ("2025-12-10", 791850),  # Dec billing cycle payment
]

txns = ynab_api.fetch_transactions()
account_txns = [
    t for t in txns if t.get("account_id") == ACCOUNT_ID and not t.get("deleted")
]

updates = []
for date_str, expected_milliunits in TARGETS:
    matches = [
        t
        for t in account_txns
        if t.get("date") == date_str
        and t.get("amount") == expected_milliunits
        and t.get("transfer_account_id")
    ]
    if len(matches) != 1:
        print(
            f"WARNING: expected 1 match for {date_str} {expected_milliunits/1000:.2f}, found {len(matches)}"
        )
        for m in matches:
            print(f"  id={m['id']} cleared={m['cleared']} payee={m.get('payee_name')}")
        continue
    t = matches[0]
    print(
        f"Found: {t['date']}  {t['amount']/1000:.2f}  cleared={t['cleared']}  {t.get('payee_name')}  id={t['id']}"
    )
    if t["cleared"] != "reconciled":
        updates.append({"id": t["id"], "cleared": "reconciled"})
    else:
        print("  → already reconciled, skipping")

if not updates:
    print("Nothing to patch.")
else:
    execute = "--execute" in sys.argv
    if execute:
        resp = ynab_api.update_transactions(updates)
        print(f"Patched: {len(resp.get('transactions', []))} transactions")
    else:
        print(
            f"Dry run: would patch {len(updates)} transactions. Pass --execute to apply."
        )
