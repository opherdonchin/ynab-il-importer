"""Check whether the 8 reconcile-marked transactions in the x5898 2026_03_16 report
are currently reconciled in YNAB (to determine if --execute was run)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.ynab_api as ynab_api

account_id = "05c8a478-b0df-4f5d-ac31-6da5d17bf86e"

ids_to_check = [
    "be059e8c-538c-4944-a28f-b973ff8f93d0",
    "97283320-b9ac-4be1-b50a-ac851d53b52e",
    "d9b47ffa-fda8-4073-bef2-c898d27a9705",
    "cc1ac5fa-9645-4a23-8c50-f0c6570b07b7",
    "0dea1fc3-b895-4991-aec2-f8e68a79967f",
    "20e6c5da-259b-4e2f-9911-f6f505a0b86d",
    "bb592269-de65-47f5-a1ca-6cf0a32968e1",
    "961cd703-193e-4ba2-ac0a-eea4b0b2e4c9",
]

txns = ynab_api.fetch_transactions()
lookup = {t["id"]: t for t in txns if t.get("account_id") == account_id}
print(f"{'date':<12} {'amount':>10}  {'cleared':<14}  payee")
print("-" * 70)
for tid in ids_to_check:
    t = lookup.get(tid)
    if t:
        amt = t["amount"] / 1000
        cleared = t["cleared"]
        payee = t.get("payee_name") or ""
        print(f"{t['date']:<12} {amt:>10.2f}  {cleared:<14}  {payee}")
    else:
        print(f"NOT FOUND: {tid}")
