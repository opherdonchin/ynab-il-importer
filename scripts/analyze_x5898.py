import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import ynab_il_importer.ynab_api as ynab_api
import pandas as pd

ACC_ID = "05c8a478-b0df-4f5d-ac31-6da5d17bf86e"

txns = ynab_api.fetch_transactions()
rows = [t for t in txns if t.get("account_id") == ACC_ID and not t.get("deleted")]
df = pd.DataFrame(rows)
df["amount_ils"] = df["amount"] / 1000
df = df.sort_values("date").reset_index(drop=True)

cleared = df[df["cleared"] == "cleared"].copy()
reconciled = df[df["cleared"] == "reconciled"].copy()

print(
    f"reconciled : {len(reconciled):3d} rows  total={reconciled['amount_ils'].sum():>10.2f}"
)
print(
    f"cleared    : {len(cleared):3d} rows  total={cleared['amount_ils'].sum():>10.2f}"
)
print(
    f"all cleared: {len(df[df['cleared'].isin(['cleared','reconciled'])]):3d} rows  total={df[df['cleared'].isin(['cleared','reconciled'])]['amount_ils'].sum():>10.2f}"
)
print()
print("=== Cleared (not reconciled) ===")
cols = ["date", "amount_ils", "payee_name", "memo", "import_id"]
print(cleared[cols].to_string(index=False))

# Group cleared by month to see payment pattern
print()
print("=== Cleared by month ===")
cleared["month"] = pd.to_datetime(cleared["date"]).dt.to_period("M")
print(cleared.groupby("month")["amount_ils"].sum().to_string())
