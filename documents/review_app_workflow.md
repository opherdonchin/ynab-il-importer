# Review App Workflow

This document describes the intended workflow for using the Streamlit review UI and updating mapping rules.

## 1) Prepare Inputs

### A) Normalize new bank/card files

```bash
pixi run python scripts/normalize_file.py --leumi data/raw/Bankin.dat
pixi run python scripts/normalize_file.py --leumi-xls data/raw/bank.xls
pixi run python scripts/normalize_file.py --max data/raw/card.xlsx
```

Directory mode (auto-detects format; skips unknown files with a warning):

```bash
pixi run python scripts/normalize_file.py --dir data/raw --out-dir data/derived
```

### B) Download YNAB transactions (API snapshot)

```bash
pixi run python scripts/download_ynab_api.py --since 2025-01-01 --until 2025-02-01 --out data/derived/ynab_api_norm.csv
```

### C) Download YNAB categories (for review dropdowns)

```bash
pixi run python scripts/download_ynab_categories.py --out outputs/ynab_categories.csv
```

### D) Build proposed transactions

```bash
pixi run python scripts/build_proposed_transactions.py \
  --source data/derived/Bankin_leumi_norm.csv \
  --source data/derived/card_max_norm.csv \
  --ynab data/derived/ynab_api_norm.csv \
  --out outputs/proposed_transactions.csv \
  --pairs-out outputs/real_matched_pairs.csv
```

Notes:
- `--map` defaults to `mappings/payee_map.csv` if omitted.
- You can use `--source-dir data/derived` instead of repeating `--source` flags.
- `--source-dir` will skip CSVs that don't contain a usable `fingerprint` column.

## 2) Review in Streamlit

Run the review UI:

```bash
pixi run python scripts/review_app.py
```

Notes:
- Category selection can switch between fingerprint-suggested categories and the full YNAB category list.
- Full-category mode includes `Internal Master Category / Uncategorized` when it exists in the downloaded YNAB categories.
- Category labels are shown as `Category Group / Category` in YNAB order.
- In grouped view, the category control has a `Show all` checkbox next to it.
- Categories are loaded once at startup from the configured categories CSV; there is no manual reload button.
- Payee selection allows free-text overrides.
- Defaults may be prefilled. Clicking **Save row** uses the currently shown payee/category values.
- **Apply to all with this fingerprint** only updates rows that are still untouched (not already updated/reviewed).
- Row/group submit actions force an immediate UI refresh so status colors and counters update right away.
- Reloading original/saved data resets row editor widgets so the visible selections match the loaded file.
- Group payee override remains a free-text field.
- Group badges use `Unsaved`, `Changed`, and `Saved` counts so they match sidebar semantics more closely.
- Freshly built `proposed_transactions.csv` files now retain `source` and `account_name`, so row titles can show the source account.
- Save defaults to `{input}_reviewed.csv` in the same folder as the input file.
- Changes are in-memory until you click **Save**.
- The sidebar save control supports `Save`, `Save and quit`, and `Quit`.
- Transfer rows (`payee_selected` starts with `Transfer :`) do not require a category to count as resolved.
- Row status badges: **Unsaved** (changed since last save), **Changed vs original**, **Reviewed** (explicitly saved/confirmed).
- You can pass `--in`, `--out`, and `--categories` to set initial paths.
- Use `--resume` to reopen the default reviewed file for your `--in` path (`{input}_reviewed.csv`):

```bash
pixi run python scripts/review_app.py --resume
```

Or provide a specific saved file:

```bash
pixi run python scripts/review_app.py --resume outputs/proposed_transactions_reviewed.csv
```

Show app-specific help:

```bash
pixi run python scripts/review_app.py --help
```

Notes:
- `streamlit run ... --help` shows Streamlit's own CLI help. Use `scripts/review_app.py --help` for app args.

## 3) Combining Payees

If you want two payees to be treated as the same:

1. Update `mappings/payee_map.csv` so the old payee is replaced by the canonical payee.
2. Rebuild `outputs/proposed_transactions.csv` so the options reflect the change.
3. (Optional) Rename payee in YNAB if you want YNAB to match the canonical name.

## 4) Transfer Rules

Transfers are modeled by setting:

- `payee_canonical` to `Transfer : {Account}`
- `category_target` blank

Example:
- `הפקדת שיק` should map to `Transfer : Cash` (inflow).

## 5) Amount-Aware Rules (amount_bucket)

Use `amount_bucket` in `mappings/payee_map.csv` to restrict rules by amount:

- `<N` or `<=N`
- `=N`
- `>N` or `>=N`
- `A-B` (inclusive range)

Examples:
- Yellow: `amount_bucket=">=150"` → `Yellow / Gas`
- Yellow: `amount_bucket="<150"` → `Gas Food / Groceries`
- Ikea: `amount_bucket=">=70"` → `Ikea / House and stuff`
- Ikea: `amount_bucket="<70"` → `Ikea Food / Groceries`

## 6) After Review

- Use `outputs/proposed_transactions_reviewed.csv` for upload preparation.
- Generate `outputs/map_updates.csv` from reviewed data when needed.
- Rebuild proposed transactions if the bank/card overlap dedupe changes; debit-card purchases can otherwise appear once from bank and once from card.

## 7) Rebuild + Reconcile After Overlap-Dedupe Changes

If `build_proposed_transactions.py` changed how bank/card overlap dedupe works, rebuild the proposed file and then port your saved review decisions onto the rebuilt file:

```bash
pixi run python scripts/build_proposed_transactions.py \
  --source-dir data/derived/2026_03_07 \
  --ynab data/derived/2026_03_07/ynab_api_norm.csv \
  --out data/paired/2026_03_07/proposed_transactions_rebuilt.csv \
  --pairs-out data/paired/2026_03_07/matched_pairs_rebuilt.csv

pixi run python scripts/reconcile_reviewed_transactions.py \
  --old-reviewed data/paired/2026_03_07/proposed_transactions_reviewed.csv \
  --new-proposed data/paired/2026_03_07/proposed_transactions_rebuilt.csv \
  --out data/paired/2026_03_07/proposed_transactions_reviewed_rebuilt.csv
```

Notes:
- `reconcile_reviewed_transactions.py` matches saved decisions by `transaction_id` first.
- If needed, it also falls back to a conservative `(date, outflow_ils, inflow_ils, fingerprint)` match.

## 8) Prepare YNAB Upload

Prepare a dry-run upload file from the reviewed CSV:

```bash
pixi run python scripts/prepare_ynab_upload.py \
  --in data/paired/2026_03_07/proposed_transactions_reviewed_rebuilt.csv \
  --out data/paired/2026_03_07/ynab_upload_ready.csv \
  --json-out data/paired/2026_03_07/ynab_upload_ready.json \
  --ready-only \
  --skip-missing-accounts
```

Notes:
- `--ready-only` keeps only rows with payee selected and category selected for non-transfer rows.
- `--skip-missing-accounts` drops rows whose `account_name` still does not map to a live YNAB account.
- The script fetches live YNAB accounts and categories, resolves category ids, and generates deterministic `import_id` values.
- Add `--execute` only when you are ready to actually create the prepared transactions in YNAB.
