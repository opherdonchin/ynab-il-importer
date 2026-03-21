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
- Prefer explicit `--source` flags for bank/card normalized files.
- Avoid `--source-dir` when the directory also contains `ynab_api_norm.csv`; YNAB snapshots can include fingerprints and may be treated as source rows.
- Weak date+amount dedupe now retains source rows when lineage (`bank_txn_id`/`card_txn_id`) or fingerprint conflicts with matched YNAB candidates, preventing false drops.

## 2) Review in Streamlit

Run the review UI:

```bash
pixi run python scripts/review_app.py
```

Launcher behavior:
- The wrapper starts Streamlit in the background and prints the active URL.
- If port `8501` is occupied, the wrapper chooses the next free port unless `--port` is specified.

Notes:
- Sidebar filters are split into primary dimensions and secondary tags:
  - Primary dimensions: `Readiness` (`Not ready`/`Ready`) and `Save state` (`Unsaved`/`Saved`).
  - Secondary tags: `Inference tag`, `Progress tag`, `Persistence tag`.
- Default filter selection is `Not ready` + `Unsaved` on primary dimensions; secondary tags default to all.
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
- Row details now expose tag values:
  - `inference_tag_initial` (sticky initial categorization)
  - `progress_tag` (`unchanged` / `resolved`)
  - `persistence_tag` (`unsaved` / `saved`)
- Expander labels include a primary-state marker (`NR/US`, `NR/S`, `R/US`, `R/S`) with color-coded state styling.
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

- Use the reviewed CSV for upload preparation (see §9).
- Generate `outputs/map_updates.csv` from reviewed data when needed.
- Rebuild proposed transactions if the bank/card overlap dedupe changes; debit-card purchases can otherwise appear once from bank and once from card.

## 7) Stamp Lineage on Existing YNAB Transactions

Before or after upload, stamp `bank_txn_id` / `card_txn_id` lineage onto transactions already present in YNAB. Always do a dry-run first (omit `--execute`), then re-run with `--execute` when the report looks correct.

**Bank (all accounts in the normalized CSV):**

```bash
pixi run python scripts/sync_bank_matches.py \
  --bank data/derived/<date>/Bankin_leumi_norm.csv \
  --report-out data/paired/<date>/bank_sync_report.csv
# add --execute when ready
```

**Card (one run per card account):**

```bash
pixi run python scripts/sync_card_matches.py \
  --account "<Card Account Name>" \
  --source data/raw/<date>/card.xlsx \
  --report-out data/paired/<date>/<account>_card_sync_report.csv
# add --execute when ready
```

## 8) Rebuild + Reconcile After Overlap-Dedupe Changes

If `build_proposed_transactions.py` changed how bank/card overlap dedupe works, rebuild the proposed file and then port your saved review decisions onto the rebuilt file:

```bash
pixi run python scripts/build_proposed_transactions.py \
  --source data/derived/2026_03_07/Bankin_leumi_norm.csv \
  --source data/derived/2026_03_07/card_max_norm.csv \
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

## 9) Prepare YNAB Upload

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
- `--ready-only` keeps only rows with payee selected and category selected for non-transfer rows, and excludes zero-amount rows.
- `--skip-missing-accounts` drops rows whose `account_name` still does not map to a live YNAB account.
- The script fetches live YNAB accounts and categories, resolves category ids, and generates deterministic `import_id` values.
- Add `--execute` only when you are ready to actually create the prepared transactions in YNAB.

## 10) Bank Reconciliation

After upload + bank lineage sync, reconcile matched bank rows in YNAB.

```bash
pixi run python scripts/reconcile_bank_statement.py \
  --bank data/derived/<date>/Bankin_leumi_norm.csv \
  --report-out data/paired/<date>/bank_reconcile_report.csv
# add --execute when ready
```

Notes:
- The script uses `bank_txn_id` lineage and statement balance anchors.
- Use dry-run first; add `--execute` only when the report shows safe updates.

## 11) Card Reconciliation

Validate that card source totals match what is in YNAB. Run once per card account.

**Mid-month (`--source` only) — validate current open cycle:**

```bash
pixi run python scripts/reconcile_card_cycle.py \
  --account "<Card Account Name>" \
  --source data/raw/<date>/card.xlsx \
  --report-out data/paired/<date>/<account>_card_reconcile_report.csv
# add --execute when ready to mark matched rows as reconciled
```

**Month-transition (`--previous + --source`) — validate closed month and payment transfer:**

```bash
pixi run python scripts/reconcile_card_cycle.py \
  --account "<Card Account Name>" \
  --previous data/paired/previous_max/<account>/<prev_month>_card_reconcile_report.csv \
  --source data/raw/<date>/card.xlsx \
  --report-out data/paired/<date>/<account>_card_reconcile_report.csv
# add --execute when ready
```

Notes:
- `--previous` is the reconcile report saved from the prior billing cycle.
- The script validates that the previous cohort total matches a unique card-account transfer inflow in YNAB with a linked bank-side counterpart.
- Save the completed report to `data/paired/previous_max/<account>/` as `<month>_card_reconcile_report.csv` for use as `--previous` in the next cycle.
- Add `--allow-reconciled-source` only when the source cycle was already reconciled by an earlier run and you intentionally want to reuse it.
