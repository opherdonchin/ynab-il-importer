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
- Category selection is restricted to the full YNAB category list.
- Payee selection allows free-text overrides.
- Defaults may be prefilled; confirm default checkboxes are optional.
- Save defaults to `{input}_reviewed.csv` in the same folder as the input file.
- You can pass `--in`, `--out`, and `--categories` to set initial paths.
- Use `--resume` to reopen the last saved file:

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
