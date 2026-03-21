# ynab-il-importer

Minimal ETL scaffold for importing Israeli bank/card exports into YNAB by learning payee/category hints from historical YNAB register data.

Directory convention:
- `data/derived/`: parsed/normalized snapshots of raw files (treated as local, not for commit).
- `outputs/`: generated review/labeling artifacts that are intended to be shared/committed.

- `documents/project_context.md` contains the file used as context for AI agents.
- `documents/plan.md` contains the current plan of action.
- `documents/review_app_workflow.md` documents the review app workflow.

## Workflow Overview

There are two distinct workflows:

- **Bootstrap (one-time):** build initial fingerprint groups and payee map using a large YNAB register export.
- **Process new sources (ongoing):** normalize new bank/card files and dedupe against YNAB via the API.

The bootstrap workflow should only be run when seeding or rebuilding the mapping tables. Day-to-day processing should use the API workflow.

## Quickstart — Bootstrap (one-time)

1. Install environment:

```bash
pixi install
```

2. Put your source files in `data/raw/`:

- `data/raw/bank.xls`
- `data/raw/card.xlsx`
- `data/raw/ynab_register.csv`

3. Normalize source inputs (default output naming):

```bash
pixi run python scripts/normalize_file.py \
  --leumi data/raw/Bankin.dat

pixi run python scripts/normalize_file.py \
  --leumi-xls data/raw/bank.xls

pixi run python scripts/normalize_file.py \
  --max data/raw/card.xlsx

pixi run python scripts/normalize_file.py \
  --ynab data/raw/ynab_register.csv
```

Default output name:
- `data/derived/{raw_file_stem}_{format}_norm.csv`

Directory mode (autodetects format; skips unknown files with a warning):

```bash
pixi run python scripts/normalize_file.py \
  --dir data/raw \
  --out-dir data/derived
```

4. Build matched pairs (bootstrap only):

```bash
pixi run python scripts/bootstrap_pairs.py \
  --source data/derived/bank_normalized.csv \
  --source data/derived/bank_normalized_other.csv \
  --source data/derived/card_normalized.csv \
  --source data/derived/card_normalized_other.csv \
  --ynab data/derived/ynab_normalized.csv \
  --out outputs/matched_pairs.csv
```

5. Build fingerprint groups for human labeling (bootstrap only):

```bash
pixi run python scripts/build_groups.py \
  --pairs outputs/matched_pairs.csv \
  --out outputs/fingerprint_groups.csv
```

Expected outputs:

- `outputs/matched_pairs.csv`
- `outputs/fingerprint_groups.csv`

## Process New Sources (ongoing workflow)

1. Normalize new bank/card source files (same as above).

2. Download YNAB transactions via API for the relevant date range:

```bash
pixi run python scripts/download_ynab_api.py \
  --since 2025-01-01 \
  --until 2025-02-01 \
  --out data/derived/ynab_api_norm.csv
```

Required configuration (any one of the following):

- `YNAB_ACCESS_TOKEN` in environment or `.env`
- `YNAB_BUDGET_ID` in environment, or `config/ynab.local.toml` with `budget_id`

3. Download YNAB categories for the review UI:

```bash
pixi run python scripts/download_ynab_categories.py \
  --out outputs/ynab_categories.csv
```

4. Build proposed transactions using the API snapshot:

```bash
pixi run python scripts/build_proposed_transactions.py \
  --source data/derived/<date>/bank_leumi_norm.csv \
  --source data/derived/<date>/card_max_norm.csv \
  --ynab data/derived/<date>/ynab_api_norm.csv \
  --map mappings/payee_map.csv \
  --out data/paired/<date>/proposed_transactions.csv \
  --pairs-out data/paired/<date>/matched_pairs.csv
```

Notes:
- Weak date+amount dedupe is lineage-aware: if a source `bank_txn_id`/`card_txn_id` conflicts with matched YNAB lineage (or fingerprint), the source row is retained for review instead of dropped.

5. Review in the Streamlit UI (see `documents/review_app_workflow.md`):

```bash
pixi run python scripts/review_app.py \
  --in data/paired/<date>/proposed_transactions.csv \
  --categories outputs/ynab_categories.csv
```

6. Stamp lineage on existing YNAB transactions (dry-run first, then add `--execute`):

```bash
pixi run python scripts/sync_bank_matches.py \
  --bank data/derived/<date>/Bankin_leumi_norm.csv \
  --report-out data/paired/<date>/bank_sync_report.csv

pixi run python scripts/sync_card_matches.py \
  --account "<Card Account Name>" \
  --source data/raw/<date>/card.xlsx \
  --report-out data/paired/<date>/<account>_card_sync_report.csv
```

7. Prepare and upload reviewed transactions (dry-run first, then add `--execute`):

```bash
pixi run python scripts/prepare_ynab_upload.py \
  --in data/paired/<date>/proposed_transactions_reviewed.csv \
  --out data/paired/<date>/ynab_upload.csv \
  --ready-only \
  --skip-missing-accounts
```

Notes:
- `--ready-only` excludes zero-amount rows (`outflow_ils == 0` and `inflow_ils == 0`) so pending placeholders are not uploaded.

8. Reconcile bank statement rows after upload + lineage sync:

```bash
pixi run python scripts/reconcile_bank_statement.py \
  --bank data/derived/<date>/Bankin_leumi_norm.csv \
  --report-out data/paired/<date>/bank_reconcile_report.csv
```

9. Reconcile card accounts (mid-month: `--source` only; end-of-month: add `--previous`):

```bash
pixi run python scripts/reconcile_card_cycle.py \
  --account "<Card Account Name>" \
  --source data/raw/<date>/card.xlsx \
  --report-out data/paired/<date>/<account>_card_reconcile_report.csv
```

## Fingerprint Mapping

Optional fingerprint canonicalization rules live in:

- `mappings/fingerprint_map.csv`

Columns:

- `rule_id`
- `is_active`
- `priority`
- `pattern` (literal substrings, `|` allowed for OR)
- `canonical_text`
- `notes`

Fingerprinting appends a log to:

- `outputs/fingerprint_log.csv`

## Account Name Mapping

Account identity now comes from source files:

- Card exports: `4 ספרות אחרונות...` -> normalized as `xNNNN`
- Bank `.dat` exports: account number from the last field

Optional mapping to YNAB account names/IDs is read from:

- `mappings/account_name_map.csv`

CSV schema:

- `source` (`card` or `bank`; optional blank for global rows)
- `source_account` (for example `x1234` or bank account id from `.dat`)
- `source_account_label` (optional, human label)
- `ynab_account_name` (target YNAB account name)
- `ynab_account_id` (target YNAB account id; optional but recommended)

Behavior:

- If the mapping file is missing, parsed account names remain as in source files.
- If a parsed account is not found in the mapping file, it remains as in source files.
- In both cases, a warning is emitted with unmatched account names.

## Payee Map Rules

`mappings/payee_map.csv` is the source of truth for deterministic payee/category mapping.

Decision:
- Rules return `(payee_canonical, category_target)` in one row; `category_target` may be blank.
- Blank key fields are wildcards and do not constrain matching.
- Rule precedence is `priority DESC`, then specificity DESC, then `rule_id ASC`.
- If the top rules tie on `(priority, specificity)`, the result is `ambiguous`.

Important behavior:
- `description_clean_norm` is human-readable.
- `fingerprint` is the stable key used for matching when present.
- If a rule has both `fingerprint` and `description_clean_norm`, `fingerprint` wins.
- `direction` is derived from amount sign (`inflow` / `outflow` / `zero`) when not provided.
- `currency` defaults to `ILS` when missing in parsed inputs.

Build review outputs:

```bash
pixi run ynab-il build-payee-map \
  --parsed data/derived/leumi_fuller_parsed.csv \
  --matched-pairs outputs/matched_pairs.csv \
  --out-dir outputs/payee_map
```

Generated files:
- `payee_map_candidates.csv`: one row per `(txn_kind, fingerprint, description_clean_norm)`.
- `payee_map_applied_preview.csv`: parsed transactions + suggested payee/category and match status.

Candidate statuses:
- `matched_uniquely`: all rows for that key matched uniquely.
- `unmatched`: no rows for that key matched any active rule.
- `ambiguous`: mixed/noisy results (ambiguous tie or partial coverage).

Safe editing guidelines for `mappings/payee_map.csv`:
- Keep `rule_id` stable and unique.
- Use blanks in optional key columns for wildcard behavior.
- Increase `priority` to force a rule to win over a more specific lower-priority rule.
- Leave `category_target` blank when only payee is known.

## Review UI (Streamlit)

The review UI edits a proposed transactions CSV and writes a reviewed copy.

Run:

```bash
pixi run python scripts/review_app.py \
  --in data/paired/<date>/proposed_transactions.csv \
  --categories outputs/ynab_categories.csv
```

Launcher behavior:
- The wrapper starts Streamlit in the background and prints the active URL.
- If `8501` is busy, it auto-selects the next free port unless you pass `--port`.

Resume a prior session:

```bash
pixi run python scripts/review_app.py --resume
```

Default behavior:
- Loads `outputs/proposed_transactions.csv` if `--in` is not given.
- Saves to `<input>_reviewed.csv` (configurable in the UI or via `--out`).
- Sidebar filters are split into:
  - Primary dimensions: `Readiness` (`Not ready`/`Ready`) and `Save state` (`Unsaved`/`Saved`)
  - Secondary tags: `Inference tag`, `Progress tag`, `Persistence tag`
- Default filter selection is `Not ready` + `Unsaved` on primary dimensions; secondary tags default to showing all.
- Row expanders include a primary state marker (`NR/US`, `NR/S`, `R/US`, `R/S`) and color-coded state styling.

See `documents/review_app_workflow.md` for the full review and upload workflow.

Hard rule for readiness:
- Both `payee_selected` and `category_selected` must be filled.
