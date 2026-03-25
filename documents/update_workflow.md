# Overall Update Workflow (Cross-Platform)

This runbook covers the full update in this exact order:

1. Family bank and card matching
2. Family review
3. Family update and reconciliation (including within-month or across-month card reconciliation)
4. Pilates bank, card, and cross-budget matching
5. Pilates review
6. Pilates update and reconciliation (including within-month or new-month card reconciliation)

## Command Style Standard

We use one shell style on both Linux and Windows:

- Every command is a single `python ...` command.
- Start the session once with `pixi shell` from repo root, then run the `python ...` commands.
- No shell variables (`$X`, `${X}`, `%X%`).
- No shell-specific line continuation (`\` or `` ` ``).
- No shell-specific control flow (`for`, `if`, pipes) in the workflow.
- Use explicit paths and quote paths that contain spaces.
- Use `/` in paths.

This keeps the markdown copy/pasteable in `bash`, `zsh`, `pwsh`, and `cmd`.

Shell activation:

```bash
pixi shell
```

## Run Constants For This Update (`2026_03_24`)

For this run, use these literal values directly in commands:

- run tag: `2026_03_24`
- previous run tag: `2026_03_19`
- Family YNAB window: set both `--since` and `--until` from Family source files
- Pilates YNAB window: set both `--since` and `--until` from Pilates source files
- Pilates previous card statements root: `data/raw/previous_leumi_card/x0602/`
- cross-budget since date: `2026-03-19` (adjust only if the Family reconciliation anchor changed)
- cross-budget until date: `2026-03-24`

Default bounded-window policy:

- Use `source_min_date - 14 days` for `--since`.
- Use `source_max_date + 14 days` for `--until`.
- Only widen this window when an analysis artifact shows missing historical context.

## 0) Prepare Run Folders

```bash
python scripts/init_update_run.py --run-tag 2026_03_24
```

## 1) Family Bank And Card Matching

Normalize Family source files:

```bash
python scripts/normalize_file.py --profile family --leumi "data/raw/2026_03_24/Bankin family.dat" --out "data/derived/2026_03_24/Bankin family_leumi_norm.csv"
python scripts/normalize_file.py --profile family --max "data/raw/2026_03_24/transaction-details_export_1774340215930.xlsx" --out "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv"
```

Extract the Family date window (one-off helper; copy the printed values into the next command):

```bash
python scripts/extract_date_window.py --label family --padding-days 14 --source "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --print-args
```

Refresh Family YNAB context:

```bash
python scripts/download_ynab_api.py --profile family --since "<family_ynab_since>" --until "<family_ynab_until>" --out "data/derived/2026_03_24/family_ynab_api_norm.csv"
python scripts/download_ynab_categories.py --profile family --out "outputs/family/ynab_categories.csv"
```

Build Family proposals:

```bash
python scripts/build_proposed_transactions.py --profile family --source "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --ynab "data/derived/2026_03_24/family_ynab_api_norm.csv" --out "data/paired/2026_03_24/family_proposed_transactions.csv" --pairs-out "data/paired/2026_03_24/family_matched_pairs.csv"
```

Print Family card account names from the normalized card file (pick the one to reconcile):

```bash
python scripts/list_unique_csv_values.py --csv "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --column account_name --drop-empty
```

Dry-run Family lineage matching:

```bash
python scripts/sync_bank_matches.py --profile family --bank "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --report-out "data/paired/2026_03_24/family_bank_sync_report.csv" --uncleared-report-out "data/paired/2026_03_24/family_bank_uncleared_ynab_report.csv"
python scripts/sync_card_matches.py --profile family --account "Opher x9922" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x9922.csv"
python scripts/sync_card_matches.py --profile family --account "Liya X7195" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x7195.csv"
python scripts/sync_card_matches.py --profile family --account "Opher X5898" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x5898.csv"
```

## 2) Family Review

```bash
python scripts/review_app.py --profile family --in "data/paired/2026_03_24/family_proposed_transactions.csv"
```

Save as:

`data/paired/2026_03_24/family_proposed_transactions_reviewed.csv`

## 3) Family Update And Reconciliation

Prepare Family upload (dry-run):

```bash
python scripts/prepare_ynab_upload.py --profile family --in "data/paired/2026_03_24/family_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/family_upload.csv" --json-out "data/paired/2026_03_24/family_upload.json" --ready-only --reviewed-only --skip-missing-accounts
```

Execute Family upload:

```bash
python scripts/prepare_ynab_upload.py --profile family --in "data/paired/2026_03_24/family_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/family_upload.csv" --json-out "data/paired/2026_03_24/family_upload.json" --ready-only --reviewed-only --skip-missing-accounts --execute
```

Execute Family lineage sync:

```bash
python scripts/sync_bank_matches.py --profile family --bank "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --report-out "data/paired/2026_03_24/family_bank_sync_report.csv" --uncleared-report-out "data/paired/2026_03_24/family_bank_uncleared_ynab_report.csv" --execute
python scripts/sync_card_matches.py --profile family --account "Opher x9922" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x9922.csv" --execute
python scripts/sync_card_matches.py --profile family --account "Liya X7195" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x7195.csv" --execute
python scripts/sync_card_matches.py --profile family --account "Opher X5898" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_sync_report_x5898.csv" --execute
```

Family bank reconciliation (dry-run then execute):

```bash
python scripts/reconcile_bank_statement.py --profile family --bank "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --report-out "data/paired/2026_03_24/family_bank_reconcile_report.csv"
python scripts/reconcile_bank_statement.py --profile family --bank "data/derived/2026_03_24/Bankin family_leumi_norm.csv" --report-out "data/paired/2026_03_24/family_bank_reconcile_report.csv" --execute
```

Family card reconciliation rules (important):

- For this run (`2026_03_24`), and for later runs in the same billing month (for example `2026_03_19` and `2026_03_24`), use transition mode with the month statement files in `data/raw/previous_max/<card>/<YYYY_MM>.xlsx`.
- Reason: MAX installment rows can carry older purchase dates, so source-only mode can block even when statement reconciliation is already correct.
- Expected result when the statement was already reconciled in a prior run: `Warning: All previous-file transactions are already reconciled.` and `Updates planned: 0`.
- If transition mode says `No card payment transfer found...`, the `--previous` file is usually wrong (not the right statement month for that card).

Family card reconciliation branch A (source-only; use only when transition is not needed):

```bash
python scripts/reconcile_card_cycle.py --profile family --account "Opher x9922" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x9922.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Opher x9922" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x9922.csv" --execute
python scripts/reconcile_card_cycle.py --profile family --account "Liya X7195" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x7195.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Liya X7195" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x7195.csv" --execute
python scripts/reconcile_card_cycle.py --profile family --account "Opher X5898" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x5898.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Opher X5898" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_report_x5898.csv" --execute
```

Family card reconciliation branch B (default for this run: statement previous + current source):

```bash
python scripts/reconcile_card_cycle.py --profile family --account "Opher x9922" --previous "data/raw/previous_max/x9922/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x9922_prevmax.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Opher x9922" --previous "data/raw/previous_max/x9922/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x9922_prevmax.csv" --execute
python scripts/reconcile_card_cycle.py --profile family --account "Liya X7195" --previous "data/raw/previous_max/x7195/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x7195_prevmax.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Liya X7195" --previous "data/raw/previous_max/x7195/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x7195_prevmax.csv" --execute
python scripts/reconcile_card_cycle.py --profile family --account "Opher X5898" --previous "data/raw/previous_max/x5898/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x5898_prevmax.csv"
python scripts/reconcile_card_cycle.py --profile family --account "Opher X5898" --previous "data/raw/previous_max/x5898/2026_03.xlsx" --source "data/derived/2026_03_24/transaction-details_export_1774340215930_max_norm.csv" --report-out "data/paired/2026_03_24/family_card_reconcile_transition_report_x5898_prevmax.csv" --execute
```

Use branch B as default for Family unless branch A is proven clean for the exact card file.
Do not continue to Pilates until Family upload, lineage sync, and reconciliation steps are all blocker-free.

## 4) Pilates Bank, Card, And Cross-Budget Matching

Normalize Pilates source files:

Parser rule for Leumi card statements:

- Use `scripts/normalize_file.py --leumi-card-html ...` for Pilates card statements.
- This flag maps to `src/ynab_il_importer/io_leumi_card_html.py` (`is_proper_format` + `read_raw`).
- Do not use `--leumi-xls` or `--max` for these files.
- Keep output filenames with `_leumi_card_html_norm.csv` so downstream commands stay consistent.

```bash
python scripts/normalize_file.py --profile pilates --leumi "data/raw/2026_03_24/Bankin pilates.dat" --out "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv"
python scripts/normalize_file.py --profile pilates --leumi-card-html "data/raw/2026_03_24/Pilates card.xls" --out "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv"
```

If the second command fails with `does not look like a valid leumi_card_html file`, stop and re-export the Pilates card statement as the supported Leumi card HTML format, then rerun this step.

Normalize the latest closed Pilates statement from `previous_leumi_card` (used for transition reconciliation):

```bash
python scripts/normalize_file.py --profile pilates --leumi-card-html "data/raw/previous_leumi_card/x0602/2026_03.html" --out "data/derived/2026_03_24/Pilates card_previous_2026_03_leumi_card_html_norm.csv"
```

Extract the Pilates date window (one-off helper; copy the printed values into the next command):

```bash
python scripts/extract_date_window.py --label pilates --padding-days 14 --source "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --print-args
```

Refresh Pilates YNAB context:

```bash
python scripts/download_ynab_api.py --profile pilates --since "<pilates_ynab_since>" --until "<pilates_ynab_until>" --out "data/derived/2026_03_24/pilates_ynab_api_norm.csv"
python scripts/download_ynab_categories.py --profile pilates --out "outputs/pilates/ynab_categories.csv"
```

Build Pilates bank/card proposals:

```bash
python scripts/build_proposed_transactions.py --profile pilates --source "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --ynab "data/derived/2026_03_24/pilates_ynab_api_norm.csv" --out "data/paired/2026_03_24/pilates_proposed_transactions.csv" --pairs-out "data/paired/2026_03_24/pilates_matched_pairs.csv"
```

Dry-run Pilates lineage matching:

```bash
python scripts/sync_bank_matches.py --profile pilates --bank "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --report-out "data/paired/2026_03_24/pilates_bank_sync_report.csv" --uncleared-report-out "data/paired/2026_03_24/pilates_bank_uncleared_ynab_report.csv"
python scripts/sync_card_matches.py --profile pilates --account "Credit card 0602" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_sync_report.csv"
```

Dry-run cross-budget matching (Family category `Pilates` -> Pilates account `In Family`):

```bash
python scripts/build_cross_budget_proposed.py --source "data/derived/2026_03_24/family_ynab_api_norm.csv" --source-profile family --source-category Pilates --ynab "data/derived/2026_03_24/pilates_ynab_api_norm.csv" --target-profile pilates --target-account "In Family" --since 2026-03-19 --until 2026-03-24 --date-tolerance-days 0 --out "data/paired/2026_03_24/pilates_cross_budget_proposed_transactions.csv" --pairs-out "data/paired/2026_03_24/pilates_cross_budget_matched_pairs.csv" --unmatched-source-out "data/paired/2026_03_24/pilates_cross_budget_unmatched_source.csv" --unmatched-target-out "data/paired/2026_03_24/pilates_cross_budget_unmatched_target.csv" --ambiguous-out "data/paired/2026_03_24/pilates_cross_budget_ambiguous_matches.csv"
```

## 5) Pilates Review

Review Pilates bank/card proposals:

```bash
python scripts/review_app.py --profile pilates --in "data/paired/2026_03_24/pilates_proposed_transactions.csv"
```

Review Pilates cross-budget proposals:

```bash
python scripts/review_app.py --profile pilates --in "data/paired/2026_03_24/pilates_cross_budget_proposed_transactions.csv"
```

Save as:

- `data/paired/2026_03_24/pilates_proposed_transactions_reviewed.csv`
- `data/paired/2026_03_24/pilates_cross_budget_proposed_transactions_reviewed.csv`

## 6) Pilates Update And Reconciliation

Prepare and execute Pilates bank/card upload:

```bash
python scripts/prepare_ynab_upload.py --profile pilates --in "data/paired/2026_03_24/pilates_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/pilates_upload.csv" --json-out "data/paired/2026_03_24/pilates_upload.json" --ready-only --reviewed-only --skip-missing-accounts
python scripts/prepare_ynab_upload.py --profile pilates --in "data/paired/2026_03_24/pilates_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/pilates_upload.csv" --json-out "data/paired/2026_03_24/pilates_upload.json" --ready-only --reviewed-only --skip-missing-accounts --execute
```

Prepare and execute Pilates cross-budget upload:

```bash
python scripts/prepare_ynab_upload.py --profile pilates --in "data/paired/2026_03_24/pilates_cross_budget_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/pilates_cross_budget_upload.csv" --json-out "data/paired/2026_03_24/pilates_cross_budget_upload.json" --ready-only --reviewed-only --skip-missing-accounts
python scripts/prepare_ynab_upload.py --profile pilates --in "data/paired/2026_03_24/pilates_cross_budget_proposed_transactions_reviewed.csv" --out "data/paired/2026_03_24/pilates_cross_budget_upload.csv" --json-out "data/paired/2026_03_24/pilates_cross_budget_upload.json" --ready-only --reviewed-only --skip-missing-accounts --execute
```

Execute Pilates lineage sync:

```bash
python scripts/sync_bank_matches.py --profile pilates --bank "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --report-out "data/paired/2026_03_24/pilates_bank_sync_report.csv" --uncleared-report-out "data/paired/2026_03_24/pilates_bank_uncleared_ynab_report.csv" --execute
python scripts/sync_card_matches.py --profile pilates --account "Credit card 0602" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_sync_report.csv" --execute
```

Pilates bank reconciliation (dry-run then execute):

```bash
python scripts/reconcile_bank_statement.py --profile pilates --bank "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --report-out "data/paired/2026_03_24/pilates_bank_reconcile_report.csv"
python scripts/reconcile_bank_statement.py --profile pilates --bank "data/derived/2026_03_24/Bankin pilates_leumi_norm.csv" --report-out "data/paired/2026_03_24/pilates_bank_reconcile_report.csv" --execute
```

Pilates card reconciliation branch A (within-month, source-only):

```bash
python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --source-date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_reconcile_report.csv"
python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --source-date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_reconcile_report.csv" --execute
```

Pilates card reconciliation branch B (new-month, previous+source):

```bash
python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --previous "data/derived/2026_03_24/Pilates card_previous_2026_03_leumi_card_html_norm.csv" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --source-date-from 2026-01-01 --previous-date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_reconcile_transition_report.csv"
python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --previous "data/derived/2026_03_24/Pilates card_previous_2026_03_leumi_card_html_norm.csv" --source "data/derived/2026_03_24/Pilates card_leumi_card_html_norm.csv" --source-date-from 2026-01-01 --previous-date-from 2026-01-01 --report-out "data/paired/2026_03_24/pilates_card_reconcile_transition_report.csv" --execute
```

Choose branch A or branch B based on whether this file is same-cycle or month-transition.

Cross-budget reconciliation (dry-run then execute):

```bash
python scripts/reconcile_cross_budget_balance.py --source-profile family --source-category Pilates --target-profile pilates --target-account "In Family" --since 2026-03-19 --date-tolerance-days 0 --out "data/paired/2026_03_24/pilates_cross_budget_reconcile_report.csv" --month-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_month_report.csv" --source-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_source_report.csv" --status-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_status_report.csv" --target-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_target_report.csv" --pairs-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_matched_pairs.csv" --unmatched-source-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_unmatched_source.csv" --unmatched-target-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_unmatched_target.csv" --ambiguous-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_ambiguous_matches.csv"
python scripts/reconcile_cross_budget_balance.py --source-profile family --source-category Pilates --target-profile pilates --target-account "In Family" --since 2026-03-19 --date-tolerance-days 0 --out "data/paired/2026_03_24/pilates_cross_budget_reconcile_report.csv" --month-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_month_report.csv" --source-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_source_report.csv" --status-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_status_report.csv" --target-report-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_target_report.csv" --pairs-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_matched_pairs.csv" --unmatched-source-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_unmatched_source.csv" --unmatched-target-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_unmatched_target.csv" --ambiguous-out "data/paired/2026_03_24/pilates_cross_budget_reconcile_ambiguous_matches.csv" --execute
```

## End Of Run Checks

- Confirm upload artifacts exist:
  - `data/paired/2026_03_24/family_upload.csv`
  - `data/paired/2026_03_24/family_upload.json`
  - `data/paired/2026_03_24/pilates_upload.csv`
  - `data/paired/2026_03_24/pilates_upload.json`
  - `data/paired/2026_03_24/pilates_cross_budget_upload.csv`
  - `data/paired/2026_03_24/pilates_cross_budget_upload.json`
- Confirm sync/reconcile reports have no unexpected blockers before execute.
- Keep all run artifacts under `data/derived/2026_03_24/` and `data/paired/2026_03_24/`.

