# Reconciliation Packets

## Purpose

A reconciliation packet is a saved, dated snapshot of one concrete run.

It should contain:
- the source inputs we reconciled against
- the YNAB-side downloads we used
- the generated reports and proposals
- any reviewed/upload artifacts
- any explicit legacy/bootstrap exceptions

The goal is simple:
- reruns should be reproducible
- debugging should not depend on live YNAB availability
- handoff between human and agent work should stay easy

This matters most for:
- bank statements
- credit-card statement cycles
- cross-budget account/category reconciliation

---

## General Packet Shape

Each packet lives in:

`data/packets/<kind>/.../<window>/`

and contains:

- `inputs/`
- `artifacts/`
- `outputs/`
- `packet_manifest.json`
- `packet_summary.md`

Where:
- `inputs/` = raw statements, normalized source files, YNAB downloads, cached month reports
- `artifacts/` = sync/reconcile/match/proposal reports
- `outputs/` = reviewed CSVs, upload-prep CSV/JSON, optional manual notes

The manifest records:
- packet kind
- scope
- original file paths
- copied packet paths
- file hashes
- acquisition method for each file
- the important commands used for the run
- short human notes
- explicit exception ids

---

## Packet Kinds

### 1. Bank

Packet root:

`data/packets/bank/<profile>/<account>/<window>/`

Recommended input roles:
- `statement_raw`
- `statement_norm`
- `ynab_snapshot`

Recommended artifact roles:
- `sync_report`
- `reconcile_report`
- `uncleared_report`
- `triage_report`

Recommended output roles:
- `proposed_reviewed`
- `upload_csv`
- `upload_json`

### 2. Card

Packet root:

`data/packets/card/<profile>/<account>/<window>/`

Recommended input roles:
- `statement_current_raw`
- `statement_previous_raw`
- `statement_current_norm`
- `statement_previous_norm`
- `ynab_snapshot`

Recommended artifact roles:
- `sync_report`
- `reconcile_report`
- `payment_transfer_report`

Recommended output roles:
- `proposed_reviewed`
- `upload_csv`
- `upload_json`

### 3. Cross-budget

Packet root:

`data/packets/cross_budget/<target_profile>/<source_profile>__<source_category>__<target_account>/<window>/`

Recommended input roles:
- `source_category_export`
- `source_ynab_snapshot`
- `target_ynab_snapshot`
- `source_month_report`

Recommended artifact roles:
- `matched_pairs`
- `unmatched_source`
- `unmatched_target`
- `ambiguous_matches`
- `proposed_transactions`
- `reconcile_summary`
- `reconcile_month_report`
- `reconcile_source_report`
- `reconcile_target_report`

Recommended output roles:
- `proposed_reviewed`
- `upload_csv`
- `upload_json`

---

## Human Downloads vs Agent Downloads

The practical split should be:

### Human / in-person downloads

These are source-of-truth financial statements and should remain saved in their original form.

Examples:
- bank statement exports
- credit-card statement exports

These should continue to live first under `data/raw/...`
and then be copied into the relevant packet `inputs/`.

When adding them to a packet, prefer:
- `--input-human ROLE=PATH`

### Agent downloads

These are workflow-supporting YNAB snapshots and derived exports.

Examples:
- `download_ynab_api.py`
- `download_ynab_categories.py`
- `io_ynab_as_source.py`
- cached cross-budget month reports

These should be saved under `data/derived/...` or `data/paired/...` as usual,
and then copied into the relevant packet `inputs/` or `artifacts/`.

When adding them to a packet, prefer:
- `--input-agent ROLE=PATH` for direct YNAB exports/downloads
- `--input-derived ROLE=PATH` for normalized or cached CSV inputs
- `--artifact ROLE=PATH` for generated reports
- `--output ROLE=PATH` for reviewed/upload files

The packet summary will then show the origin clearly:
- `human_download`
- `agent_download`
- `derived_input`
- `agent_generated`
- `human_reviewed`
- `upload_payload`

---

## Month Reports

### Bank

Bank reconciliation is statement-first.

The real “monthly packet” is the bank statement itself plus the reconcile report.
There is no separate YNAB month cache requirement today.

### Card

Card reconciliation is cycle-first.

The real packet boundary is the statement cycle:
- previous statement
- current statement
- current sync report
- current reconcile report

Again, the statement files are the primary monthly/cycle record.

### Cross-budget

Cross-budget reconciliation is different.

It needs a saved Family-side month history because the anchor search depends on month-end category balances.

So for cross-budget packets we should explicitly save:
- `source_month_report.csv`

This can be reused on reruns so we do not spend YNAB API quota refetching every historical month detail.

---

## Explicit Exceptions

Sometimes a historical/base row is real and should stay in the account,
but does not have a clean line-by-line source-side counterpart.

Those should not be hidden.

Instead:
- keep the row in YNAB
- record the row id explicitly
- include it in the packet manifest under `exceptions`

Examples:
- `ignore_target_ids`
- `ignore_source_ids`

That gives us a documented bootstrap/base exception instead of a silent special case.

---

## Current Tooling

Packet creation is now supported by:

```powershell
pixi run python scripts/package_reconciliation_packet.py ...
```

The script can:
- copy files into a packet folder
- or write a manifest-only packet with references to the original files
- preserve the important commands used for the run
- preserve short handoff notes

It writes:
- `packet_manifest.json`
- `packet_summary.md`

This is deliberately simple and does not try to infer workflow state automatically.
We should pass the intended files and roles explicitly.

### Example: bank packet

```powershell
pixi run python scripts/package_reconciliation_packet.py `
  --kind bank `
  --profile pilates `
  --account "Bank Leumi 225237" `
  --window-start 2025-11-01 `
  --window-end 2026-03-19 `
  --input-human statement_raw=data/raw/pilates_bootstrap/Bankin\ 2025_11_01\ to\ 2026_03_19.dat `
  --input-derived statement_norm=data/derived/pilates_bootstrap/Bankin\ 2025_11_01\ to\ 2026_03_19_leumi_norm.csv `
  --input-agent ynab_snapshot=data/derived/pilates/ynab_api_norm.csv `
  --artifact sync_report=data/paired/pilates_live/bank_sync_report.csv `
  --artifact reconcile_report=data/paired/pilates_live/bank_reconcile_report_verify.csv `
  --command "pixi run python scripts/sync_bank_matches.py --profile pilates --bank data/derived/pilates_bootstrap/Bankin 2025_11_01 to 2026_03_19_leumi_norm.csv --report-out data/paired/pilates_live/bank_sync_report.csv" `
  --command "pixi run python scripts/reconcile_bank_statement.py --profile pilates --bank data/derived/pilates_bootstrap/Bankin 2025_11_01 to 2026_03_19_leumi_norm.csv --report-out data/paired/pilates_live/bank_reconcile_report_verify.csv" `
  --note "Verified bank window with zero uncleared rows and zero planned updates."
```

### Example: card packet

```powershell
pixi run python scripts/package_reconciliation_packet.py `
  --kind card `
  --profile pilates `
  --account "Credit card 0602" `
  --window-start 2026-01-01 `
  --window-end 2026-03-19 `
  --input-human statement_current_raw=data/raw/pilates_bootstrap/pilates\ card\ 2026_04.html `
  --input-human statement_previous_raw=data/raw/pilates_bootstrap/pilates\ card\ 2026_03.html `
  --input-derived statement_current_norm=data/derived/pilates_bootstrap/pilates\ card\ 2026_04_leumi_card_html_norm.csv `
  --input-derived statement_previous_norm=data/derived/pilates_bootstrap/pilates\ card\ 2026_03_leumi_card_html_norm.csv `
  --input-agent ynab_snapshot=data/derived/pilates/ynab_api_norm.csv `
  --artifact sync_report=data/paired/pilates_live/card_2026_04_sync_report_boundary.csv `
  --artifact reconcile_report=data/paired/pilates_live/card_2026_03_to_2026_04_reconcile_report_boundary.csv `
  --command "pixi run python scripts/sync_card_matches.py --profile pilates --account \"Credit card 0602\" --source data/derived/pilates_bootstrap/pilates card 2026_04_leumi_card_html_norm.csv --date-from 2026-01-01 --report-out data/paired/pilates_live/card_2026_04_sync_report_boundary.csv" `
  --command "pixi run python scripts/reconcile_card_cycle.py --profile pilates --account \"Credit card 0602\" --previous data/derived/pilates_bootstrap/pilates card 2026_03_leumi_card_html_norm.csv --source data/derived/pilates_bootstrap/pilates card 2026_04_leumi_card_html_norm.csv --source-date-from 2026-01-01 --previous-date-from 2026-01-01 --report-out data/paired/pilates_live/card_2026_03_to_2026_04_reconcile_report_boundary.csv" `
  --note "Boundary-forward exact card reconciliation starts at the clean itemized period."
```

### Example: cross-budget packet

```powershell
pixi run python scripts/package_reconciliation_packet.py `
  --kind cross_budget `
  --source-profile family `
  --source-category Pilates `
  --target-profile pilates `
  --target-account "In Family" `
  --window-start 2025-11-01 `
  --window-end 2026-03-24 `
  --input-agent source_ynab_snapshot=data/derived/family/ynab_api_norm.csv `
  --input-agent target_ynab_snapshot=data/derived/pilates/ynab_api_norm.csv `
  --input-derived source_month_report=data/paired/pilates_cross_budget_live/anchored_reconcile_after_history_upload_month_report.csv `
  --artifact reconcile_summary=data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_cached_report.csv `
  --artifact reconcile_month_report=data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_cached_month_report.csv `
  --artifact reconcile_source_report=data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_cached_source_report.csv `
  --artifact reconcile_target_report=data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_cached_target_report.csv `
  --command "pixi run python scripts/reconcile_cross_budget_balance.py --source-profile family --source-category Pilates --target-profile pilates --target-account \"In Family\" --since 2025-11-01 --source-month-report-in data/paired/pilates_cross_budget_live/anchored_reconcile_after_history_upload_month_report.csv --out data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_cached_report.csv" `
  --note "Cached Family month history avoids repeat YNAB month-detail downloads and rate-limit churn."
```

---

## Recommended Next Step

Use saved packets as a normal part of the workflow:

1. finish a run
2. save the relevant source files and reports into a packet
3. treat that packet as the reproducible record for that run

If we expect to revisit the run later, also add:
4. the main commands that produced the run
5. a short note about any accepted legacy/base exceptions

For Pilates cross-budget work, the next packet should include:
- Family category export
- Pilates YNAB snapshot
- cached source month report
- final matched/unmatched reports
- reviewed proposals
- upload artifacts
- final reconcile reports
- any accepted legacy exception ids
