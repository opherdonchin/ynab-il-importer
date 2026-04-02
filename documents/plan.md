# Active Plan

## Workstream

Use `code-review-refactor` as the acceptance-test branch for the real `2026_04_01` update run.

Current intent:
- treat the refactor as done enough to validate in live workflow use
- run the full update in order: Family, then Pilates, then Aikido
- improve workflow clarity and repeatability while we work
- merge back to `main` if the run is smooth and any remaining issues are understood

## Current Goal

Complete the `2026_04_01` operational run end to end and use it to answer two questions:

1. Does the refactored review workflow hold up in real use?
2. What needs to be standardized so future runs can be partially or fully automated?

## Current Status

Done:
- read project context and prior plan state
- audited the current runbook and active scripts before execution
- identified command drift in docs: cross-budget builder is `scripts/build_cross_budget_review_rows.py`
- normalized the new raw drop under `data/derived/2026_04_01/`:
  - `Bankin family_leumi_norm.csv`
  - `transaction-details_export_1775044561886_max_norm.csv`
  - `Bankin pilates_leumi_norm.csv`
  - `Pilates card_leumi_card_html_norm.csv`
- extracted bounded YNAB windows for Family and Pilates from source dates
- refreshed Family YNAB context for the new run:
  - `data/derived/2026_04_01/family_ynab_api_norm.csv`
  - `outputs/family/ynab_categories.csv`
- built fresh Family review artifacts:
  - `data/paired/2026_04_01/family_proposed_transactions.csv`
  - `data/paired/2026_04_01/family_matched_pairs.csv`
- ran Family dry-run lineage matching:
  - `family_bank_sync_report.csv`
  - `family_bank_uncleared_ynab_report.csv`
  - `family_card_sync_report_x9922.csv`
  - `family_card_sync_report_x7195.csv`
  - `family_card_sync_report_x5898.csv`
- built smaller Family review helper files for the actual decision surface:
  - `family_proposed_transactions_focus_source_only.csv`
  - `family_proposed_transactions_focus_ambiguous_detail.csv`
  - `family_proposed_transactions_focus_ambiguous_grouped.csv`
- added an explicit legacy review translator:
  - `scripts/translate_review_csv.py`
- changed review loading to a strict two-step flow:
  - unified review CSVs load normally
  - legacy institutional review CSVs fail with guidance to translate first
- translated old reviewed files into explicit unified artifacts:
  - `data/paired/2026_03_24/family_proposed_transactions_reviewed_unified_v1.csv`
  - `data/paired/2026_03_24/pilates_proposed_transactions_reviewed_unified_v1.csv`
- added focused regression coverage for legacy review detection and translation

Current Family state:
- Family source window:
  - `2026-01-18` through `2026-04-01`
- Family YNAB fetch window:
  - `2026-01-04` through `2026-04-15`
- Family proposal rows:
  - `685`
- active Family review surface:
  - `54` `source_only` rows
  - `32` grouped ambiguous items
- Family bank dry-run sync:
  - matched `100`
  - updates planned `4`
  - unmatched `32`
  - blocked `0`
- Family card dry-run sync:
  - `Opher x9922`: unmatched `7`, blocked `0`
  - `Liya X7195`: unmatched `8`, blocked `0`
  - `Opher X5898`: unmatched `3`, blocked `0`

Workflow findings already confirmed:
- per-account reconciliation must stay explicit for every relevant account
- translated legacy review files are useful reference artifacts, but old create-target decisions cannot be blindly replayed onto a fresh post-upload proposal
- some previously reviewed map-update candidates were never promoted into `mappings/payee_map.csv`, so repeat fingerprints are still appearing without suggestions

Known Family mapping follow-up candidates from this run:
- likely missing or not yet applied:
  - `spareeat`
  - `mei sheva`
  - `clalit`
  - `oren meshi`
  - `spotify stockholm עסקת חו`
  - `kahoot asa oslo`
  - `גרנד fox`
- likely already represented but worth verifying against current behavior:
  - `paypal facebook עסקת חו`
  - `yellow`

## Working Rules For This Run

- Keep work in this order:
  1. Family
  2. Pilates
  3. Aikido
- Each section is not complete until review, upload/sync, and all required reconciliations are done.
- Reconciliation coverage must include every relevant account, not just the main budget-level flow.
- Prefer explicit, reusable artifacts over ad hoc terminal reasoning.
- When a workflow gap appears, capture the smallest durable fix:
  - naming convention
  - helper script
  - review artifact
  - documented command sequence

## Standardization Direction

The run should keep pushing toward a workflow app or checklist-driven process. Current preferred direction:

- use column-based format detection rather than filename-only assumptions
- keep translated legacy review files explicit in naming, for example `_unified_v1.csv`
- preserve dated run roots under `data/derived/<run_tag>/` and `data/paired/<run_tag>/`
- keep per-stage commands stable and profile-driven
- generate smaller review helper artifacts when the raw proposal is too noisy
- close the loop from reviewed map-update candidates into real map files or an explicit pending queue

## Next Steps

1. Finish Family mapping decisions and complete Family review.
2. Execute Family upload, lineage sync, bank reconciliation, and all three card reconciliations.
3. Refresh Pilates YNAB context, build both institutional and cross-budget artifacts, then review and reconcile all Pilates accounts.
4. Run the Aikido cross-budget flow, review it, upload it, and reconcile it.
5. Keep notes on friction points and convert them into doc or script improvements during the run.
6. If the full run is clean, prepare merge-back to `main`.

## Merge Readiness Criteria

- Family, Pilates, and Aikido all complete without unexplained blockers
- documentation reflects the actual current workflow
- legacy review translation path is explicit and tested
- script names and runbook commands match the codebase
- remaining rough edges are either fixed or clearly documented as follow-up
