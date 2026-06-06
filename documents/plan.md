# Active Plan

## Workstream

Process the `2026_06_06` context update through review, upload prep, and closeout without mixing source review, upload, and reconciliation scopes.

## Current State

- `family / 2026_06_06` is ready for human review:
  - raw sources present: bank + MAX card
  - normalized sources: bank `139` rows, MAX card `194` rows
  - source-windowed YNAB snapshot: `1,478` rows for `2025-11-25` through `2026-06-18`
  - matched pairs: `120` rows
  - proposal artifact: `113` rows
  - review app: `http://localhost:8502`
- `pilates / 2026_06_06` is ready for human review:
  - raw sources present: bank + Leumi card HTML
  - normalized sources: bank `12` rows, Leumi card `3` rows, family category source `56` rows
  - previous Leumi card `x0602 / 2026_06` normalized to `14` rows
  - source-windowed YNAB snapshot: `222` rows for `2025-11-22` through `2026-06-19`
  - matched pairs: `71` rows
  - proposal artifact: `20` rows
  - review app: `http://localhost:8501`
- `aikido / 2026_06_06` has no proposed review rows:
  - normalized family category source: `83` rows
  - source-windowed YNAB snapshot: `88` rows for `2025-11-13` through `2026-06-01`
  - matched pairs: `86` rows
  - proposal artifact: `0` rows

## Recently Completed

- normalized the June previous Leumi card statement for Pilates account `x0602`
- normalized family direct bank/card sources for `2026_06_06`
- downloaded the family source-windowed YNAB snapshot
- normalized Pilates direct sources and family-category source rows
- normalized Aikido family-category source rows
- downloaded Pilates and Aikido source-windowed YNAB snapshots
- built family, Pilates, and Aikido review proposals
- started review apps for the contexts that have review rows

## Next Steps

1. Complete human review for `family / 2026_06_06` and save the reviewed artifact.
2. Complete human review for `pilates / 2026_06_06` and save the reviewed artifact.
3. Run `context-run-status` for reviewed contexts and confirm only expected upload/report artifacts are missing.
4. Prepare upload artifacts with:
   - `pixi run python scripts/prepare_ynab_upload.py family 2026_06_06 --ready-only --skip-missing-accounts`
   - `pixi run python scripts/prepare_ynab_upload.py pilates 2026_06_06 --ready-only --skip-missing-accounts`
5. After review and upload prep look correct, proceed with upload, sync, and reconciliation closeout one context at a time.
6. For Pilates card closeout, use the normalized previous statement:
   - `data/derived/previous_leumi_card/x0602/2026_06_leumi_card_html_norm.parquet`
7. Run `context-run-status --verify-live` after each closing step until reports and live checks are clean.

## Working Rules

- Keep review decisions explicit in the review artifact; do not let unresolved accounting rows appear reviewed.
- Fix lineage or matching drift at the source/proposal boundary rather than by manually relinking live YNAB rows.
- Rebase saved reviewed artifacts onto the freshly rebuilt proposal before resuming review.
- Keep source scope, review scope, and closeout scope separate when they are semantically different.
