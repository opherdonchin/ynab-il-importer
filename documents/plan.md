# Active Plan

## Workstream

Close out `family / 2026_06_06` first, then use the reconciled Family state as the boundary for Pilates and Aikido.

## Current State

- `family / 2026_06_06` is ready for human review after a source-boundary mapping fix:
  - raw sources present: bank + MAX card
  - normalized sources: bank `139` rows, MAX card `194` rows
  - source-windowed YNAB snapshot: `1,478` rows for `2025-11-25` through `2026-06-18`
  - matched pairs: `120` rows
  - proposal artifact: `113` rows
  - the `2026-05-24` Bank Leumi `+3500` transfer from account `22523701` now fingerprints to `pilates leumi` and proposes `Transfer : Pilates Leumi / Pilates`
- Pilates and Aikido artifacts for `2026_06_06` were generated during initial processing, but they are deferred:
  - do not review, upload, or reconcile Pilates until Family is reconciled
  - do not review, upload, or reconcile Aikido until Family is reconciled

## Recently Completed

- normalized the June previous Leumi card statement for Pilates account `x0602`
- normalized family direct bank/card sources for `2026_06_06`
- downloaded the family source-windowed YNAB snapshot
- normalized Pilates direct sources and family-category source rows
- normalized Aikido family-category source rows
- downloaded Pilates and Aikido source-windowed YNAB snapshots
- built family, Pilates, and Aikido review proposals
- fixed Leumi transfer fingerprint matching so numeric counterparty account identifiers can drive fingerprint-map rules before long digit runs are stripped from the final fingerprint
- rebuilt the Family normalized artifacts and proposal after the transfer mapping fix

## Next Steps

1. Review `family / 2026_06_06` only.
2. Save the Family reviewed artifact.
3. Run `context-run-status family 2026_06_06` and confirm only expected upload/report artifacts are missing.
4. Prepare the Family upload artifact:
   - `pixi run python scripts/prepare_ynab_upload.py family 2026_06_06 --ready-only --skip-missing-accounts`
5. After the Family upload prep looks correct, proceed with Family upload, sync, and reconciliation closeout.
6. Run `context-run-status family 2026_06_06 --verify-live` after each Family closing step until reports and live checks are clean.
7. Only after Family is reconciled, return to Pilates and Aikido with fresh source-windowed review artifacts if needed.

## Working Rules

- Keep review decisions explicit in the review artifact; do not let unresolved accounting rows appear reviewed.
- Fix lineage or matching drift at the source/proposal boundary rather than by manually relinking live YNAB rows.
- Rebase saved reviewed artifacts onto the freshly rebuilt proposal before resuming review.
- Keep source scope, review scope, and closeout scope separate when they are semantically different.
