# Active Plan

## Workstream

Keep the institutional workflow explicit while closing the active May 2, 2026 Family and Pilates runs without manual lineage cleanup or hidden upload assumptions.

## Current State

- `family / 2026_05_02` remains fully clean on disk and in live verification:
  - artifact-only status: `reports clean=9`
  - live status: `reports clean=9 | live clean=10`
- `pilates / 2026_05_02` is now the active run and the proposal/review boundary is back in a sane state:
  - proposal artifact rebuilt with explicit deterministic target payees for source-only creates
  - reviewed artifact now keeps only the actually resolved subset reviewed: `reviewed=12`
  - upload prep succeeds for that reviewed subset and writes `11` create rows
  - category-account reconcile is clean for the reviewed subset: `already_reconciled=8`
- The remaining open Pilates work is real review/accounting work, not lineage drift:
  - `9` `In Family` backlog rows still need Pilates-side category decisions before they can be approved
  - bank still has `1` stamp+clear row, `1` unmatched row, and `1` reconcile blocker
  - card `0602` still has `2` genuinely missing new rows and `2` reconcile blockers tied to them

## Recently Completed

- fixed MAX card lineage drift and explicit Family card closeout scoping so `family / 2026_05_02` is clean on disk and live
- fixed Leumi HTML pending-to-posted card lineage drift for Pilates card imports
- corrected the active Pilates run from `2026_04_28` to `2026_05_02`
- rebuilt the missing May 2 Pilates artifacts:
  - context YNAB snapshot
  - matched pairs artifact
  - proposal artifact
- tightened proposal generation so source-only review rows materialize deterministic target payees and singleton target options without pretending unknown categories are resolved
- regenerated the May 2 reviewed/upload/category artifacts so approved rows flow cleanly while unresolved backlog rows remain in review

## Next Steps

1. Review the remaining `9` unresolved `In Family` backlog rows in `pilates / 2026_05_02` and choose the correct Pilates-side categories.
2. After those review decisions are made, regenerate the reviewed artifact and rerun:
   - `pixi run python scripts/prepare_ynab_upload.py pilates 2026_05_02 --reviewed-only`
   - `pixi run reconcile-category-account pilates 2026_05_02`
3. Finish the non-review closeout items for the May 2 Pilates run:
   - create the missing bank transaction `2026-04-28 | 1404.0 | מס הכנסה עצמ-י`
   - stamp+clear the existing `2026-04-28 | 7000.0` transfer row
   - create the two missing card transactions for `Credit card 0602`
4. Rerun `pixi run context-run-status -- pilates 2026_05_02 --verify-live` after each closing step until the run is fully clean.

## Working Rules

- Keep review decisions explicit in the review artifact; do not let unresolved accounting rows appear reviewed.
- Materialize deterministic target selections early, but leave genuinely ambiguous categories for human review.
- Fix lineage or matching drift at the source/proposal boundary rather than by manually relinking live YNAB rows.
- Keep source scope, review scope, and closeout scope separate when they are semantically different.
