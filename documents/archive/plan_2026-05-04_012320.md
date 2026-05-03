# Active Plan

## Workstream

Keep institutional workflow boundaries explicit while maintaining reproducible clean closeout states for the active Family and Pilates runs.

## Current State

- `family / 2026_05_02` remains fully clean both on disk and in live verification:
  - artifact-only status: `reports clean=9`
  - live status: `reports clean=9`, `live clean=10`
- `pilates / 2026_04_28` now has clean card closeout state again after fixing Leumi pending-to-posted lineage drift:
  - artifact-only status: `reports clean=5 | pending=1`
  - live status: `reports clean=5 | pending=1`, `live clean=6 | pending=1`
- the only remaining Pilates status is the bank uncleared recency window:
  - `recent_pending=2`
  - rows dated `2026-04-28` and `2026-05-01`
  - no stale orphan rows and no candidate source matches
- Pilates card closeout is now clean on disk and live for `Credit card 0602`:
  - saved sync report: `noop=18`
  - saved reconcile report: `already_reconciled=24 | keep_cleared=18`
  - live sync: `matched=18 | updates=0 | unmatched=0 | blocked=0`
  - live reconcile: `ok=True | updates=0`

## Recently Completed

- added explicit MAX lineage compatibility for sheet drift and explicit context closeout scoping for Family
- refreshed the saved `family / 2026_05_02` closeout reports so disk state matches current live YNAB state
- fixed Leumi HTML card lineage compatibility for pending-to-posted drift, including:
  - section-title drift
  - transaction-type drift
  - pending secondary-date drift seen on first-of-month pending rows
- re-normalized `pilates / 2026_04_28` so the Leumi card artifact carries the metadata needed for alias matching
- refreshed the saved Pilates card sync report for `Credit card 0602`
- verified current Pilates status with:
  - `pixi run context-run-status -- pilates 2026_04_28`
  - `pixi run context-run-status -- pilates 2026_04_28 --verify-live`

## Next Steps

1. Leave `family / 2026_05_02` closed unless a new source file or live YNAB change requires reopening it.
2. Leave `pilates / 2026_04_28` open only for the bank recency window; rerun status after the bank source advances enough to clear the two recent pending rows.
3. If another Leumi HTML run shows pending-to-posted identity drift, treat it as a regression against the new alias coverage and tighten the compatibility boundary rather than doing manual live cleanup.

## Working Rules

- Keep source scope and closeout scope separate when they are semantically different.
- Preserve debit-card source rows when they help overlap/dedupe behavior, but do not force them through statement-reconciliation workflows.
- Prefer explicit per-source configuration over name-based inference or account-type guesswork.
- When card identities drift because a source re-buckets the same transaction between pending and posted views, fix that at the lineage boundary instead of manually relinking old YNAB rows.
