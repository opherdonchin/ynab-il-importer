# Active Plan

## Workstream

Keep the May 2, 2026 closeout workflow rerunnable and explicit, with rebuilt proposals staying empty once a context is truly closed.

## Current State

- `family / 2026_05_02` is fully clean on disk and in live verification:
  - artifact-only status: `reports clean=9`
  - live status: `reports clean=9 | live clean=10`
- `pilates / 2026_05_02` is fully clean on disk and in live verification.
- `aikido / 2026_05_02` is now fully clean on disk and in live verification:
  - proposal artifact: `rows=0`
  - reviewed artifact: `rows=0`
  - upload artifacts: `create=0 | update=0`
  - category reconcile report: `rows=0`
  - live status: `reports clean=1 | live clean=2`
- Closed category-account runs no longer resurface stale reviewed rows on reopen:
  - review matching now pairs existing target transactions by canonical lineage when present
  - category-source rows without source `import_id` now also pair by the deterministic `YNAB:<amount>:<date>:<occurrence>` import id that upload prep would generate
  - `review-context` now rebases the saved reviewed artifact onto the current proposal before auto-resume

## Recently Completed

- fixed MAX card lineage drift and explicit Family card closeout scoping so `family / 2026_05_02` is clean on disk and live
- fixed Leumi HTML pending-to-posted card lineage drift for Pilates card imports
- completed the full `pilates / 2026_05_02` closeout
- fixed category-source proposal resurfacing for already-uploaded Aikido rows
- hardened review resume so stale reviewed artifacts cannot repopulate an empty rebuilt proposal
- refreshed the Aikido May 2 artifacts so the saved on-disk state matches the clean live state

## Next Steps

1. Take the next context/run-tag closeout in priority order as directed by the user.
2. When revisiting a previously closed context, refresh the YNAB snapshot and rebuild the proposal before opening review.
3. Keep the review boundary strict:
   - empty rebuilt proposal should mean empty reviewed artifact after rebase
   - no stale upload payloads should remain attached to a zero-row reviewed artifact

## Working Rules

- Keep review decisions explicit in the review artifact; do not let unresolved accounting rows appear reviewed.
- Fix lineage or matching drift at the source/proposal boundary rather than by manually relinking live YNAB rows.
- Rebase saved reviewed artifacts onto the freshly rebuilt proposal before resuming review.
- Keep source scope, review scope, and closeout scope separate when they are semantically different.
