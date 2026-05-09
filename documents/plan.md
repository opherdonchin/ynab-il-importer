# Active Plan

## Workstream

Keep the May 2026 context closeout workflow rerunnable and explicit, with uploaded runs staying clean after sync, reconciliation, and report refresh.

## Current State

- `family / 2026_05_07` is fully uploaded, synced, and clean in live verification:
  - upload artifact: `create=55 | update=0`
  - upload execution: `newly saved=60 | duplicate_import_ids=0 | matched_existing=0`
  - bank sync execution: `patched=3`
  - bank reconcile execution: `patched=36`
  - final artifact-only status: `reports clean=9`
  - final live status: `live clean=10`
- The restored handoff artifacts for `family / 2026_05_07` came from [family_2026_05_07_handoff.zip](sync_handoffs/family_2026_05_07_handoff.zip).
- The local raw input directory for `2026_05_07` is still absent on this machine; canonical derived and paired artifacts are present.
- `family / 2026_05_02`, `pilates / 2026_05_02`, and `aikido / 2026_05_02` remain closed from the previous workstream.

## Recently Completed

- recovered the office-machine `family / 2026_05_07` generated artifacts from the committed handoff zip
- reviewed all 55 proposed family rows
- uploaded the reviewed family rows to YNAB
- synced the three bank lineage stamps needed after upload
- reconciled the Bank Leumi statement through the May 7 run
- refreshed bank and card closeout reports so on-disk status matches clean live state

## Next Steps

1. Take the next context/run-tag closeout in priority order as directed by the user.
2. When revisiting `family / 2026_05_07`, note that live state and reports are clean, but raw source files are not present locally.
3. When starting a new run:
   - normalize the context sources
   - download a source-windowed YNAB snapshot
   - build and review the proposal
   - prepare upload artifacts before any `--execute`
   - run `context-run-status --verify-live` after each closing step until reports and live checks are clean

## Working Rules

- Keep review decisions explicit in the review artifact; do not let unresolved accounting rows appear reviewed.
- Fix lineage or matching drift at the source/proposal boundary rather than by manually relinking live YNAB rows.
- Rebase saved reviewed artifacts onto the freshly rebuilt proposal before resuming review.
- Keep source scope, review scope, and closeout scope separate when they are semantically different.
