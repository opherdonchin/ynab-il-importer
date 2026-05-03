# Active Plan

## Workstream

Keep institutional workflow boundaries explicit while maintaining a fully reproducible clean closeout state for the active Family run.

## Current State

- `family / 2026_05_02` is now fully clean both on disk and in live verification:
  - artifact-only status: `reports clean=9`
  - live status: `reports clean=9`, `live clean=10`
- saved closeout reports were refreshed in dry-run mode for:
  - Family bank sync
  - Family bank uncleared triage
  - Family bank reconcile
  - Family card sync/reconcile for `Liya X7195`
  - Family card sync/reconcile for `Opher X5898`
  - Family card sync/reconcile for `Opher x9922`
- Family MAX source scope still includes:
  - `Bank Leumi`
  - `Liya X7195`
  - `Opher X5898`
  - `Opher x9922`
- Family card closeout scope remains explicitly narrower:
  - `Bank Leumi` stays in source scope for overlap/dedupe behavior
  - only the statement-reconciled Family card accounts participate in card closeout
- Family has no review/upload work pending for this run:
  - proposal rows `0`
  - reviewed rows `0`
  - live upload preflight manual matches `0`

## Recently Completed

- refreshed the saved `family / 2026_05_02` closeout reports so disk state matches current live YNAB state
- verified refreshed artifact-only status:
  - `pixi run context-run-status -- family 2026_05_02`
- verified refreshed live status:
  - `pixi run context-run-status -- family 2026_05_02 --verify-live`
- confirmed final clean summaries:
  - bank sync report `noop=40`
  - bank reconcile report `already_reconciled=33 | anchor_history=7`
  - `Liya X7195` sync `noop=28`, reconcile `already_reconciled=26 | keep_cleared=27`
  - `Opher X5898` sync `noop=10`, reconcile `already_reconciled=9 | keep_cleared=10`
  - `Opher x9922` sync `noop=26`, reconcile `already_reconciled=42 | keep_cleared=26`

## Next Steps

1. Leave Family `2026_05_02` closed unless a new source file or live YNAB change requires reopening it.
2. Keep `x0602` for the separate Pilates closeout workflow.
3. When the next active run starts, preserve the same explicit distinction between source scope and closeout scope where needed.

## Working Rules

- Keep source scope and closeout scope separate when they are semantically different.
- Preserve debit-card source rows when they help overlap/dedupe behavior, but do not force them through statement-reconciliation workflows.
- Prefer explicit per-source configuration over name-based inference or account-type guesswork.
