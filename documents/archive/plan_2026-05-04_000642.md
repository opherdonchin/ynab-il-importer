# Active Plan

## Workstream

Keep the institutional workflow boundaries explicit: Family debit-card MAX rows stay available for source overlap handling, but only statement-reconciled card accounts participate in Family card closeout.

## Current State

- `family / 2026_05_02` is clean in live closeout state:
  - bank sync `matched=40 | updates=0 | unmatched=0 | blocked=0`
  - bank reconcile `ok=True | updates=0 | unmatched=0`
  - card sync/reconcile clean for:
    - `Liya X7195`
    - `Opher X5898`
    - `Opher x9922`
- Family MAX source scope still includes `Bank Leumi`, `Liya X7195`, `Opher X5898`, and `Opher x9922`
- Family card closeout scope is now explicitly narrower than Family MAX source scope:
  - `Bank Leumi` stays in source scope for overlap/dedupe behavior
  - `Bank Leumi` does not participate in Family card closeout because those `x0740` / `x0849` rows do not have monthly statement reconciliation
- canonical card lineage compatibility remains in place for known MAX sheet drift:
  - `עסקאות לידיעה`
  - `עסקאות חו"ל ומט"ח`
- saved May 2 closeout reports on disk are still historical and do not fully reflect the current live-clean state

## Recently Completed

- added explicit source-level closeout account scope in context config:
  - raw-backed sources can now declare `closeout_account_names`
  - when omitted, closeout scope defaults to full `target_account_names`
- updated Family MAX context so:
  - `target_account_names` still includes `Bank Leumi`
  - `closeout_account_names` only includes the true statement-reconciled Family card accounts
- updated card carryforward/status logic to use closeout scope rather than full source target scope:
  - [build_context_review.py](build_context_review.py)
  - [context_run_status.py](../src/ynab_il_importer/context_run_status.py)
- verified config/build/status coverage:
  - `tests/test_context_config.py`
  - `tests/test_build_context_review_script.py`
  - `tests/test_context_run_status.py`
- verified live status:
  - `pixi run context-run-status -- family 2026_05_02 --verify-live`

## Next Steps

1. Decide whether to refresh the saved `2026_05_02` Family closeout reports/artifacts so on-disk status matches the current live-clean state.
2. If that refresh is desired, rerun the Family closeout reports against current live YNAB and replace the stale saved report files.
3. Leave `x0602` for the separate Pilates closeout workflow.

## Working Rules

- Keep source scope and closeout scope separate when they are semantically different.
- Preserve debit-card source rows when they help overlap/dedupe behavior, but do not force them through statement-reconciliation workflows.
- Prefer explicit per-source configuration over name-based inference or account-type guesswork.
