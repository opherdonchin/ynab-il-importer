# Active Plan

## Workstream

Close the `family / 2026_05_02` card boundary cleanly, starting with the historical MAX lineage drift that was blocking `Opher X5898`, then moving through the remaining in-scope Family card accounts one at a time.

## Current State

- `family / 2026_05_02` live bank closeout is clean:
  - sync `matched=40 | updates=0 | unmatched=0 | blocked=0`
  - reconcile `ok=True | updates=0 | unmatched=0`
- the May 2 Family card source contains only one live Family blocker now:
  - `Bank Leumi` still has `updates=58 | unmatched=6` and no inferred previous snapshot
- `Opher X5898` is now clean live after fixing MAX lineage compatibility:
  - sync `matched=10 | updates=0 | unmatched=0 | blocked=0`
  - reconcile `updates=0`
- `Liya X7195` is also clean live after the same lineage fix:
  - sync `matched=28 | updates=0 | unmatched=0 | blocked=0`
  - reconcile `updates=0`
- `Opher x9922` was already clean live and remains clean:
  - sync `matched=26 | updates=0 | unmatched=0 | blocked=0`
  - reconcile `updates=0`
- the canonical transaction boundary now preserves MAX identity inputs needed for lineage compatibility:
  - `max_sheet`
  - `max_txn_type`
  - `max_original_amount`
  - `max_original_currency`
- card lineage matching now accepts the known MAX sheet drift between:
  - `עסקאות לידיעה`
  - `עסקאות חו"ל ומט"ח`

## Recently Completed

- reproduced the `Opher X5898` blocker against `family / 2026_05_02`
- confirmed the root cause:
  - the same March MAX transactions changed `CARD:V1` ids between the April 1 and May 2 exports because the row moved between MAX sheet buckets while the hash still included `max_sheet`
- fixed the boundary without rewriting historical YNAB card ids:
  - canonical `transaction_v1` artifacts now retain the MAX identity inputs needed to reason about historical card ids
  - card sync/reconcile now recognizes the known MAX sheet-compatible alias set instead of treating those rows as unrelated transactions
- rebuilt `data/derived/2026_05_02/family_max_norm.parquet`
- verified with focused tests:
  - `tests/test_card_identity.py`
  - `tests/test_card_reconciliation.py`
  - `tests/test_transaction_artifacts.py`
  - `tests/test_build_context_review_script.py`
  - `tests/test_context_run_status.py`
- verified live status with:
  - `pixi run context-run-status -- family 2026_05_02 --verify-live`

## Next Steps

1. Leave `x0602` out of Family work; it belongs to the separate Pilates Leumi-card closeout.
2. Confirm there is no remaining Family work for `Opher x9922`; keep it closed unless a fresh live rerun regresses.
3. Confirm there is no remaining Family work for `Liya X7195`; keep it closed unless a fresh live rerun regresses.
4. Decide how to handle Family `Bank Leumi`:
   - keep it in Family scope and fix previous-snapshot inference plus the 6 ambiguous rows, or
   - explicitly move/defer that account if it should not be part of the current Family closeout
5. If Family `Bank Leumi` stays in scope, make status/review use the declared account-map boundary for previous-card lookup instead of inferring from digits in the YNAB account name.

## Working Rules

- Prefer explicit account-map boundaries over name-based inference.
- Keep card identity compatibility at the canonical source boundary, not as ad hoc live cleanup.
- Do not mutate live YNAB just to paper over an unstable local identity rule.
