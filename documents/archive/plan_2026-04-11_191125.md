# Active Plan

## Workstream

Keep the context/run-tag institutional workflow stable while tightening the review-state model so the app is explicit, fast enough to use, and honest about what still needs user action.

## Current State

- canonical transaction artifacts are Parquet `transaction_v1`
- canonical review artifacts are Parquet `review_v4`
- the review app and upload prep operate on one flat Polars working dataframe
- the active closeout path is:
  - `pixi run normalize-context -- <context> <run_tag>`
  - `pixi run download-context-ynab -- <context> <run_tag>`
  - `pixi run build-context-review -- <context> <run_tag>`
  - `pixi run review-context -- <context> <run_tag>`
  - `pixi run python scripts/prepare_ynab_upload.py <context> <run_tag> ...`
  - `pixi run sync-bank-matches -- <context> <run_tag>`
  - `pixi run reconcile-bank-statement -- <context> <run_tag>`
  - `pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM`
  - `pixi run sync-card-matches -- <context> <run_tag> --account "<account>"`
  - `pixi run reconcile-card-cycle -- <context> <run_tag> --account "<account>" --previous <normalized_previous.parquet>`

## Recently Completed

- review-state redesign for the active app path:
  - user-facing primary states are now:
    - `Needs fix`
    - `Needs decision`
    - `Needs review`
    - `Settled`
  - `No decision` is no longer treated like a generic fix blocker; it maps to `Needs decision`
  - applied edits now implicitly unsettle previously settled rows instead of leaving stale accepted state behind
  - row/group accept actions now operate on the currently staged decisions instead of requiring a separate “mark reviewed” step
- explicit target-only institutional defaults:
  - [scripts/build_proposed_transactions.py](../scripts/build_proposed_transactions.py) now leaves all `target_only` rows on `decision_action = No decision`, `reviewed = False`
  - `matched_cleared` rows also start unaccepted with `keep_match`, so system defaults land in `Needs review` instead of `Settled`
  - on the rebuilt Pilates artifact:
    - total rows: `4056`
    - `target_only` rows: `4022`
    - `target_only` rows still on explicit `No decision`: `4022`
- clearer blocker messaging in the app:
  - [validation.py](../src/ynab_il_importer/review_app/validation.py) now surfaces `Decision required` as a first-class blocker label
  - [app.py](../src/ynab_il_importer/review_app/app.py) now explains common blockers, including the explicit-decision requirement for existing YNAB-only rows
  - row/group button wording now matches the workflow more closely:
    - `Apply edits`
    - `Accept row`
    - `Apply group edits`
    - `Accept group`
    - `Accept all reviewable rows`
- grouped-edit persistence tightened:
  - group widgets now render from the currently staged session values instead of silently snapping back to computed defaults
  - this keeps grouped category/decision changes aligned with the actual apply/accept path
- active docs aligned with the shipped model:
  - [README.md](../README.md)
  - [documents/context_workflow_spec.md](context_workflow_spec.md)
  - [documents/decisions/unified_review_model_schema.md](decisions/unified_review_model_schema.md)

## Recent Validation

```bash
pixi run pytest tests/test_build_proposed_transactions.py tests/test_review.py tests/test_review_app.py -q
pixi run build-context-review -- pilates 2026_04_01
pixi run python -  # Pilates state sanity check on data/paired/2026_04_01/pilates_proposed_transactions.parquet
```

Result:

- focused review/build tests: `118 passed`
- Pilates review rebuild: OK
- Pilates state sanity check:
  - `target_only_total = 4022`
  - `target_only_no_decision = 4022`
  - state counts:
    - `Needs decision = 4022`
    - `Needs fix = 1`
    - `Needs review = 33`

## Next Steps

1. Re-open the Pilates review app and verify the new state model feels coherent in the live workflow:
   - `target_only` rows should clearly read as `Needs decision`
   - grouped edits should persist and accept cleanly
   - blocker text should explain why a row cannot yet be settled
2. Decide whether the active workflow needs an explicit `update_target` mutation path for editing existing YNAB transactions instead of only creating new targets.
3. Remove the review app's remaining dependency on [workflow_profiles.py](../src/ynab_il_importer/workflow_profiles.py) for category-cache path resolution.
4. Decide whether to add dedicated pixi aliases for upload prep and category-cache refresh.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
