# Active Plan

## Workstream

Keep the context/run-tag institutional workflow stable while tightening the review workflow so grouped edits are fast enough to use, target-only rows are explicit about the decision they need, and existing YNAB rows can be updated deliberately instead of only created or deleted.

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

- grouped review edits now apply to the exact visible row indices instead of routing back through the broader same-fingerprint mutation path:
  - [app.py](../src/ynab_il_importer/review_app/app.py) now calls [model.py](../src/ynab_il_importer/review_app/model.py) `apply_to_indices(...)` for grouped edits
  - this keeps `Apply group edits` scoped to the rows the user is actually looking at
  - focused grouped-edit coverage now exists in [test_review_app.py](../tests/test_review_app.py)
- existing institutional target-only YNAB rows now have an explicit keep-and-edit decision:
  - [validation.py](../src/ynab_il_importer/review_app/validation.py) now allows `update_target` for `target_present && !source_present` institutional rows
  - the row help text in [app.py](../src/ynab_il_importer/review_app/app.py) now tells the user to choose `update_target` when they want to preserve and edit an existing YNAB-only row
- upload prep now supports reviewed `update_target` rows in addition to reviewed `create_target` rows:
  - [upload_prep.py](../src/ynab_il_importer/upload_prep.py) now treats both decisions as uploadable mutation actions
  - prepared upload rows carry `existing_transaction_id` for `update_target`
  - payload generation is split into create and update batches so `prepare_ynab_upload.py` can call either the YNAB create or patch endpoint as appropriate
  - focused upload-prep coverage now exists in [test_upload_prep.py](../tests/test_upload_prep.py)
- focused validation for the bundled review/upload change passed:
  - `pixi run pytest tests/test_review_app.py tests/test_upload_prep.py tests/test_prepare_ynab_upload_script.py -q`
  - result: `92 passed`

## Next Steps

1. Re-open the Pilates review app and verify the `transfer in family` group now behaves coherently in the live UI:
   - `Apply group edits` should only touch the visible rows
   - target-only rows should offer `update_target`
   - `Accept group` should become available once `update_target` is selected
2. Dry-run the upload prep on the updated workflow and inspect the emitted create/update payload batches:
   - `pixi run python scripts/prepare_ynab_upload.py pilates 2026_04_01`
3. Remove the review app's remaining dependency on [workflow_profiles.py](../src/ynab_il_importer/workflow_profiles.py) for category-cache path resolution.
4. Decide whether to add dedicated pixi aliases for upload prep and category-cache refresh.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
