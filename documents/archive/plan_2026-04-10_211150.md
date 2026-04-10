# Active Plan

## Workstream

Keep the context/run-tag institutional workflow stable, well-documented, and easy to execute while finishing the remaining cleanup around the active path.

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

- documentation cleanup and alignment:
  - active docs rewritten around the current workflow and code
  - stale prompts, implementation plans, and CSV/profile-era runbooks removed
  - README now points to a smaller, clearer doc path
- previous MAX normalization boundary fixed:
  - [scripts/normalize_previous_max.py](../scripts/normalize_previous_max.py) now writes Parquet outputs that match the canonical normalization boundary
  - [tests/test_normalize_previous_max_script.py](../tests/test_normalize_previous_max_script.py) covers that path

## Recent Validation

```bash
pixi run pytest tests/test_normalize_previous_max_script.py tests/test_download_ynab_api_script.py tests/test_prepare_ynab_upload_script.py -q
pixi run normalize-previous-max --help
```

Result:

- tests: `5 passed`
- help command: OK

## Next Steps

1. Verify the Pilates workflow end to end on the context/run-tag path.
2. Verify the Aikido workflow on the same path, or explicitly archive any remaining non-active pieces.
3. Remove the review app's remaining dependency on [workflow_profiles.py](../src/ynab_il_importer/workflow_profiles.py) for category-cache path resolution.
4. Decide whether to add dedicated pixi aliases for upload prep and category-cache refresh.
5. Keep shrinking the legacy script surface when a helper is no longer part of the active workflow.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
