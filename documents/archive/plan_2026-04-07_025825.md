# Active Plan

## Workstream

Split-aware review on the `handle_splits` branch, with strict canonical Parquet boundaries and Polars-first working data.

## Current Direction

- keep canonical transaction and review artifacts as Parquet, not CSV
- keep hierarchy only where it is semantically real:
  - transaction `splits`
  - persisted `review_v4` transaction structs
- keep ordinary app/upload logic on one centralized flat working dataframe
- keep that working dataframe Polars-first
- fail fast at artifact/load boundaries instead of repairing stale or malformed inputs in the app

## Current Status

Done:
- Step 4 split editing is complete:
  - modal split editor uses explicit per-line widgets
  - committed split edits round-trip through save/load/reconcile/upload prep
  - one-line split saves collapse back to a normal transaction
- normalized proposal-build inputs now require direct canonical parquet artifacts
- the builder now stays Arrow/Polars-first through:
  - canonical input load
  - source/target prep
  - institutional pairing
  - matched/unmatched review row assembly
  - review-target suggestion application
- the review-app working-schema boundary is now Polars-first:
  - [working_schema.py](src/ynab_il_importer/review_app/working_schema.py) owns construction/normalization of the flat working dataframe
  - split columns stay normalized as real split-record lists rather than generic object blobs
- the review-app IO boundary is now stricter:
  - [load_review_artifact](src/ynab_il_importer/review_app/io.py) is parquet-only
  - [project_review_artifact_to_working_dataframe](src/ynab_il_importer/review_app/io.py) is the explicit canonical-review-artifact -> Polars-working-dataframe projection
  - app/upload/builder callers now use that two-step boundary explicitly
- live working-dataframe consumers that should not round-trip through the persisted artifact boundary now normalize directly through [working_schema.py](src/ynab_il_importer/review_app/working_schema.py)
- the focused builder/review/app/upload test slice is green after the refactor

## Immediate Goal

Resume the April 2 review safely from rebuilt artifacts on top of the corrected boundaries.

That means:
1. rebuild the April 2 proposed artifact from canonical parquet inputs
2. reconcile the saved reviewed artifact forward onto the rebuilt proposal
3. stop if any split-bearing source or target transactions were previously flattened or mistransferred
4. continue review only from the recovered artifact

## Next Steps

1. Rebuild the April 2 proposal and reviewed artifact on the current strict parquet boundary.
2. Sanity-check recovered rows that involve splits before continuing review.
3. After review recovery is stable, continue simplifying review-app IO:
   - remove leftover CSV/legacy translation helpers from [io.py](src/ynab_il_importer/review_app/io.py)
   - keep loader and projector responsibilities separate
4. Continue shrinking remaining pandas islands where the code is already naturally columnar.

## Validation / Test Baseline

Latest green slice:

```bash
pixi run pytest tests/test_build_proposed_transactions.py tests/test_review.py tests/test_review_io.py tests/test_review_reconcile.py tests/test_review_app.py tests/test_upload_prep.py -q
```

Result at last run: `167 passed`
