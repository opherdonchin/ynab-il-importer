# Active Plan

## Workstream

Split-transaction support on the `handle_splits` branch.

Current direction:
- keep canonical Parquet transaction artifacts as the real transaction model
- keep transactions nested only where hierarchy is semantically real, especially `splits`
- use a mostly flat canonical review artifact with split-only nesting
- move the review app toward a Polars-first working table instead of pandas
- keep review semantics parent-transaction-oriented until split editing is designed

## Current Goal

Stabilize the Step 3 pivot to a flat Polars review model and keep moving the review app onto that model.

That means:
1. keep the completed Step 1 and Step 2 canonical transaction/upload path green
2. keep the new flat review artifact green across builders, app IO, app helpers, and upload prep
3. continue replacing pandas-oriented app logic with Polars/dataframe-level logic
4. preserve split detail as nested data while keeping ordinary review logic mostly flat

## Current Status

Done:
- Step 1 is implemented and verified:
  - canonical transaction Parquet artifacts exist
  - parser/normalizer boundaries can emit canonical transaction artifacts directly
  - builder performance on representative Family data is healthy again
- Step 2 is implemented:
  - YNAB download is centered on canonical transactions
  - uploads are assembled from canonical review artifacts and support new split creation
  - institutional parser modules now expose `read_canonical(...)`
- real-data Family sanity checks succeeded:
  - canonical Family proposed-review artifact built successfully
  - reviewed Family artifact was converted to canonical Parquet successfully
  - upload prep dry-run from canonical reviewed Parquet completed successfully
- the earlier nested review-artifact approach has now been corrected:
  - review artifacts are now mostly flat
  - `source_transaction` / `target_transaction` are no longer part of the canonical review schema
  - only `source_splits` / `target_splits` remain nested in review artifacts

In progress:
- Step 3 implementation is underway against the flatter review model
- completed pivot slice:
  - [review_schema.py](src/ynab_il_importer/artifacts/review_schema.py#L1) now defines a mostly flat review artifact with split-only nesting
  - [review_app/io.py](src/ynab_il_importer/review_app/io.py#L1) now round-trips that flat review artifact and can still flatten older nested review rows during migration
  - [build_proposed_transactions.py](scripts/build_proposed_transactions.py#L1259) now returns review rows normalized through the flat canonical review artifact
  - [build_cross_budget_review_rows.py](scripts/build_cross_budget_review_rows.py#L686) now does the same
  - [upload_prep.py](src/ynab_il_importer/upload_prep.py#L109) now accepts the flat canonical review artifact and preserves compatibility fields needed by upload tests
- completed app/helper slice:
  - [state.py](src/ynab_il_importer/review_app/state.py#L1) now derives split counts, split text, and search text from the new flat review schema
  - [app.py](src/ynab_il_importer/review_app/app.py#L1234) now reads split detail from `source_splits` / `target_splits` instead of nested source/target transactions
  - focused review/app/upload/builder tests are green against the flat review schema

## Working Rules For This Phase

- Keep canonical transaction artifacts as the real transaction model.
- Keep the canonical review artifact mostly flat.
- Use nested data in review artifacts only where hierarchy is actually needed, currently splits.
- Prefer Polars/dataframe-level processing over Python dict/list helper logic.
- Do not spend effort preserving pandas compatibility that does not serve the current app path.
- Commit after each successful sub-step.
- Update `documents/plan.md` before each commit on this branch.

## Risks To Watch

- drifting back toward embedded nested source/target transaction objects in review artifacts
- keeping too much app logic in Python row loops instead of Polars/dataframe expressions
- letting split display force premature split-edit semantics into Step 3
- losing important target-side creation context when `target_present` is false
- leaving builders or upload prep on stale nested in-memory review shapes while persisted artifacts are flat

## Next Step

Step 3 remains underway.

Next:
1. continue moving review-app logic onto the flat Polars working model
   - reduce remaining pandas-heavy paths in [app.py](src/ynab_il_importer/review_app/app.py#L1), [state.py](src/ynab_il_importer/review_app/state.py#L1), [validation.py](src/ynab_il_importer/review_app/validation.py#L1), and [model.py](src/ynab_il_importer/review_app/model.py#L1)
2. push helper logic further toward dataframe-native expressions
   - filtering
   - search
   - grouping summaries
   - split badges and split detail support
3. keep split handling read-only in the app for now
   - display yes
   - editing deferred to Step 4
4. after the flat review model settles, update the longer-form split plan document to match the new Step 3 direction
