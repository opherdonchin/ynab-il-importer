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

Start Step 4 cleanly:

1. bring the long-form Step 4 split-editor spec up to the same detail level as Steps 1 through 3
2. implement the first vertical split-editing slice end to end
3. keep the existing Step 1-3 path green while the split editor lands

That first Step 4 slice should cover:
1. canonical review-schema support for reviewed split-edit state
2. review-app editing of reviewed split state
3. save/resume persistence of reviewed split edits
4. upload-prep consumption of reviewed target split edits

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

- Step 3 is complete under the current architecture choice:
  - the canonical review artifact is flat with split-only nesting
  - the app displays split transactions read-only from that schema
  - the app’s working/helper path is Polars-first
  - the remaining pandas usage is explicitly isolated instead of being mixed through the app
- completed pivot slice:
  - [review_schema.py](src/ynab_il_importer/artifacts/review_schema.py#L1) now defines a mostly flat review artifact with split-only nesting
  - [review_app/io.py](src/ynab_il_importer/review_app/io.py#L1) now round-trips that flat review artifact and remains the explicit boundary where transaction-shaped review data is flattened for the app
  - [build_proposed_transactions.py](scripts/build_proposed_transactions.py#L1259) now returns review rows normalized through the flat canonical review artifact
  - [build_cross_budget_review_rows.py](scripts/build_cross_budget_review_rows.py#L686) now does the same
  - [upload_prep.py](src/ynab_il_importer/upload_prep.py#L109) now accepts the flat canonical review artifact and preserves compatibility fields needed by upload tests
- completed app/helper slice:
  - [state.py](src/ynab_il_importer/review_app/state.py#L1) now derives split counts, split text, and search text from the new flat review schema
  - [app.py](src/ynab_il_importer/review_app/app.py#L1234) now reads split detail from `source_splits` / `target_splits` instead of nested source/target transactions
  - focused review/app/upload/builder tests are green against the flat review schema
- completed working-view slice:
  - [review_data_view](src/ynab_il_importer/review_app/state.py#L161) now builds a pure Polars data view from the live edited review dataframe
  - [review_filter_state_view](src/ynab_il_importer/review_app/state.py#L286) now builds a separate Polars state/filter overlay for UI-facing labels such as blocker and save state
  - [filtered_row_indices_from_views](src/ynab_il_importer/review_app/state.py#L340) now drives the main app filter path from those two tables together
  - [app.py](src/ynab_il_importer/review_app/app.py#L794) now computes and stores separate data and state views as part of derived app state
- completed schema-tightening slice:
  - [io.py](src/ynab_il_importer/review_app/io.py#L1) now guarantees a fuller flat review-app schema at the boundary via canonical normalization instead of relying on sparse-column tolerance in app helpers
  - [state.py](src/ynab_il_importer/review_app/state.py#L1) now builds its Polars working view from explicit schema columns rather than `_selected_expr`, `_optional_text_expr`, and similar “maybe present” helpers
  - split columns are now coerced to a stable Polars list/struct dtype for working-view helpers and canonical search text
  - [tests/test_review.py](tests/test_review.py#L1) now routes review-row fixtures through the app boundary so tests exercise the real flat review schema instead of ad hoc sparse frames
- completed projection-simplification slice:
  - [project_review_artifact_to_flat_dataframe](src/ynab_il_importer/review_app/io.py#L584) is now a thinner app projection from the canonical review table instead of a second broad schema-normalization pass
  - split columns are normalized at that projection boundary so pandas/Arrow array-like values still arrive in the app as ordinary split-record lists
  - the unused [canonical_search_text_series](src/ynab_il_importer/review_app/state.py#L1) helper has been removed, and search-text assertions now exercise the real [review_data_view](src/ynab_il_importer/review_app/state.py#L153) runtime path
  - [state.py](src/ynab_il_importer/review_app/state.py#L1) now normalizes review-data text/bool columns earlier in the Polars working-view path, so helper expressions can rely more on direct `pl.col(...)` access
- completed derived-state Polars slice:
  - [app.py](src/ynab_il_importer/review_app/app.py#L271) now keeps canonical helper tables as Polars plus row-lookup maps instead of eagerly converting them to pandas helper frames
  - [app.py](src/ynab_il_importer/review_app/app.py#L795) now uses `data_view` / `state_view` directly for filter options, matrix counts, and row/group readiness lookups in the easy derived-state paths
  - the app still keeps editable review rows in pandas, but the cached helper/state dataframe work is now more clearly Polars-backed
- completed adapter-island slice:
  - the remaining mutation/review/reconcile hotspots now have explicit pandas-inside adapters instead of forcing pandas outward across the surrounding app code
  - [apply_row_edit](src/ynab_il_importer/review_app/state.py#L1098), [apply_to_same_fingerprint](src/ynab_il_importer/review_app/model.py#L57), [apply_competing_row_resolution](src/ynab_il_importer/review_app/model.py#L142), [apply_review_state](src/ynab_il_importer/review_app/validation.py#L500), [apply_review_state_best_effort](src/ynab_il_importer/review_app/validation.py#L569), and [reconcile_reviewed_transactions](src/ynab_il_importer/review_reconcile.py#L108) are now Polars-only on the public boundary
  - pandas is now only an internal implementation detail inside the corresponding private helpers
  - app call sites now perform the explicit boundary conversion rather than depending on flexible `pd|pl` function signatures
- completed grouped-summary cleanup slice:
  - [review_filter_state_view](src/ynab_il_importer/review_app/state.py#L284) now carries additional row-state booleans used by grouped UI summaries
  - [app.py](src/ynab_il_importer/review_app/app.py#L540) now uses lookup-driven group summary/default helpers instead of repeated pandas mask slicing for the grouped header badges and defaults
  - easy helper signatures were tightened so the non-island path no longer advertises unnecessary `pandas | polars` flexibility

Step 4 is now beginning.

The next real work is no longer about split display. It is about reviewed split state:

- snapshot split columns remain factual
- reviewed split-edit columns will carry user intent
- the app will derive effective split state from mode + snapshot + reviewed selection
- upload prep will consume explicit reviewed split state rather than inferring everything from grouped flat rows

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
- letting split editing leak into matching semantics
- losing important target-side creation context when `target_present` is false
- leaving builders or upload prep on stale nested in-memory review shapes while persisted artifacts are flat
- making split removal ambiguous by relying on `None` vs `[]` instead of an explicit mode field

## Next Step

Step 3 is complete.

Immediate next steps:
1. finish the long-form Step 4 specification in `documents/handle_splits_implementation_plan.md`
2. add explicit reviewed split-edit fields to the canonical review schema:
   - split mode
   - reviewed selected split lines
3. wire the review app to edit and persist reviewed split state
4. teach upload prep to consume reviewed target split edits
5. keep review app, upload prep, and save/resume tests green throughout
