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

Re-close Step 3 with a cleaner review-model boundary before restarting Step 4.

That means:
1. keep the completed Step 1 and Step 2 transaction/upload path green
2. keep the shipped Step 3 split-display and Polars-first app improvements green
3. redesign the review model so the app works from:
   - a flat working table for normal review logic
   - nested split columns only where split editing/display actually needs them
   - immutable original source/target transaction structs as reference objects
4. rethink Step 4 split editing on top of that cleaner model instead of continuing the reverted reviewed-split-state design

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

- Step 3 is complete under the currently shipped architecture:
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

The first attempted Step 4 implementation was reverted.

Why it was reverted:
- it introduced explicit reviewed split-mode and reviewed selected split columns
- that design was workable but too state-heavy
- it did not match the newer direction for the app model:
  - flat working projection
  - nested splits only where genuinely needed
  - immutable original source/target transaction structs used as reference objects

The current architectural correction is:
- keep ordinary app processing flat and dataframe-oriented
- keep splits nested because they are genuinely hierarchical
- attach immutable original source/target transactions as structured reference objects
- explode/flatten those reference objects only when the app needs comparison or display context
- avoid carrying duplicate mutable “current vs selected” state across many flat columns just to infer change status

## Working Rules For This Phase

- Keep canonical transaction artifacts as the real transaction model.
- Keep the canonical review artifact mostly flat.
- Use nested data in review artifacts only where hierarchy is actually needed, currently splits.
- Prefer Polars/dataframe-level processing over Python dict/list helper logic.
- Do not spend effort preserving pandas compatibility that does not serve the current app path.
- Prefer immutable reference objects plus flat working projections over duplicating mutable review state across many flat columns.
- Commit after each successful sub-step.
- Update `documents/plan.md` before each commit on this branch.

## Risks To Watch

- drifting back toward embedded nested source/target transaction objects in review artifacts
- keeping too much app logic in Python row loops instead of Polars/dataframe expressions
- letting split editing leak into matching semantics
- losing important target-side creation context when `target_present` is false
- leaving builders or upload prep on stale nested in-memory review shapes while persisted artifacts are flat
- reintroducing a second heavy mutable review-state layer instead of using immutable originals plus a flat working projection

## Next Step

Step 3 is functionally complete, but the review-model boundary needs one more architectural pass before Step 4 resumes.

Immediate next steps:
1. update the longer-form split plan so it reflects the rollback and the new schema direction
2. redesign the review model around:
   - flat working review rows
   - nested split columns
   - immutable original source/target transaction structs
3. decide exactly where flatten/explode/compare happens at the app boundary
4. only then restart Step 4 split-editor implementation
