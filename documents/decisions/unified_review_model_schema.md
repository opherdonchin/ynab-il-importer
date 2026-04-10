# Unified Review Model Schema

## Canonical Persisted Artifact

The persisted review artifact is canonical `review_v4` Parquet, defined in [src/ynab_il_importer/artifacts/review_schema.py](../../src/ynab_il_importer/artifacts/review_schema.py).

It contains:

- review/control fields
- `source_current`
- `target_current`
- `source_original`
- `target_original`

Those four transaction fields use the canonical `transaction_v1` struct from [src/ynab_il_importer/artifacts/transaction_schema.py](../../src/ynab_il_importer/artifacts/transaction_schema.py).

## Canonical Control Fields

The persisted artifact carries these top-level review fields:

- row identity and relation state
- `match_status` and `match_method`
- `payee_options` and `category_options`
- `decision_action`
- `reviewed`
- `changed`
- `memo_append`
- side-presence flags and row ids
- side-specific selected values
- `update_maps`

Required naming rules:

- side-specific selected fields are canonical:
  - `source_payee_selected`
  - `source_category_selected`
  - `target_payee_selected`
  - `target_category_selected`
- unsuffixed `payee_selected` and `category_selected` are working-dataframe aliases only
- `decision_action` is the stored row action
- `reviewed` is the stored review gate
- `changed` is persisted review state, not a derived UI-only hint

## Working Projection

The persisted artifact is not the everyday UI shape.

The active app/upload shape is a flat working dataframe built through:

1. [review_app/io.project_review_artifact_to_working_dataframe](../../src/ynab_il_importer/review_app/io.py)
2. [review_app/working_schema.build_working_dataframe](../../src/ynab_il_importer/review_app/working_schema.py)

That working dataframe contains:

- review/control columns
- exploded current source/target scalar columns
- split columns
- current/original transaction reference columns
- unsuffixed `payee_selected` and `category_selected` aliases for the target side

The working projection is derived. It is not a second persisted artifact format.

## Invariants

The persisted artifact must satisfy:

- repeated `source_row_id` values are allowed
- repeated `target_row_id` values are allowed
- side-specific selected values must stay consistent across rows sharing the same source or target transaction
- `decision_action` is stored per row
- `reviewed` propagates across the connected component induced by shared source and target ids
- if `changed = FALSE`, current and original transaction structs must be equal for both sides
- split totals must equal the parent transaction amount for both current and original transaction structs

## Builder Defaults

Current institutional builders seed:

- `matched_auto` -> `reviewed = FALSE`, `decision_action = keep_match`
- `matched_cleared` -> `reviewed = TRUE`, `decision_action = keep_match`
- `source_only` -> `reviewed = FALSE`, `decision_action = create_target`
- unsettled `target_only` -> `reviewed = FALSE`, `decision_action = No decision`
- auto-settled `target_only` rows -> `reviewed = TRUE`, `decision_action = ignore_row`

## Review Validation Rules

A row or connected component cannot be reviewed if:

- any reviewed row still has `decision_action = No decision`
- an institutional-source row uses `create_source`, `delete_source`, or `delete_both`
- a transaction is both matched and deleted
- a source transaction ends up with more than one reviewed match or create outcome
- a target transaction ends up with more than one reviewed match or create outcome

Unreviewed inconsistency is allowed. Reviewed inconsistency is not.

## Execution Boundary

The active upload path prepares only explicit reviewed `create_target` rows.

Other stored actions still matter because they must round-trip cleanly through:

- builder output
- review save/load
- rebuild reconciliation
- validation and reporting
