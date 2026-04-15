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
- `reviewed` is the stored acceptance gate
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

Transfer-specific UI behavior may derive a transfer relation layer from that working dataframe, but that relation layer is not itself a second persisted review artifact. See [transfer_review_mode_design.md](transfer_review_mode_design.md).

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
- `matched_cleared` -> `reviewed = FALSE`, `decision_action = keep_match`
- `source_only` -> `reviewed = FALSE`, `decision_action = create_target`
- `target_only` -> `reviewed = FALSE`, `decision_action = No decision`

This means fresh builder output intentionally leaves existing YNAB-only rows in an explicit
needs-decision state. They do not silently default to `ignore_row`.

## Review App State Model

The review app derives four user-facing states from blockers, decision, and acceptance:

- `Needs fix`
  The row is invalid or incomplete.
- `Needs decision`
  The row is valid enough to act on, but `decision_action = No decision`.
- `Needs review`
  The row is valid and has a concrete decision, but `reviewed = FALSE`.
- `Settled`
  The row is valid and accepted with `reviewed = TRUE`.

Applied edits implicitly unsettle previously settled rows:

- if the edit creates a blocker -> `Needs fix`
- if the edit clears the decision -> `Needs decision`
- if the edit leaves a valid decision -> `Needs review`

## Review Validation Rules

A row or connected component cannot be accepted if:

- any reviewed row still has `decision_action = No decision`
- an institutional-source row uses `create_source`, `delete_source`, or `delete_both`
- a transaction is both matched and deleted
- a source transaction ends up with more than one reviewed match or create outcome
- a target transaction ends up with more than one reviewed match or create outcome

Unaccepted inconsistency is allowed. Accepted inconsistency is not.

## Execution Boundary

The active upload path prepares only explicit accepted `create_target` rows.

Other stored actions still matter because they must round-trip cleanly through:

- builder output
- review save/load
- rebuild reconciliation
- validation and reporting
