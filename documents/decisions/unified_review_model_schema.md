# Unified Review Model - Technical Contract

## Hard Cutover

The persisted review CSV is the only review artifact. There is no `--out-v2` flag and no legacy proposal-format path to maintain.

## Canonical Persisted Fields

The persisted file should carry:
- row identity and relation state
- source-side snapshot fields
- target-side snapshot fields
- side-specific selected values
- row decision fields
- review state

Required naming and shape decisions:
- side-specific selected fields are canonical:
  - `source_payee_selected`
  - `source_category_selected`
  - `target_payee_selected`
  - `target_category_selected`
- unsuffixed `payee_selected` and `category_selected` do not belong in the persisted review CSV
- the row action column is `decision_action`
- the review gate is `reviewed`
- map update intent lives in `update_maps`

## Invariants

- Repeated `source_row_id` values are allowed.
- Repeated `target_row_id` values are allowed.
- Source-side selected values must stay consistent across rows sharing the same source transaction.
- Target-side selected values must stay consistent across rows sharing the same target transaction.
- `decision_action` is stored per row.
- `reviewed` propagates across the connected component induced by shared source and target IDs.

## Default Builder Outputs

- `matched_auto` -> `reviewed = FALSE`, `decision_action = keep_match`
- `source_only` -> `reviewed = FALSE`, `decision_action = create_target`
- `target_only` -> `reviewed = FALSE`, `decision_action = create_source`
- `ambiguous` -> `reviewed = FALSE`, `decision_action = No decision`

These are defaults, not approvals.

## Review Validation Rules

A row or connected component cannot be reviewed if:
- any reviewed row still has `decision_action = No decision`
- an institutional-source row uses `create_source`, `delete_source`, or `delete_both`
- a transaction is both matched and deleted
- a source transaction ends up with more than one reviewed match or create outcome
- a target transaction ends up with more than one reviewed match or create outcome

Unreviewed inconsistency is allowed. Reviewed inconsistency is not.

## Current Execution Boundary

For the current cutover, upload preparation should only prepare explicit reviewed `create_target` rows. Other actions still need coherent persistence and validation even if their execution path is deferred.

## `update_maps`

`update_maps` replaces the old boolean `update_map`. The minimal intent set discussed so far is:
- add source to fingerprint-map options
- limit fingerprint-map options to this source
- add fingerprint to payee-map options
- limit payee-map options to this fingerprint

The storage format can be simple, but it should be explicit and non-boolean.
