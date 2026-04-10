# Unified Review Model Design

## Purpose

Keep one explicit review model between deterministic proposal building and any YNAB mutation.

The human should reason about:

- source
- target
- selected values
- one explicit decision

not about older file formats or multiple review representations.

## Stable Meanings

The active repo workflow is institutional review:

- source = normalized bank or card transaction
- target = current YNAB transaction in the same budget/account

The model is intentionally source/target-shaped so it can describe other workflows if they return later, but institutional import is the only active day-to-day path.

## Decision Vocabulary

- `No decision`
  Unresolved. Valid only while unreviewed.
- `keep_match`
  Accept this source/target pairing.
- `create_target`
  Create the missing target-side transaction.
- `create_source`
  Present in the vocabulary, but not allowed for institutional source data.
- `delete_source`, `delete_target`, `delete_both`
  Explicit deletion decisions where the workflow allows them.
- `ignore_row`
  Reviewed no-op.

## Review Semantics

- Unreviewed rows may be inconsistent.
- Review is the point where contradictions must disappear.
- Review state propagates across the connected component defined by shared source and target transaction ids.
- The app blocks review when the connected component still has contradictions or unresolved `No decision` rows.
- The app should explain blockers rather than silently normalizing them away.

## Builder Defaults

Current institutional builders seed:

- `matched_auto` -> `decision_action = keep_match`, `reviewed = FALSE`
- `matched_cleared` -> `decision_action = keep_match`, `reviewed = TRUE`
- `source_only` -> `decision_action = create_target`, `reviewed = FALSE`
- unsettled `target_only` -> `decision_action = No decision`, `reviewed = FALSE`
- auto-settled `target_only` rows such as cleared/manual/transfer counterparts -> `decision_action = ignore_row`, `reviewed = TRUE`

These are defaults, not approvals, except for the explicitly auto-settled cases.

## Value Propagation

- selected payee/category values propagate by transaction identity
- `decision_action` remains row-scoped by default
- non-ignore reviewed actions can push competing rows for the same source or target transaction to `ignore_row`

## Persisted Vs Working Representation

- the persisted artifact is canonical `review_v4` Parquet
- the persisted artifact keeps source/target current and original transaction structs
- the review app and upload prep operate on a flat derived working dataframe built from that artifact

That split is deliberate:

- persisted artifacts keep real structure
- ordinary UI and upload logic stays flat and dataframe-friendly

## Institutional Restrictions

For institutional source data, the UI and validation must reject `create_source`, `delete_source`, and `delete_both`.

## Map Updates

Map update intent is stored explicitly in `update_maps`. It is not a boolean flag.
