# Unified Review Model - Design Decisions

## Purpose

One review experience should cover institutional import and cross-budget comparison. The user should reason about source, target, and an explicit decision, not about separate legacy file formats.

## Stable Meanings

Institutional review:
- source = imported bank or card row
- target = current YNAB row in the same budget/account

Cross-budget review:
- source = source-budget YNAB row
- target = target-budget YNAB row

## Decision Vocabulary

- `No decision` = unresolved; valid only while unreviewed
- `keep_match` = accept this source/target pairing
- `create_target` = create the missing target-side transaction and then treat the pair as matched
- `create_source` = create the missing source-side transaction where the workflow allows it
- `delete_source`, `delete_target`, `delete_both` = explicit deletion decisions
- `ignore_row` = reviewed no-op

## Review Semantics

- Unreviewed rows may be inconsistent.
- Review is the moment contradictions must disappear.
- Reviewing a row propagates across the connected source/target component.
- Unreviewing propagates across the same connected component.
- The app should block review when the connected component still has contradictions or unresolved `No decision` rows.
- The app should explain what blocks review.

## Action Propagation

Selected payee and category values propagate by transaction ID.

`decision_action` stays row-scoped by default, but the UI should offer explicit propagation of an action to rows sharing the same source transaction, target transaction, or both. There is no history-based undo for propagation.

## Institutional Restrictions

If the source side is institutional data, the UI should not offer `create_source`, `delete_source`, or `delete_both`. Validation and sync must reject them as well.

## Manual Relinking

Chooser-based relinking is deferred. For this pass, rows that need a different source/target pairing can stay unresolved or be handled manually outside the app.

## Map Updates

Map updates stay minimal in this pass. The field is `update_maps`, not `update_map`, because the project may carry more than one mapping action. The exact encoding can stay simple as long as the stored intent is explicit.
