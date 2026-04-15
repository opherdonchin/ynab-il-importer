# Transfer Review Mode Design

## Purpose

Define the active direction for transfer-specific review behavior without introducing a second canonical review artifact.

The goal is to make transfers easier to inspect and decide as one accounting event while preserving the existing persisted boundaries:

- canonical transaction artifact: `transaction_v1`
- canonical review artifact: `review_v4`
- flat working review dataframe as the main mutable review surface

## Decision

The active direction is:

1. keep one canonical persisted review artifact
2. derive transfer relations from the working review dataframe
3. expose a transfer review mode inside the review app for rows that belong to a transfer relation
4. have transfer review actions write through to the existing underlying review rows

This is intentionally smaller than creating a second canonical transfer review schema.

## Why

Transfers are one accounting event, but they are not well modeled as one ordinary source/target row.

Examples:

- internal on-budget bank-to-bank transfer:
  - source row in bank A
  - source row in bank B
  - YNAB row in account A
  - YNAB row in account B
- budget-boundary transfer:
  - one source side may exist
  - YNAB may still need a transfer to be created
- card payment transfer:
  - payment transfer belongs to card closeout logic, not ordinary merchant-row matching

The current review artifact is still the right persisted control point, but the UI needs a better transfer-aware view.

## Non-Goals

This design does not:

- create a second canonical review artifact
- add transfer-specific persisted acceptance state
- replace source-specific closeout logic
- move card payment validation out of card closeout

## Transfer Relation Layer

The transfer relation layer is derived, not canonical.

One relation row represents one inferred transfer relation built from existing review rows.

Phase-1 relation fields should include:

- `transfer_relation_id`
- `relation_kind`
- `relation_status`
- `date`
- `amount_abs_ils`
- `account_a`
- `account_b`
- `row_positions`
- `source_row_ids`
- `target_row_ids`
- `account_a_source_present`
- `account_a_target_present`
- `account_b_source_present`
- `account_b_target_present`
- `peer_review_row_present`
- `ambiguous_relation`

The relation id should be derived from stable transaction facts, not filtered row order.

## Transfer Review Mode

Transfer review mode is a UI surface, not a second persistence layer.

For a row that belongs to a transfer relation, the app should render a transfer section that shows:

- transfer kind
- relation status
- account A summary
- account B summary
- which source and target sides are currently present
- whether the peer review row is missing this run

The transfer review mode should allow relation-level actions that write through to the linked underlying review rows.

Phase 1 focuses on relation-level decision and acceptance propagation.

Per-side payee/category editing remains in the ordinary row editor for now.

## Review Semantics

The persisted unit of truth remains the existing row-level review data.

Transfer review mode should not invent a new independent state machine.

Instead:

- transfer relation decisions map onto the existing `decision_action` field for the member rows
- transfer acceptance maps onto the existing `reviewed` field and component validation
- ordinary row validation and component validation remain the final gate

## Source And Closeout Ownership

Transfer review mode does not replace closeout ownership.

- internal in-budget transfer counterparts that are only mirrored YNAB rows should still stay out of ordinary institutional review
- card payment transfers are still validated in card closeout
- transfer review mode is for the transfer-linked rows that remain visible in ordinary review

## Phase Plan

### Phase 1

- derive transfer relations from the working review dataframe
- expose transfer summaries in row details
- add relation-level decision and accept actions that propagate to member rows
- add focused tests for partial internal transfer cases

### Phase 2

- add richer transfer drawer/panel UX
- expose relation-level warnings and clearer missing-peer-source messaging
- improve relation-key handling for repeated same-day same-amount transfers

### Phase 3

- revisit whether transfer editing needs persisted relation state
- only then consider whether a dedicated canonical transfer review artifact is justified

## Tradeoff

This design keeps the canonical model small and stable, but it means transfer review mode is initially a write-through UI layer rather than a fully independent persisted object.

That is intentional.

The repo should earn a second canonical review schema only if the derived relation layer proves insufficient in practice.
