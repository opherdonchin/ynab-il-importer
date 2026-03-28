# Active Plan

## Workstream

Unified review model hard cutover on branch `cross-budget-review-v2`.

## Goal

Replace the legacy proposal workflow with one persisted source/target review schema shared by institutional import and cross-budget review.

Hard constraints:
- no `--out-v2`
- no backward compatibility
- remove v1 and legacy proposal-format code
- the persisted review CSV keeps only the new schema

## Settled Decisions

- Source and target are both editable.
- Persisted selected fields are side-specific only.
- Unsuffixed selected fields are removed from the review CSV.
- `decision_action` stores the row action or default suggestion.
- `reviewed` is the approval gate; reviewed rows cannot carry `No decision`.
- Institutional sources cannot use `create_source`, `delete_source`, or `delete_both`.
- `update_map` becomes `update_maps`.
- Chooser-based manual relinking is deferred.

See `documents/decisions/` for the full design and schema contract.

## Current Code State

Already done in the working tree:
- legacy `scripts/build_cross_budget_proposed.py` removed
- legacy `src/ynab_il_importer/proposed_defaults.py` removed
- legacy cross-budget proposal test removed
- review-app model and state work started toward side-specific fields and target-side propagation
- focused test runs are using the `pixi` environment

Still in progress:
1. finish builder and output cutover so only the unified review schema remains
2. finish review app IO, validation, state, and app behavior around the new schema
3. make upload prep honor reviewed explicit `create_target` rows only
4. remove remaining legacy scripts and tests
5. rerun focused `pixi` tests until the cutover is coherent

## Working Order

1. Keep the persisted schema and validation rules consistent first.
2. Finish the review app around that contract.
3. Tighten upload prep and apply behavior.
4. Rewrite focused tests and rerun them in `pixi`.

## Deferred

- chooser-based manual relinking UI
- broader sync execution for every non-`create_target` action
- richer `update_maps` ergonomics beyond the minimal explicit form
