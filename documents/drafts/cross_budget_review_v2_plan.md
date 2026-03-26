# ynab-il-importer - Cross-Budget Review Refactor Plan

## Goal

Replace the current split proposal logic with one review model that treats every workflow as a source-vs-target comparison.

This unified model must cover:
1. Institutional import review:
   - source = normalized bank/card transaction
   - target = existing YNAB transaction in the same budget/account
2. Cross-budget review:
   - source = source-budget YNAB transaction
   - target = target-budget YNAB transaction

The review artifact must support:
1. existing automatic matches
2. source-only rows
3. target-only rows
4. ambiguous candidate relations
5. source-side recategorization/removal from the cross-budget stream
6. target-side creation or deletion when explicitly reviewed

---

## Why This Refactor Is Next

The current Family/Pilates/Aikido work exposed a structural limit in the old proposal format.

Current behavior:
1. `scripts/build_cross_budget_proposed.py` only turns `unmatched_source` rows into proposed review rows.
2. `unmatched_target` and `ambiguous` rows are emitted as diagnostics, not first-class review rows.
3. The review app is built around a single transaction plus suggestions, not an explicit source/target relation.
4. Pilates was made to work through sidecar fix manifests and manual cleanup, not one coherent bidirectional review workflow.
5. Aikido exposed the remaining gap more clearly:
   - target-only/manual rows can be real
   - source-side false positives need recategorization so they leave the cross-budget stream
   - some rows need deletion rather than upload

So the current code is not wrong. It is incomplete for the workflow we now know we need.

---

## Decisions Already Settled

Keep these decisions:
1. Pilates and Aikido should use one cross-budget workflow.
2. Neither side is the sole source of truth.
3. `source_*` and `target_*` keep stable meanings across workflows.
4. We do not need backward compatibility with the old review CSV shape.
5. The phase order remains:
   - schema and matcher
   - app
   - apply
6. Matcher statuses stay simple in the first pass:
   - `matched_auto`
   - `source_only`
   - `target_only`
   - `ambiguous`
7. Nothing should be deleted or rewritten automatically outside an explicit reviewed action.
8. Validation must use both Pilates and Aikido.

---

## Scope Boundaries For This Refactor

Out of scope for this pass:
1. reconciliation redesign
2. stale/live drift protection beyond current normal safeguards
3. a persisted generic grouping taxonomy
4. speculative heuristics like `likely_duplicate` or `likely_mistake`
5. broad transaction mutation such as:
   - date edits
   - amount edits
   - account reassignment
   - transfer surgery

Mutable fields in v1 should stay narrow:
1. payee
2. category
3. limited memo handling if needed
4. create/delete/link decisions

---

## Canonical Model

### 1. Row Grain

Every review row represents one candidate source/target relation or singleton.

A row may contain:
1. source only
2. target only
3. source + target

The same source transaction may appear in multiple rows.
The same target transaction may appear in multiple rows.
Each row still needs its own unique row/proposal ID.

Do not describe this as a generic "comparison bucket". That wording is too vague and leads to confusion between:
1. transaction grain
2. relation grain
3. UI grouping

### 2. Stable Meaning Of Source And Target

Institutional review:
1. source = imported source row
2. target = matched/current YNAB row in the same budget

Cross-budget review:
1. source = source-budget YNAB row
2. target = target-budget YNAB row

This is the main unification rule.

### 3. Transaction-Local Vs Row-Local State

This distinction is mandatory.

Transaction-local state:
1. applies to a specific transaction ID on one side
2. propagates across every row containing that same source or target ID
3. must remain value-consistent after save/reload

Row-local state:
1. applies only to one candidate relation row
2. does not propagate
3. can differ between rows that share a source or target transaction

### 4. Invariants

These invariants must be explicit before we finalize columns:
1. repeated source IDs across rows are allowed
2. repeated target IDs across rows are allowed
3. transaction-local edits propagate across all rows containing that ID
4. row-local decisions do not propagate
5. repeated IDs must remain value-consistent after save
6. grouped views are derived from row data, not stored as a separate persistent structure

### 5. Logical State To Persist

The persisted schema should carry these logical pieces of information.
Exact final column names are still to be finalized after the grain/invariant pass.

Identity and relation status:
1. row/proposal ID
2. workflow type
3. match status
4. match method

Source-side snapshot:
1. source-present flag
2. source transaction ID
3. source budget/account/date
4. source payee/category/memo/fingerprint/import ID

Target-side snapshot:
1. target-present flag
2. target transaction ID
3. target budget/account/date
4. target payee/category/memo/fingerprint/import ID

Transaction-local final values per side:
1. final source payee
2. final source category
3. final target payee
4. final target category
5. optional memo append / limited memo field if needed

Row-local decisions:
1. keep this relation
2. ignore this relation
3. create source
4. create target
5. delete source
6. delete target
7. manual-link this source and target

### 6. Decision Model

The old draft overloaded the decision block.
That should be treated as rejected/provisional, not as the schema to implement.

Guiding principle:
1. compact row action field(s)
2. compact transaction-local final values per side
3. no multiple competing ways to express the same final state

Open design question:
1. whether `mode` fields survive into the persisted schema, or whether we store only final values plus a compact basis/source-of-choice field

Do not lock the CSV columns until the above is resolved.

### 7. Manual Linking

Manual linking in v1 should be pairwise only.

Expected behavior:
1. the user explicitly links one source transaction ID to one target transaction ID
2. this creates or marks a manual relation row
3. the app does not try to collapse all surrounding ambiguity automatically
4. rerun/manual review handles the aftermath

### 8. Rerun / Rematch

Rerun must be a first-class workflow step.

It is especially important after:
1. source-side recategorization
2. payee-map changes
3. fingerprint-map changes
4. manual linking

It is acceptable for rerun to rebuild relations and leave some rows unresolved again.

### 9. Source-Side Recategorization

This is a core behavior, not a side effect.

Source-side payee/category correction may:
1. keep the row in the cross-budget stream with different values
2. remove the row from the cross-budget stream on rerun

It should not be modeled as automatically implying target creation or deletion.

---

## UI Direction

Grouped views should be UI-generated from row data rather than persisted structurally.

Useful dynamic views:
1. rows sharing a source ID
2. rows sharing a target ID
3. ambiguous rows
4. source-only rows
5. target-only rows
6. rows sharing a fingerprint
7. reviewed vs unreviewed

App capabilities needed in v1:
1. side-by-side source/target display
2. same transaction card structure in row and grouped views
3. side-specific payee/category editing
4. explicit delete-source and delete-target actions
5. explicit manual-link action
6. consistency enforcement when repeated source/target IDs appear across rows

Collapsed summary should at least show:
1. date
2. amount
3. fingerprint
4. source payee
5. target payee
6. match status
7. current decision

---

## Apply Architecture

The review artifact is unified.
The mutation engine does not need to be one monolithic two-sided executor.

Apply should operate one side at a time.

Institutional review:
1. target-side apply only

Cross-budget review:
1. source-side apply when needed
2. target-side apply when needed
3. each side can be planned and executed separately

This is simpler and safer than forcing one simultaneous two-sided mutation function.

---

## Implementation Phases

## Phase 1 - Schema And Matchers

### Objective

Define the row grain and propagation model first, then implement unified matcher outputs.

### Deliverables

1. Define row grain precisely:
   - relation/singleton row
   - repeated IDs allowed
2. Define transaction-local vs row-local state.
3. Define propagation invariants for repeated IDs.
4. Only then finalize the persisted columns.
5. Refactor cross-budget matching to emit the new row format directly.
6. Refactor institutional matching/build-proposed logic to emit the same row format.
7. Add tests that cover:
   - matched rows
   - source-only rows
   - target-only rows
   - ambiguous rows
   - repeated source IDs
   - repeated target IDs
   - propagation invariants
8. Add rerun/rematch fixtures showing:
   - source-side recategorization
   - pairwise manual linking
9. Run the new builders on current Pilates and Aikido artifacts and compare counts against known current outputs.

### Notes

1. Do not start with heuristics.
2. Do not move raw directories yet.
3. Do not change the review app in this phase.

## Phase 2 - Review App

### Objective

Make the app support only the new schema and enforce repeated-ID consistency.

### Deliverables

1. Replace the current single-row mental model with side-by-side source/target display.
2. Use the same transaction card layout in grouped and row views.
3. Generate grouped views dynamically from row data.
4. Add side-specific transaction-local final value editing.
5. Add row-local decision controls.
6. Add explicit delete-source/delete-target/manual-link actions.
7. Enforce consistency when the same source or target ID appears across multiple rows.
8. Preserve the current save/reopen CSV workflow.

## Phase 3 - Apply Layer

### Objective

Turn reviewed decisions into concrete changes safely, one side at a time.

### Deliverables

1. Institutional target-side apply:
   - create target rows
   - patch target metadata where needed
2. Cross-budget source-side apply when needed.
3. Cross-budget target-side apply when needed.
4. Verification reports after apply.
5. Rerun/rematch after apply as a standard workflow step when source/target state changed materially.

### Safety Rule

Nothing should be deleted or rewritten automatically outside an explicit reviewed action.

---

## Validation Strategy

Validate against both live problem cases.

### Pilates

Use Pilates because it already has:
1. a historically successful cross-budget run
2. direct-fix manifests
3. final reconciled reports

Key artifacts:
1. `data/paired/pilates_cross_budget_live/proposed_transactions.csv`
2. `data/paired/pilates_cross_budget_live/family_direct_fix_candidates.csv`
3. `data/paired/pilates_cross_budget_live/pilates_direct_fix_candidates.csv`
4. `data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_post_user_fix_report.csv`

### Aikido

Use Aikido because it still exposes the missing capabilities:
1. target-only/manual rows
2. source-side false positives
3. incomplete backlog needing bidirectional review

Key artifacts:
1. `data/paired/2026_03_25_aikido/aikido_full_backlog_to_current_proposed_transactions.csv`
2. `data/paired/2026_03_25_aikido/aikido_full_backlog_to_current_unmatched_source.csv`
3. `data/paired/2026_03_25_aikido/aikido_cross_budget_reconcile_report.csv`

---

## Branching Plan

1. Keep `aikido-workflow` as the checkpoint branch.
2. Create a new working branch from it:
   - `cross-budget-review-v2`
3. Build the schema/matcher refactor there.
4. Merge back into `aikido-workflow` once the new matcher outputs are stable.

---

## Questions To Pressure-Test

1. Is the relation/singleton row grain correct for both institutional and cross-budget workflows?
2. Are the transaction-local vs row-local invariants sufficient and enforceable?
3. Should persisted schema keep `mode` fields, or only final values plus a compact basis field?
4. Is pairwise manual linking enough for v1?
5. Are there hidden risks in making rerun/rematch a normal workflow step?
6. Is any important case missing from the one-side-at-a-time apply model?
7. Are we missing any critical fields while still keeping v1 narrow?

---

## Immediate Next Step

Do not start by finalizing column names in the abstract.

Start Phase 1 with:
1. row grain
2. transaction-local vs row-local state
3. propagation invariants for repeated IDs
4. rerun/rematch expectations

Only after those are explicit should we finalize the CSV columns and matcher outputs.
