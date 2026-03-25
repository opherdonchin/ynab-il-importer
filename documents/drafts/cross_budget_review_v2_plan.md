# ynab-il-importer - Next Implementation Plan

## Goal

Replace the current split proposal logic with one review model that treats every workflow as a source-vs-target comparison.

This new model must cover:
1. Institutional import review:
   - source = normalized bank/card transaction
   - target = existing YNAB transaction in the same budget/account
2. Cross-budget review:
   - source = source-budget YNAB transaction
   - target = target-budget YNAB transaction

The review artifact must support both directions of work:
1. recognize existing matches
2. propose missing target rows
3. propose missing source rows
4. surface ambiguous cases
5. support deletion and recategorization decisions on either side

---

## Why This Is The Next Priority

The current Family/Pilates/Aikido work exposed a structural limit in the existing proposal format.

Current behavior:
1. `scripts/build_cross_budget_proposed.py` only turns `unmatched_source` rows into proposed review rows.
2. `unmatched_target` and `ambiguous` rows are emitted as diagnostics, not as first-class review rows.
3. The app is built around a single transaction plus suggestions, not a source/target pair.
4. Pilates was made to work through extra fix manifests and manual live cleanup, not through one coherent bidirectional review workflow.
5. Aikido made the missing capabilities obvious:
   - target-only/manual rows can be real and need to propagate back
   - Family-side false positives need source-side recategorization so they leave the cross-budget stream
   - some rows need deletion, not upload

So the current code is not wrong, but it is incomplete for the workflow we actually want.

---

## Working Decisions Already Agreed

1. Pilates and Aikido should use the same cross-budget workflow.
2. Neither side is the sole source of truth.
3. We want one proposal format, centered on stable `source_*` and `target_*` meanings.
4. We do not need backward compatibility with the old review format.
5. We should reuse existing scripts and tests where that is convenient, but not preserve old structure for its own sake.
6. The work should proceed in this order:
   - schema for proposed matches
   - cross-budget matching function with tests
   - institutional matching update with tests
   - review app update
   - apply/update layer
7. For the first pass, matcher statuses should stay neutral and simple:
   - `matched_auto`
   - `source_only`
   - `target_only`
   - `ambiguous`
8. The app must eventually support:
   - deletion of source and/or target rows
   - source-side payee/category override
   - target-side payee/category override
   - clearer display of source, target, fingerprint, and current decision state

---

## Current Limits In The Code

Primary files:
1. [scripts/build_cross_budget_proposed.py](/d:/Repositories/ynab-il-importer/scripts/build_cross_budget_proposed.py)
2. [src/ynab_il_importer/cross_budget_pairing.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/cross_budget_pairing.py)
3. [scripts/build_proposed_transactions.py](/d:/Repositories/ynab-il-importer/scripts/build_proposed_transactions.py)
4. [src/ynab_il_importer/pairing.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/pairing.py)
5. [src/ynab_il_importer/review_app/io.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/review_app/io.py)
6. [src/ynab_il_importer/review_app/app.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/review_app/app.py)

Observed limitations:
1. Cross-budget proposals only contain `unmatched_source` candidates.
2. Institutional proposals contain only source rows that survived dedupe against YNAB.
3. The review CSV does not model source and target as explicit peers.
4. The app cannot represent delete/backfill/link decisions as first-class actions.
5. The app shows source/target context inconsistently across row and grouped views.

---

## Target Design

### 1. One Review Row Model

Every review row should represent a comparison bucket with explicit source and target fields.

Suggested column groups:

1. Identity and status
   - `proposal_id`
   - `workflow_type`
   - `match_status`
   - `match_method`
   - `group_key`
   - `signed_amount`
   - `reviewed`
   - `decision_action`

2. Source side
   - `source_present`
   - `source_row_id`
   - `source_budget`
   - `source_account`
   - `source_date`
   - `source_payee_current`
   - `source_category_current`
   - `source_memo`
   - `source_fingerprint`
   - `source_import_id`

3. Target side
   - `target_present`
   - `target_row_id`
   - `target_budget`
   - `target_account`
   - `target_date`
   - `target_payee_current`
   - `target_category_current`
   - `target_memo`
   - `target_fingerprint`
   - `target_import_id`

4. Review decisions
   - `source_payee_mode`
   - `source_payee_selected`
   - `source_payee_override`
   - `source_category_mode`
   - `source_category_selected`
   - `source_category_override`
   - `target_payee_mode`
   - `target_payee_selected`
   - `target_payee_override`
   - `target_category_mode`
   - `target_category_selected`
   - `target_category_override`
   - `delete_source`
   - `delete_target`
   - `manual_link_key`
   - `update_map_source`
   - `update_map_target`

Mode fields should support the app behavior we discussed:
1. `selection`
2. `override`
3. `blank`

If needed, `uncategorized` can stay a value rather than a separate mode.

### 2. Stable Meaning Of Source And Target

Source and target must keep the same meaning across workflows.

Institutional review:
1. source = imported source row
2. target = existing YNAB row in the same budget

Cross-budget review:
1. source = source-budget YNAB row
2. target = target-budget YNAB row

This is the main unification rule.

### 3. Action Model

The matcher should stay descriptive. The reviewer chooses the action.

Expected first-pass actions:
1. `keep_match`
2. `create_target`
3. `create_source`
4. `update_target`
5. `update_source`
6. `delete_target`
7. `delete_source`
8. `link_manual`
9. `ignore`

We can rename these after the schema draft if a cleaner vocabulary appears.

---

## Implementation Phases

## Phase 1 - Schema And Matchers

### Objective

Make the proposal format and matching engines real before changing the app.

### Deliverables

1. Define the new review-row schema in code and in one short document/example fixture.
2. Refactor cross-budget matching to emit the new row format directly.
3. Refactor institutional matching/build-proposed logic to emit the same row format.
4. Add tests that cover:
   - matched rows
   - source-only rows
   - target-only rows
   - ambiguous rows
   - source/target overrides carried in the row format
5. Run the new builders on current Pilates and Aikido artifacts and compare counts against the known current outputs.

### Notes

1. Do not start with heuristics like "likely duplicate" or "likely mistake".
2. Do not move raw directories yet.
3. Do not change the review app in this phase.

## Phase 2 - Review App

### Objective

Make the app understand only the new schema and present source/target information clearly.

### Deliverables

1. Replace the current single-row mental model with a side-by-side source/target display.
2. Use the same transaction card layout in grouped and row display modes.
3. Show collapsed summary with:
   - date
   - amount
   - fingerprint
   - source payee
   - target payee
   - match status
   - current decision
4. Add side-specific payee/category controls with mode switches.
5. Add delete-source and delete-target controls.
6. Preserve the current save/reopen workflow with CSV as the source of truth.

## Phase 3 - Apply Layer

### Objective

Turn reviewed decisions into concrete changes safely.

### Deliverables

1. Institutional apply path:
   - create target transactions
   - patch target metadata where needed
2. Cross-budget apply path:
   - create missing rows on either side
   - patch payee/category on either side
   - delete rows on either side when explicitly approved
3. Verification reports after apply.

### Safety Rule

Nothing should be deleted or rewritten automatically outside an explicit reviewed action.

---

## Validation Strategy

We should validate against both live problem cases, not just one.

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

## Questions For Review

These are the questions we want another agent to pressure-test:

1. Is the proposed source/target schema the right abstraction for both institutional and cross-budget workflows?
2. Are the phase boundaries sensible, especially delaying app work until matcher outputs are stable?
3. Are the proposed action types sufficient without becoming too complicated too early?
4. Are there hidden risks in unifying institutional and cross-budget proposal building under one row model?
5. Is there a cleaner way to represent source-side recategorization and deletion without overloading the review CSV?
6. What is the safest path from reviewed CSV decisions to concrete YNAB mutations?

---

## Immediate Next Step

Start Phase 1 by drafting the exact review-row schema and building fixture-driven tests for both:
1. cross-budget pairing output
2. institutional pairing output

Only after those outputs look right should we touch the review app.
