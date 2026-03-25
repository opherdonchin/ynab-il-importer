# Cross-Budget Review Refactor Handoff

## Purpose Of This Handoff

This document is for another agent who is reviewing our planned refactor before we start implementing it.

We want a critique of:
1. the problem framing
2. the proposed schema direction
3. the implementation sequence
4. the likely risks or blind spots

The attached zip file includes the current plans, the relevant code, and example artifacts from both Pilates and Aikido.

---

## What Exists Today

The project already has three kinds of workflow:
1. ordinary Family bank/card import review
2. Family -> Pilates cross-budget comparison/review/reconciliation
3. Family -> Aikido cross-budget bootstrap/review

Important current files:
1. [scripts/build_cross_budget_proposed.py](/d:/Repositories/ynab-il-importer/scripts/build_cross_budget_proposed.py)
2. [src/ynab_il_importer/cross_budget_pairing.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/cross_budget_pairing.py)
3. [scripts/build_proposed_transactions.py](/d:/Repositories/ynab-il-importer/scripts/build_proposed_transactions.py)
4. [src/ynab_il_importer/pairing.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/pairing.py)
5. [src/ynab_il_importer/review_app/app.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/review_app/app.py)
6. [src/ynab_il_importer/review_app/io.py](/d:/Repositories/ynab-il-importer/src/ynab_il_importer/review_app/io.py)

The current system works reasonably well for ordinary import review and was stretched to make Pilates work, but the shape of the current proposal format is now the main bottleneck.

---

## The Core Problem

The current proposal format was designed around a single incoming transaction that may or may not match a YNAB transaction.

That works for institutional import review, where the main question is:
1. should this source row create a YNAB transaction?
2. if so, with what payee/category?

It does not fully work for cross-budget review, where either side can contain the "real" row first.

Examples:
1. A Family-side `Aikido` category transaction may need to be created in the Aikido budget.
2. A manual Aikido-budget cash entry may be real and need a reflected Family-side row.
3. A Family-side transaction may be incorrectly categorized as `Aikido` and should be recategorized in Family, not copied into Aikido.
4. A target-side row may be wrong and need deletion.

Current cross-budget code only promotes one of those cases into the review app:
1. `unmatched_source` becomes proposed rows
2. `unmatched_target` stays in a sidecar CSV
3. `ambiguous` stays in another sidecar CSV

That means the review app cannot act as the true decision surface for cross-budget work.

---

## Why Pilates Still Worked

Pilates succeeded because we added manual scaffolding around the current model:
1. cross-budget matching reports
2. Family-side direct-fix manifests
3. Pilates-side direct-fix manifests
4. targeted live cleanup
5. repeated reruns until the snapshots agreed

So Pilates proved the domain logic is real and worth supporting.
It also proved that the current proposal artifact is not expressive enough on its own.

---

## Why Aikido Forced The Issue

Aikido is the clearer test case because it is cross-budget only.

It exposed all of the missing review behaviors at once:
1. target-only/manual rows can be real
2. Family-side false positives can keep feeding the cross-budget stream until the Family source row is recategorized
3. some rows need deletion, not upload
4. source and target cannot be treated as "source of truth" in one fixed direction

In other words, Aikido made it obvious that we do not just need another workflow wrapper.
We need a better review model.

---

## Main Design Direction We Settled On

We agreed on these principles:

1. Pilates and Aikido should use one cross-budget workflow.
2. Neither side is the sole source of truth.
3. We should use one proposal format centered on stable `source_*` and `target_*` fields.
4. We do not need backward compatibility with the old review CSV format.
5. The app should move only after the new matcher outputs are stable.

We also agreed that the first matcher statuses should stay simple:
1. `matched_auto`
2. `source_only`
3. `target_only`
4. `ambiguous`

The reviewer should assume this is intentional. We want to avoid premature heuristics in the first pass.

---

## What We Think The New Review Artifact Should Do

Each row should explicitly describe:
1. what exists on the source side
2. what exists on the target side
3. how they matched, if at all
4. what the user wants to do next

That means the review CSV should support, at minimum:
1. create target
2. create source
3. update target payee/category
4. update source payee/category
5. delete target
6. delete source
7. keep an automatic match
8. manually link a source row and a target row that did not auto-match

The app also needs side-specific control of payee/category values, including:
1. choose from suggestions
2. override with free text
3. blank out the field when appropriate

---

## Why We Want One Schema For Institutional And Cross-Budget Work

This is the biggest architectural choice.

We think both workflows can be modeled as:
1. source row
2. target row
3. match status
4. decision/action

Institutional review:
1. source = normalized bank/card row
2. target = matched/current YNAB row in the same budget

Cross-budget review:
1. source = source-budget YNAB row
2. target = target-budget YNAB row

The hoped-for payoff is:
1. one review app mental model
2. one proposal-row schema
3. less hidden special casing
4. clearer tests

The risk is:
1. institutional and cross-budget apply logic are still different
2. one schema might become too abstract if we are not careful

This is one of the main areas where critique would help.

---

## Proposed Build Sequence

We are intentionally sequencing this in phases.

### Phase 1

1. define the new proposed-match schema
2. update cross-budget matching to emit it
3. update institutional matching to emit it
4. add tests for both
5. validate against both Pilates and Aikido artifacts

### Phase 2

1. rebuild the review app around the new schema
2. show source and target side by side
3. add deletion and override controls

### Phase 3

1. build the apply/update layer from reviewed decisions
2. verify those decisions safely against live YNAB data

We think this is safer than changing the app first.

---

## What We Want The Reviewer To Pressure-Test

Please critique these questions directly:

1. Is source/target the right abstraction for both institutional and cross-budget workflows?
2. Are we missing an important intermediate state or action type?
3. Is it wise to avoid backward compatibility entirely here?
4. Are we underestimating any app-side complexity?
5. Are there better ways to model deletion and source-side recategorization?
6. Are there obvious apply-layer hazards we should design for now rather than later?
7. Is the phase ordering sound, or would you reorder the work?

---

## Packet Contents

The zip file includes:
1. the original plan snapshot
2. the updated implementation-facing plan
3. this explanation
4. project context and workflow docs
5. current cross-budget and institutional matcher/builder code
6. current review app code
7. targeted tests
8. Pilates and Aikido example artifacts that show both the current success case and the current gap case

That packet is meant to be enough for a thoughtful design critique without sending the whole repository.
