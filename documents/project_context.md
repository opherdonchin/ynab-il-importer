# ynab-il-importer Project Context

## Purpose

Build a deterministic, human-reviewable workflow for getting Israeli bank and card activity into YNAB while keeping every step explainable, rerunnable, and safe.

## Active Scope

The active repo scope is institutional import:

- source = normalized bank or card transaction
- target = an existing or newly created YNAB transaction in the same budget/account

Older cross-budget work is archived and is not part of the current day-to-day workflow.

## Project Priorities

1. Deterministic outputs over hidden inference
2. Human review before mutation
3. Canonical Parquet artifacts at workflow boundaries
4. Idempotent upload, sync, and reconciliation behavior
5. Small explicit rules instead of broad magic

## Stable Principles

- Context plus run tag is the human-facing workflow key.
- Review artifacts are the main control point between inference and mutation.
- Nested data is kept only where it is semantically real, especially transaction splits.
- Ordinary app and upload logic should run on one flat working dataframe.
- Reruns are expected and should be safe.

## Current Architecture Direction

The repo is now organized around two canonical artifact types:

- `transaction_v1`
  Canonical normalized transaction artifacts in Parquet.
- `review_v4`
  Canonical review artifacts in Parquet, with source/target current and original transaction structs.

The review app and upload prep do not edit those persisted structs directly. They work through one flat working projection built from the canonical review artifact.

## Reading Order

1. [plan.md](plan.md)
2. [architecture_overview.md](architecture_overview.md)
3. [context_workflow_spec.md](context_workflow_spec.md)
4. [upload_reconcile_cutover_spec.md](upload_reconcile_cutover_spec.md)
5. [decisions/unified_review_model_design.md](decisions/unified_review_model_design.md)
6. [decisions/unified_review_model_schema.md](decisions/unified_review_model_schema.md)
