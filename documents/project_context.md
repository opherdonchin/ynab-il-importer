# ynab-il-importer - Project Context

## Purpose

Build a deterministic, human-reviewable workflow for getting transactions into YNAB and keeping the result explainable, rerunnable, and safe.

## Core Workflows

1. Institutional import
   - source = normalized bank or card transaction
   - target = existing YNAB transaction in the same budget/account
2. Cross-budget review
   - source = a transaction from one YNAB budget/account
   - target = the related transaction in another YNAB budget/account

Both workflows are converging on one source/target review model.

## Project Priorities

1. Deterministic outputs over hidden inference
2. Human correction before mutation
3. CSV artifacts that can be inspected, edited, rerun, and versioned
4. Idempotent sync and upload behavior
5. Small explicit rules instead of broad magic

## What Is Stable

- Fingerprints and mapping tables remain important.
- Review artifacts are the main human control point.
- Reruns are expected and should be safe.
- Source and target keep stable meanings within a workflow.

## Current Architectural Direction

The active refactor is a hard cutover from the old proposal CSV to one unified review-row schema. That cutover is intentionally not backward compatible. Builders, the review app, validation, and upload prep should all speak the same source/target review model.

## Reading Order

1. `plan.md`
2. `decisions/unified_review_model_design.md`
3. `decisions/unified_review_model_schema.md`
4. `reference/` when a task needs older operational detail
