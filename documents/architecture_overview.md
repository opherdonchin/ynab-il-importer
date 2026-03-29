# Architecture Overview

This document is the high-level map for the codebase.
It is meant to answer a newcomer's first question: "Which modules own which stage of the workflow?"

## Core Idea

The project builds deterministic CSV artifacts first, asks a human to review them, and only then mutates YNAB.

That means the codebase is best read as a staged pipeline rather than as one large application:

1. Normalize raw bank/card/YNAB inputs into a common tabular shape.
2. Build identities, fingerprints, matches, and rule-driven suggestions.
3. Produce review-row CSVs.
4. Let the human review and edit those rows in the review app.
5. Prepare upload/sync/reconcile actions from reviewed artifacts.

## Main Stages

### 1. Input Normalization

Purpose:
- read raw bank/card/YNAB exports
- normalize them into stable CSV columns
- derive account identity and other workflow inputs

Key modules:
- `src/ynab_il_importer/io_leumi.py`
- `src/ynab_il_importer/io_leumi_xls.py`
- `src/ynab_il_importer/io_leumi_card_html.py`
- `src/ynab_il_importer/io_max.py`
- `src/ynab_il_importer/io_ynab.py`
- `src/ynab_il_importer/normalize.py`
- `src/ynab_il_importer/account_map.py`

Common entry points:
- `scripts/normalize_file.py`
- `scripts/download_ynab_api.py`
- `scripts/download_ynab_categories.py`

### 2. Matching, Fingerprints, and Rule Application

Purpose:
- derive stable text fingerprints
- pair source rows with YNAB rows
- apply payee/category rules
- identify ambiguous or unmatched cases

Key modules:
- `src/ynab_il_importer/fingerprint.py`
- `src/ynab_il_importer/pairing.py`
- `src/ynab_il_importer/rules.py`
- `src/ynab_il_importer/map_updates.py`
- `src/ynab_il_importer/workflow_profiles.py`

Common entry points:
- `scripts/build_proposed_transactions.py`
- `scripts/bootstrap_pairs.py`
- `scripts/build_groups.py`
- `scripts/bootstrap_payee_map.py`

### 3. Review Artifact Shape

Purpose:
- keep one human-editable review-row schema as the control point between inference and mutation
- preserve explicit source/target semantics

Primary design docs:
- `documents/decisions/unified_review_model_design.md`
- `documents/decisions/unified_review_model_schema.md`

Important note:
- unsuffixed columns like `payee_selected` and `category_selected` are compatibility/readability aliases for the target side in some flows
- persisted review data is side-specific: `source_*_selected` and `target_*_selected`

### 4. Review App

Purpose:
- load proposed review rows
- derive blocker/state/filter information
- let the user edit source/target selections and decisions
- save reviewed rows back to CSV

Package:
- `src/ynab_il_importer/review_app/`

Submodules:
- `app.py`: Streamlit UI, session state, orchestration
- `state.py`: derived series, filtering, row-edit helpers
- `validation.py`: blocker logic, allowed actions, component validation
- `model.py`: edit application and competing-row behavior
- `io.py`: load/save of review CSVs

Entry points:
- `scripts/review_app.py`
- `documents/review_app_workflow.md`

### 5. Upload Preparation and Execution

Purpose:
- convert reviewed rows into YNAB-ready payloads
- keep upload behavior explicit, dry-runnable, and idempotent

Key modules:
- `src/ynab_il_importer/upload_prep.py`
- `src/ynab_il_importer/ynab_api.py`
- `src/ynab_il_importer/export.py`

Entry points:
- `scripts/prepare_ynab_upload.py`

### 6. Reconciliation and Post-Review Flows

Purpose:
- sync lineage onto existing YNAB rows
- reconcile bank/card/cross-budget states after upload or review

Key modules:
- `src/ynab_il_importer/bank_reconciliation.py`
- `src/ynab_il_importer/card_reconciliation.py`
- `src/ynab_il_importer/cross_budget_reconciliation.py`
- `src/ynab_il_importer/review_reconcile.py`

Entry points:
- `scripts/sync_bank_matches.py`
- `scripts/sync_card_matches.py`
- `scripts/reconcile_bank_statement.py`
- `scripts/reconcile_card_cycle.py`
- `scripts/reconcile_cross_budget_balance.py`
- `scripts/reconcile_reviewed_transactions.py`

## Two Kinds of Entry Point

There are two parallel "front doors" in the repo:

- `scripts/`
  - operational wrappers used directly in day-to-day runs
  - often handle CLI parsing, path defaults, and process launching
- `src/ynab_il_importer/cli.py`
  - package CLI surface for commands that have been promoted into the main application interface

As a rule of thumb, business logic should live in `src/`, while `scripts/` should stay thin wrappers when practical.

## Where To Start Reading

If you want to understand behavior:

1. Read `README.md` for the runnable workflow.
2. Read `documents/project_context.md`.
3. Read the unified review model docs in `documents/decisions/`.
4. Read this file.
5. Then jump into the stage you care about.

Suggested code-reading paths:

- Review app:
  - `scripts/review_app.py`
  - `src/ynab_il_importer/review_app/app.py`
  - `src/ynab_il_importer/review_app/state.py`
  - `src/ynab_il_importer/review_app/validation.py`

- Import/build pipeline:
  - `scripts/normalize_file.py`
  - `scripts/build_proposed_transactions.py`
  - `src/ynab_il_importer/pairing.py`
  - `src/ynab_il_importer/rules.py`

- Upload/reconcile pipeline:
  - `scripts/prepare_ynab_upload.py`
  - `src/ynab_il_importer/upload_prep.py`
  - `src/ynab_il_importer/bank_reconciliation.py`
  - `src/ynab_il_importer/card_reconciliation.py`

## Current Pain Points

The main architectural debt that still matters:

- `src/ynab_il_importer/review_app/app.py` is still too large
- some scripts still hold more workflow logic than ideal
- a few compatibility conventions are implicit rather than explained in code

Those are active cleanup areas, not signs that the core model is unstable.
