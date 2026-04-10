# Architecture Overview

This is the high-level map of the current codebase.

The active workflow is a staged pipeline:

1. resolve one `context` and one `run_tag`
2. normalize raw source files into canonical transaction Parquet
3. download a canonical YNAB transaction snapshot
4. build a canonical review artifact
5. project that artifact into one flat working dataframe for review and upload logic
6. prepare upload, sync lineage, and reconcile accounts

## Core Boundaries

### 1. Context configuration

Purpose:

- declare exact source files for a context
- declare artifact names
- resolve budget ids and map paths

Key code:

- [src/ynab_il_importer/context_config.py](../src/ynab_il_importer/context_config.py)
- [contexts/defaults.toml](../contexts/defaults.toml)
- [contexts/family/context.toml](../contexts/family/context.toml)

### 2. Canonical transaction artifacts

Purpose:

- normalize source files and YNAB downloads into one canonical transaction schema
- preserve real nested split data where it exists

Key code:

- [src/ynab_il_importer/artifacts/transaction_schema.py](../src/ynab_il_importer/artifacts/transaction_schema.py)
- [src/ynab_il_importer/artifacts/transaction_io.py](../src/ynab_il_importer/artifacts/transaction_io.py)
- [src/ynab_il_importer/normalize_runner.py](../src/ynab_il_importer/normalize_runner.py)
- importer modules such as:
  - [src/ynab_il_importer/io_leumi.py](../src/ynab_il_importer/io_leumi.py)
  - [src/ynab_il_importer/io_max.py](../src/ynab_il_importer/io_max.py)
  - [src/ynab_il_importer/ynab_api.py](../src/ynab_il_importer/ynab_api.py)

Active entrypoints:

- [scripts/normalize_context.py](../scripts/normalize_context.py)
- [scripts/download_ynab_api.py](../scripts/download_ynab_api.py)
- [scripts/normalize_previous_max.py](../scripts/normalize_previous_max.py)

### 3. Review build

Purpose:

- pair source rows against YNAB rows
- apply deterministic payee/category suggestions
- build the persisted review artifact

Key code:

- [scripts/build_context_review.py](../scripts/build_context_review.py)
- [scripts/build_proposed_transactions.py](../scripts/build_proposed_transactions.py)
- [src/ynab_il_importer/pairing.py](../src/ynab_il_importer/pairing.py)
- [src/ynab_il_importer/rules.py](../src/ynab_il_importer/rules.py)

### 4. Canonical review artifact and working projection

Purpose:

- persist review state as `review_v4` Parquet
- keep source/target current and original transaction structs intact
- derive one flat working dataframe for app and upload logic

Key code:

- [src/ynab_il_importer/artifacts/review_schema.py](../src/ynab_il_importer/artifacts/review_schema.py)
- [src/ynab_il_importer/review_app/io.py](../src/ynab_il_importer/review_app/io.py)
- [src/ynab_il_importer/review_app/working_schema.py](../src/ynab_il_importer/review_app/working_schema.py)
- [src/ynab_il_importer/review_reconcile.py](../src/ynab_il_importer/review_reconcile.py)

Important rule:

- the persisted artifact is canonical
- the flat working dataframe is derived and not itself a persisted format contract

### 5. Review app

Purpose:

- load the working projection
- show row and grouped review state
- let the human edit selected values and decisions
- save back to the canonical review artifact

Key code:

- [scripts/review_context.py](../scripts/review_context.py)
- [scripts/review_app.py](../scripts/review_app.py)
- [src/ynab_il_importer/review_app/app.py](../src/ynab_il_importer/review_app/app.py)
- [src/ynab_il_importer/review_app/state.py](../src/ynab_il_importer/review_app/state.py)
- [src/ynab_il_importer/review_app/validation.py](../src/ynab_il_importer/review_app/validation.py)
- [src/ynab_il_importer/review_app/model.py](../src/ynab_il_importer/review_app/model.py)

### 6. Upload, sync, and reconciliation

Purpose:

- prepare explicit `create_target` rows for YNAB upload
- stamp lineage markers onto existing YNAB rows
- reconcile bank and card accounts against canonical normalized inputs

Key code:

- [scripts/prepare_ynab_upload.py](../scripts/prepare_ynab_upload.py)
- [src/ynab_il_importer/upload_prep.py](../src/ynab_il_importer/upload_prep.py)
- [scripts/sync_bank_matches.py](../scripts/sync_bank_matches.py)
- [scripts/reconcile_bank_statement.py](../scripts/reconcile_bank_statement.py)
- [scripts/sync_card_matches.py](../scripts/sync_card_matches.py)
- [scripts/reconcile_card_cycle.py](../scripts/reconcile_card_cycle.py)
- [src/ynab_il_importer/bank_reconciliation.py](../src/ynab_il_importer/bank_reconciliation.py)
- [src/ynab_il_importer/card_reconciliation.py](../src/ynab_il_importer/card_reconciliation.py)

## Current Legacy Edges

These are still present, but they are not the preferred workflow model:

- [src/ynab_il_importer/workflow_profiles.py](../src/ynab_il_importer/workflow_profiles.py)
  Still supplies the review app's default category-cache path.
- older one-off scripts under [scripts/](../scripts/)
  Useful for diagnostics or migration support, but not the default runbook.

## Suggested Reading Paths

If you want to understand the workflow end to end:

1. [README.md](../README.md)
2. [context_workflow_spec.md](context_workflow_spec.md)
3. [upload_reconcile_cutover_spec.md](upload_reconcile_cutover_spec.md)
4. Then follow the code in the stage you care about.

If you want to understand the review model first:

1. [decisions/unified_review_model_design.md](decisions/unified_review_model_design.md)
2. [decisions/unified_review_model_schema.md](decisions/unified_review_model_schema.md)
3. [src/ynab_il_importer/review_app/io.py](../src/ynab_il_importer/review_app/io.py)
4. [src/ynab_il_importer/review_app/working_schema.py](../src/ynab_il_importer/review_app/working_schema.py)
