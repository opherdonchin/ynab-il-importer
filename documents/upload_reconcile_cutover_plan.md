# Upload And Reconcile Cutover Plan

## Goal

Move the active closeout path onto strict canonical Parquet + Polars-first boundaries so Family upload/sync/reconcile can run cleanly from the reviewed artifact and normalized source artifacts.

## Slice 1: Upload Prep Boundary Cutover

### Deliverables

- [src/ynab_il_importer/upload_prep.py](src/ynab_il_importer/upload_prep.py) has strict public boundaries
- public upload-prep helpers are Polars-first
- [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) is the real strict path, not a compatibility wrapper
- Family upload dry run works from the canonical reviewed parquet

### Work

- split artifact projection from working-frame preparation
- remove mixed public input handling
- remove pandas-first filter flow in the script
- make readiness/account/category checks operate on one schema-guaranteed working dataframe
- convert prepared upload construction, grouping, and preflight to Polars-first logic

### Validation

- focused upload-prep tests
- script parser tests
- live Family dry run from:
  - reviewed artifact: `data/paired/2026_04_01/family_proposed_transactions_reviewed_current.parquet`

## Slice 2: Context-Driven Upload Entrypoint

### Deliverables

- [scripts/prepare_context_upload.py](scripts/prepare_context_upload.py)
- upload outputs resolved from context + run tag
- no active upload path depends on [workflow_profiles.py](src/ynab_il_importer/workflow_profiles.py)

### Work

- resolve reviewed artifact path from context + run tag
- resolve output CSV/JSON names from defaults + context
- resolve budget id from context config
- keep script logic thin

### Validation

- entrypoint help/smoke test
- Family dry run through the context-driven upload script

## Slice 3: Bank Sync/Reconcile Cutover

### Deliverables

- [src/ynab_il_importer/bank_reconciliation.py](src/ynab_il_importer/bank_reconciliation.py) is Polars-first for canonical input paths
- canonical normalized bank parquet is the only active source input
- context-driven bank sync/reconcile scripts exist

### Work

- separate canonical input preparation from reconciliation logic
- remove repeated coercion of schema-guaranteed columns
- rewrite report construction to Polars where practical
- keep only narrow Python helpers for lineage decisions and patch serialization

### Validation

- focused bank reconciliation tests
- Family bank sync dry run + execute
- Family bank reconciliation dry run + execute

## Slice 4: Explicit Previous-Statement Normalization

### Deliverables

- previous card statement files are normalized explicitly before reconciliation
- reconciliation no longer reads raw previous statements directly

### Work

- add explicit normalization path for `data/raw/previous_max/...`
- decide stable output naming for previous statement canonical artifacts
- make card reconciliation consume only canonical normalized previous/current card artifacts

### Validation

- focused tests for previous-statement normalization handoff
- Family previous statement canonical artifacts built successfully

## Slice 5: Card Sync/Reconcile Cutover

### Deliverables

- [src/ynab_il_importer/card_reconciliation.py](src/ynab_il_importer/card_reconciliation.py) is Polars-first on canonical inputs
- context-driven card sync/reconcile scripts exist
- Family card sync and reconciliation are verified

### Work

- remove raw-file loading from reconciliation core logic
- move source filtering and account-targeting to canonical/Polars path
- remove repeated coercion of schema-guaranteed columns
- keep transition-mode logic, but only over canonical previous/current artifacts

### Validation

- focused card reconciliation tests
- Family card sync dry run + execute
- Family card reconciliation dry run + execute

## Slice 6: Full Family Closeout Verification

### Deliverables

- Family closeout path is fully verified on the new architecture
- reports written under `data/paired/2026_04_01/`
- remaining blockers, if any, are real data issues rather than stale workflow code

### Work

- run full Family sequence:
  - upload dry run
  - upload execute
  - bank sync dry run
  - bank sync execute
  - card sync dry run
  - card sync execute
  - bank reconciliation dry run
  - bank reconciliation execute
  - card reconciliation dry run
  - card reconciliation execute
- inspect reports for residual blockers

### Validation

- live Family end-to-end results captured in the plan

## Recommended Commit Order

1. Slice 1
2. Slice 2
3. Slice 3
4. Slice 4
5. Slice 5
6. Slice 6

## Immediate Next Step

Start Slice 1:

- refactor [src/ynab_il_importer/upload_prep.py](src/ynab_il_importer/upload_prep.py)
- convert [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) to the strict path
- make the Family upload dry run succeed from the canonical reviewed artifact
