# Upload And Reconcile Cutover Spec

## Purpose

Define the target-state Family/Pilates/Aikido closeout workflow after review is complete:

1. prepare upload payloads from the canonical reviewed artifact
2. upload to YNAB
3. sync lineage markers onto matched YNAB transactions
4. reconcile bank and card accounts against canonical normalized source artifacts

This cutover follows the same architectural rules as the rest of the branch:

- canonical persisted artifacts are Parquet
- active working dataframes are Polars-first
- validation happens at clear boundaries
- scripts resolve context + run-tag paths, but core module logic does not know repo layout
- no compatibility wrappers for stale CSV/pandas-first paths

## Current Problems

The active upload/reconcile path is still on the old boundary model.

### Upload prep

- [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) still loads the reviewed artifact and immediately converts it to pandas.
- [src/ynab_il_importer/upload_prep.py](src/ynab_il_importer/upload_prep.py) still accepts mixed input shapes:
  - path
  - pandas dataframe
  - generic object
- `_review_artifact_to_working_frame(...)` conflates:
  - canonical review artifact
  - already-flat working dataframe
- readiness, account filtering, and amount validation are therefore applied against ambiguous input shapes instead of one schema-guaranteed working dataframe.

### Bank and card closeout

- [scripts/sync_bank_matches.py](scripts/sync_bank_matches.py), [scripts/sync_card_matches.py](scripts/sync_card_matches.py), [scripts/reconcile_bank_statement.py](scripts/reconcile_bank_statement.py), and [scripts/reconcile_card_cycle.py](scripts/reconcile_card_cycle.py) still depend on [workflow_profiles.py](src/ynab_il_importer/workflow_profiles.py).
- [src/ynab_il_importer/bank_reconciliation.py](src/ynab_il_importer/bank_reconciliation.py) is still pandas-first and still treats normalized input as CSV-shaped rather than canonical-schema-shaped.
- [src/ynab_il_importer/card_reconciliation.py](src/ynab_il_importer/card_reconciliation.py) is also pandas-first and still mixes:
  - raw source loading
  - normalization/coercion
  - reconciliation logic
  - transition-month statement handling

### Previous statement handling

- `previous_max` statement files are still handled ad hoc inside card reconciliation.
- That work should move to an explicit normalization boundary so reconciliation consumes canonical normalized artifacts only.

## Target State

## Canonical boundaries

### Reviewed artifact input

- upload prep consumes only:
  - a canonical reviewed artifact parquet file, or
  - an already-projected Polars working dataframe with the working schema
- it does not accept pandas dataframes as a public API shape
- it does not guess whether an input is already flat

### Source input

- bank sync/reconcile consumes canonical normalized bank parquet
- card sync/reconcile consumes canonical normalized card parquet
- previous card statement files are normalized explicitly before reconciliation and then treated like any other canonical card artifact

### YNAB live data

- live YNAB accounts, transactions, and categories are fetched once at the script boundary
- core modules receive those as explicit inputs
- modules do not resolve profile/context/budget information themselves

## Dataframe model

- ordinary filtering, joins, grouping, readiness checks, and report construction should be Polars-first
- nested split data stays nested only where semantically real:
  - upload subtransactions
  - split-bearing reviewed transaction structs
- Python row loops are allowed only for:
  - payload serialization
  - API patch dict construction
  - narrow identity logic that is not naturally columnar

## Script model

Scripts are thin adapters from:

- `contexts/defaults.toml`
- `contexts/<context>/context.toml`
- `run_tag`

to explicit module calls.

Active closeout entrypoints should be context-driven and stable:

```text
scripts/
  prepare_context_upload.py
  sync_context_bank.py
  sync_context_card.py
  reconcile_context_bank.py
  reconcile_context_card.py
```

If [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) remains, it must be the real strict path, not a legacy wrapper.

## Upload prep target behavior

[src/ynab_il_importer/upload_prep.py](src/ynab_il_importer/upload_prep.py) should provide:

- one strict artifact-to-working boundary
- one strict working-to-prepared-upload boundary
- one strict prepared-upload-to-payload boundary

Conceptually:

1. canonical reviewed artifact -> working review Polars dataframe
2. working review Polars dataframe -> prepared upload Polars dataframe
3. prepared upload Polars dataframe -> transaction payload records

Public helpers should be explicit about which stage they accept.

The module should own:

- decision filtering for `create_target`
- readiness validation
- account/category resolution
- import-id generation
- split explosion and grouping
- upload preflight and verification

It should not own:

- repo path resolution
- context lookup
- ad hoc CSV-vs-parquet guessing

## Bank sync and reconciliation target behavior

[src/ynab_il_importer/bank_reconciliation.py](src/ynab_il_importer/bank_reconciliation.py) should consume:

- one canonical normalized bank table
- live YNAB accounts
- live YNAB transactions

and produce:

- a Polars sync report + patch payloads
- a Polars uncleared-triage report + triage counts
- a Polars reconciliation report + patch payloads

The core logic should rely on canonical guarantees for:

- account name / account id mapping columns
- `date`
- `secondary_date`
- `outflow_ils`
- `inflow_ils`
- `balance_ils` when reconciliation requires it
- `bank_txn_id`

Repeated pandas coercion of those columns should be removed.

## Card sync and reconciliation target behavior

[src/ynab_il_importer/card_reconciliation.py](src/ynab_il_importer/card_reconciliation.py) should consume:

- one canonical normalized current card table
- optionally one canonical normalized previous statement card table
- live YNAB accounts
- live YNAB transactions

and produce:

- a Polars sync report + patch payloads
- a Polars reconciliation report + patch payloads

The module should no longer read raw `.xlsx` / `.html` / `.csv` files directly as part of reconciliation.

Transition-mode reconciliation remains valid, but `previous_max` input must be normalized before the reconciliation function sees it.

## Validation requirements

The refactored path is considered done only when all of the following are true:

1. Family upload dry run succeeds from the canonical reviewed parquet.
2. Family upload execute succeeds from the same path.
3. Bank lineage sync dry run and execute succeed from canonical normalized bank parquet.
4. Card lineage sync dry run and execute succeed from canonical normalized card parquet.
5. Bank reconciliation dry run and execute succeed from canonical normalized bank parquet.
6. Card reconciliation dry run and execute succeed from canonical normalized current + previous canonical card parquet.
7. All of the above run through context-driven scripts, not `workflow_profiles.py`.

## Explicit non-goals

- preserving old CSV-based active workflows
- public mixed `pandas | polars | path | object` signatures in upload/reconcile core modules
- hiding migration failures behind compatibility branches
- leaving previous statement normalization implicit inside card reconciliation
