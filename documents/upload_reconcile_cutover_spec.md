# Upload And Reconcile Workflow

## Purpose

Describe the current closeout path after review is complete:

1. prepare upload payloads from the canonical reviewed artifact
2. optionally upload them to YNAB
3. sync lineage markers onto matched bank/card transactions already in YNAB
4. reconcile bank and card accounts against canonical normalized source artifacts

## Core Boundaries

### Reviewed artifact input

Upload prep starts from:

- a canonical reviewed `review_v4` Parquet file, or
- a Polars working dataframe already in the working schema

The public loader is [load_upload_working_frame](../src/ynab_il_importer/upload_prep.py), which goes through:

1. [review_app/io.load_review_artifact](../src/ynab_il_importer/review_app/io.py)
2. [review_app/io.project_review_artifact_to_working_dataframe](../src/ynab_il_importer/review_app/io.py)
3. [review_app/working_schema.build_working_dataframe](../src/ynab_il_importer/review_app/working_schema.py)

### Source input

- bank sync and bank reconciliation consume canonical normalized bank Parquet
- card sync and card reconciliation consume canonical normalized card Parquet
- previous MAX statements are normalized explicitly before card reconciliation via [scripts/normalize_previous_max.py](../scripts/normalize_previous_max.py)

### Live YNAB data

Scripts fetch live YNAB accounts, transactions, and categories at the script boundary and pass those into the core modules. Core modules do not resolve context names or repo paths.

## Active Commands

### Upload prep

```bash
pixi run python scripts/prepare_ynab_upload.py <context> <run_tag> --ready-only --skip-missing-accounts
```

Defaults resolve:

- reviewed artifact path from `data/paired/<run_tag>/`
- upload CSV and JSON names from [contexts/defaults.toml](../contexts/defaults.toml)
- budget id from the context config

Outputs:

- `data/paired/<run_tag>/<context>_upload.csv`
- `data/paired/<run_tag>/<context>_upload.json`

The script can also `--execute`, run upload preflight, summarize upload results, and verify returned split/transfer behavior.

### Bank closeout

```bash
pixi run sync-bank-matches -- <context> <run_tag>
pixi run reconcile-bank-statement -- <context> <run_tag>
```

These scripts:

- resolve the one declared bank source for the context
- load canonical bank Parquet
- fetch live YNAB accounts and transactions
- write CSV reports under `data/paired/<run_tag>/`
- optionally `--execute` YNAB patch calls

### Card closeout

```bash
pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"
pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM
pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous data/derived/previous_max/<account_suffix>/YYYY_MM_max_norm.parquet
```

Card reconciliation supports:

- current-source-only reconciliation
- previous-plus-current transition reconciliation
- context-declared `allow_reconciled_source` overrides for known sequencing edge cases

## Module Responsibilities

### [src/ynab_il_importer/upload_prep.py](../src/ynab_il_importer/upload_prep.py)

Owns:

- `create_target` filtering
- readiness validation
- account/category resolution
- import-id generation
- split explosion and regrouping for upload payloads
- upload preflight and response verification

Does not own:

- repo path resolution
- context lookup
- legacy CSV/parquet guessing

### [src/ynab_il_importer/bank_reconciliation.py](../src/ynab_il_importer/bank_reconciliation.py)

Owns:

- bank-source preparation from canonical columns
- lineage sync planning
- uncleared YNAB triage
- reconciliation planning and patch payload generation

### [src/ynab_il_importer/card_reconciliation.py](../src/ynab_il_importer/card_reconciliation.py)

Owns:

- card-source preparation from canonical columns
- lineage sync planning
- current-cycle and previous-plus-current reconciliation
- transfer and statement-window validation

## Current Caveats

- [scripts/prepare_ynab_upload.py](../scripts/prepare_ynab_upload.py) is part of the active workflow but still has no dedicated pixi alias.
- Previous MAX normalization is explicit, but it still lives outside `contexts/<context>/context.toml`; it resolves from `data/raw/previous_max/<account_suffix>/`.
- The review app's category-cache path is still profile-based rather than context-config-based. That affects review UX, not upload or reconciliation logic.
