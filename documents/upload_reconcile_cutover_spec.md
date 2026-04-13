# Upload And Reconcile Workflow

## Purpose

Describe the current closeout path after review is complete:

1. prepare upload payloads from the canonical reviewed artifact
2. optionally upload them to YNAB
3. run the source-kind-specific closeout path

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
- `ynab_category` reconcile consumes:
  - the canonical reviewed artifact for the target context
  - the live source-budget month detail for the run month
  - the live target-budget account and transaction snapshot
- previous card statements are normalized explicitly before card reconciliation via [scripts/normalize_previous_max.py](../scripts/normalize_previous_max.py)
  - `kind = max` reads `data/raw/previous_max/<account_suffix>/` and writes `data/derived/previous_max/<account_suffix>/`
  - `kind = leumi_card_html` reads `data/raw/previous_leumi_card/<account_suffix>/` and writes `data/derived/previous_leumi_card/<account_suffix>/`
  - by default, the helper infers the kind from the context's declared card source
- review build excludes already settled YNAB rows by default, including reconciled exact matches, reconciled transfer counterparts, and other reconciled target-side candidates; use `pixi run build-context-review -- <context> <run_tag> --include-reconciled-ynab` only for explicit historical inspection

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
pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous <normalized_previous.parquet>
```

Card reconciliation supports:

- current-source-only reconciliation
- previous-plus-current transition reconciliation
- context-declared `allow_reconciled_source` overrides for known sequencing edge cases

### YNAB-category closeout

```bash
pixi run reconcile-category-account -- <context> <run_tag>
```

This path is for contexts whose active source is another budget's YNAB category history rather than a bank/card statement.

It:

- selects the reviewed rows that came from the declared `ynab_category` source
- resolves live target transactions by:
  - existing target transaction id for `keep_match`
  - prepared upload import id for `create_target`
  - existing target transaction id for `update_target`
- verifies parity between the live source category balance and the live target account balances
- patches the resolved target transactions to `cleared = reconciled` on `--execute`

This replaces the old archived cross-budget anchored reconcile for active Aikido-style workflows. It is intentionally strict and works only on the reviewed source rows for the declared category/account pair.

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

### [src/ynab_il_importer/ynab_category_reconciliation.py](../src/ynab_il_importer/ynab_category_reconciliation.py)

Owns:

- selecting reviewed rows for one `ynab_category` source
- resolving the live source category and target account
- resolving target transactions from reviewed/uploaded state
- parity checks for category/account closeout
- reconciliation patch planning for target rows

## Current Caveats

- [scripts/prepare_ynab_upload.py](../scripts/prepare_ynab_upload.py) is part of the active workflow but still has no dedicated pixi alias.
- Previous card normalization is explicit, but it still lives outside `contexts/<context>/context.toml`; it resolves from the previous statement roots (`data/raw/previous_max/<account_suffix>/` for MAX and `data/raw/previous_leumi_card/<account_suffix>/` for Leumi HTML).
- The review app's category-cache path is still profile-based rather than context-config-based. That affects review UX, not upload or reconciliation logic.
