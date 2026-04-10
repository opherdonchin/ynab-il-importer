# ynab-il-importer

Deterministic, human-reviewable workflow for importing Israeli bank and card activity into YNAB.

The active workflow is no longer the old CSV/profile pipeline described in earlier docs. The current day-to-day path is:

- context-driven
- run-tag driven
- canonical Parquet at artifact boundaries
- Polars-first in active working data

## Read This First

If you are new to the repository, read these in order:

1. `README.md`
2. `documents/project_context.md`
3. `documents/plan.md`
4. `documents/context_workflow_spec.md`
5. `documents/upload_reconcile_cutover_spec.md`
6. `documents/decisions/unified_review_model_design.md`
7. `documents/architecture_overview.md`

## Where The Current Workflow Lives

The current workflow is spread across a small set of docs with different jobs:

- `documents/plan.md`: current status, the active next steps, and the latest validated commands
- `documents/context_workflow_spec.md`: the target context/run-tag workflow model
- `documents/upload_reconcile_cutover_spec.md`: the current post-review upload/sync/reconcile flow
- `contexts/defaults.toml`: shared artifact naming and run-directory conventions
- `contexts/<context>/context.toml`: source files, map files, and YNAB artifact names for each context

If the README and `documents/plan.md` ever disagree, treat `documents/plan.md` as the source of truth for the current runbook.

## Repository Layout

- `documents/project_context.md`: durable project orientation
- `documents/plan.md`: active execution plan
- `documents/decisions/`: durable architecture and model decisions
- `documents/archive/`: archived plans and older workflow notes
- `contexts/`: active workflow configuration by context
- `mappings/`: versioned mapping tables referenced by context configs
- `data/raw/`: raw source inputs by run tag
- `data/derived/`: normalized canonical Parquet artifacts
- `data/paired/`: built review artifacts, reviewed artifacts, and closeout outputs
- `outputs/`: generated utility outputs and placeholders
- `tmp/` and `tests_runtime/`: disposable scratch space

See `REPOSITORY_LAYOUT.md` for retention and layout rules.

## Current Workflow

The current human-facing workflow is one context plus one run tag, for example `family 2026_04_01` or `pilates 2026_04_01`.

### 1. Install the environment

```bash
pixi install
```

### 2. Put raw files under the run tag directory

Place source files in:

```text
data/raw/<run_tag>/
```

The required filenames and source kinds come from:

- `contexts/defaults.toml`
- `contexts/<context>/context.toml`

### 3. Normalize the declared source files

```bash
pixi run normalize-context -- <context> <run_tag>
```

This writes canonical normalized Parquet artifacts to:

```text
data/derived/<run_tag>/
```

### 4. Download the YNAB snapshot for that context

```bash
pixi run download-context-ynab -- <context> <run_tag>
```

Budget resolution comes from one of:

- `--budget-id`
- the context's `budget_id_env`
- `config/ynab.local.toml`

You also need `YNAB_ACCESS_TOKEN` available in the environment or `.env`.

### 5. Build the proposal review artifact

```bash
pixi run build-context-review -- <context> <run_tag>
```

This builds the canonical review artifact in:

```text
data/paired/<run_tag>/
```

### 6. Review in the app

```bash
pixi run review-context -- <context> <run_tag>
```

This launches the Streamlit review app against the built proposal artifact and resumes from the standard reviewed artifact if one already exists.

### 7. Prepare upload artifacts from the reviewed Parquet

```bash
pixi run python scripts/prepare_ynab_upload.py <context> <run_tag> \
  --ready-only \
  --skip-missing-accounts
```

This writes:

- `data/paired/<run_tag>/<context>_upload.csv`
- `data/paired/<run_tag>/<context>_upload.json`

Add `--execute` only when the dry run looks right.

### 8. Sync lineage markers onto existing YNAB transactions

Bank:

```bash
pixi run sync-bank-matches -- <context> <run_tag>
```

Card:

```bash
pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"
```

Add `--execute` only after checking the dry-run report.

### 9. Reconcile bank and card accounts

Bank:

```bash
pixi run reconcile-bank-statement -- <context> <run_tag>
```

For cards that need a prior statement normalized first:

```bash
pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM
```

Then run card reconciliation:

```bash
pixi run reconcile-card-cycle -- <context> <run_tag> \
  --account "<Card Account Name>" \
  --previous data/derived/previous_max/<account_suffix>/YYYY_MM_max_norm.parquet
```

## Active Entry Points

The current pixi tasks for the context workflow are:

- `pixi run normalize-context -- <context> <run_tag>`
- `pixi run download-context-ynab -- <context> <run_tag>`
- `pixi run build-context-review -- <context> <run_tag>`
- `pixi run review-context -- <context> <run_tag>`
- `pixi run sync-bank-matches -- <context> <run_tag>`
- `pixi run reconcile-bank-statement -- <context> <run_tag>`
- `pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM`
- `pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"`
- `pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous <normalized_previous.parquet>`

`scripts/prepare_ynab_upload.py` is also part of the active workflow, even though it does not yet have a dedicated pixi alias.

## Mapping Configuration

The active workflow uses context-specific mapping files declared in `contexts/<context>/context.toml`.

Typical entries look like:

- `mappings/<context>/account_name_map.csv`
- `mappings/<context>/fingerprint_map.csv`
- `mappings/<context>/payee_map.csv`

Those files are the active source of truth for:

- source-account to YNAB-account mapping
- fingerprint canonicalization
- deterministic payee/category suggestions

## Notes On Older Docs And Scripts

Some older scripts and docs still exist in the repo for migration support, diagnostics, or historical reference. They are not the default day-to-day workflow anymore.

In particular, treat these as legacy unless `documents/plan.md` explicitly says otherwise:

- the old CSV-centered normalize/build/review instructions
- `workflow_profiles.py`-style profile wiring
- bootstrap-era matched-pairs and grouping commands
- older docs that refer to `documents/review_app_workflow.md`
