# ynab-il-importer

Deterministic, human-reviewable workflow for importing Israeli bank and card activity into YNAB.

The active repo path is:

- context-driven
- run-tag driven
- canonical Parquet at artifact boundaries
- Polars-first in active working data

The default workflow is institutional import. Older cross-budget material is historical only and lives under [documents/archive/](documents/archive/).

## Start Here

Read these in order:

1. [documents/README.md](documents/README.md)
2. [documents/project_context.md](documents/project_context.md)
3. [documents/plan.md](documents/plan.md)
4. [documents/architecture_overview.md](documents/architecture_overview.md)

## Quick Workflow

Use one `context` such as `family`, `pilates`, or `aikido`, plus one `run_tag` such as `2026_04_01`.

### 1. Install the environment

```bash
pixi install
```

### 2. Put raw files under the run directory

```text
data/raw/<run_tag>/
```

The exact filenames and source kinds come from:

- [contexts/defaults.toml](contexts/defaults.toml)
- [contexts/<context>/context.toml](contexts/family/context.toml)

Some contexts use raw bank/card files. Others can declare derived sources such as a YNAB category from another context's snapshot for the same run tag.

### 3. Normalize the declared raw sources

```bash
pixi run normalize-context -- <context> <run_tag>
```

This writes canonical transaction artifacts to `data/derived/<run_tag>/`.

### 4. Download the YNAB snapshot

```bash
pixi run download-context-ynab -- <context> <run_tag>
```

Budget resolution comes from one of:

- `--budget-id`
- the context's `budget_id_env`
- `config/ynab.local.toml`

You also need `YNAB_ACCESS_TOKEN` available in the environment or `.env`.

### 5. Build the review artifact

```bash
pixi run build-context-review -- <context> <run_tag>
```

This writes the canonical `review_v4` artifact to `data/paired/<run_tag>/`.

By default, fresh review builds exclude YNAB rows that are already settled (`cleared` or `reconciled`), including reconciled exact matches and reconciled target-only history. Use `--include-reconciled-ynab` only for explicit historical inspection.

### 6. Review in the app

```bash
pixi run review-context -- <context> <run_tag>
```

The launcher always opens the built proposal review artifact for that context/run-tag pair. If the standard reviewed artifact already exists in `data/paired/<run_tag>/`, it automatically passes that file as `--resume` and reopens the saved review state instead of starting from a blank session.

Useful options:

```bash
pixi run review-context -- <context> <run_tag> --resume
pixi run review-context -- <context> <run_tag> --resume path\\to\\other_reviewed.parquet
pixi run review-context -- <context> <run_tag> --foreground
pixi run review-context -- <context> <run_tag> --port 8502
```

- `--resume` with no path resumes from the standard reviewed artifact path
- `--resume <path>` resumes from an explicit reviewed artifact
- `--foreground` keeps the wrapper attached instead of returning immediately
- `--port <n>` asks Streamlit to use a specific port

The app also expects a YNAB category cache; if that cache is missing or stale, use the app's `Refresh categories from YNAB` button after launch.

The current review-state model is:

- `Needs fix`: the row is invalid or incomplete
- `Needs decision`: the row is valid, but still on `No decision`
- `Needs review`: the row is valid and has a decision, but has not been accepted yet
- `Settled`: the row has been accepted

Fresh proposal defaults matter:

- matched rows start as `keep_match` and land in `Needs review`
- source-only rows start as `create_target` and land in `Needs review`
- target-only rows start as `No decision` and land in `Needs decision`

Accept actions use the decision currently staged in the UI. Applying edits to a settled row implicitly reopens it back to `Needs review`, `Needs decision`, or `Needs fix` depending on the result.

### 7. Prepare upload artifacts

```bash
pixi run python scripts/prepare_ynab_upload.py <context> <run_tag> --ready-only --skip-missing-accounts
```

This writes:

- `data/paired/<run_tag>/<context>_upload.csv`
- `data/paired/<run_tag>/<context>_upload.json`

Add `--execute` only after the dry run looks correct.

### 8. Run the source-specific closeout path

Bank:

```bash
pixi run sync-bank-matches -- <context> <run_tag>
```

Card:

```bash
pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"
```

Add `--execute` only after checking the dry-run report.

### 9. Reconcile accounts

Bank:

```bash
pixi run reconcile-bank-statement -- <context> <run_tag>
```

Cards that need a previous statement first normalize that statement explicitly. The helper infers the statement kind from the context's declared card source (`max` -> `data/raw/previous_max/...`, `leumi_card_html` -> `data/raw/previous_leumi_card/...`):

```bash
pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM
pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous <normalized_previous.parquet>
```

YNAB category source:

```bash
pixi run reconcile-category-account -- <context> <run_tag>
```

This closeout is for contexts like Aikido whose source is another budget's YNAB category history instead of a bank/card statement. It verifies live category/account parity and marks the resolved target account rows `reconciled`.

## Active Entry Points

Current workflow commands:

- `pixi run normalize-context -- <context> <run_tag>`
- `pixi run download-context-ynab -- <context> <run_tag>`
- `pixi run build-context-review -- <context> <run_tag>`
- `pixi run review-context -- <context> <run_tag>`
- `pixi run sync-bank-matches -- <context> <run_tag>`
- `pixi run reconcile-bank-statement -- <context> <run_tag>`
- `pixi run reconcile-category-account -- <context> <run_tag>`
- `pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM`
- `pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"`
- `pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous <normalized_previous.parquet>`

The upload step is also active, but it is still invoked directly through [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) rather than a dedicated pixi alias.

## Documentation Map

- [documents/README.md](documents/README.md): guide to the active docs
- [documents/context_workflow_spec.md](documents/context_workflow_spec.md): current context/run-tag configuration model
- [documents/upload_reconcile_cutover_spec.md](documents/upload_reconcile_cutover_spec.md): current post-review upload, sync, and reconcile path
- [documents/decisions/unified_review_model_design.md](documents/decisions/unified_review_model_design.md): durable review semantics
- [documents/decisions/unified_review_model_schema.md](documents/decisions/unified_review_model_schema.md): durable review artifact contract
- [REPOSITORY_LAYOUT.md](REPOSITORY_LAYOUT.md): where things live in the repo

If a workflow doc and [documents/plan.md](documents/plan.md) disagree, treat the code plus [documents/plan.md](documents/plan.md) as the source of truth.
