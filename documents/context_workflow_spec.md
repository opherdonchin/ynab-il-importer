# Context Workflow Specification

## Purpose

Define the current context/run-tag workflow model for normalization, YNAB snapshot download, review building, review app launch, and source-specific closeout.

The active goals are:

1. one explicit source of truth for context-specific paths and names
2. deterministic source-file selection
3. canonical Parquet artifacts at workflow boundaries
4. repo-path logic in scripts, not in core modules

## Core Terms

- `context`
  Named workflow identity such as `family`, `pilates`, or `aikido`.
- `run_tag`
  Stable directory name under `data/raw`, `data/derived`, and `data/paired`, such as `2026_04_01`.
- `defaults`
  Repo-wide path roots and filename templates from [contexts/defaults.toml](../contexts/defaults.toml).
- `context config`
  Context-specific non-secret configuration from `contexts/<context>/context.toml`.

## Current Directory Model

```text
contexts/
  defaults.toml
  family/context.toml
  pilates/context.toml
  aikido/context.toml

data/
  raw/<run_tag>/
  derived/<run_tag>/
  paired/<run_tag>/
  raw/previous_max/<account_suffix>/
  derived/previous_max/<account_suffix>/
  raw/previous_leumi_card/<account_suffix>/
  derived/previous_leumi_card/<account_suffix>/
```

The active context configs still reference shared CSV rule tables under [mappings/](../mappings/). That is intentional for now; `contexts/` owns non-tabular config and artifact naming, not every editable table in the repo.

## What Lives In Each Config

### `contexts/defaults.toml`

Owns repo-wide path and naming conventions:

- artifact roots
- default review artifact names
- upload/report filenames

### `contexts/<context>/context.toml`

Owns context-specific facts:

- `name`
- `budget_id_env`
- `[maps]` paths for account, fingerprint, and payee maps
- `[ynab]` normalized YNAB artifact name
- `[[sources]]` declarations

Current source declarations support:

- `id`
- `kind`
- for raw-backed sources:
  - exactly one of `raw_file` or `raw_match`
  - `target_account_names`
- for `ynab_category` sources:
  - `from_context`
  - exactly one of `category_name` or `category_id`
  - `target_account_name`
  - optional `target_account_id`
- `normalized_name`
- `allow_reconciled_source` for card reconciliation edge cases

Contexts that participate in `normalize-context` or `build-context-review` must declare at least one `[[sources]]` entry. A context with no declared sources is not runnable on the active normalization/review path.
Raw-backed sources must also declare explicit `target_account_names` so review matching stays within the YNAB accounts actually covered by the provided source files.

## Source Resolution Rules

Source resolution is intentionally strict:

- `raw_file` must exist exactly at `data/raw/<run_tag>/<raw_file>`
- `raw_match` must match exactly one file inside `data/raw/<run_tag>/`
- `ynab_category` sources resolve from the sibling context's normalized YNAB snapshot in `data/derived/<run_tag>/`
- zero matches or multiple matches are hard failures

This prevents the old "scan the folder and hope the right file wins" behavior.

## Budget Resolution

Budget ids are resolved in this order:

1. explicit `--budget-id`
2. the context's `budget_id_env`
3. [config/ynab.local.toml](../config/ynab.local.toml), if present

Committed config stores only the environment-variable name, not the live budget id.

## Active Entry Points

### Normalize raw source files

```bash
pixi run normalize-context -- <context> <run_tag>
```

Resolves declared sources from `data/raw/<run_tag>/` and writes canonical transaction Parquet to `data/derived/<run_tag>/`.

For `ynab_category` sources, normalization is still run-tag based, but the input comes from another context's normalized YNAB snapshot for the same run tag rather than a raw bank/card file.

### Download YNAB snapshot

```bash
pixi run download-context-ynab -- <context> <run_tag>
```

Writes the declared normalized YNAB artifact to `data/derived/<run_tag>/`.

### Build review artifact

```bash
pixi run build-context-review -- <context> <run_tag>
pixi run build-context-review -- <context> <run_tag> --include-reconciled-ynab
```

Loads the declared normalized source artifacts plus the normalized YNAB artifact and writes the canonical review artifact to `data/paired/<run_tag>/`.

Default behavior is intentionally conservative:

- YNAB rows already marked `cleared` or `reconciled` are excluded from the review artifact
- YNAB target rows are limited to the explicit target-account scope declared by the context's active sources
- this applies both to exact matched rows and to unmatched `target_only` YNAB rows, including transfer counterparts and ambiguous candidate rows whose YNAB side is already settled
- use `--include-reconciled-ynab` only when you explicitly want settled YNAB history back in review

This means a context like `family` will only review YNAB rows from accounts declared by its active bank/card sources, rather than every account in the Family budget.

### Post-review closeout

Closeout is source-kind specific:

- bank sources:
  - `pixi run sync-bank-matches -- <context> <run_tag>`
  - `pixi run reconcile-bank-statement -- <context> <run_tag>`
- card sources:
  - `pixi run sync-card-matches -- <context> <run_tag> --account "<Card Account Name>"`
  - `pixi run normalize-previous-max -- <context> <account_suffix> --cycle YYYY_MM`
  - `pixi run reconcile-card-cycle -- <context> <run_tag> --account "<Card Account Name>" --previous <normalized_previous.parquet>`
- `ynab_category` sources:
  - `pixi run reconcile-category-account -- <context> <run_tag>`

For `ynab_category` sources, the closeout step is not bank/card lineage sync. It verifies live parity between:

- the source budget category balance for the run month
- the target account balance
- the target account cleared balance

and then patches the resolved target-side rows to `cleared = reconciled`.

### Launch review app

```bash
pixi run review-context -- <context> <run_tag>
```

Resolves:

- the proposal review artifact
- the standard reviewed artifact path
- the profile name passed to the review app wrapper

Behavior:

- the proposal review artifact is always passed as the base input
- if the standard reviewed artifact already exists, the launcher automatically passes it as `--resume`
- if no reviewed artifact exists yet, the app starts from the proposal artifact only

Current review-state model inside the app:

- `Needs fix`
  The row is invalid or incomplete.
- `Needs decision`
  The row is valid, but `decision_action` is still `No decision`.
- `Needs review`
  The row is valid, has a decision, and still needs acceptance.
- `Settled`
  The row has been accepted.

Current fresh-build defaults are intentional:

- matched rows start with `decision_action = keep_match`, `reviewed = FALSE`
- source-only rows start with `decision_action = create_target`, `reviewed = FALSE`
- target-only rows start with `decision_action = No decision`, `reviewed = FALSE`
- already reconciled YNAB rows are omitted from fresh builds unless explicitly re-included with `--include-reconciled-ynab`

This means existing YNAB-only rows require an explicit decision in review; they do not auto-default to `ignore_row`.

Supported options on the launcher:

```bash
pixi run review-context -- <context> <run_tag> --resume
pixi run review-context -- <context> <run_tag> --resume path\to\reviewed.parquet
pixi run review-context -- <context> <run_tag> --foreground
pixi run review-context -- <context> <run_tag> --port 8502
```

- `--resume` with no path means “resume from the standard reviewed artifact path”
- `--resume <path>` overrides the default and resumes from that explicit reviewed artifact
- `--foreground` keeps the wrapper process attached instead of returning immediately
- `--port <n>` forwards a preferred Streamlit port

One current caveat remains: the review app still resolves its default category-cache path through [src/ynab_il_importer/workflow_profiles.py](../src/ynab_il_importer/workflow_profiles.py), not through `contexts/defaults.toml`.

## Responsibilities

### Code under `src/`

Should know:

- how to normalize one file type
- how to read and write canonical artifacts
- how to build review artifacts
- how to project review artifacts into working dataframes

Should not know:

- repo root paths
- run-tag directory layout
- context-specific file discovery rules

### Scripts

Should know:

- how to load defaults and context config
- how to resolve `run_tag` directories
- how to map context config onto explicit module calls

Should not know:

- business logic that belongs in `src/`
- ad hoc directory sweeps for context-sensitive workflows

## Current Non-Goals

These are not part of the active context workflow model today:

- reviving the old profile/path CSV workflow as the default path
- moving every mapping CSV into `contexts/`
- documenting legacy diagnostic scripts as first-class workflow entrypoints
