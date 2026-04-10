# Context Workflow Specification

## Purpose

Define the current context/run-tag workflow model for normalization, YNAB snapshot download, review building, and review app launch.

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
- exactly one of:
  - `raw_file`
  - `raw_match`
- `normalized_name`
- `allow_reconciled_source` for card reconciliation edge cases

## Source Resolution Rules

Source resolution is intentionally strict:

- `raw_file` must exist exactly at `data/raw/<run_tag>/<raw_file>`
- `raw_match` must match exactly one file inside `data/raw/<run_tag>/`
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

### Download YNAB snapshot

```bash
pixi run download-context-ynab -- <context> <run_tag>
```

Writes the declared normalized YNAB artifact to `data/derived/<run_tag>/`.

### Build review artifact

```bash
pixi run build-context-review -- <context> <run_tag>
```

Loads the declared normalized source artifacts plus the normalized YNAB artifact and writes the canonical review artifact to `data/paired/<run_tag>/`.

### Launch review app

```bash
pixi run review-context -- <context> <run_tag>
```

Resolves:

- the proposal review artifact
- the standard reviewed artifact path
- the profile name passed to the review app wrapper

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
