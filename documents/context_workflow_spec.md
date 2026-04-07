# Context Workflow Specification

## Purpose

Define one explicit, context-driven workflow model for normalization, review building,
review recovery, and upload preparation.

The design goals are:

1. one source of truth for every context fact
2. deterministic file selection and artifact naming
3. clear separation between:
   - repo/workflow organization
   - context definition
   - business logic modules
4. no implicit "use everything in this folder" behavior for context-sensitive workflows

## Core Terms

- `context`
  - a named workflow identity such as `family`, `pilates`, or `aikido`
- `run_tag`
  - a dated or otherwise stable folder name under raw/derived/paired, such as `2026_04_01`
- `context config`
  - the non-tabular facts that define a context
- `rule tables`
  - CSV data that is genuinely tabular and should stay tabular

## Directory Model

Target layout:

```text
contexts/
  defaults.toml
  family/
    context.toml
    fingerprint_map.csv
    payee_map.csv
  pilates/
    context.toml
    fingerprint_map.csv
    payee_map.csv
  aikido/
    context.toml
    fingerprint_map.csv
    payee_map.csv

data/
  raw/<run_tag>/
  derived/<run_tag>/
  paired/<run_tag>/
```

Notes:

- `contexts/` replaces the current overloaded role of `mappings/`.
- `data/raw/<run_tag>/`, `data/derived/<run_tag>/`, and `data/paired/<run_tag>/` remain
  the stable artifact roots.
- `tmp/` remains disposable and outside the canonical workflow.

## Source Of Truth

Each context fact should live in exactly one place.

### 1. Context Identity

Lives in:
- `contexts/<context>/context.toml`

Includes:
- context name
- budget environment variable name
- exact source declarations
- account bindings
- any context-specific non-tabular workflow facts

### 2. Account Bindings

End-state source of truth:
- `contexts/<context>/context.toml`

Why:
- account bindings are structural context configuration, not fuzzy rule data
- they are small
- they affect source selection and account targeting directly

Example shape:

```toml
[[accounts]]
source_system = "bank"
source_account = "67833011333622"
ynab_account_name = "Bank Leumi"
ynab_account_id = "..."

[[accounts]]
source_system = "card"
source_account = "x0849"
ynab_account_name = "Bank Leumi"
ynab_account_id = "..."
```

### 3. Fingerprint Rules

Lives in:
- `contexts/<context>/fingerprint_map.csv`

Why:
- many rows
- naturally tabular
- already fits review/edit workflow

### 4. Payee/Category Rules

Lives in:
- `contexts/<context>/payee_map.csv`

Why:
- many rows
- naturally tabular
- needs spreadsheet-style editing

### 5. Repo-Wide Path And Naming Conventions

Lives in:
- `contexts/defaults.toml`

Includes only workflow-wide conventions such as:
- root directories
- standard artifact filename templates
- standard output filenames like fingerprint logs or categories snapshots

This is repo organization, not context identity.

### 6. Budget IDs

Committed source of truth:
- `contexts/<context>/context.toml` stores the environment variable key to read

Local secret value source of truth:
- environment variables and/or the existing local uncommitted config

Committed config should not store live budget ids.

## Context Config Model

Each `context.toml` should be intentionally small.

Target shape:

```toml
name = "family"
budget_id_env = "YNAB_FAMILY_BUDGET_ID"

[[sources]]
id = "family_bank"
kind = "leumi"
raw_file = "Bankin family.dat"
normalized_name = "family_leumi_norm.parquet"

[[sources]]
id = "family_card"
kind = "max"
raw_match = "^transaction-details_export_\\d+\\.xlsx$"
normalized_name = "family_max_norm.parquet"

[[sources]]
id = "family_ynab"
kind = "ynab_api"
normalized_name = "family_ynab_api_norm.parquet"

[[accounts]]
source_system = "bank"
source_account = "67833011333622"
ynab_account_name = "Bank Leumi"
ynab_account_id = "..."
```

Important source declaration rules:

- do not use include/exclude globbing as the main model
- each source must be declared explicitly
- a source may use either:
  - `raw_file`
  - or `raw_match`
- `raw_match` must resolve to exactly one file inside `data/raw/<run_tag>/`
- `0` matches or `>1` matches are hard failures

This supports timestamped downloads without requiring manual renaming while remaining strict.

## Defaults Config Model

`contexts/defaults.toml` should stay small and boring.

Example:

```toml
raw_root = "data/raw"
derived_root = "data/derived"
paired_root = "data/paired"
outputs_root = "outputs"

[files]
fingerprint_log = "fingerprint_log.csv"
categories = "ynab_categories.csv"
proposed_review = "{context}_proposed_transactions.parquet"
reviewed_review = "{context}_proposed_transactions_reviewed.parquet"
```

## Script And Module Responsibilities

### Modules Under `src/`

Modules should know:
- how to normalize one file
- how to build review artifacts
- how to reconcile review artifacts
- how to project review artifacts into working frames

Modules should not know:
- where the repo stores raw/derived/paired folders
- how a context chooses its source files
- how run tags map to directories

### Scripts

Scripts are the bridge between repo organization and module APIs.

Scripts should:
- read `defaults.toml`
- read `context.toml`
- resolve `run_tag` to real directories
- resolve declared source files under `data/raw/<run_tag>/`
- pass explicit parameters into module functions

Scripts should not:
- contain business logic that belongs in `src/`
- guess context membership from "all files in a directory"

## Human-Facing Workflow

Target usage pattern:

```bash
pixi run context:normalize -- family 2026_04_01
pixi run context:build-review -- family 2026_04_01
pixi run context:reconcile-review -- family 2026_04_01
pixi run context:review-app -- family 2026_04_01
```

The human supplies:
- context
- run tag

Everything else is resolved through:
- `contexts/defaults.toml`
- `contexts/<context>/context.toml`

## Script Organization

Target organization:

```text
scripts/
  normalize_context.py
  build_context_review.py
  reconcile_context_review.py
  review_context_app.py
  prepare_context_upload.py
  diagnostics/
  bootstrap/
  archive/
```

Top-level scripts are stable workflow entrypoints only.

## Migration Constraints

The migration should be staged.

Near-term coexistence is acceptable for:
- old `workflow_profiles.py`
- old `mappings/` paths
- legacy scripts

But the target state is:
- `contexts/` is the active configuration root
- context-driven scripts are the official workflow entrypoints
- ad hoc folder sweeps are removed from context-sensitive workflows
