# Context Workflow Implementation Plan

## Goal

Move from profile/path defaults plus ad hoc script arguments to one explicit
context workflow system with:

- `contexts/defaults.toml`
- `contexts/<context>/context.toml`
- context-driven workflow scripts
- strict source resolution from declared files

## Guiding Constraints

1. keep modules in `src/` free of repo-path assumptions
2. keep scripts as the bridge between repo organization and module APIs
3. do not widen compatibility layers
4. make each slice small enough to test and review

## Slice 1: Introduce Context Config Loading

### Deliverables

- `contexts/defaults.toml`
- `contexts/<context>/context.toml` for existing contexts
- a typed config loader in `src/`
- strict source-file resolution from exact file names or unique regex matches

### Scope

- add config models and validation
- keep existing workflow code intact
- no broad script cleanup yet

### Tests

- config load
- source spec validation
- exact file resolution
- regex file resolution with:
  - unique match success
  - zero-match failure
  - multi-match failure

## Slice 2: Add A Context-Driven Normalize Entrypoint

### Deliverables

- a stable normalize runner script that accepts:
  - `context`
  - `run_tag`
- normalization uses context-declared sources only
- no "normalize entire directory and hope the right files are there" path

### Scope

- reuse existing normalization logic
- do not refactor all normalization internals yet
- start solving the `x0602` style selection mistake at the entrypoint

### Tests

- selected raw files map to expected normalize calls
- context-run path resolution writes to the right `derived/<run_tag>` location

## Slice 3: Add A Context-Driven Review Build Entrypoint

### Deliverables

- a stable build-review runner that accepts:
  - `context`
  - `run_tag`
- it resolves:
  - normalized source artifacts
  - YNAB artifact
  - payee map
  - output artifact names

### Scope

- use the new context config instead of free-form path bags
- keep builder internals mostly unchanged for this slice

### Tests

- context-run build path resolution
- declared normalized sources only
- output path naming is correct

## Slice 4: Migrate Account Bindings Into Context Config

### Deliverables

- account bindings move from `account_name_map.csv` into `context.toml`
- account mapping loader reads context config instead of CSV

### Scope

- keep fingerprint and payee rules in CSV
- remove `account_name_map.csv` from active workflow paths

### Risks

- several normalizers and reconciliation helpers currently assume CSV account maps
- this slice should be isolated and well tested

### Tests

- account binding application for bank/card rows
- context-specific account resolution

## Slice 5: Replace Workflow Profile Usage In Active Scripts

### Deliverables

- active workflow scripts use context config instead of `workflow_profiles.py`
- `pixi` tasks expose the stable entrypoints

### Scope

- normalize
- build review
- reconcile review
- review app
- upload prep

### Tests

- focused smoke tests for the entrypoints

## Slice 6: Script Cleanup And Reorganization

### Deliverables

- top-level `scripts/` contains only stable entrypoints
- one-off and diagnostic scripts move into:
  - `scripts/diagnostics/`
  - `scripts/bootstrap/`
  - `scripts/archive/`

### Scope

- rename for consistency
- remove stale scripts where clearly dead
- keep reviewable changes small

## Slice 7: Remove Legacy Paths

### Deliverables

- `mappings/` no longer drives active workflow behavior
- `workflow_profiles.py` is reduced or removed once no active script depends on it

### Scope

- remove compatibility branches only after the new path is proven

## Implementation Order

Recommended order:

1. Slice 1
2. Slice 2
3. Slice 3
4. validate April 2 workflow on the new path
5. Slice 4
6. Slice 5
7. Slice 6
8. Slice 7

## Immediate First Implementation Step

Implement Slice 1 and start Slice 2:

- add `contexts/defaults.toml`
- add `contexts/family/context.toml`, `contexts/pilates/context.toml`, `contexts/aikido/context.toml`
- add a typed loader and strict source resolver
- add a context-driven normalize entrypoint

That first step is enough to make the workflow safer immediately while staying small.
