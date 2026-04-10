# Repository Layout

## Core Code

- `src/ynab_il_importer/`
  Main application code.
- `src/ynab_il_importer/artifacts/`
  Canonical transaction and review artifact schemas plus Parquet IO.
- `src/ynab_il_importer/review_app/`
  Streamlit review app plus working-schema, validation, and state helpers.
- `scripts/`
  CLI entrypoints and operational helpers. The active workflow uses the context/run-tag scripts; many other scripts are diagnostics or older utilities.
- `tests/`
  Automated tests and test fixtures.

## Configuration

- `contexts/defaults.toml`
  Shared roots and filename templates.
- `contexts/<context>/context.toml`
  Context-specific source declarations, YNAB artifact names, and map locations.
- `mappings/`
  Versioned CSV rule tables still referenced by the active context configs.

## Documents

- `README.md`
  Top-level workflow summary.
- `documents/`
  Active project docs.
- `documents/decisions/`
  Durable architecture and model contracts.
- `documents/reference/`
  Small set of durable operational references.
- `documents/archive/`
  Historical notes, old plans, and superseded workflow material. Archive docs are not expected to match the current code.

## Local Working State

- `data/raw/<run_tag>/`
  Raw bank/card inputs for one run.
- `data/derived/<run_tag>/`
  Canonical normalized Parquet artifacts.
- `data/paired/<run_tag>/`
  Review artifacts, reviewed artifacts, upload artifacts, and reconcile reports.
- `data/raw/previous_max/<account_suffix>/`
  Raw previous MAX statements used for transition reconciliation.
- `data/derived/previous_max/<account_suffix>/`
  Canonical normalized previous MAX Parquet artifacts.
- `data/packets/`
  Saved reconciliation packets.
- `outputs/`
  Category caches, review-app sessions, and other generated support files.
- `tmp/`, `tests_runtime/`
  Disposable scratch space.

## Retention Rules

- Keep durable guidance in `documents/`, not at the repo root.
- Keep historical notes under `documents/archive/`, not mixed with active docs.
- Keep committed source-of-truth rule tables in `mappings/`.
- Keep run-specific artifacts under `data/`, grouped by run tag or packet.
- Do not leave disposable exports or scratch files at the repository root.
