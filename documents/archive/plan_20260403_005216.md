# Active Plan

## Workstream

Implement Step 1 of split-transaction support on the `handle_splits` branch.

Current intent:
- establish canonical Parquet transaction artifacts with nested split support
- move transaction-processing boundaries toward Polars and PyArrow
- preserve current workflow behavior while the storage and loading layers change underneath
- keep the review app pandas-based internally for now, but let it accept flat data from Polars/Arrow at the boundary

## Current Goal

Finish Step 1: change the transaction representation and file-format plumbing without changing review, matching, or upload semantics.

That means:
1. canonical transaction-like artifacts become Parquet-backed
2. existing flat CSV workflows keep working through explicit projections or compatibility loaders
3. current split-blind logic stays split-blind where it is split-blind today

## Current Status

Done:
- read project context, prior plan state, and split-handling design notes
- created `handle_splits` branch from the prior working branch
- wrote the staged split implementation plan:
  - `documents/handle_splits_implementation_plan.md`
- updated dependency management in `pixi.toml` and `pixi.lock`:
  - added `polars`
  - added `pyarrow`
- added canonical transaction artifact foundations:
  - `src/ynab_il_importer/artifacts/transaction_schema.py`
  - `src/ynab_il_importer/artifacts/transaction_io.py`
  - `src/ynab_il_importer/artifacts/transaction_projection.py`
- added review-app boundary adapters:
  - `src/ynab_il_importer/review_app/io.py` now accepts CSV paths, pandas DataFrames, Polars DataFrames, and Arrow tables, then normalizes once to pandas
- added a first canonical YNAB producer:
  - `src/ynab_il_importer/ynab_api.py::transactions_to_canonical_table(...)`
- updated YNAB producer scripts to write canonical Parquet sidecars alongside the current CSV projections:
  - `scripts/download_ynab_api.py`
  - `scripts/io_ynab_as_source.py`
- added regression coverage for the new artifact and script behavior:
  - `tests/test_transaction_artifacts.py`
  - `tests/test_review_io.py`
  - `tests/test_ynab_api.py`
  - `tests/test_io_ynab_as_source.py`
  - `tests/test_download_ynab_api_script.py`

## Working Rules For This Phase

- Keep Step 1 behavior-preserving.
- Prefer compatibility boundaries over deep rewrites.
- Treat Parquet artifacts as authoritative for transaction-like data as new paths are migrated.
- Keep human-edited control files such as maps in CSV.
- Commit after each successful sub-step.
- Update `documents/plan.md` before each commit on this branch.

## Step 1 Remaining Slices

1. Migrate normalized source artifacts so normalization outputs also write canonical Parquet alongside current CSV projections.
2. Introduce centralized flat-transaction loaders so downstream builders can consume CSV or Parquet through one boundary.
3. Refactor `build_proposed_transactions.py` and related downstream scripts to use the centralized artifact loaders instead of direct authoritative CSV reads.
4. Extend the same treatment to cross-budget builder paths where practical in Step 1.
5. Run equivalence-focused verification on representative workflows before calling Step 1 complete.

## Risks To Watch

- accidental semantic drift in matching caused by loader or dtype changes
- writing canonical artifacts that do not preserve enough lineage for later split-aware phases
- widening scope by trying to make pairing or the review UI split-aware too early
- destabilizing the review app by moving more than the IO boundary in Step 1

## Next Step

Move normalized source artifact producers onto the same pattern as the YNAB producers:
- write canonical Parquet
- keep current CSV projection outputs
- preserve current normalized columns and downstream behavior
