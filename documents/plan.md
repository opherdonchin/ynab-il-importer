# Active Plan

## Workstream

Keep the context/run-tag workflow strict while finishing the missing closeout path for `ynab_category` sources, so Aikido-style contexts can complete review, upload, and reconciliation entirely on the active institutional workflow.

## Current State

- canonical transaction artifacts are Parquet `transaction_v1`
- canonical review artifacts are Parquet `review_v4`
- `build-context-review` excludes settled YNAB history by default:
  - exact matched rows whose YNAB side is already `cleared` or `reconciled`
  - target-only rows whose YNAB side is already `cleared` or `reconciled`
  - reconciled transfer counterparts and reconciled ambiguous target candidates
- context sources now support both:
  - raw-backed `raw_file` / `raw_match` entries
  - derived `ynab_category` entries sourced from another context's normalized YNAB snapshot for the same run tag
- closeout now has three strict source-kind paths:
  - bank: sync plus statement reconciliation
  - card: sync plus cycle reconciliation
  - `ynab_category`: category/account parity reconciliation

## Recently Completed

- added first-class `ynab_category` source support:
  - [src/ynab_il_importer/context_config.py](../src/ynab_il_importer/context_config.py)
  - [scripts/normalize_context.py](../scripts/normalize_context.py)
  - [src/ynab_il_importer/ynab_category_source.py](../src/ynab_il_importer/ynab_category_source.py)
- wired Aikido to its real active source:
  - [contexts/aikido/context.toml](../contexts/aikido/context.toml)
  - source = Family YNAB category `Aikido`
  - target account = `Personal In Leumi`
- propagated category-source provenance into review rows and kept settled YNAB history out of fresh review:
  - [scripts/build_proposed_transactions.py](../scripts/build_proposed_transactions.py)
- added the missing `ynab_category` closeout path:
  - [src/ynab_il_importer/ynab_category_reconciliation.py](../src/ynab_il_importer/ynab_category_reconciliation.py)
  - [scripts/reconcile_category_account.py](../scripts/reconcile_category_account.py)
  - [pixi.toml](../pixi.toml)
- added focused validation for the new closeout planner:
  - [tests/test_ynab_category_reconciliation.py](../tests/test_ynab_category_reconciliation.py)
  - [tests/test_context_config.py](../tests/test_context_config.py)
- updated active docs:
  - [documents/context_workflow_spec.md](../documents/context_workflow_spec.md)
  - [documents/upload_reconcile_cutover_spec.md](../documents/upload_reconcile_cutover_spec.md)
  - [README.md](../README.md)
- focused validation passed:
  - `pixi run pytest tests/test_ynab_category_reconciliation.py tests/test_context_config.py -q`
  - result: `17 passed`

## Current Aikido Status

- `pixi run normalize-context -- aikido 2026_04_01` succeeds and writes:
  - `data/derived/2026_04_01/aikido_family_category_norm.parquet`
  - result: `41` normalized source rows from the Family `Aikido` category
- `pixi run build-context-review -- aikido 2026_04_01` succeeds and writes:
  - `data/paired/2026_04_01/aikido_matched_pairs.parquet`
  - `data/paired/2026_04_01/aikido_proposed_transactions.parquet`
- Aikido review has been completed for the single live row:
  - `2026-03-31`
  - payee `Tayo`
  - `decision_action = create_target`
  - `reviewed = TRUE`
- the new dry-run closeout command is now working against live Aikido data:
  - `pixi run reconcile-category-account -- aikido 2026_04_01`
  - current blocker is expected and explicit:
    - reviewed row resolves to `missing_uploaded_transaction_in_live_ynab`
    - source category balance `8351.98`
    - target account balance `9032.88`

## Next Steps

1. Upload the reviewed Aikido row:
   - `pixi run python scripts/prepare_ynab_upload.py aikido 2026_04_01 --ready-only --skip-missing-accounts`
   - if the dry run looks right:
   - `pixi run python scripts/prepare_ynab_upload.py aikido 2026_04_01 --ready-only --skip-missing-accounts --execute`
2. Run the new Aikido closeout step:
   - `pixi run reconcile-category-account -- aikido 2026_04_01`
   - and then:
   - `pixi run reconcile-category-account -- aikido 2026_04_01 --execute`
3. Keep `ynab_category` as the active pattern for contexts whose source of truth is another budget's YNAB category rather than a bank/card export.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
