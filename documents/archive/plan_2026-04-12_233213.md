# Active Plan

## Workstream

Keep the context/run-tag workflow strict while extending Aikido onto the active institutional path through a first-class derived source, without dragging settled YNAB history back into review.

## Current State

- canonical transaction artifacts are Parquet `transaction_v1`
- canonical review artifacts are Parquet `review_v4`
- `build-context-review` excludes settled YNAB history by default:
  - exact matched rows whose YNAB side is already `cleared` or `reconciled`
  - target-only rows whose YNAB side is already `cleared` or `reconciled`
  - reconciled transfer counterparts and reconciled ambiguous target candidates
- the review-build override remains explicit:
  - `pixi run build-context-review -- <context> <run_tag> --include-reconciled-ynab`
- context sources now support both:
  - raw-backed `raw_file` / `raw_match` entries
  - derived `ynab_category` entries sourced from another context's normalized YNAB snapshot for the same run tag

## Recently Completed

- added first-class `ynab_category` source support:
  - [src/ynab_il_importer/context_config.py](../src/ynab_il_importer/context_config.py)
  - [scripts/normalize_context.py](../scripts/normalize_context.py)
  - [src/ynab_il_importer/ynab_category_source.py](../src/ynab_il_importer/ynab_category_source.py)
- wired Aikido to its real active source:
  - [contexts/aikido/context.toml](../contexts/aikido/context.toml)
  - source = Family YNAB category `Aikido`
  - target account = `Personal In Leumi`
- propagated category-source provenance into review rows:
  - [scripts/build_proposed_transactions.py](../scripts/build_proposed_transactions.py)
  - source rows now carry `ynab_parent_category_match` / `ynab_split_category_match` context instead of pretending to be direct raw imports
- tightened reconciled-row exclusion so settled Aikido history stays out of fresh review even when it appears as:
  - ambiguous matched candidates
  - target-only transfer counterparts
- updated active docs:
  - [documents/context_workflow_spec.md](../documents/context_workflow_spec.md)
  - [documents/upload_reconcile_cutover_spec.md](../documents/upload_reconcile_cutover_spec.md)
  - [README.md](../README.md)
- focused validation passed:
  - `pixi run pytest tests/test_context_config.py tests/test_build_proposed_transactions.py tests/test_ynab_category_source.py tests/test_ynab_api.py -q`
  - result: `47 passed`

## Current Aikido Status

- live Aikido YNAB remains fully reconciled:
  - `Personal In Leumi`: `1046` reconciled
  - `Meshulam`: `17` reconciled
- `pixi run normalize-context -- aikido 2026_04_01` now succeeds and writes:
  - `data/derived/2026_04_01/aikido_family_category_norm.parquet`
  - result: `41` normalized source rows from the Family `Aikido` category
- `pixi run build-context-review -- aikido 2026_04_01` now succeeds and writes:
  - `data/paired/2026_04_01/aikido_matched_pairs.parquet`
  - `data/paired/2026_04_01/aikido_proposed_transactions.parquet`
- fresh Aikido review is now reduced to one live row:
  - `2026-03-31`
  - account `Personal In Leumi`
  - payee `Tayo`
  - `match_status = source_only`
  - `decision_action = create_target`

## Next Steps

1. Open the Aikido review app for `2026_04_01` and confirm the single proposed row is correct:
   - `pixi run review-context -- aikido 2026_04_01`
2. If review is clean, continue with the normal upload/sync closeout for Aikido.
3. Keep `ynab_category` as the pattern for contexts whose only active source is another budget's YNAB category history.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
