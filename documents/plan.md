# Active Plan

## Workstream

Keep the context/run-tag institutional workflow strict and explainable while we extend it to the next context without dragging settled YNAB history back into review.

## Current State

- canonical transaction artifacts are Parquet `transaction_v1`
- canonical review artifacts are Parquet `review_v4`
- `build-context-review` now excludes already reconciled YNAB rows by default:
  - matched rows whose YNAB side is already `reconciled`
  - unmatched `target_only` YNAB rows already marked `reconciled`
- the review-build override is now explicit:
  - `pixi run build-context-review -- <context> <run_tag> --include-reconciled-ynab`
- [contexts/aikido/context.toml](../contexts/aikido/context.toml) now loads cleanly again; the misplaced `sources = []` under `[ynab]` is gone
- contexts with no declared `[[sources]]` now fail fast on normalization and review-build instead of silently no-oping

## Recently Completed

- review-build policy/spec/code now agree that settled YNAB history should stay out of fresh review artifacts unless explicitly re-included:
  - [scripts/build_proposed_transactions.py](../scripts/build_proposed_transactions.py) now drops `matched_cleared` and `target_only_cleared` rows by default
  - [scripts/build_context_review.py](../scripts/build_context_review.py) now exposes `--include-reconciled-ynab`
  - [documents/context_workflow_spec.md](../documents/context_workflow_spec.md) and [documents/upload_reconcile_cutover_spec.md](../documents/upload_reconcile_cutover_spec.md) now document that default
- context resolution now fails honestly when a context has no declared source inputs:
  - [src/ynab_il_importer/context_config.py](../src/ynab_il_importer/context_config.py)
  - focused coverage updated in [tests/test_context_config.py](../tests/test_context_config.py)
- focused validation passed:
  - `pixi run pytest tests/test_build_proposed_transactions.py tests/test_context_config.py -q`
  - result: `30 passed`
- real-world verification passed on Pilates:
  - `pixi run build-context-review -- pilates 2026_04_01`
  - result: `24` matched pairs and `283` proposal rows after reconciled YNAB history was excluded from review

## Current Aikido Status

- live Aikido YNAB is still fully reconciled:
  - `Personal In Leumi`: `1046` reconciled
  - `Meshulam`: `17` reconciled
  - no uncleared or cleared-but-unreconciled backlog
- `pixi run download-context-ynab -- aikido 2026_04_01` now succeeds and writes:
  - `data/derived/2026_04_01/aikido_ynab_api_norm.parquet`
- the active workflow is still blocked before review prep because Aikido has no declared active sources:
  - `pixi run normalize-context -- aikido 2026_04_01`
  - `pixi run build-context-review -- aikido 2026_04_01`
  - both now fail clearly with `Context 'aikido' has no declared sources.`

## Next Steps

1. Decide what the active Aikido source should be on the current context/run-tag workflow:
   - declare real current raw source files under `contexts/aikido/context.toml`, or
   - explicitly keep Aikido outside the active institutional normalization/review path for now
2. Once Aikido has a declared source, rerun:
   - `pixi run normalize-context -- aikido <run_tag>`
   - `pixi run download-context-ynab -- aikido <run_tag>`
   - `pixi run build-context-review -- aikido <run_tag>`
3. Keep the reconciled-row exclusion as the default review policy unless we are doing explicit historical inspection.

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
