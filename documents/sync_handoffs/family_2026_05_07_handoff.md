# Family 2026-05-07 Handoff

Created on 2026-05-07 at the office machine before switching machines.

## What Happened

- Normalized the new `family` raw inputs for run tag `2026_05_07`.
- Downloaded a source-windowed YNAB snapshot for `family`.
- Built the family review proposal.
- Fixed Streamlit startup on fresh machines by disabling first-run onboarding prompts in the launcher.
- Fixed payee-map amount matching so bare numeric `amount_bucket` values like `600` behave as exact amount matches.
- Rebuilt the proposal so the `600` ILS `הפקדות קופג-י` Bank Leumi row is staged as:
  - payee: `Transfer : Unplanned Opher`
  - category: `None`

## Artifacts In The ZIP

The companion ZIP should be beside this file:

```text
documents/sync_handoffs/family_2026_05_07_handoff.zip
```

It contains:

- this handoff note
- the changed code/docs/tests
- `data/derived/2026_05_07/family_leumi_norm.parquet`
- `data/derived/2026_05_07/family_max_norm.parquet`
- `data/derived/2026_05_07/family_ynab_api_norm.parquet`
- `data/paired/2026_05_07/family_matched_pairs.parquet`
- `data/paired/2026_05_07/family_proposed_transactions.parquet`

It intentionally does not contain `.env`, `config/ynab.local.toml`, or other secret files.

## Current State

Run status after rebuilding:

```text
normalized bank rows: 60
normalized card rows: 160
YNAB snapshot rows: 893
matched pairs: 156
proposal rows: 55
reviewed artifact: not created yet
upload artifacts: not created yet
```

The review app was relaunched on the office machine at:

```text
http://localhost:8502
```

At home, after pulling the synced branch, restore or keep local YNAB secrets separately, then continue with:

```bash
pixi run review-context -- family 2026_05_07
```

If the generated `data/` artifacts are missing on the home checkout because `data/` is ignored, unpack the companion ZIP from the repo root.

## Checks Run

```text
pixi run pytest tests/test_rules.py -q
pixi run pytest tests/test_review_app_wrapper.py -q
pixi run pytest tests/test_context_config.py tests/test_download_ynab_api_script.py tests/test_ynab_api.py -q
pixi run ruff check scripts/download_ynab_api.py src/ynab_il_importer/context_config.py src/ynab_il_importer/ynab_api.py tests/test_context_config.py tests/test_download_ynab_api_script.py tests/test_ynab_api.py
pixi run ruff check scripts/review_app.py tests/test_review_app_wrapper.py
pixi run ruff check src/ynab_il_importer/rules.py tests/test_rules.py
```
