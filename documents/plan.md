# Active Plan

## Workstream

Split-aware review on the `handle_splits` branch, with strict canonical Parquet boundaries and Polars-first working data.

Parallel workflow cleanup has now started:
- replace ad hoc profile/path/script wiring with explicit context configs
- make source selection deterministic per context + run tag
- keep repo organization logic in scripts, not in core modules
- cut upload/sync/reconcile over from stale pandas/profile wrappers to strict canonical Parquet + Polars-first boundaries

## Current Direction

- keep canonical transaction and review artifacts as Parquet, not CSV
- keep hierarchy only where it is semantically real:
  - transaction `splits`
  - persisted `review_v4` transaction structs
- keep ordinary app/upload logic on one centralized flat working dataframe
- keep that working dataframe Polars-first
- fail fast at artifact/load boundaries instead of repairing stale or malformed inputs in the app

## Current Status

Done:
- Step 4 split editing is complete:
  - modal split editor uses explicit per-line widgets
  - committed split edits round-trip through save/load/reconcile/upload prep
  - one-line split saves collapse back to a normal transaction
- normalized proposal-build inputs now require direct canonical parquet artifacts
- the builder now stays Arrow/Polars-first through:
  - canonical input load
  - source/target prep
  - institutional pairing
  - matched/unmatched review row assembly
  - review-target suggestion application
- the review-app working-schema boundary is now Polars-first:
  - [working_schema.py](src/ynab_il_importer/review_app/working_schema.py) owns construction/normalization of the flat working dataframe
  - split columns stay normalized as real split-record lists rather than generic object blobs
- the review-app IO boundary is now stricter:
  - [load_review_artifact](src/ynab_il_importer/review_app/io.py) is parquet-only
  - [project_review_artifact_to_working_dataframe](src/ynab_il_importer/review_app/io.py) is the explicit canonical-review-artifact -> Polars-working-dataframe projection
  - app/upload/builder callers now use that two-step boundary explicitly
- live working-dataframe consumers that should not round-trip through the persisted artifact boundary now normalize directly through [working_schema.py](src/ynab_il_importer/review_app/working_schema.py)
- tracing the April 1/2 pipeline from raw inputs through normalization and proposal build confirmed:
  - the canonical April 1 YNAB parquet already preserved real nested split lines
  - the old builder/proposal path was flattening those splits during review-row construction
  - the builder now preserves canonical target transaction structs, so split-bearing YNAB targets survive into the rebuilt proposal
  - [project_review_artifact_to_working_dataframe](src/ynab_il_importer/review_app/io.py) now builds the working frame with full-row schema inference so later split-bearing rows do not fail the projection step
- linked debit-card source overlap handling is now fixed in [build_proposed_transactions.py](scripts/build_proposed_transactions.py):
  - `_dedupe_source_overlaps(...)` now works on canonical `source_system` rows instead of the stale flat `source` column
  - bank/card linkage is derived from canonical fields:
    - bank `description_raw` card suffix
    - card `source_account`
  - linked bank/card duplicates now collapse before pairing, including:
    - two-way bank/card duplicates
    - four-way `2 bank + 2 card` duplicate clusters that should reduce to two real candidates
  - `--source-dir` now honors the strict parquet-only canonical boundary
- a fresh linked-source rebuild from the traced April 1 normalized Parquet inputs now produces:
  - `723` rows instead of the intermediate `796`
  - `74` ambiguous rows instead of `186`
  - no remaining four-way ambiguous target groups
- reconciling the saved reviewed artifact onto that corrected proposal now preserves most prior review work again:
  - `603` reviewed rows
  - `120` unresolved rows
  - split-affected rows reopened in `data/paired/2026_04_01/family_proposed_transactions_reviewed_split_fixup_linked.parquet`
- the remaining false BIT-style ambiguity bug is now fixed in [build_proposed_transactions.py](scripts/build_proposed_transactions.py):
  - `_prepare_review_source_rows(...)` now carries canonical source lineage from `import_id` / `transaction_id` instead of zeroing it out
  - `_source_lineage_id(...)` now falls back to canonical ids when the old `bank_txn_id` / `card_txn_id` helper columns are absent
  - exact-import candidate narrowing now works again for canonical source rows, so false `2 x 4` March BIT clusters collapse to their exact imported bank matches
- Family maps and row-view category controls have now been tightened:
  - [mappings/fingerprint_map.csv](mappings/fingerprint_map.csv) now canonicalizes `סטימצקי` variants to `steimatzky`
  - [mappings/payee_map.csv](mappings/payee_map.csv) now has exact high-priority defaults for:
    - `Steimatzky -> Steimatzky / House and stuff`
    - `Tzomet Sfarim -> Tzomet Sfarim / House and stuff`
  - the March 12 `89.9` `סטימצקי גרנד` bank/card duplicate now collapses correctly during Family rebuild
  - row-view `Show all` category toggling in [app.py](src/ynab_il_importer/review_app/app.py) no longer sits inside a form, so the category list rerenders immediately instead of staying stuck on the filtered options
- rebuilding Family `2026_04_01` through the new context-driven flow now produces the stable shape we expected:
  - `680` proposal rows in `data/paired/2026_04_01/family_proposed_transactions.parquet`
  - the March 3 `200 ILS` BIT university rows reduce to the `2` exact imported bank matches
  - the March 10 `275 ILS` Reuven Mordechai row reduces to `1` exact imported bank match
  - reconciling `family_proposed_transactions_reviewed_split_fixup_linked.parquet` onto that proposal yields:
    - `647` reviewed rows
    - `33` unresolved rows
    - resume artifact `data/paired/2026_04_01/family_proposed_transactions_reviewed_context_fixed.parquet`
- target-state context workflow design is now documented in:
  - [context_workflow_spec.md](documents/context_workflow_spec.md)
  - [context_workflow_implementation_plan.md](documents/context_workflow_implementation_plan.md)
- Slice 1 of the context workflow refactor is now in place:
  - `contexts/defaults.toml`
  - `contexts/<context>/context.toml` for `family`, `pilates`, and `aikido`
  - typed config/source loading in [context_config.py](src/ynab_il_importer/context_config.py)
  - shared normalization runner in [normalize_runner.py](src/ynab_il_importer/normalize_runner.py)
  - context-driven normalize entrypoint in [normalize_context.py](scripts/normalize_context.py)
  - `pixi run normalize-context -- <context> <run_tag>`
- Slice 2 / early Slice 3 are now in place:
  - contexts now declare their normalized YNAB artifact names
  - run-tag path resolution and artifact-name resolution live in [context_config.py](src/ynab_il_importer/context_config.py)
  - [build_proposed_transactions.py](scripts/build_proposed_transactions.py) now exposes a narrow `run_build(...)` seam
  - context-driven review-build entrypoint in [build_context_review.py](scripts/build_context_review.py)
  - `pixi run build-context-review -- <context> <run_tag>`
- the context workflow is now tighter at the active boundaries:
  - [normalize_runner.py](src/ynab_il_importer/normalize_runner.py) now writes direct canonical parquet outputs only
  - [normalize_context.py](scripts/normalize_context.py) now writes the declared `.parquet` artifact names directly instead of routing through synthetic `.csv` paths
  - [download_ynab_api.py](scripts/download_ynab_api.py) is now a context/run-tag entrypoint that writes only the declared canonical YNAB parquet artifact
  - `pixi` now exposes:
    - `pixi run download-context-ynab -- <context> <run_tag>`
    - `pixi run review-context -- <context> <run_tag>`
  - [review_context.py](scripts/review_context.py) now closes the path from built proposal artifact to the review app without hand-assembled paths
- low-value pandas use has been reduced further in the review working boundary:
  - [working_schema.py](src/ynab_il_importer/review_app/working_schema.py) no longer uses pandas scalar coercion helpers for working-frame normalization
  - [review_app/io.py](src/ynab_il_importer/review_app/io.py) now uses direct scalar normalization for text/float cleanup instead of pandas scalar coercion
- another focused pandas cleanup pass is now in place:
  - [build_proposed_transactions.py](scripts/build_proposed_transactions.py) now keeps target suggestion rule application behind one narrow pandas adapter instead of converting the caller in and out ad hoc
  - [transaction_io.py](src/ynab_il_importer/artifacts/transaction_io.py) now canonicalizes flat transaction projections in Polars and writes flat CSV exports from Polars instead of converting whole frames to pandas first
  - [upload_prep.py](src/ynab_il_importer/upload_prep.py) no longer uses pandas series coercion for single-value amount parsing
- the focused builder/review/app/upload test slice is green after the refactor
- a further pandas cleanup pass is now committed (`b1fa757`):
  - dead `_dedupe_sources` function and its helpers (`_candidate_import_ids`, `_protect_from_weak_dedupe`, `_account_key_candidates`) removed from [build_proposed_transactions.py](scripts/build_proposed_transactions.py)
  - `pd.Series(row)` wrappers removed from 3 `map_elements` lambdas in `build_review_rows` (helper functions already accept `Mapping`)
  - `pd.isna` replaced with `isinstance(value, float) and math.isnan(value)` in `_optional_text` and `_to_string_set`
  - `build_review_rows` now returns `tuple[pl.DataFrame, pl.DataFrame]` instead of converting to pandas; caller `run_build` converts to pandas only at the CSV write boundary
  - `import ynab_il_importer.pairing as pairing` removed (was only used by dead `_dedupe_sources`)
  - 6 dead `_dedupe_sources` tests removed from [tests/test_build_proposed_transactions.py](tests/test_build_proposed_transactions.py)
- a further focused pandas cleanup pass is now committed (`ba4b822`):
  - `pairs_out` in [build_proposed_transactions.py](scripts/build_proposed_transactions.py) now writes a native Polars Parquet file instead of converting to pandas and writing CSV; `.to_pandas()` call at the pairs boundary removed
  - CSV fallback in `run_build` removed — artifact output is always `save_review_artifact` (Parquet)
  - `matched_pairs` filename template in [contexts/defaults.toml](contexts/defaults.toml) and the `DefaultsConfig` default in [context_config.py](src/ynab_il_importer/context_config.py) updated to `.parquet`
  - `cli.py` now loads matched-pairs files via a dedicated `_load_pairs_paths` that reads `.parquet` with `pd.read_parquet` and falls back to CSV — no longer routes through transaction-schema normalization
  - `pd.NA` scalar guards removed from all five scalar normalization helpers in [review_app/io.py](src/ynab_il_importer/review_app/io.py) (`_normalize_text`, `_normalize_float`, `_normalize_split_records`, `_normalize_transaction_record`, `_preferred_summary_number`); replaced with `value is None` or `isinstance(value, float) and math.isnan(value)` where float-NaN coverage is needed

  - [upload_reconcile_cutover_spec.md](documents/upload_reconcile_cutover_spec.md)
  - [upload_reconcile_cutover_plan.md](documents/upload_reconcile_cutover_plan.md)
- Slice 1 of the upload/reconcile cutover is now in place:
  - [scripts/prepare_ynab_upload.py](scripts/prepare_ynab_upload.py) is now the active context/run-tag upload entrypoint instead of a profile/path wrapper
  - it now loads the reviewed artifact through the explicit canonical-review-artifact -> working-dataframe boundary instead of converting the artifact to pandas first
  - [src/ynab_il_importer/upload_prep.py](src/ynab_il_importer/upload_prep.py) now has a stricter public upload boundary:
    - [load_upload_working_frame](src/ynab_il_importer/upload_prep.py) is the explicit artifact -> working-frame step
    - readiness/account filtering and upload preparation now operate on one Polars working dataframe instead of mixed artifact/pandas/object inputs
    - upload prep now prefers persisted source lineage from the working frame / reviewed artifact instead of falling back to synthetic `src_*` ids
  - [tests/test_upload_prep.py](tests/test_upload_prep.py) now exercises the stricter working-frame boundary directly
- Family upload dry run now works again from the canonical reviewed artifact:
  - input: `data/paired/2026_04_01/family_proposed_transactions_reviewed_current.parquet`
  - outputs:
    - `data/paired/2026_04_01/family_upload.csv`
    - `data/paired/2026_04_01/family_upload.json`
  - dry-run result:
    - `53` prepared rows
    - `2` transfer rows
    - `0` split rows
    - `0` duplicate payload keys
    - `0` existing import-id hits
    - `2` possible manual matches
    - `0` transfer payload issues
- bank reconciliation is now on the new strict path:
  - [src/ynab_il_importer/artifacts/transaction_schema.py](src/ynab_il_importer/artifacts/transaction_schema.py) now preserves canonical `balance_ils`
  - [src/ynab_il_importer/artifacts/transaction_io.py](src/ynab_il_importer/artifacts/transaction_io.py) now preserves:
    - mapped `account_id` from `ynab_account_id`
    - canonical `balance_ils`
  - [src/ynab_il_importer/bank_reconciliation.py](src/ynab_il_importer/bank_reconciliation.py) now reads canonical bank parquet via Polars and prepares bank rows from schema-guaranteed canonical fields instead of CSV-shaped input
  - the active bank scripts are now strict context/run-tag entrypoints instead of profile/path wrappers:
    - [sync_bank_matches.py](scripts/sync_bank_matches.py)
    - [reconcile_bank_statement.py](scripts/reconcile_bank_statement.py)
  - [src/ynab_il_importer/context_config.py](src/ynab_il_importer/context_config.py) now resolves budget ids from:
    - explicit `--budget-id`
    - declared env var in `contexts/<context>/context.toml`
    - `config/ynab.local.toml`
  - `pixi` tasks now expose:
    - `pixi run sync-bank-matches -- <context> <run_tag>`
    - `pixi run reconcile-bank-statement -- <context> <run_tag>`
- Family bank dry runs on the new path now behave as expected:
  - `pixi run sync-bank-matches -- family 2026_04_01`
    - `132` source rows
    - `100` matched
    - `4` updates planned
    - `32` unmatched
    - `6` uncleared YNAB triage rows
  - `pixi run reconcile-bank-statement -- family 2026_04_01`
    - canonical bank statement loaded correctly
    - dry run blocks at row `96` for a real unresolved lineage gap, not a broken CSV/pandas boundary
    - current reason: upload/sync state in live YNAB is not yet fully up to date for post-anchor rows
- card reconciliation is now on the new strict path too:
  - [src/ynab_il_importer/card_reconciliation.py](src/ynab_il_importer/card_reconciliation.py) now loads canonical normalized card parquet instead of raw `.xlsx` / `.html` / `.csv`
  - current/previous card-cycle planning now starts from schema-guaranteed canonical fields, with preserved card lineage in `transaction_id`
  - dead raw-loader helpers for pending-row filtering and inline `card_txn_id` construction have been removed from the active reconciliation path
  - the active card scripts are now strict context/run-tag entrypoints:
    - [sync_card_matches.py](scripts/sync_card_matches.py)
    - [reconcile_card_cycle.py](scripts/reconcile_card_cycle.py)
  - previous-cycle MAX snapshots are now normalized explicitly before reconciliation through:
    - [normalize_previous_max.py](scripts/normalize_previous_max.py)
    - `pixi run normalize-previous-max -- <context> <account_suffix> [--cycle YYYY_MM]`
  - `pixi` tasks now expose:
    - `pixi run normalize-previous-max -- <context> <account_suffix> [--cycle YYYY_MM]`
    - `pixi run sync-card-matches -- <context> <run_tag> --account "<account>"`
    - `pixi run reconcile-card-cycle -- <context> <run_tag> --account "<account>" --previous <normalized_previous.parquet>`
- Family x9922 card dry runs on the new path now behave as expected:
  - `pixi run normalize-previous-max -- family x9922 --cycle 2026_03`
    - wrote `data/derived/previous_max/x9922/2026_03_max_norm.parquet`
  - `pixi run sync-card-matches -- family 2026_04_01 --account "Opher x9922"`
    - `30` source rows
    - `23` matched
    - `0` updates planned
    - `7` unmatched
  - `pixi run reconcile-card-cycle -- family 2026_04_01 --account "Opher x9922" --previous data/derived/previous_max/x9922/2026_03_max_norm.parquet`
    - previous rows loaded from canonical parquet
    - current/source mismatch now blocks for a real live-state reason:
      - `7` current rows still have no exact match in YNAB
    - no raw-file or pandas-boundary failure remains in the active path
- the last Family upload/sync boundary bug is now fixed:
  - source-side institutional lineage now survives the builder -> review artifact -> working dataframe path:
    - [build_proposed_transactions.py](scripts/build_proposed_transactions.py) now persists bank/card lineage into source-side canonical `import_id`
    - [project_review_artifact_to_working_dataframe](src/ynab_il_importer/review_app/io.py) now reconstructs `source_bank_txn_id` / `source_card_txn_id`
    - [working_schema.py](src/ynab_il_importer/review_app/working_schema.py) now keeps those lineage columns in the working frame
    - [upload_prep.py](src/ynab_il_importer/upload_prep.py) now uses those lineage columns to assign upload `import_id`
  - [review_reconcile.py](src/ynab_il_importer/review_reconcile.py) now preserves edited transaction payloads only when `changed = TRUE`; unchanged rows now take rebuilt proposal current/original transactions instead of stale pre-fix payloads
  - bank sync now has the same deterministic legacy-import-id recovery that card sync already had:
    - [bank_reconciliation.py](src/ynab_il_importer/bank_reconciliation.py) computes `legacy_import_id` for canonical bank rows
    - exact bank sync can now recover transactions that were uploaded earlier under fallback `YNAB:...` import ids and then stamp real `bank_txn_id` memo markers
- a fresh Family rebuild + rebase now shows the fixed upload shape:
  - `data/paired/2026_04_01/family_proposed_transactions.parquet` remains at `679` rows
  - `data/paired/2026_04_01/family_proposed_transactions_reviewed_current_rebased.parquet` preserves the current review state while taking fresh source/target payloads from the rebuilt proposal
  - a fresh upload dry run from that rebased artifact produces canonical bank/card lineage import ids for source-only upload rows
- one genuine Family review-state problem remains:
  - there is still exactly one reviewed `source_only + keep_match` row on `Opher x9922` for `2026-03-29` / `735.0`
  - the repo has an explicit map rule for that fingerprint (`paypal facebook עסקת חו` on `Opher x9922` -> `Facebook / Aikido`)
  - the rebuilt proposal still leaves that row undecided, so it needs a final deterministic data repair or manual review before x9922 card reconciliation can fully close

## Immediate Goal

Finish the Family closeout path on the new architecture:

1. verify the refreshed Family upload dry run from the rebased reviewed artifact
2. finish bank sync/reconcile with the new legacy-import recovery
3. repair the one remaining x9922 reviewed `source_only + keep_match` row
4. verify card sync/reconcile with explicitly normalized `previous_max` inputs

Then move on cleanly to the Pilates and Aikido workflows.

The important recovery problems are now addressed:
1. split-bearing YNAB targets survive the rebuild
2. linked debit-card bank/card duplicates collapse before pairing
3. canonical source lineage survives into institutional pairing, so exact-import narrowing works again
4. the rebuilt proposal is materially back near the original reviewed state
5. obvious linked-source duplicates such as the March `סטימצקי גרנד` debit/card pair now collapse again
6. the remaining unresolved rows should now be a real review queue, not a builder regression

## Next Steps

1. Commit the upload/reconcile cutover slice now that lineage survives rebuild -> rebase -> upload-prep again.
2. Re-run `pixi run sync-bank-matches -- family 2026_04_01 --execute` and verify the two previously uploaded fallback-import bank rows are recovered and stamped.
3. Re-run `pixi run reconcile-bank-statement -- family 2026_04_01` and confirm only true recent/unmatched bank rows remain.
4. Repair the one x9922 `source_only + keep_match` row using the explicit `Facebook / Aikido` mapping (or reopen it honestly if that repair cannot be made deterministically).
5. Re-run x9922 card upload/sync/reconcile, then confirm x5898 and x7195 remain clean on the new path.
6. Decide whether [reconcile_card_payment_transfers.py](scripts/reconcile_card_payment_transfers.py) should be converted to the new context/canonical path or removed in favor of the main card-cycle reconciliation flow.
7. Once Family closeout is stable end to end, move on to Pilates and then Aikido.
8. Finish the remaining active-path cleanup before Pilates normalization:
   - remove or quarantine the remaining legacy CSV translation helpers in [review_app/io.py](src/ynab_il_importer/review_app/io.py)
   - decide whether [download_ynab_api.py](scripts/download_ynab_api.py) should become [download_context_ynab.py](scripts/download_context_ynab.py) for naming consistency
   - either convert or isolate the remaining real pandas islands in:
     - [upload_prep.py](src/ynab_il_importer/upload_prep.py)
     - ~~[build_proposed_transactions.py](scripts/build_proposed_transactions.py) row-level rule application and legacy `_dedupe_sources(...)`~~ — done: dead code removed; remaining pandas is the intentional `_build_target_suggestions_pandas` adapter
     - ~~[transaction_io.py](src/ynab_il_importer/artifacts/transaction_io.py) legacy flat projection loaders~~ — kept: serves real input-layer callers (parsers, `save_review_artifact`); explicit multi-type boundary, not defensive code

## Validation / Test Baseline

Latest green slice:

```bash
pixi run pytest tests/test_build_proposed_transactions.py tests/test_review.py tests/test_review_io.py tests/test_review_reconcile.py tests/test_review_app.py tests/test_upload_prep.py -q
```

Result at last run: `167 passed`

Additional recovery trace slice:

```bash
pixi run pytest tests/test_build_proposed_transactions.py -q
```

Result at last run: `23 passed`

Context workflow slice:

```bash
pixi run pytest tests/test_context_config.py tests/test_build_proposed_transactions.py -q
pixi run python scripts/normalize_context.py --help
pixi run python scripts/build_context_review.py --help
```

Result at last run:
- tests: `28 passed`
- normalize context help: OK
- build context review help: OK

Latest bug-fix slice:

```bash
pixi run pytest tests/test_build_proposed_transactions.py -q
pixi run normalize-context -- family 2026_04_01
pixi run build-context-review -- family 2026_04_01
pixi run python scripts/reconcile_reviewed_transactions.py --old-reviewed data/paired/2026_04_01/family_proposed_transactions_reviewed_split_fixup_linked.parquet --new-proposed data/paired/2026_04_01/family_proposed_transactions.parquet --out data/paired/2026_04_01/family_proposed_transactions_reviewed_context_fixed.parquet
```

Result at last run:
- builder tests: `24 passed`
- Family proposal rebuilt to `680` rows
- reconciled Family reviewed artifact restored to `647` reviewed / `33` unresolved

Latest mapping/UI slice:

```bash
pixi run pytest tests/test_build_proposed_transactions.py tests/test_review_app.py -q
pixi run normalize-context -- family 2026_04_01
pixi run build-context-review -- family 2026_04_01
pixi run python scripts/reconcile_reviewed_transactions.py --old-reviewed data/paired/2026_04_01/family_proposed_transactions_reviewed_context_fixed.parquet --new-proposed data/paired/2026_04_01/family_proposed_transactions.parquet --out data/paired/2026_04_01/family_proposed_transactions_reviewed_context_fixed_refreshed.parquet
```

Result at last run:
- tests: `71 passed`
- Family proposal rebuilt to `679` rows
- March `89.9` `סטימצקי גרנד` duplicate collapsed to one matched row
- refreshed reviewed artifact written to `family_proposed_transactions_reviewed_context_fixed_refreshed.parquet`
- refreshed reviewed artifact now stands at `647` reviewed / `32` unresolved

Latest upload-prep slice:

```bash
pixi run pytest tests/test_upload_prep.py tests/test_prepare_ynab_upload_script.py -q
pixi run python scripts/prepare_ynab_upload.py --profile family --in data/paired/2026_04_01/family_proposed_transactions_reviewed_current.parquet --out data/paired/2026_04_01/family_upload.csv --json-out data/paired/2026_04_01/family_upload.json
```

Result at last run:
- tests: `34 passed`
- Family upload dry run succeeded from the canonical reviewed parquet
- `53` prepared rows written
- `2` possible manual matches flagged for inspection during upload verification

Latest bank cutover slice:

```bash
pixi run pytest tests/test_transaction_artifacts.py tests/test_bank_reconciliation.py -q
pixi run normalize-context -- family 2026_04_01
pixi run sync-bank-matches -- family 2026_04_01
pixi run reconcile-bank-statement -- family 2026_04_01
```

Result at last run:
- tests: `20 passed`
- Family bank canonical parquet now preserves `balance_ils`
- Family bank sync dry run succeeded on the new context/canonical path:
  - `100` matched
  - `4` updates planned
  - `32` unmatched
- Family bank reconcile dry run now reaches real reconciliation logic on the canonical path and blocks at row `96` because live YNAB state is not yet fully caught up

Latest card cutover slice:

```bash
pixi run pytest tests/test_card_reconciliation.py tests/test_context_config.py -q
pixi run python scripts/normalize_previous_max.py --help
pixi run python scripts/sync_card_matches.py --help
pixi run python scripts/reconcile_card_cycle.py --help
pixi run normalize-previous-max -- family x9922 --cycle 2026_03
pixi run sync-card-matches -- family 2026_04_01 --account "Opher x9922"
pixi run reconcile-card-cycle -- family 2026_04_01 --account "Opher x9922" --previous data/derived/previous_max/x9922/2026_03_max_norm.parquet
```

Result at last run:
- tests: `29 passed`
- `previous_max/x9922/2026_03.xlsx` now normalizes explicitly to canonical parquet before reconciliation
- Family x9922 card sync dry run succeeded on the new context/canonical path:
  - `23` matched
  - `0` updates planned
- `7` unmatched
- Family x9922 card reconcile dry run now reaches real reconciliation logic on the canonical path and blocks only on `7` unmatched current rows

Focused pandas cleanup slice:

```bash
pixi run pytest tests/test_transaction_artifacts.py tests/test_build_proposed_transactions.py tests/test_review_io.py tests/test_upload_prep.py -q
```

Result at last run:
- tests: `78 passed`
- [transaction_io.py](src/ynab_il_importer/artifacts/transaction_io.py) now keeps flat projection canonicalization and CSV export Polars-first
- [build_proposed_transactions.py](scripts/build_proposed_transactions.py) now exposes a Polars-facing target suggestion helper with one narrow pandas translation inside it
- [review_app/io.py](src/ynab_il_importer/review_app/io.py) no longer spins up pandas just to normalize one boolean scalar
- [upload_prep.py](src/ynab_il_importer/upload_prep.py) no longer spins up pandas just to parse one numeric scalar
