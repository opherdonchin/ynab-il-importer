# Pilates Workflow

## Objective

Bring the Pilates budget onto the same import/review/upload/sync/reconcile workflow as the Family budget while keeping all state isolated per profile.

The steady-state target is:

normalize -> match -> review -> upload -> sync -> reconcile

with as much shared code as possible.

---

## What Reuses Existing Code

The Pilates work is built on the existing Family pipeline rather than a parallel implementation.

Shared scripts/workflows:
- `scripts/normalize_file.py`
- `scripts/build_proposed_transactions.py`
- `scripts/review_app.py`
- `scripts/prepare_ynab_upload.py`
- `scripts/download_ynab_api.py`
- `scripts/download_ynab_categories.py`
- `scripts/sync_bank_matches.py`
- `scripts/reconcile_bank_statement.py`
- `scripts/sync_card_matches.py`
- `scripts/reconcile_card_cycle.py`
- `scripts/bootstrap_pairs.py`
- `scripts/bootstrap_payee_map.py`
- `scripts/build_groups.py`

Shared libraries extended for Pilates:
- `src/ynab_il_importer/workflow_profiles.py`
- `src/ynab_il_importer/fingerprint.py`
- `src/ynab_il_importer/io_ynab.py`
- `src/ynab_il_importer/card_reconciliation.py`
- `src/ynab_il_importer/review_app/app.py`

---

## What Was Generalized

1. Profile-aware workflow resolution
- Budgets can now be selected by profile instead of assuming a single Family budget.
- Profile-scoped defaults now route mappings and outputs to isolated folders.

2. Profile-scoped mapping state
- Fingerprint maps and logs can now be supplied explicitly.
- Pilates now has its own `mappings/pilates/...` and `outputs/pilates/...` state.

3. Parser extensibility
- The normalizer now supports a new source type for Leumi credit-card HTML exports.
- YNAB export normalization now works directly from `.zip` exports.

4. Card matching generalization
- Card sync/reconcile can now read MAX xlsx exports and Leumi HTML card exports through the same downstream logic.
- Billing-date fallback via `secondary_date` is now available for sync/reconcile matching.
- Sync now stamps lineage markers for billing-date fallback matches as well.

5. Review workflow reuse
- The review app now supports `--profile`, so Pilates can automatically use `outputs/pilates/ynab_categories.csv` instead of the Family categories file.

---

## Pilates Profile Layout

Profile-scoped state:
- account map: `mappings/pilates/account_name_map.csv`
- fingerprint map: `mappings/pilates/fingerprint_map.csv`
- payee map: `mappings/pilates/payee_map.csv`
- category export: `outputs/pilates/ynab_categories.csv`
- fingerprint groups: `outputs/pilates/fingerprint_groups.csv`

Working datasets:
- raw bootstrap inputs: `data/raw/pilates_bootstrap`
- normalized bootstrap inputs: `data/derived/pilates_bootstrap`
- live YNAB snapshot: `data/derived/pilates/ynab_api_norm.csv`
- Family-budget snapshot for cross-plan analysis: `data/derived/family/ynab_api_norm.csv`
- bootstrap pairing outputs: `data/paired/pilates_bootstrap`
- live proposal outputs: `data/paired/pilates_live`

Pilates YNAB account names currently in scope:
- `Bank Leumi 225237`
- `Credit card 0602`
- `In Family`
- `From family`

Mapped source accounts already wired:
- Leumi bank source account `67833022523701` -> `Bank Leumi 225237`
- Leumi card source account `x0602` / `0602` -> `Credit card 0602`

---

## Current Bootstrap Status

Completed artifacts:
- All provided Pilates raw files normalized successfully.
- Historical Pilates YNAB register export normalized from the zipped export.
- Historical matched pairs built from statements + YNAB export.
- Pilates payee map bootstrapped and validated.
- Pilates fingerprint groups built.
- Live Pilates categories downloaded.
- Live Pilates transactions downloaded.
- Live proposed transactions built.

Notable current outputs:
- `data/paired/pilates_bootstrap/matched_pairs.csv`
- `mappings/pilates/payee_map.csv`
- `outputs/pilates/fingerprint_groups.csv`
- `data/paired/pilates_live/proposed_transactions.csv`

High-level data observations:
- Live Pilates card rows in YNAB currently span `2025-01-01` through `2026-03-19` (refreshed from API on `2026-03-21`).
- 2025 still contains legacy grouped/aggregated rows imported before card lineage markers existed.
- Clean itemized lineage appears from late 2025 onward; the conservative exact-sync/reconcile boundary is `2026-01-01`.
- Parts of the 2025 Pilates card history were imported into YNAB as aggregated summary rows rather than exact statement lines.

---

## Recommended Pilates Workflow

### 1. Normalize source files

Bank/card/raw YNAB export bootstrap:
```powershell
pixi run python scripts/normalize_file.py --profile pilates --dir data/raw/pilates_bootstrap --out-dir data/derived/pilates_bootstrap
```

For new ad hoc files:
```powershell
pixi run python scripts/normalize_file.py --profile pilates --file <raw-file>
```

### 2. Refresh live YNAB context

Categories:
```powershell
pixi run python scripts/download_ynab_categories.py --profile pilates
```

Transactions:
```powershell
pixi run python scripts/download_ynab_api.py --profile pilates --since 2025-01-01
```

### 3. Build proposed transactions

Against live Pilates YNAB data:
```powershell
pixi run python scripts/build_proposed_transactions.py --profile pilates --source-glob "data/derived/pilates_bootstrap/*_norm.csv" --ynab data/derived/pilates/ynab_api_norm.csv --out data/paired/pilates_live/proposed_transactions.csv --pairs-out data/paired/pilates_live/matched_pairs.csv
```

Notes:
- Exclude the normalized YNAB export file when using statement sources only.
- The live proposal file is the right starting point for 2026 card/bank upload work.

### 4. Review transactions

```powershell
pixi run python scripts/review_app.py --profile pilates --in data/paired/pilates_live/proposed_transactions.csv
```

Review goals:
- resolve ambiguous payees/categories
- confirm `Uncategorized` fallbacks only where appropriate
- mark map updates where the resolved choice should become a reusable rule

### 5. Prepare upload

```powershell
pixi run python scripts/prepare_ynab_upload.py --profile pilates --in data/paired/pilates_live/proposed_transactions_reviewed.csv --ready-only --reviewed-only
```

When satisfied, execute:
```powershell
pixi run python scripts/prepare_ynab_upload.py --profile pilates --in data/paired/pilates_live/proposed_transactions_reviewed.csv --ready-only --reviewed-only --execute
```

### 6. Sync bank matches

```powershell
pixi run python scripts/sync_bank_matches.py --profile pilates --bank <normalized-bank-file> --report-out data/paired/pilates_live/bank_sync_report.csv
```

### 7. Reconcile bank statement

```powershell
pixi run python scripts/reconcile_bank_statement.py --profile pilates --bank <normalized-bank-file> --report-out data/paired/pilates_live/bank_reconcile_report.csv
```

Current caveat:
- bank anchor selection still needs hardening in shared code before this is fully trustworthy across messy historical files.

### 8. Sync current card statement rows

```powershell
pixi run python scripts/sync_card_matches.py --profile pilates --account "Credit card 0602" --source <normalized-card-file> --date-from 2026-01-01 --report-out data/paired/pilates_live/card_sync_report.csv
```

Use this to stamp/clear already-existing YNAB rows when exact or billing-date fallback matches exist.

### 9. Reconcile card cycle

```powershell
pixi run python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --previous <previous-normalized-card-file> --source <current-normalized-card-file> --source-date-from 2026-01-01 --previous-date-from 2026-01-01 --report-out data/paired/pilates_live/card_reconcile_report.csv
```

Use source-only mode only for mid-cycle validation:
```powershell
pixi run python scripts/reconcile_card_cycle.py --profile pilates --account "Credit card 0602" --source <current-normalized-card-file> --source-date-from 2026-01-01
```

---

## Current Pilates-Specific Gaps

### 1. Legacy aggregated card history

This is the main reason full historical card reconciliation is still blocked.

Observed behavior:
- statement files contain many small exact rows
- YNAB history contains some larger aggregated rows for the same period
- examples include memo/date-range summaries such as `01.11-06.11`

Implication:
- the exact-lineage card reconciliation algorithm should not be loosened to guess across these grouped historical imports
- instead, we should either define a clean bootstrap boundary or build a dedicated aggregation-aware bootstrap helper

### 2. Missing 2026 card uploads in the Pilates budget

This was true earlier, but after the clean-forward upload and refresh the card account now includes rows through `2026-03-19`.
Current focus is no longer "missing all 2026 rows"; it is keeping exact sync/reconcile constrained to the clean itemized boundary.

### 3. `In Family` cross-budget handling

The Pilates budget references Family via `In Family`, and the Family-budget transaction snapshot is now available locally in `data/derived/family/ynab_api_norm.csv`.

This flow now has a dedicated cross-budget comparison/proposal pipeline.

Working rule:
- source side = Family budget category `Pilates`
- target side = Pilates budget account `In Family`
- history/bootstrap ends conservatively at `2025-11-05`
- recurring update runs start a little early at `2025-11-01`
- exact live dedupe stays strict
- bootstrap may use a `+/- 1 day` window with text as a tie-breaker

The comparison layer is separate from upload/review:
- `matched_pairs.csv` = rows already represented in Pilates
- `unmatched_source.csv` = Family-side rows missing in Pilates
- `unmatched_target.csv` = Pilates-side manual/history rows with no current Family `Pilates` match
- `ambiguous_matches.csv` = rows that need human interpretation because a safe automatic match is not unique

### Cross-budget bootstrap

Use the saved Family YNAB snapshot directly:
```powershell
pixi run python scripts/bootstrap_cross_budget_pairs.py --source data/derived/family/ynab_api_norm.csv --source-profile family --source-category Pilates --ynab data/derived/pilates/ynab_api_norm.csv --target-profile pilates --target-account "In Family" --since 2025-01-01 --until 2025-11-05 --date-tolerance-days 1
```

Current dry-run on the saved local snapshots produced:
- matched pairs: `94`
- unmatched Family source rows: `20`
- unmatched Pilates target rows: `44`
- ambiguous buckets: `3`

Generated artifacts:
- `data/paired/pilates_cross_budget_bootstrap/matched_pairs.csv`
- `data/paired/pilates_cross_budget_bootstrap/unmatched_source.csv`
- `data/paired/pilates_cross_budget_bootstrap/unmatched_target.csv`
- `data/paired/pilates_cross_budget_bootstrap/ambiguous_matches.csv`

To bootstrap payee-map candidates from those historical matches:
```powershell
pixi run python scripts/bootstrap_payee_map.py --pairs data/paired/pilates_cross_budget_bootstrap/matched_pairs.csv --out data/paired/pilates_cross_budget_bootstrap/payee_map_candidates.csv
```

### Cross-budget recurring proposals

Build the reviewable proposal file from the overlapping live window:
```powershell
pixi run python scripts/build_cross_budget_proposed.py --source data/derived/family/ynab_api_norm.csv --source-profile family --source-category Pilates --ynab data/derived/pilates/ynab_api_norm.csv --target-profile pilates --target-account "In Family" --since 2025-11-01 --date-tolerance-days 0
```

Current dry-run on the saved local snapshots produced:
- matched existing rows in Pilates: `14`
- new proposal rows: `28`
- unmatched target rows: `7`
- ambiguous rows: `0`

Generated artifacts:
- `data/paired/pilates_cross_budget_live/proposed_transactions.csv`
- `data/paired/pilates_cross_budget_live/matched_pairs.csv`
- `data/paired/pilates_cross_budget_live/unmatched_source.csv`
- `data/paired/pilates_cross_budget_live/unmatched_target.csv`
- `data/paired/pilates_cross_budget_live/ambiguous_matches.csv`

The proposal file uses:
- `account_name = "In Family"` for upload/review
- `source_account` to preserve the original Family-side account as audit context

Review with the ordinary Pilates review flow:
```powershell
pixi run python scripts/review_app.py --profile pilates --in data/paired/pilates_cross_budget_live/proposed_transactions.csv
```

### Optional live category export

When we want a fresh Family category export instead of reusing `data/derived/family/ynab_api_norm.csv`, use the profile-aware YNAB-as-source export and point it at the fingerprint map we want for this workflow:
```powershell
pixi run python scripts/io_ynab_as_source.py --profile family --category Pilates --fingerprint-map mappings/pilates/fingerprint_map.csv --out data/derived/family/ynab_category_business_pilates_live.csv
```

---

## Recommended Next Generalizations

These would help Pilates now and Aikido later.

1. Aggregation-aware legacy bootstrap helper
- purpose: explain or pair grouped historical YNAB rows against many statement rows without weakening steady-state reconcile safety
- scope: bootstrap analysis only, not regular reconciliation

2. Multi-account bank reconcile behavior
- reconcile each mapped account independently when a source file spans multiple accounts
- warn/skip unmapped accounts instead of failing globally

3. Profile-aware workflow wrappers
- keep pushing profile defaults down into wrappers so business-specific runs become short, predictable commands

4. Shared bootstrap conventions for future businesses
- Aikido should reuse the same profile layout:
  - `mappings/aikido/...`
  - `outputs/aikido/...`
  - profile-based budget/category/account resolution

---

## Working Rule For Now

Use the shared pipeline everywhere possible.
Keep Pilates-specific behavior limited to:
- account mappings
- fingerprint/payee data
- the Leumi card HTML parser
- explicit documentation around legacy aggregated card history

Do not relax exact reconciliation rules just to force historical grouped data through the steady-state workflow.
