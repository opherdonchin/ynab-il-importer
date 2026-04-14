# Active Plan

## Workstream

Run the April 14 workflow cleanly across Family, Pilates, and Aikido while keeping card closeout strict and source selection date-agnostic.

## Current State

- canonical transaction artifacts are Parquet `transaction_v1`
- canonical review artifacts are Parquet `review_v4`
- `build-context-review` excludes settled YNAB history by default
- closeout remains strict by source kind:
  - bank: sync plus statement reconciliation
  - card: sync plus cycle reconciliation
  - `ynab_category`: category/account parity reconciliation
- `2026_04_01` is operationally closed:
  - Pilates bank and card closeout complete
  - Aikido category/account parity restored and reconciled
- `2026_04_14` raw inputs are now present:
  - Family bank + MAX export
  - Pilates bank + Leumi card HTML
  - previous full-month card statements staged under `previous_max` and `previous_leumi_card`
- Pilates card source selection is now date-agnostic via `raw_match` so new run tags do not hard-fail on the exact HTML filename

## Recently Completed

## Recently Completed

- cleaned up Aikido March drift directly in live YNAB:
  - removed stale Family March 25 duplicate/manual rows
  - created the missing Aikido-side Bakr `+70` and Facebook `-735` rows
  - restored exact Family-category vs Aikido-account parity
- closed `aikido / 2026_04_01` with category/account reconciliation:
  - [data/paired/2026_04_01/aikido_aikido_family_category_category_account_reconcile_report.csv](../data/paired/2026_04_01/aikido_aikido_family_category_category_account_reconcile_report.csv)
- fixed Pilates context selection for recurring Leumi card HTML filenames:
  - [contexts/pilates/context.toml](../contexts/pilates/context.toml)

## Next Steps

1. Normalize previous full-month card statements for the April 14 run:
   - `family x9922`, `family x7195`, `family x5898`, `pilates x0602`
2. Download fresh YNAB snapshots for `family`, `pilates`, and `aikido` under `2026_04_14`.
3. Run `normalize-context` and `build-context-review` for all three contexts.
4. Review any non-empty proposals.
5. Execute upload plus closeout by source kind:
   - Family and Pilates: bank sync/reconcile, card sync/reconcile
   - Aikido: category/account reconcile

## Working Rules

- Prefer strict canonical boundaries over compatibility wrappers.
- Keep nested data only where it is semantically real.
- Treat active docs plus code as the source of truth; move history to `documents/archive/` instead of keeping duplicate active docs.
