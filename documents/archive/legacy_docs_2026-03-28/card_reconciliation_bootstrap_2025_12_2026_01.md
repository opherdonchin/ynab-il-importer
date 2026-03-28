# Card Bootstrap Fix List: 2025_12 -> 2026_01 (`Opher x9922`)

## Summary

- The `previous` cohort (`2025_12.xlsx`) matches cleanly:
  - `39/39` rows
  - matched YNAB total `-10587.50 ILS`
- The `source` cohort (`2026_01.xlsx`) has `10` blockers.
- None of these `10` rows appear again in the later `2026_02.xlsx` or `2026_03.xlsx`
  snapshots by `card_txn_id`, so this is not another "wait for a later statement" batch.
- These blockers fall into three buckets:
  - `3` deferred rows that already exist in YNAB as unique exact date+amount matches, but are still `reconciled` and lack `card_txn_id` markers
  - `1` row that exists in YNAB under the right amount/payee but with a two-day date drift
  - `6` genuinely missing deferred rows that need to be created in YNAB

## Blocking Rows

| Row | Raw date | Statement date | Merchant | Amount (ILS) | Plausible match | Likely explanation | Proposed fix |
| --- | --- | --- | --- | ---: | --- | --- | --- |
| 32 | 2025-10-11 | 2026-01-11 | Facebook | 486.00 | Exact YNAB `Facebook` row on `2025-10-11`, currently `reconciled` | Deferred row was reconciled too early and has no lineage marker | Add the `card_txn_id` marker and change `reconciled -> uncleared`. |
| 33 | 2025-10-16 | 2026-01-11 | Kindle | 20.05 | Exact YNAB `Kindle` row on `2025-10-16`, currently `reconciled` | Deferred row was reconciled too early and has no lineage marker | Add the `card_txn_id` marker and change `reconciled -> uncleared`. Preserve the `5.99 USD` memo text. |
| 34 | 2025-10-18 | 2026-01-11 | Facebook | 326.25 | Exact YNAB `Facebook` row on `2025-10-18`, currently `reconciled` | Deferred row was reconciled too early and has no lineage marker | Add the `card_txn_id` marker and change `reconciled -> uncleared`. |
| 35 | 2025-10-23 | 2026-01-11 | Lime | 16.00 | YNAB `Lime` row on `2025-10-21`, currently `uncleared` | Small historical date drift | Change the YNAB date from `2025-10-21` to `2025-10-23` and add the `card_txn_id` marker. Leave `uncleared`. |
| 36 | 2025-10-23 | 2026-01-11 | Apple | 3.90 | No exact current YNAB counterpart; later monthly Apple rows establish the pattern and category | Missing deferred row | Create a new `Apple` row dated `2025-10-23`, amount `3.90`, category `House and stuff`, status `uncleared`, with the January `card_txn_id` marker. |
| 37 | 2025-10-24 | 2026-01-11 | Kindle | 19.66 | No exact current YNAB counterpart; nearby Kindle rows establish payee/category pattern | Missing deferred row | Create a new `Kindle` row dated `2025-10-24`, amount `19.66`, category `House and stuff`, status `uncleared`, with the January `card_txn_id` marker. Preserve the `5.99 USD` memo pattern. |
| 38 | 2025-10-26 | 2026-01-11 | Zotero | 65.64 | No current-row counterpart in `x9922`, but there is historical `Zotero` usage in YNAB under category `University` | Missing deferred row | Create a new `Zotero` row dated `2025-10-26`, amount `65.64`, category `University`, status `uncleared`, with the January `card_txn_id` marker. |
| 39 | 2025-10-27 | 2026-01-11 | MLMCMEETING | 320.80 | No current YNAB counterpart; MAX export classifies it under government/administrative spending | Missing deferred row | Create a new `MLMCMEETING` row dated `2025-10-27`, amount `320.80`, status `uncleared`, with the January `card_txn_id` marker. Category choice is uncertain; use `University` as the least-bad working bootstrap category for now. |
| 40 | 2025-10-31 | 2026-01-11 | Kindle | 19.63 | No exact current YNAB counterpart; nearby Kindle rows establish payee/category pattern | Missing deferred row | Create a new `Kindle` row dated `2025-10-31`, amount `19.63`, category `House and stuff`, status `uncleared`, with the January `card_txn_id` marker. Preserve the `5.99 USD` memo pattern. |
| 41 | 2025-10-31 | 2026-01-11 | UKVI ETA | 72.70 | No current YNAB counterpart; MAX export classifies it under government/administrative spending | Missing deferred row | Create a new `UKVI ETA` row dated `2025-10-31`, amount `72.70`, status `uncleared`, with the January `card_txn_id` marker. Category choice is uncertain; use `University` as the least-bad working bootstrap category for now. |

## Expected State After Fixes

If these edits are applied and `12/01` is rerun:

- `2025_12` should be fully eligible to become `reconciled`
- `2026_01` should be fully eligible to become `cleared`
- the line-match totals should become:
  - previous `39/39`, `-10587.50 ILS`
  - source `55/55`, `-21514.74 ILS`

## Supporting Analysis

- Blocked-row analysis:
  - `data/paired/previous_max/x9922/2026_01_blocked_analysis.csv`
- Reconciliation report:
  - `data/paired/previous_max/x9922/2026_01_card_reconcile_report.csv`
