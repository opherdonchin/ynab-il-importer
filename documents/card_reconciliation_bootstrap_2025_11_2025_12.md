# Card Bootstrap Fix List: 2025_11 -> 2025_12 (`Opher x9922`)

## Summary

- The `previous` cohort (`2025_11.xlsx`) matches cleanly:
  - `29/29` rows
  - matched YNAB total `-12255.36 ILS`
- The `source` cohort (`2025_12.xlsx`) has `14` blockers.
- These blockers fall into three buckets:
  - `11` deferred / foreign rows that already exist in YNAB as unique exact date+amount matches, but were reconciled too early and have no `card_txn_id` marker yet
  - `2` domestic rows that exist in YNAB one day later than the source file date
  - `1` KSP installment row with no existing YNAB counterpart

## Blocking Rows

| Row | Raw date | Statement date | Merchant | Amount (ILS) | Plausible match | Likely explanation | Proposed fix |
| --- | --- | --- | --- | ---: | --- | --- | --- |
| 0 | 2025-08-28 | 2025-12-10 | KSP (`תשלום 4 מתוך 4`) | 729.00 | No current YNAB counterpart; older KSP installment rows already exist for `2/4` and `3/4` | Missing historical installment row | Create a new YNAB `KSP` row dated `2025-08-28`, amount `729.00`, status `uncleared`, with the December `card_txn_id` marker. |
| 25 | 2025-12-05 | 2025-12-10 | Cacao Natural | 511.00 | YNAB `Cacao Natural` on `2025-12-06`, currently `uncleared` | One-day date shift | Change YNAB date from `2025-12-06` to `2025-12-05` and add the `card_txn_id` marker. Leave `uncleared`. |
| 27 | 2025-12-05 | 2025-12-10 | גוסי שיווק והפצה / Juicy | 182.25 | YNAB `Juicy` on `2025-12-06`, currently `uncleared` | One-day date shift | Change YNAB date from `2025-12-06` to `2025-12-05` and add the `card_txn_id` marker. Leave `uncleared`. |
| 28 | 2025-09-08 | 2025-12-10 | Facebook | 440.00 | Exact YNAB row on `2025-09-08`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 29 | 2025-09-13 | 2025-12-10 | Society for Neuroscience | 2203.67 | Exact YNAB row on `2025-09-13`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 30 | 2025-09-13 | 2025-12-10 | Society for Neuroscience | 807.45 | Exact YNAB row on `2025-09-13`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 31 | 2025-09-16 | 2025-12-10 | Kindle | 33.62 | Exact YNAB row on `2025-09-16`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 32 | 2025-09-18 | 2025-12-10 | Facebook | 227.73 | Exact YNAB row on `2025-09-18`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 33 | 2025-09-23 | 2025-12-10 | Apple.com | 3.90 | Exact YNAB row on `2025-09-23`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 34 | 2025-09-24 | 2025-12-10 | Amazon | 233.94 | Exact YNAB row on `2025-09-24`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 35 | 2025-09-24 | 2025-12-10 | Kindle | 26.90 | Exact YNAB row on `2025-09-24`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 36 | 2025-09-28 | 2025-12-10 | Apple.com | 24.90 | Exact YNAB row on `2025-09-28`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 37 | 2025-10-03 | 2025-12-10 | Facebook | 462.00 | Exact YNAB row on `2025-10-03`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |
| 38 | 2025-10-06 | 2025-12-10 | Kindle | 19.79 | Exact YNAB row on `2025-10-06`, currently `reconciled` | Deferred foreign row first evidenced in December statement | Add marker and change `reconciled -> uncleared`. |

## Expected State After Fixes

If these edits are applied and `11/12` is rerun:

- `2025_11` should be fully eligible to become `reconciled`
- `2025_12` should be fully eligible to become `cleared`
- the line-match totals should remain:
  - previous `29/29`, `-12255.36 ILS`
  - source `39/39`, `-10587.50 ILS`

## Supporting Analysis

- Blocked-row analysis:
  - `data/paired/previous_max/x9922/2025_12_blocked_analysis.csv`
- Reconciliation report:
  - `data/paired/previous_max/x9922/2025_12_card_reconcile_report.csv`
