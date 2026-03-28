# Card Bootstrap Fix List: 2026_01 -> 2026_02 (`Opher x9922`)

## Summary

- The `previous` cohort (`2026_01.xlsx`) matches cleanly:
  - `55/55` rows
  - matched YNAB total `-21514.74 ILS`
- The `source` cohort (`2026_02.xlsx`) has `61` blockers.
- This batch is much cleaner than it first looked:
  - `58` blockers already have unique exact `date + amount` matches in live YNAB
  - all `58` of those exact matches are already `uncleared`
  - they are blocking only because the historical rows do not yet carry `card_txn_id` markers
  - the remaining `3` blockers are simple date drifts to nearby `uncleared` rows

So operationally this is:

- a bulk marker-stamping pass on `58` exact open rows
- plus `3` date corrections

No new rows should need to be created for this month.

## Blocking Buckets

### Bucket 1: Exact open rows, just missing lineage (`58`)

These rows already exist in YNAB with:

- exact same date
- exact same amount
- unique match
- status `uncleared`

They should remain `uncleared`; they just need the January/February `card_txn_id` marker appended so the reconciler can claim them exactly.

This bucket includes the bulk of the November travel/deferred rows on the `2026-02-10` statement, including:

- Kindle
- travel / parking / hotel / rideshare rows
- Booking / Lyft / Wodify / restaurants / groceries
- the other exact deferred rows in the statement cohort

### Bucket 2: Date-drift rows (`3`)

| Row | Raw date | Statement date | Merchant | Amount (ILS) | YNAB candidate | Proposed fix |
| --- | --- | --- | --- | ---: | --- | --- |
| 109 | 2025-11-16 | 2026-02-10 | Lyft | 104.83 | `Lyft` on `2025-11-17`, currently `uncleared` | Move the YNAB row from `2025-11-17` to `2025-11-16` and append the `card_txn_id` marker. Leave `uncleared`. |
| 138 | 2025-11-23 | 2026-02-10 | Apple | 3.90 | `Apple.com` on `2025-11-27`, currently `uncleared` | Move the YNAB row from `2025-11-27` to `2025-11-23` and append the `card_txn_id` marker. Leave `uncleared`. |
| 139 | 2025-11-24 | 2026-02-10 | Kindle | 19.75 | `Kindle` on `2025-11-27`, currently `uncleared` | Move the YNAB row from `2025-11-27` to `2025-11-24` and append the `card_txn_id` marker. Leave `uncleared`. |

## Expected State After Fixes

If these edits are applied and `01/02` is rerun:

- `2026_01` should be fully eligible to become `reconciled`
- `2026_02` should be fully eligible to become `cleared`
- the line-match totals should become:
  - previous `55/55`, `-21514.74 ILS`
  - source `148/148`, `-27992.76 ILS`

## Supporting Analysis

- Blocked-row analysis:
  - `data/paired/previous_max/x9922/2026_02_blocked_analysis.csv`
- Reconciliation report:
  - `data/paired/previous_max/x9922/2026_02_card_reconcile_report.csv`
