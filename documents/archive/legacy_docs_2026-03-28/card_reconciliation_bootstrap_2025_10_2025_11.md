# Card Bootstrap Fix List: 2025_10 -> 2025_11 (`Opher x9922`)

## Summary

- The `previous` cohort (`2025_10.xlsx`) is fully matched.
- All `29` blockers are in the `source` cohort (`2025_11.xlsx`).
- The blockers fall into three buckets:
  - `22` rows already exist in YNAB as unique same-date/same-amount matches and mostly just need lineage stamping. Some are currently `reconciled` too early and should be moved back to `cleared`.
  - `5` rows are another grouped `Migdal` case.
  - `2` rows are date/statement-edge problems:
    - one KSP installment row
    - one Klal row dated two days later in YNAB

## Complete Blocking List

| Row | Raw date | Statement date | Merchant | Amount (ILS) | Plausible match | Likely explanation | Proposed fix |
| --- | --- | --- | --- | ---: | --- | --- | --- |
| 0 | 2025-08-28 | 2025-11-10 | KSP (`תשלום 3 מתוך 4`) | 729.00 | YNAB `KSP` rows on `2025-09-28` and `2025-10-18`; later raw hit in `2025_12.xlsx` with same raw date and `תשלומים` | Installment row; not a simple same-date match | Prefer a matcher/bootstrap rule for installment rows rather than another manual edit. If we do edit YNAB, the likely target is the `2025-10-18` `KSP` row, but this needs a deliberate rule first. |
| 1 | 2025-10-10 | 2025-11-10 | Harel life insurance | 252.70 | Exact YNAB row on `2025-10-10` | Unique exact match, currently reconciled too early | Add memo marker for this `card_txn_id` and change `reconciled -> cleared`. |
| 2 | 2025-10-12 | 2025-11-10 | Cacao Natural | 509.00 | Exact YNAB row on `2025-10-12` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 3 | 2025-10-12 | 2025-11-10 | Spirulina Full Life | 303.45 | Exact YNAB row on `2025-10-12` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 4 | 2025-10-16 | 2025-11-10 | HOT | 175.73 | Exact YNAB row on `2025-10-16` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 5 | 2025-10-18 | 2025-11-10 | Teva Bari | 636.50 | Exact YNAB row on `2025-10-18` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 6 | 2025-10-20 | 2025-11-10 | Machsanei Hashuk | 565.09 | Exact YNAB row on `2025-10-20` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 7 | 2025-10-21 | 2025-11-10 | Migdal health and life | 215.59 | Grouped YNAB `Migdal` row on `2025-10-21` for `335.48` | Grouped historical YNAB row | Delete the grouped row and create split `cleared` rows with exact source amounts and `card_txn_id`s. |
| 8 | 2025-10-21 | 2025-11-10 | Migdal health and life | 48.07 | Grouped YNAB `Migdal` row on `2025-10-21` for `335.48` | Grouped historical YNAB row | Same fix as row 7. |
| 9 | 2025-10-21 | 2025-11-10 | Migdal health and life | 38.61 | Grouped YNAB `Migdal` row on `2025-10-21` for `335.48` | Grouped historical YNAB row | Same fix as row 7. |
| 10 | 2025-10-21 | 2025-11-10 | Migdal health and life | 24.46 | Grouped YNAB `Migdal` row on `2025-10-21` for `335.48` | Grouped historical YNAB row | Same fix as row 7. |
| 11 | 2025-10-21 | 2025-11-10 | Migdal health and life | 8.75 | Grouped YNAB `Migdal` row on `2025-10-21` for `335.48` | Grouped historical YNAB row | Same fix as row 7. |
| 12 | 2025-10-21 | 2025-11-10 | Kvish 6 | 308.88 | Exact YNAB row on `2025-10-21` | Unique exact match, currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 13 | 2025-10-24 | 2025-11-10 | Aroma | 200.00 | Exact YNAB row on `2025-10-24` | Unique exact match, currently already cleared | Add memo marker only. |
| 14 | 2025-10-25 | 2025-11-10 | Tootsi | 14.00 | Exact YNAB row on `2025-10-25` | Unique exact match, currently already cleared | Add memo marker only. |
| 15 | 2025-10-26 | 2025-11-10 | Kvish 6 | 6.82 | Exact YNAB row on `2025-10-26` | Unique exact match, currently already cleared | Add memo marker only. |
| 16 | 2025-10-27 | 2025-11-10 | Cello park | 20.02 | Exact YNAB row on `2025-10-27` | Unique exact match, currently already cleared | Add memo marker only. |
| 17 | 2025-10-29 | 2025-11-10 | Klal insurance | 89.40 | YNAB `Klal insurance` row on `2025-10-31`; older same-amount rows also exist | Border/date-shift case | Change the `2025-10-31` `Klal insurance` row to `2025-10-29`, add memo marker, keep it `cleared`. |
| 18 | 2025-10-29 | 2025-11-10 | Klal insurance | 142.87 | Exact YNAB row on `2025-10-29` | Unique exact match, currently already cleared | Add memo marker only. |
| 19 | 2025-10-31 | 2025-11-10 | Libra Insurance | 30.00 | Exact YNAB row on `2025-10-31` | Unique exact match, currently already cleared | Add memo marker only. |
| 20 | 2025-11-01 | 2025-11-10 | Icon | 169.00 | Exact YNAB row on `2025-11-01` | Unique exact match, currently already cleared | Add memo marker only. |
| 21 | 2025-11-01 | 2025-11-10 | Zeev Erlich / Aikido | 350.00 | Exact YNAB row on `2025-11-01` | Unique exact match, currently already cleared | Add memo marker only. |
| 22 | 2025-11-02 | 2025-11-10 | Ayala tours | 6,251.00 | Exact YNAB row on `2025-11-02` | Unique exact match, currently already cleared | Add memo marker only. |
| 23 | 2025-11-03 | 2025-11-10 | Maccabi | 322.63 | Exact YNAB row on `2025-11-03` | Unique exact match, currently already cleared | Add memo marker only. |
| 24 | 2025-11-05 | 2025-11-10 | Duty free | 57.84 | Exact YNAB row on `2025-11-05` | Unique exact match, currently already cleared | Add memo marker only. |
| 25 | 2025-11-05 | 2025-11-10 | El Al | 131.19 | Exact YNAB row on `2025-11-05` | Unique exact match, currently already cleared | Add memo marker only. |
| 26 | 2025-08-23 | 2025-11-10 | Intuition Exactly Fashion | 218.96 | Exact YNAB row on `2025-08-23` | Deferred/foreign row first evidenced in the November statement; currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 27 | 2025-09-02 | 2025-11-10 | Lime | 31.00 | Exact YNAB row on `2025-09-02` | Deferred/foreign row first evidenced in the November statement; currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |
| 28 | 2025-09-03 | 2025-11-10 | Audible | 404.80 | Exact YNAB row on `2025-09-03` | Deferred/foreign row first evidenced in the November statement; currently reconciled too early | Add memo marker and change `reconciled -> cleared`. |

## Recommended Order of Operations

1. Handle the easy exact-match rows first.
   - Stamp memo markers on rows `1-6`, `12-16`, `18-28`.
   - Move rows `1-6`, `12`, and `26-28` from `reconciled` back to `cleared`.
2. Replace the grouped `Migdal` row on `2025-10-21` with five split `cleared` rows.
3. Fix the `Klal 89.40` date from `2025-10-31` to `2025-10-29`.
4. Leave the KSP installment row for last and decide whether to:
   - bootstrap it with a matcher rule, or
   - make one more careful YNAB historical edit.

## Notes

- Supporting machine-readable analysis is in:
  - `data/paired/previous_max/x9922/2025_11_blocked_analysis.csv`
- The grouped `Migdal` candidate currently visible in live YNAB is:
  - `2025-10-21`, `Migdal health and life`, `335.48`, currently `reconciled`
- The likely YNAB row to retarget for `Klal 89.40` is:
  - `2025-10-31`, `Klal insurance`, `89.40`, currently `cleared`

## Current Status After Cleanup

- The October/November cleanup batch has been applied in live YNAB.
- Backup:
  - `data/paired/previous_max/x9922/bootstrap_2025_10_2025_11_backup.json`
- Action log:
  - `data/paired/previous_max/x9922/bootstrap_2025_10_2025_11_actions.csv`
- Re-running the dry run leaves exactly one blocker:
  - row `0`, `KSP`, `2025-08-28`, `secondary_date=2025-11-10`, `729.00`, `תשלום 3 מתוך 4`
