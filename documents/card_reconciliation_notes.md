# Card Reconciliation Notes

## Working assumptions

- Card reconciliation should be separate from bank reconciliation.
- Card reconciliation should be stateless: YNAB is the only source of truth for what is already reconciled.
- We should support two run shapes:
  - `--source` only: mid-month validation/sync, no promotion to `reconciled`
  - `--previous + --source`: month-transition reconciliation
- `approved but not registered` rows should be ignored.

## Current understanding

- A finished-month MAX export behaves more like a statement cohort.
- An ongoing-month MAX export behaves more like the current outstanding transaction list.
- The two files are not expected to overlap row-for-row.
- For reconciliation, `חיוב עסקות מיידי` should not be excluded globally.
  - If such a row is present in the finished-month export and is still cleared/unreconciled in YNAB, it belongs in the settlement cohort.

## Desired safety rules

- `--source` only should fail when there are older cleared-but-unreconciled YNAB rows before the first source transaction date.
- `--previous + --source` should warn or no-op when the entire previous cohort is already reconciled.
- `--previous + --source` should fail if some `--source` rows are already reconciled, because that likely means the files are reversed or stale.
- Mixed reconcile state inside the previous cohort should fail for manual inspection.
- `--previous + --source` should also require a payment-transfer check:
  - the absolute previous-cohort total must match a unique transfer inflow in the card account
  - that transfer must have a linked bank-side counterpart in YNAB
  - the bank-side counterpart must carry the opposite amount

## Identity concerns

- Bank reconciliation became much safer once bank rows had an explicit versioned lineage id.
- Card reconciliation probably needs the same treatment unless the current card-side `import_id` scheme is already strong enough in practice.
- That audit confirmed the generic `YNAB:<amount>:<date>:<occurrence>` import ids are too weak on their own.
- Card rows now need a versioned `card_txn_id`, and reconciliation should prefer:
  - `import_id == card_txn_id`
  - memo marker lineage
  - only then a stricter legacy fallback

## Reporting requirements

- The card reconcile report needs to tell us:
  - which account was reconciled
  - which file served as `previous`
  - which file served as `source`
  - previous cohort total
  - current cohort total
  - matched / ambiguous / unmatched counts
  - whether a bank payment match was found
  - exactly which rows would be promoted to `reconciled`

## Historical bootstrap lessons

- Older card history in YNAB predates `card_txn_id`, so many real transactions have:
  - blank `import_id`
  - blank memo lineage
  - dates that do not exactly match the earliest raw snapshot they are justified by
- When bootstrapping historical card months, there are at least three patterns:
  - grouped manual rows in YNAB that represent several raw rows
  - border rows that first appear on the following monthly snapshot
  - deferred / foreign rows that first appear one or more months later
- That means strict exact-lineage matching is right for current data, but too strict for historical bootstrap without either:
  - one-time YNAB cleanup
  - or explicit bootstrap-aware matching rules

## Implementation notes

- Real temporary MAX-style `.xlsx` snapshots were good enough for testing the parser and reconciliation flow together.
- The fake workbook shape has to mirror the real export closely:
  - three preface rows
  - one header row
  - then the transaction rows
- `approved but not registered` rows need to be filtered before reconciliation, not just ignored later in planning logic.

## Relationship to bank reconciliation

- Bank reconciliation still needs an anchor-selection fix.
- The bank fix is orthogonal, but the same lesson applies:
  - reports must be explicit enough to support manual cleanup when they block.
