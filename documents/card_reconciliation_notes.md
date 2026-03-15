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
