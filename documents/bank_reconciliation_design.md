# Bank Reconciliation Design

## Goal

Keep bank import and bank reconciliation as separate phases.

- Import gets bank-backed transactions into YNAB and marks matched/imported rows `cleared`.
- Reconciliation later promotes eligible cleared rows to `reconciled`.
- Reconciliation state lives in YNAB, not in this repo.

## Bank Identity

Bank rows now carry a versioned lineage key:

- `bank_txn_id = BANK:V1:<24-hex-digest>`

The v1 digest is deterministic over raw bank-native fields:

- `source`
- `source_account`
- `date`
- `secondary_date`
- signed amount in milliunits
- `ref`
- `description_raw`

Only bank sources currently emit `bank_txn_id`.

## End-To-End Flow

1. Normalize bank files.
2. Preserve `bank_txn_id` through proposed/reviewed/upload artifacts.
3. Upload new bank rows with `import_id = bank_txn_id`.
4. Run a separate bank sync step to stamp lineage onto pre-existing YNAB rows that were matched conservatively.
5. Run a separate bank reconciliation step on normalized bank files with `balance_ils`.

## Import Behavior

For newly uploaded bank transactions:

- `import_id = bank_txn_id`

For already-existing YNAB bank transactions:

- never delete/recreate them
- never try to update `import_id`
- append a memo marker when we establish safe lineage:
  - `[ynab-il bank_txn_id=BANK:V1:xxxxxxxxxxxxxxxxxxxxxxxx]`
- if the matched YNAB row is `uncleared`, update it to `cleared`

## Sync Matching Rules

Exact lineage lookup order:

1. `transaction.import_id == bank_txn_id`
2. memo marker contains `bank_txn_id`

If exact lineage is missing, allow one conservative fallback only for stamping existing rows:

- same YNAB account
- same date
- same signed amount
- YNAB memo, after stripping lineage markers and normalizing, exactly equals normalized bank `description_raw`
- exactly one candidate
- candidate has no `import_id`
- candidate has no existing `bank_txn_id` memo marker

Weak `date + amount` matching alone is not enough.

## Reconciliation Inputs

Auto-reconciliation only operates on normalized bank CSVs that include:

- `bank_txn_id`
- `balance_ils`

Reconciliation uses live YNAB account and transaction state.

## Reconciliation Lookup Rules

Exact lineage only:

1. `transaction.import_id == bank_txn_id`
2. memo marker contains `bank_txn_id`

No heuristic fallback is allowed during reconciliation.

## Guard And Anchor Rules

### When `last_reconciled_at` exists

- the bank file must start at least 7 days before `last_reconciled_at`
- from the start of the bank file, the first 7 rows must:
  - resolve by exact lineage
  - already be `reconciled` in YNAB
- the bank `balance_ils` on the 7th row becomes the opening anchor balance

If that opening streak is missing, auto-reconciliation fails.

### When `last_reconciled_at` is missing/null

- find the starting-balance transaction as the earliest non-deleted YNAB transaction in the account
- require the bank file to start on that starting-balance date
- use the starting-balance transaction amount as the opening anchor balance

If that anchor cannot be established, auto-reconciliation fails.

## Balance Replay Rule

From the opening anchor forward:

- every bank row must resolve by exact lineage
- replay the signed bank amounts in file order
- every replayed balance must equal the row’s `balance_ils`
- the final replayed balance must equal the ending bank balance

Rows currently `uncleared` are still eligible. On success they can be patched straight to `reconciled`.

## Patch Rules

On successful sync:

- update matched `uncleared` rows to `cleared`
- append memo markers where conservative fallback established safe lineage

On successful reconciliation:

- patch all exact post-anchor matches to `cleared = reconciled`
- do not create adjustment transactions
- do not downgrade already reconciled rows

## `last_reconciled_at`

`last_reconciled_at` is used only as a guard. The implementation does not assume:

- that clients can set it directly
- that patching transactions to `reconciled` will necessarily advance it

## Scope Notes

- v1 is bank-only and requires `balance_ils`
- credit-card reconciliation is separate future work
- approval-state handling is intentionally out of scope for this slice
