## Bank Reconciliation Lessons

### Core lessons

- Exact lineage matters more than heuristic matching. Date-plus-amount is useful for investigation, but it is not safe enough to be the basis for automated reconciliation.
- Reconciliation has to be separate from import. Import can clear bank-backed rows; reconciliation should only promote rows to `reconciled` after stronger checks pass.
- Running balances are the best truth source when they exist. They let us prove that a candidate anchor and the subsequent row sequence are coherent.
- YNAB account state is the source of truth. We should not keep separate repo-owned reconciliation state that can drift from manual YNAB activity.
- Bank files need a stable source-native identity. The versioned `bank_txn_id` solved the “same date, same amount, repeated merchant” cases much better than heuristic matching.

### What went wrong

- Pre-import dedupe was too optimistic. Treating any account/date/amount YNAB hit as “already imported” hid real ambiguity and let wrong lineage survive.
- Legacy YNAB rows without bank lineage were harder to recover than expected. We needed a conservative memo-stamping path for old rows that predated `bank_txn_id`.
- Reporting was initially too shallow. “Blocked” without candidate transaction detail was not actionable enough to debug real mismatches.
- Anchor detection was too rigid at first. Requiring the file to begin with a full reconciled streak failed on realistic data even when a valid later anchor existed.
- YNAB auto-matching can attach an imported source identity to the wrong visible transaction. That means imported rows need post-upload verification, not just optimistic success handling.

### What worked well

- Versioned lineage ids in both `import_id` and memo markers made it possible to repair bad historical matches without inventing local state.
- Dry-run CSV reports with candidate summaries made manual cleanup feasible.
- Treating ambiguous rows as blockers instead of silently “best guessing” them kept the cleanup small and explainable.
- Preserving bank-side audit fields such as `balance_ils`, `ref`, `secondary_date`, and `card_suffix` paid off later when manual diagnosis was needed.

### Implications for cards

- Card reconciliation should also be lineage-first, not date-plus-amount-first.
- Card normalization must preserve the fields needed to reconstruct billing buckets and statement totals.
- Pre-import card dedupe must be conservative. Ambiguous YNAB matches should remain reviewable, not silently disappear.
- YNAB exports used for dedupe should preserve lineage and state fields such as `id`, `import_id`, `matched_transaction_id`, `cleared`, and `approved`.
