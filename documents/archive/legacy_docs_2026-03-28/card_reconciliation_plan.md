## Credit Card Cleanup And Reconciliation Plan

### Goals

- Make the `2026_03_07` MAX card flow review-ready.
- Preserve the fields we will need later for card reconciliation and sync.
- Reduce review noise by tightening fingerprinting and payee/category defaults without overfitting.
- Make pre-import dedupe conservative enough that ambiguous card rows are never silently dropped.

### Findings

#### Normalization

- Current MAX normalization already preserves most reconciliation-critical fields:
  - `account_name`
  - `source_account`
  - `card_suffix`
  - `date`
  - `secondary_date`
  - `max_sheet`
  - `max_txn_type`
  - `max_original_amount`
  - `max_original_currency`
  - `max_report_owner`
  - `max_report_scope`
  - `max_report_period`
- This is enough to support statement-style reconciliation later, because totals can be reconstructed by card, charge date, and billing bucket.
- One bug remains: card suffixes are being numericized on CSV round-trip, which collapses `0849` into `849` and weakens card-specific rules.

#### Pairing

- `build_proposed_transactions.py` currently removes source rows whenever there is any YNAB date+amount+account match.
- That is too aggressive. Many card rows have multiple YNAB candidates on the same key, especially after old duplicate imports or repeated same-amount merchants.
- Ambiguous keys must stay in the proposed file instead of being silently deduped away.

#### Mapping

- The current map is already strong for most card fingerprints.
- On the live `2026_03_07` pass, only a small number of surviving card rows remain unmapped after dedupe.
- The missing rules that are clearly supported by existing YNAB history are:
  - `lyft ride fri 4pm lyft` -> payee `Lyft`
  - `apple bill cork עסקת חו` -> payee `Apple`, category `House and stuff`
  - `kindle svcs` -> payee `Kindle`, category `House and stuff`
- Additional likely-useful card rules backed by current YNAB history:
  - `ds passport` -> payee `US Consulate`, category `Unplanned extra expense`
  - `beer sheva municipality` -> payee `Beer Sheva City`, category `Unplanned extra expense`

### Implementation plan

1. Preserve card suffixes as true strings.
- Normalize card suffixes to zero-padded four-digit strings in MAX and Leumi parsers.
- Make CSV-loading paths preserve `card_suffix`, `ref`, `bank_txn_id`, and similar ID-like columns as strings.

2. Make source-vs-YNAB dedupe conservative.
- Change proposed-transaction dedupe so only unambiguous matched keys are dropped.
- Keep ambiguous source rows in the proposed file for human review.
- Preserve the pairs report so ambiguous keys can still be inspected.

3. Improve YNAB export for future exact matching.
- Extend normalized YNAB CSV exports to include lineage/state fields:
  - `ynab_id`
  - `import_id`
  - `matched_transaction_id`
  - `cleared`
  - `approved`
- This will support stronger future card dedupe and later reconciliation work.

4. Tighten card mapping with low-risk rules only.
- Add the clearly-supported recurring card rules identified above.
- Keep category blank when history is not stable enough to justify a category default.

5. Regenerate and evaluate.
- Re-normalize the `2026_03_07` MAX file.
- Re-download YNAB for the same window using the richer normalized export.
- Rebuild proposed transactions and inspect:
  - surviving card rows
  - unmatched card fingerprints
  - ambiguous card keys
  - whether any clearly-duplicate card rows still need better pairing logic

### Review-readiness criteria

- Card suffixes stay stable as strings end to end.
- Ambiguous card/YNAB matches no longer disappear from proposed output.
- The `2026_03_07` card proposed set is small and understandable.
- The remaining unmatched card rows are either truly new or explicitly understandable enough for a short manual review pass.

### Future reconciliation direction

- Card reconciliation will likely be statement-based, not running-balance-based.
- The main reconciliation key will be:
  - card account
  - `secondary_date` / billing date
  - `max_sheet`
  - `max_txn_type`
- Reconciled proof will come from reproducing statement totals and bucket totals from normalized rows, not from a per-row running balance.
