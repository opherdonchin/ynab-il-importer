# ynab-il-importer — Execution Plan (current)

## Goal (Milestone 1)

Deliver a repeatable end-to-end workflow that:
1. Ingests bank + card exports
2. Normalizes into one schema
3. Dedupes against YNAB for a chosen date range
4. Applies `payee_map.csv` to infer payee + category
5. Produces a reviewable proposed-transactions CSV
6. Uploads only safe rows to YNAB with deterministic ids
7. Reconciles bank and card accounts separately from import

Success criteria:
- >=90% of new transactions default to a usable payee/category
- Upload is idempotent
- Bank reconciliation is exact-lineage based and safe to re-run
- Card reconciliation is stateless, account-scoped, and safe to re-run
- Review produces explicit map-candidate artifacts instead of hidden state

---

## Current State

Completed:
- Unified normalization for Leumi bank, Leumi xls/html, MAX card, and YNAB exports.
- Fingerprint generation with optional `mappings/fingerprint_map.csv` canonicalization.
- `balance_ils` is preserved for Leumi normalized bank outputs.
- MAX normalization preserves billing-bucket details (`max_sheet`, `max_txn_type`), `secondary_date`, report period metadata, and `card_suffix`.
- Bank transaction lineage is implemented via versioned `bank_txn_id` values.
- Separate bank sync and bank reconciliation flows exist:
  - `scripts/sync_bank_matches.py`
  - `scripts/reconcile_bank_statement.py`
- Bank uploads use `bank_txn_id` as `import_id` for newly created bank rows.
- Existing YNAB bank rows can be stamped via memo markers for reconciliation lineage.
- Leumi card-payment rows preserve `card_suffix`, and `לאומי ויזה` transfer rules are disambiguated by card suffix.
- Proposed transactions preserve audit fields such as `source_account`, `secondary_date`, `ref`, `balance_ils`, `bank_txn_id`, and `card_suffix`.
- Proposed transactions default unknown payees to the row fingerprint when there are no payee options, and default missing non-transfer categories to `Uncategorized`.
- Review UI supports resume, grouped editing, explicit reviewed/save state, viewing reviewed rows, viewing defaulted rows, accepting defaults in bulk, and deterministic map-update export.
- Upload-prep supports dry-run artifacts plus optional live execution, with deterministic import ids and transfer handling.
- YNAB exports preserve lineage/state fields needed for stronger matching:
  - `ynab_id`
  - `account_id`
  - `import_id`
  - `matched_transaction_id`
  - `cleared`
  - `approved`
- Card transaction lineage is now implemented via versioned `card_txn_id` values.
- MAX normalization emits `card_txn_id`.
- Card uploads use `card_txn_id` as `import_id` for newly created card rows.
- A new stateless card reconciliation flow exists:
  - `scripts/reconcile_card_cycle.py`
  - `src/ynab_il_importer/card_reconciliation.py`
- Card reconciliation supports:
  - `--source` only for mid-month validation/sync
  - `--previous + --source` for month-transition reconciliation
- Card reconciliation ignores `approved but not registered` rows and zero-charged rows.
- Card transition reconciliation now validates the payment transfer:
  - previous cohort total must match a unique card-account transfer inflow
  - that transfer must have a linked bank-side counterpart in YNAB
  - the bank-side counterpart amount must match the opposite amount
- Card reconciliation has targeted tests with real temporary MAX-style `.xlsx` snapshots plus YNAB-like live transaction payloads.
- Card reconciliation supports `--allow-reconciled-source` flag to handle cycles where source rows were already reconciled by a prior run (e.g., a later cycle reconcile ran first).
- All three card accounts (x5898, x7195, x9922) are reconciled through at least `2026_03`.
- All card-side payment transfers for reconciled cycles are confirmed reconciled in YNAB.
- `scripts/reconcile_card_payment_transfers.py` correctly scans all report variants (e.g. `_after_cleanup`, `_filtered_previous`) per cycle to find the executed reconcile report, and checks/patches payment transfer `cleared` state.
- x5898 Nov-10, Dec-10 transfers confirmed reconciled. All 5 cycles processed.
- x9922: 5 cycles with reconcile evidence — all payment transfers already reconciled.
- x7195: 2 cycles with reconcile evidence (2025_12, 2026_03) — both already reconciled. 4 earlier cycles (2025_10, 2025_11, 2026_01, 2026_02) bootstrapped via sync+direct-YNAB and have no reconcile report; payment transfers for those cycles are not auto-verified (no report evidence to compute expected amount).
- **Separately-settled card charges are now handled automatically** in `reconcile_card_cycle.py` (as of 2026-03-17). MAX sometimes debits certain charges (subscriptions, government fees) directly from the bank account before the monthly billing cycle. These rows appear in the billing statement with an earlier `secondary_date` than the main billing date. The algorithm: the latest `secondary_date` is identified as the main billing date; rows with earlier secondary_dates are "separately settled" and do not roll into the monthly payment transfer; they are reconciled directly as card expenses (action = `reconcile_separate`). `reconcile_card_payment_transfers.py` correctly excludes `reconcile_separate` rows from the payment transfer amount lookup.
- x9922 2026_03 (March 16 snapshot) reconciled in full: 46 main-cycle rows via 14,563.73 ILS transfer + 12 separately-settled rows (Facebook, Netflix, ChatGPT, Passport fees, Evernote, Apple) reconciled directly. 58 transactions patched.
- x7195 2026_03 (March 16 snapshot) reconciled: 38 rows via 18,582.46 ILS transfer. 38 transactions patched.
- 2026_03_16 full cycle complete: YNAB upload (65/67), bank sync, bank reconcile, all card syncs (x5898/x7195/x9922), all card reconciles (x5898/x7195/x9922) — all done.

In progress:
- Bank reconciliation anchor selection is too permissive and can choose a late streak near the end of the file, resulting in zero planned updates even when older cleared rows should be promoted.
- Bank reconciliation still needs explicit per-account grouping / skip-warn behavior for multi-account source files.

---

## Immediate Next Steps

1. Bank reconciliation correction
- Constrain anchor selection so the anchor window must occur before the first unreconciled matched row.
- Reconcile per mapped YNAB account.
- Warn and skip unmapped accounts by default instead of failing the whole run.
- Improve blocked-run reporting so manual cleanup is straightforward.

2. Card reconciliation hardening
- Add `CARD:V2` identity generation with occurrence discriminator for exact duplicate card rows (x9922 had collisions in 2026_02 raw source).
- Add stronger sanity checks that `--previous` is older and `--source` is newer/current.
- ~~Encode "separately settled immediate charges" rule so it doesn't require manual filtered-previous CSV workaround.~~ **Done (2026-03-17)**: handled automatically via `secondary_date` grouping.

3. End-to-end verification
- ~~Run bank sync + bank reconcile for 2026_03_16 cycle.~~ **Done.**
- ~~Upload any remaining approved transactions to YNAB (`prepare_ynab_upload.py --execute`).~~ **Done (65/67).**
- ~~Card reconcile x5898, x7195, x9922 for 2026_03.~~ **Done.**
- Keep documentation current as the implementation stabilizes.

---

## Deliverables to Watch

- `data/derived/*/*_norm.csv`
- `data/paired/*/proposed_transactions.csv`
- `data/paired/*/proposed_transactions_reviewed.csv`
- `data/paired/*/ynab_upload.csv`
- `data/paired/*/bank_sync_report.csv`
- `data/paired/*/bank_reconcile_report.csv`
- future: `data/paired/*/card_reconcile_report.csv`
- `outputs/fingerprint_log.csv`
- `outputs/ynab_categories.csv`
- review map-update artifacts next to reviewed CSVs
- `mappings/payee_map.csv`
- `documents/bank_reconciliation_lessons.md`
- `documents/card_reconciliation_plan.md`
