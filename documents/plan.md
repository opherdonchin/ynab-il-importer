# ynab-il-importer — Execution Plan (current)

## Goal (Milestone 1)

Deliver a repeatable end-to-end workflow that:
1) Ingests bank + card exports
2) Normalizes into one schema
3) Dedupes against YNAB for a chosen date range
4) Applies `payee_map.csv` to infer payee + category
5) Produces a reviewable proposed-transactions CSV
6) Uploads only safe rows to YNAB with deterministic ids
7) Reconciles bank statements separately from import

Success criteria:
- ≥90% of new transactions default to a usable payee/category
- Upload is idempotent
- Bank reconciliation is exact-lineage based and safe to re-run
- Review produces explicit map-candidate artifacts instead of hidden state

---

## Current State

Completed:
- Unified normalization for Leumi bank, Leumi xls/html, MAX card, and YNAB exports.
- Fingerprint generation with optional `mappings/fingerprint_map.csv` canonicalization.
- `balance_ils` is preserved for Leumi normalized bank outputs.
- MAX normalization now preserves both billing buckets and subtype details (`max_sheet`, `max_txn_type`) and card suffixes.
- Bank transaction lineage is implemented via versioned `bank_txn_id` values.
- Separate bank sync and bank reconciliation flows exist:
  - `scripts/sync_bank_matches.py`
  - `scripts/reconcile_bank_statement.py`
- Bank uploads use `bank_txn_id` as `import_id` for newly created bank rows.
- Existing YNAB bank rows can be stamped via memo markers for reconciliation lineage.
- Leumi card-payment rows now preserve `card_suffix`, and `לאומי ויזה` transfer rules are disambiguated by card suffix.
- Proposed transactions now preserve key audit fields such as `source_account`, `secondary_date`, `ref`, `balance_ils`, `bank_txn_id`, and `card_suffix`.
- Proposed transactions now default unknown payees to the row fingerprint when there are no payee options.
- Review UI supports resume, grouped editing, “apply to all with this fingerprint”, and explicit reviewed/save state.
- Upload-prep supports dry-run artifacts plus optional live execution, with deterministic import ids and transfer handling.

In progress:
- Review/upload behavior still treats missing non-transfer categories as blocking rather than defaulting to YNAB `Uncategorized`.
- The review UI does not yet offer a bulk “accept remaining defaults” action.
- The review flow preserves `update_map`, but there is still no maintained exported log of payee/category mapping candidates.
- Existing proposed/reviewed CSVs need rebuilding whenever defaulting rules change.

---

## Immediate Next Steps

1) Review-flow defaults
- Default unresolved non-transfer categories to the YNAB `Uncategorized` category from the downloaded category list.
- Keep these defaulted rows unreviewed until explicitly accepted.
- Add a review action to accept remaining valid defaults in bulk.

2) Map-candidate artifacts
- Generate and maintain a sorted payee-edit log artifact from review decisions instead of writing directly to `payee_map.csv`.
- Capture rows where the final reviewed payee/category differs from the original suggestion or where `update_map` is requested.
- Keep the artifact deterministic so it can be reviewed and copied into `payee_map.csv` manually.

3) Full workflow validation
- Rebuild 2026-03-07 and 2026-03-09 proposed files with the new defaults.
- Walk through normalize → pair → review → upload → sync → reconcile using the real files.
- Verify that bank reconciliation works only on imported bank rows and that statement-balance checks still hold.

4) Remaining cleanup
- Continue manual curation of `mappings/payee_map.csv` and `mappings/fingerprint_map.csv`.
- Resolve any remaining account-name gaps on MAX rows before final upload passes.

---

## Deliverables to Watch

- `data/derived/*/*_norm.csv`
- `data/paired/*/proposed_transactions.csv`
- `data/paired/*/proposed_transactions_reviewed.csv`
- `data/paired/*/ynab_upload.csv`
- `data/paired/*/bank_sync_report.csv`
- `data/paired/*/bank_reconcile_report.csv`
- `outputs/fingerprint_log.csv`
- `outputs/ynab_categories.csv`
- `outputs/map_updates.csv` or successor review-log artifact
- `mappings/payee_map.csv`
