## Card Reconciliation Bootstrap Notes — 2026-03-16

This note records the manual/bootstrap decisions used to bring the historical
card accounts up to the `03-07` reconciliation point without large code
changes.

### x9922

Status:
- Reconciled through `2026_02 -> 2026_03`
- March current rows are cleared

Key decisions:
- `CARD:V1` collision issue was discovered in `2026_02.xlsx`.
- Four exact duplicate transaction pairs in the raw source received the same
  `card_txn_id` because the `V1` hash has no occurrence discriminator.
- For the affected duplicate groups, the memo markers were stripped from the
  live YNAB rows so reconciliation could fall back to distinct legacy import
  ids.
- Three genuinely missing March rows were created directly in YNAB:
  - `2026-03-06` `Dabbah` `1162.92`
  - `2026-03-07` `Libra Insurance` `30.00`
  - `2026-03-06` `Netflix` `69.90`
- Two separately settled immediate-charge rows were excluded from the
  `2026_02 -> 2026_03` monthly payment cohort:
  - `2026-02-01` `ChatGPT` `62.45`
  - `2026-02-06` `Netflix` `69.90`
- Those two rows were marked `reconciled` directly in YNAB.
- A filtered previous snapshot was written for the transition:
  - `data/paired/previous_max/x9922/2026_02_previous_excluding_separate_settlements.csv`
- Final successful transition used that filtered previous file.

Outstanding technical debt:
- Introduce a `CARD:V2` identity scheme with an occurrence discriminator for
  exact duplicate source rows.
- Encode the “separately settled immediate charges” rule in code instead of
  relying on a filtered previous CSV.

### x7195

Status:
- Reconciled through `2026_02 -> 2026_03`
- March current rows are cleared

Key decisions:
- Historical bootstrap was done with `sync_card_matches.py` first whenever
  possible.
- `2025_09` and `2025_10` synced cleanly except for a small number of genuine
  missing rows.
- For missing historical rows, direct YNAB creations were used with:
  - `import_id = card_txn_id` when possible
  - raw description in memo
  - pragmatic payee/category choices where needed
- `2025_10 -> 2025_11` initially blocked because `2025_11` rows were only
  partly reconciled; the remaining matched November rows were promoted to
  `reconciled` directly in YNAB to stabilize the monthly chain.
- Final two missing March rows were created directly in YNAB using the
  `03-09` proposed mappings:
  - `2026-03-05` `Food order` `157.00`
  - `2026-03-07` `Golda` `83.40`

Notable oddities:
- Some direct `create_transactions` calls returned duplicate-import behavior
  that did not immediately line up with fetched transaction state. In practice,
  the reconciliation report remained the most reliable truth check.

### Reports and Artifacts

Useful success artifacts:
- `data/paired/previous_max/x9922/2026_03_card_reconcile_report_filtered_previous.csv`
- `data/paired/previous_max/x7195/2026_03_card_reconcile_report_after_cleanup.csv`

Useful audit artifacts:
- `data/paired/previous_max/x9922/bootstrap_*`
- `data/paired/previous_max/x7195/bootstrap_*`

### Follow-up Work

- Add `CARD:V2` identity generation for exact duplicate card rows.
- Revisit whether card sync should ever stamp memo markers on rows where the
  same `card_txn_id` appears more than once in the source snapshot.
- Add an explicit modeled rule for immediate charges that are settled
  separately before the monthly statement payment.
