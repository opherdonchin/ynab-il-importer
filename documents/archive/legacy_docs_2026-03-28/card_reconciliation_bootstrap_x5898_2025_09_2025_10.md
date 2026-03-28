## x5898 bootstrap notes (`2025_09 -> 2025_10`)

Date: 2026-03-16

### Why this bootstrap was needed

Initial transition run was fully blocked (`122` blocked rows):
- `101` `weak_unique_date_amount` (legacy rows present in YNAB but no card lineage markers)
- `19` `no_date_amount_match`
- `2` `ambiguous_date_amount_candidates`

### What we changed

1. Ran card sync on both snapshots and executed:
- `2025_09`: matched `85`, patched `85`
- `2025_10`: matched `16`, patched `16`

2. Fast bootstrap for remaining blockers:
- Created `19` missing rows for the unmatched set.
- Patched `2` ambiguous duplicate candidates with memo markers.

3. Follow-up fixes after first fast-bootstrap run:
- `7` historical rows still missing due YNAB reporting duplicate `import_id` conflicts against hidden/deleted historical ids.
- Re-created those `7` rows **without** `import_id`, using memo marker lineage only.

4. Duplicate `CARD:V1` collision workaround:
- One duplicate marker was removed for `CARD:V1:2e772f5417b62b65b7fd5fad` so reconciliation could proceed.
- This is a temporary workaround for `CARD:V1` exact-duplicate collisions.

5. State normalization to satisfy transition constraints:
- Downgraded source rows that were already `reconciled` back to `cleared`.
- Set all previous snapshot rows to `reconciled` to avoid mixed previous-state block.

### Final result

`2025_09 -> 2025_10` transition now runs cleanly:
- Previous match: `97/97`, total `-22156.72`
- Source match: `25/25`, total `-3579.06`
- Payment transfer check passed (`2025-09-10`, card `+22156.72` vs bank `-22156.72`)
- Execute patched `9` rows

## Follow-up (`2025_10 -> 2025_11`)

After finishing `09->10`, we moved one step forward:

- Initial `10->11` block was `8` rows (`6` weak + `2` no-date).
- Ran `sync_card_matches` on `2025_11` and patched `7` rows.
- Created the remaining `2` missing source rows.
- `10->11` now has full line matching (`25/25` previous, `9/9` source).

Current blocker is only payment-transfer validation:
- Previous total from `2025_10.xlsx`: `-3579.06`
- Available card payment transfer on `2025-10-10`: `+2417.30`
- Script blocks with: `No card payment transfer found for previous total 3579.06 ILS.`

So the next step is not matching cleanup; it is deciding the correct settled cohort for `2025_10` (or handling separately settled charges similarly to the earlier `x9922` workaround).

### Artifacts

- Reconcile report (final): `data/paired/previous_max/x5898/2025_10_card_reconcile_report_after_fastfix.csv`
- Sync reports:
  - `data/paired/previous_max/x5898/2025_09_card_sync_report.csv`
  - `data/paired/previous_max/x5898/2025_10_card_sync_report.csv`
- Blocked analysis:
  - `data/paired/previous_max/x5898/2025_10_blocked_analysis.csv`
  - `data/paired/previous_max/x5898/2025_10_blocked_cross_account_check.csv`
  - `data/paired/previous_max/x5898/2025_10_remaining_blockers_card_id_hits.csv`
- `2025_11` step artifacts:
  - `data/paired/previous_max/x5898/2025_11_card_sync_report.csv`
  - `data/paired/previous_max/x5898/2025_11_card_reconcile_report.csv`
- Fast-fix backup/actions:
  - `data/paired/previous_max/x5898/bootstrap_2025_09_2025_10_fastfix_backup_20260316_175857.json`
  - `data/paired/previous_max/x5898/bootstrap_2025_09_2025_10_fastfix_actions_20260316_175857.csv`

### Technical debt to revisit

- `CARD:V1` cannot disambiguate exact duplicate rows in the same snapshot.
- We should move to an occurrence-aware card lineage version (`CARD:V2`) later.
- Hidden/deleted historical `import_id` collisions can block create calls for old rows.
