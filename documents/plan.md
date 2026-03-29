# Active Plan

## Workstream

Branch `code-review-refactor` off `main`.

Current focus:
- address understandability and performance debt identified in the code review (`documents/code_review_prompt.md`)
- all refactor work stays on this branch until validated; `main` remains operational

Previous workstream (Aikido forward updates) is paused but ready to resume on `main`.

## Current Goal

Execute the prioritized refactor plan from the code review:

### Priority 1 (correctness + performance)
- A. Refactor review app around a real state/model boundary — move review semantics out of `app.py`
- B. Remove repeated whole-dataframe component traversals from the rerun path
- C. Replace repeated per-fingerprint full scans in grouped mode
- D. Audit every `astype(bool)` on string-backed review data

### Priority 2 (maintainability)
- E. Pull proposal-generation logic out of `scripts/build_proposed_transactions.py` into `src/`
- F. Collapse duplicate helper families into shared utilities
- G. Make the review-row column contract explicit and singular

### Priority 3 (cleanup)
- H. Reduce duplicated state derivations in the app

## Settled Product Decisions

- Source and target are both editable.
- Persisted selected fields are side-specific only.
- Unsuffixed selected fields are removed from the review CSV.
- `decision_action` stores the row action or default suggestion.
- `reviewed` is the approval gate; reviewed rows cannot carry `No decision`.
- Institutional sources cannot use `create_source`, `delete_source`, or `delete_both`.
- `update_map` becomes `update_maps`.
- Chooser-based manual relinking is deferred.
- Review-app primary state language is:
  - `Fix`
  - `Decide`
  - `Settled`
- Choosing a substantive row action automatically resolves competing rows:
  - matching or create/delete actions auto-set competing rows to `ignore_row`
  - `ignore_row` itself does not propagate
- Upload prep may fall back hidden or missing target categories to live YNAB `Uncategorized`.

See `documents/decisions/` for the schema and design contract behind the unified review model.

## Current Code State

Done:
- unified review-row hard cutover merged into `aikido-workflow`
- YNAB export normalization now runs the shared fingerprint path
- fresh Aikido bootstrap exports normalized under `data/derived/aikido_bootstrap_2026_03_28/`
- Aikido categories rebuilt into `outputs/aikido/ynab_categories.csv`
- bootstrap matching artifacts built under `data/paired/aikido_bootstrap_2026_03_28/`
- Aikido payee map rebuilt from bootstrap matched pairs
- historical unresolved Aikido review rows isolated in:
  - `data/paired/aikido_bootstrap_2026_03_28/historical_unresolved_review_rows.csv`
- forward Aikido backlog review rows isolated in:
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_review_rows.csv`
- Aikido payee-map rules updated from review decisions, including trial-lesson handling and explicit reviewed-map corrections
- review app primary status language changed to `Fix / Decide / Settled`
- review app now shows a 3-color legend for those primary states
- review app filter set now matches triage needs more closely:
  - `State`
  - `Save status`
  - `Row kind`
  - `Action`
  - `Blocker`
  - `Suggestions`
  - `Map updates`
  - `Search`
- row review flow now:
  - marks reviewed explicitly
  - advances to the next row in Row view after successful review
  - supports `Accept all set decisions`
  - keeps non-review actions open in place
- row and group actions now auto-ignore competing rows instead of relying on manual propagation checkboxes
- review detail panels are shared between Row and Grouped views
- category refresh is aligned to the workflow profile and uses the live YNAB category file shape
- hidden categories are excluded from the review-app target category choices
- upload prep now:
  - honors `memo_append`
  - prepares only explicit reviewed `create_target` rows
  - falls back hidden or missing category names to YNAB `Uncategorized`
- Aikido reviewed backlog upload artifacts were prepared successfully:
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_upload.csv`
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_upload.json`
- Aikido reviewed backlog upload was executed successfully:
  - `68` rows uploaded
  - `68` newly saved
  - `0` duplicates
  - `0` matched existing
- fresh post-upload Family and Aikido YNAB exports were normalized under:
  - `data/derived/aikido_baseline_2026_03_29/`
- post-upload dry-run reconcile proved the carry-forward balance anchor:
  - exact anchor month `2025-09-01`
  - full balance difference `0.0`
  - forward window from `2025-11-01` had `68` matched, `0` unmatched source, `0` unmatched target, `0` ambiguous
- historical unresolved Aikido rows were inspected in detail against the live post-upload snapshot
- the broken `2024-07-26` target-side Miles Kessler row was fixed live in YNAB by splitting it into:
  - `Integral dojo -250`
  - `Ying Jin +80`
- live Aikido historical pairing was rebuilt under:
  - `data/paired/aikido_cross_budget_live/`
- using a wider historical comparison window (`date-tolerance-days = 10`) collapses all non-ambiguous historical rows
- the remaining March 2024 duplicate Member Fees ambiguity has been explicitly settled in:
  - `data/paired/aikido_cross_budget_live/history_review_rows_pre_2025_11_settled.csv`
- the anchored cached month report has been frozen at:
  - `data/paired/aikido_cross_budget_live/anchored_reconcile_after_history_upload_month_report.csv`
- the forward cached reconcile has been executed successfully:
  - `68` reconcile updates applied
  - verification rerun shows `updates planned = 0`
  - target rows now read `reconciled = 1046`, `cleared = 0`, `uncleared = 0`
- the Aikido baseline has been packeted locally at:
  - `data/packets/cross_budget/aikido/family__aikido__personal_in_leumi/baseline_reconciled_state/`

Validated recently:
- focused review-app tests
- focused YNAB/fingerprint/payee-map tests
- Aikido payee-map validation
- upload-prep dry run for the reviewed Aikido backlog
- live post-upload cross-budget reconcile dry run
- live historical review rebuild after the July 2024 split
- forward cached reconcile execute + verify pass

## Aikido Baseline Snapshot

Fresh bootstrap window:
- Family Aikido source rows: `2024-01-02` through `2026-03-25`
- Aikido reconciled target history through `2025-10-18`

Post-upload baseline snapshot:
- fresh Family export rows: `6462`
- fresh Aikido export rows: `1071`
- Family `Aikido` slice rows: `240`
- Aikido `Personal In Leumi` rows: `1054`
- target-side `Cleared` rows after upload: `68`

Historical baseline status:
- strict zero-day historical compare after upload:
  - matched pairs: `159`
  - unresolved historical rows: `25`
- widened historical compare (`date-tolerance-days = 10`):
  - matched pairs: `170`
  - unmatched source: `0`
  - unmatched target: `0`
  - remaining open rows: only the March 2024 duplicate Member Fees ambiguity
- settled historical review artifact:
  - `data/paired/aikido_cross_budget_live/history_review_rows_pre_2025_11_settled.csv`
  - final reviewed open rows in that file: `4`
  - outcome: `2 keep_match`, `2 ignore_row`

Forward backlog slice:
- backlog review rows: `68`
- rows with suggested target payee/category from the rebuilt map: `30`
- reviewed backlog rows: `68`
- upload-prep rows prepared: `68`
- uploaded backlog rows: `68`

Forward reconcile status:
- cached forward reconcile from `2025-11-01` is fully executed
- verify pass result:
  - matched `68`
  - unmatched source `0`
  - unmatched target `0`
  - ambiguous `0`
  - updates planned `0`

## Next Steps

1. Hand `documents/cleanup_plan.md` to Codex for implementation (Tasks 0–10)
2. Review Codex output — verify test pass rate, diff size, and correctness
3. After merge, resume Aikido forward updates on `main`

## Code Review Findings Summary

Verified file sizes (lines):
- `review_app/app.py`: 2180 (61 functions, `main()` ~756 lines from L1421–L2177)
- `card_reconciliation.py`: 1507
- `bank_reconciliation.py`: 1741
- `cross_budget_reconciliation.py`: 1116
- `upload_prep.py`: 748
- `scripts/build_proposed_transactions.py`: 1164
- `scripts/build_cross_budget_review_rows.py`: 593

Key confirmed issues:
- `connected_component_mask` duplicated in `state.py` (L226) and `validation.py` (L93), both identical BFS
- `_component_error_lookup` iterates all rows calling `connected_component_mask` per unseen row; `review_component_errors` calls it again internally — O(n²) graph traversals per rerun
- Grouped mode rescans full df per fingerprint: `df[df["fingerprint"]... == fp]` in a loop, plus `iterrows()` to collect options
- 18 occurrences of `astype(bool)` across 8 files, several on CSV string columns (`reviewed`, `hidden`, `source_present`, `target_present`)
- No safe boolean parser exists anywhere in the codebase
- `_normalize_text` redefined in `bank_identity.py`, `bank_reconciliation.py`; inline `.astype("string").fillna("").str.strip()` chains in 15+ locations

## Deferred

- chooser-based manual relinking UI
- broader sync execution for every non-`create_target` action
- richer `update_maps` ergonomics beyond the minimal explicit form
