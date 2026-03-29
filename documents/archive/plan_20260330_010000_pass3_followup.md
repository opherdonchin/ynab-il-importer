# Active Plan

## Workstream

Branch `code-review-refactor` off `main`.

Current focus:
- address understandability and performance debt identified in the code review (`documents/code_review_prompt.md`)
- all refactor work stays on this branch until validated; `main` remains operational

Previous workstream (Aikido forward updates) is paused but ready to resume on `main`.

## Current Goal

Third hostile audit (Pass 3) complete at commit `68bbec5`. All 5 items from Pass 2 FIX LIST addressed correctly. Union-find is verified correct. 210 tests passing. No regressions.

Pass 3 broadened scope to code style, idiom quality, and performance micro-analysis. Found the true per-mutation bottleneck is NOT the graph algorithm (now 9ms with union-find) but pandas per-row overhead: `validate_row` called 2× per row, and `normalize_decision_actions` wraps scalars in 1-element `pd.Series` (measured 1,027× overhead vs scalar string ops). See `documents/hostile_audit_report.md` Pass 3 section.

Current focus:
- address Pass 3 FIX LIST items (15 items, 0 blocking, 3 HIGH)
- merge the cleanup branch into `main` once remaining findings are triaged

Pass 3 FIX LIST (prioritized):
1. HIGH Perf — `validate_row` called 2× per row; precompute row errors once
2. HIGH Perf — `normalize_decision_actions` wraps scalars in pd.Series (1,027× overhead)
3. HIGH Style — `main()` is 739 lines, `_render_row_controls` is 289 lines
4. MEDIUM Perf — `apply_row_edit` ignores available `component_map`
5. MEDIUM Style — `.astype("string").fillna("").str.strip()` repeated 15 times
6. MEDIUM Style — Decision actions as raw strings; should be `StrEnum`
7. MEDIUM Style — `iterrows()` in 5 hot-path call sites
8. MEDIUM Test — `test_prepare_ynab_upload_script.py` covers 2 of 6 valid bool values
9. LOW Perf — `precompute_component_errors` passes full DataFrame for 2-row slices
10. LOW Style — Dead code `accept_defaults_mask()`
11. LOW Style — Circular import via function-body `from ... import`
12. LOW Style — Editable-column list duplicated 3 times
13. LOW Test — Perf test asserts timing but not correctness of output
14. LOW Test — Multi-assertion round-trip test hides individual failures
15. LOW Doc — README `build-payee-map` example missing required args

Cleanup pass completed on `code-review-refactor`:

### Priority 1 (correctness + performance) — FIRST PASS
- A. Refactor review app around a real state/model boundary — completed
- B. Remove repeated whole-dataframe component traversals from the rerun path — completed across reruns
- C. Replace repeated per-fingerprint full scans in grouped mode — completed
- D. Audit every `astype(bool)` on string-backed review data — completed

### Priority 1 (performance) — SECOND PASS
- E. Cache all derived series between non-mutation reruns via generation counter — completed
- F. Pass cached component map to `apply_review_state` to avoid redundant traversal — completed
- G. Add performance regression test with 500-row synthetic dataset — completed (bound retained after algorithmic improvement)

### Priority 2 (maintainability)
- H. Pull proposal-generation logic out of `scripts/build_proposed_transactions.py` into `src/` — not started (out of scope this pass)
- I. Collapse duplicate helper families into shared utilities — done for review/upload path
- J. Make the review-row column contract explicit and singular — improved, not fully closed
- K. Extract 3 remaining business logic functions from app.py to state.py — completed
- L. Add targeted io.py tests — completed

### Priority 3 (cleanup)
- H. Reduce duplicated state derivations in the app — partially improved, app still large and still carries rendering debt

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
- cleanup plan was rewritten to match the real repo state and current branch goals
- baseline review-app wrapper drift was fixed so the test suite is trustworthy again
- shared safe boolean parsing now lives in:
  - `src/ynab_il_importer/safe_types.py`
- unsafe CSV-backed boolean coercion was removed from the main review/upload path:
  - `upload_prep.py`
  - `map_updates.py`
  - `review_app/state.py`
  - `review_app/app.py`
  - `scripts/prepare_ynab_upload.py`
- category snapshot exports now write `hidden` as boolean data instead of `"False"` strings
- connected-component traversal is now single-sourced in `review_app/validation.py`
- component membership and component errors are precomputed instead of recomputed per row in the app blocker path
- grouped review mode now reuses precomputed fingerprint-to-index groupings instead of rescanning the full dataframe per fingerprint
- grouped option aggregation no longer uses repeated per-group `iterrows()` scans for payee/category option collection
- review semantics moved out of `review_app/app.py` into:
  - `review_app/state.py`
  - `review_app/validation.py`
  - `review_app/model.py`
- `review_app/state.py::changed_mask()` now marks rows absent from the baseline as changed
- review-app focused unit coverage was expanded for:
  - blocker derivation
  - primary-state derivation
  - allowed actions
  - competing-row resolution
  - connected-component precomputation
  - search text
  - uncategorized detection
  - safe boolean normalization
- post-cleanup full test suite passes:
  - `200` passed
- hostile-audit fix pass added generation-based caching for derived review-app state in:
  - `review_app/app.py`
- the derived-state cache now reuses component maps between non-mutation reruns
- `apply_review_state()` now accepts a cached component map and no longer recomputes components when one is already available
- the last pure business-logic helpers were moved from `review_app/app.py` into:
  - `review_app/state.py`
- targeted review io tests were added in:
  - `tests/test_review_io.py`
- performance smoke/regression coverage was added in:
  - `tests/test_review_perf.py`
- post-hostile-audit-fix full test suite passes:
  - `208` passed
- second hostile-audit cleanup pass added:
  - real `documents/review_app_workflow.md`
  - strict `--approved` CLI parsing in `scripts/prepare_ynab_upload.py`
  - focused parser coverage in `tests/test_prepare_ynab_upload_script.py`
  - removal of unusable rows from `mappings/account_name_map.csv`
  - updated `REPOSITORY_LAYOUT.md` for the new review-app structure
- component discovery in `review_app/validation.py` now uses a union-find style pass instead of repeated whole-dataframe BFS expansion per component
- focused local blocker timing after the component rewrite measured approximately:
  - `50` rows: `0.47s`
  - `100` rows: `0.87s`
  - `240` rows: `1.99s`
  - `500` rows: `4.00s`
- post-second-audit-cleanup full test suite passes:
  - `210` passed

Validated recently:
- focused review-app tests
- focused YNAB/fingerprint/payee-map tests
- Aikido payee-map validation
- upload-prep dry run for the reviewed Aikido backlog
- live post-upload cross-budget reconcile dry run
- live historical review rebuild after the July 2024 split
- forward cached reconcile execute + verify pass
- full project pytest after cleanup commit
- focused review performance/cache tests
- full project pytest after hostile-audit fix commit

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

1. Commit the second-audit cleanup pass and archive/update this plan
2. Run another hostile audit against the current committed branch state
3. Triage any new findings into:
   - must-fix before merge
   - good follow-up but not blocking
   - rejected / false positive
4. If the audit is clean or all blocking findings are resolved, prepare the branch for human review
5. After merge, resume Aikido forward updates on `main`

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
- duplicated `connected_component_mask` in the review app — fixed
- repeated whole-dataframe component traversals in blocker derivation — fixed across reruns in second pass
- grouped mode rescans full df per fingerprint — fixed in first pass
- unsafe string-backed boolean coercion in the review/upload path — fixed in first pass
- no shared safe boolean parser — fixed
- missing review-app rerun caching for derived state — fixed in second pass
- `_normalize_text` redefined in `bank_identity.py`, `bank_reconciliation.py`; inline `.astype("string").fillna("").str.strip()` chains in 15+ locations

## Deferred

- chooser-based manual relinking UI
- broader sync execution for every non-`create_target` action
- richer `update_maps` ergonomics beyond the minimal explicit form
