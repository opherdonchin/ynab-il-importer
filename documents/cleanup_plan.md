# Cleanup Implementation Plan — Post-Second-Audit Follow-Up

Branch: `code-review-refactor` after commit `b12d2e3`
Updated: 2026-03-30

This plan covered the remaining items from the second hostile audit.
That follow-up pass is now complete locally.

## What Is Done

All critical and moderate items from the original cleanup plan and both hostile audit passes:
- ✅ Safe boolean handling (`safe_types.py`, all CSV-backed review/upload paths)
- ✅ Component precomputation (single-sourced in `validation.py`)
- ✅ Generation-counter caching (non-mutation reruns skip all expensive computation)
- ✅ Component map passed to `apply_review_state` (no redundant traversal)
- ✅ Fingerprint groupby precomputation in grouped mode
- ✅ `changed_mask` NaN-comparison bug fixed
- ✅ 9 business logic functions extracted from `app.py`
- ✅ Performance regression test (500-row blocker + cache verification)
- ✅ IO round-trip tests
- ✅ strict `--approved` CLI parsing with focused test coverage
- ✅ live `documents/review_app_workflow.md` added for README workflow links
- ✅ invalid/truncated rows removed from `mappings/account_name_map.csv`
- ✅ `REPOSITORY_LAYOUT.md` updated for `review_app/` package structure and `safe_types.py`
- ✅ component discovery rewritten from repeated BFS expansion to a union-find style pass
- ✅ 210 tests passing
- ✅ Stop condition for "repeated whole-dataframe work" is satisfied

## What Changed In This Follow-Up Pass

### Task A: Keep the perf bound and fix the algorithm instead

The original idea was to loosen the `< 10s` test bound. That was rejected in favor of reducing actual latency. `review_app/validation.py` now builds connected components with a union-find style pass keyed by shared `source_row_id` and `target_row_id`, which cuts the measured 500-row blocker time roughly in half while keeping the test bound at `< 10s`.

Measured focused timings after the rewrite:
- `50` rows: about `0.47s`
- `100` rows: about `0.87s`
- `240` rows: about `1.99s`
- `500` rows: about `4.00s`

### Task B: Restore the missing workflow doc

`documents/review_app_workflow.md` now exists and captures the current review loop, inputs, views, guardrails, and downstream handoff to upload or reconcile flows.

### Task C: Clean up `mappings/account_name_map.csv`

The global account map had two unusable rows:
- `bank,0005,,,`
- `card,1950`

There was no trustworthy repo-local mapping information to complete them, and the loader already discards rows with blank required fields. They were removed as dead data rather than filled with guessed account IDs.

### Task D: Tighten `--approved` parsing

`scripts/prepare_ynab_upload.py` now parses `--approved` through an explicit boolean parser and rejects invalid values instead of silently treating arbitrary strings as `True`.

### Task E: Refresh repository layout docs

`REPOSITORY_LAYOUT.md` now calls out the `review_app/` subpackage boundaries and `safe_types.py` as a shared coercion utility.

## Remaining Follow-Up Ideas

- further reduce mutation-time validation cost by cutting the per-row `iterrows()` blocker-label loop if future datasets get larger
- `_render_row_controls` mutation/rendering split
- `main()` view-mode decomposition
- `scripts/build_proposed_transactions.py` library extraction
- YNAB API rate limiting
- New product features
- Broad deduplication outside the review/upload path
