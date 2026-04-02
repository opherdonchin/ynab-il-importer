# Review App Architecture Notes

## Purpose

This note captures how the current review app works, where the logic lives, what kinds of bugs and performance issues we are seeing, and what a future consultation/refactor should focus on.

It is written from the perspective of the `2026_04_01` Family acceptance run, where the app and builder were stress-tested against real workflow use.

## Current Role In The Workflow

The review app sits between deterministic proposal building and any mutation of YNAB.

Current flow:

1. Normalize source files into `data/derived/<run_tag>/`.
2. Build review rows into `data/paired/<run_tag>/*_proposed_transactions.csv`.
3. Open the Streamlit review app on that CSV.
4. Save review decisions into `*_reviewed.csv`.
5. Export map-update candidates from reviewed changes.
6. Upload/sync/reconcile outside the app.

The app is the main human control point. Because of that, slowness or confusing state here has an outsized effect on the full workflow.

## Main Files

- `scripts/review_app.py`
  Wrapper that launches Streamlit, chooses a port, writes session logs, and manages quit requests.

- `src/ynab_il_importer/review_app/app.py`
  Main Streamlit UI. Also contains a large amount of derived-state logic, filtering, group behavior, form handling, and save/reload actions.

- `src/ynab_il_importer/review_app/io.py`
  Review CSV loader/saver. Also handles strict review schema behavior and legacy-format rejection.

- `src/ynab_il_importer/review_app/model.py`
  Shared review-model helpers, especially option parsing, transfer logic, fingerprint propagation, and category normalization.

- `src/ynab_il_importer/review_app/state.py`
  Derived-state helpers: row kind, primary state, search text, filter application, uncategorized/missing-category detection, common-value helpers.

- `src/ynab_il_importer/review_app/validation.py`
  Row validation, allowed decisions, connected-component checks, blocker labels, and review-state application.

- `scripts/build_proposed_transactions.py`
  Institutional builder. Produces the review rows the app consumes.

- `src/ynab_il_importer/review_reconcile.py`
  Carries saved review decisions forward onto a rebuilt proposal.

## Core Review Row Model

The app now works against a source/target review schema rather than the old proposal shape.

Important concepts:

- `source_*`
  The imported source-side transaction fields.

- `target_*`
  The existing YNAB-side transaction fields.

- `*_selected`
  The working human-reviewed selections.

- `match_status`
  Examples: `source_only`, `target_only`, `ambiguous`, `matched_auto`, `matched_cleared`.

- `decision_action`
  Examples: `keep_match`, `create_target`, `delete_target`, `ignore_row`, `No decision`.

- `reviewed`
  Means the row is considered settled in-memory, not just edited.

## How The App Computes State

The app does not store a separate compact state model. Instead, on each Streamlit rerun it recomputes a derived layer from the current dataframe.

Important derived concepts:

- blocker
  Computed from row validation plus connected-component consistency.

- primary state
  `Fix`, `Decide`, or `Settled`.

- row kind
  Human-friendly label derived from `match_status`.

- changed / unsaved / saved
  Computed by comparing current dataframe vs original/base copies.

- grouped view
  Groups rows by fingerprint, then renders group-level controls and row-level editors.

## Recent Issues Confirmed In Real Use

### 1. Historical `target_only` rows were surfacing as active review work

Problem:
- Old YNAB transactions, including reconciled January `bit` and `roasters` rows, were still surfacing in Family review.
- These were not current review tasks. They were historical YNAB rows with no current source counterpart.

Cause:
- The builder emitted all institutional `target_only` rows into the review surface.
- It also gave them the invalid default action `create_source`, which is not allowed for institutional rows.

Effect:
- Historical rows polluted fingerprint groups.
- The app showed false `Fix` states driven by `Institutional source mutation`.

Current fix:
- Reconciled/cleared `target_only` institutional rows are auto-settled as reviewed `ignore_row`.
- Transfer counterpart `target_only` rows are also auto-settled.
- Resume reconciliation now preserves these new auto-settled rows instead of letting stale reviewed files overwrite them.

### 2. Grouped view was rendering full fingerprint groups instead of filtered groups

Problem:
- Hidden/settled rows still reappeared inside an otherwise active fingerprint group.

Cause:
- The grouped view filtered the fingerprint list correctly but then expanded each fingerprint from the full dataframe.

Effect:
- One live row could drag a large historical group back into view.

Current fix:
- Grouped rendering now uses filtered row indices only.
- Group-level apply actions are constrained to the visible filtered subset.

### 3. Transfer category semantics were overloaded

Problem:
- Blank category could mean either "missing category" or "no category required".

Cause:
- Transfer handling relied on implicit blank-category behavior.

Current fix:
- Explicit `None` now means "no category required".
- Transfers using `Uncategorized` normalize to `None` where appropriate.

### 4. Saved review reconciliation could reintroduce stale defaults

Problem:
- Rebuilding a proposal and then resuming from a saved reviewed file could bring back old builder defaults.

Current fix:
- If a rebuilt row is already auto-settled in the new proposal and the old reviewed row was not actually reviewed, the new auto-settled row wins.

## Performance Problems

### Full Streamlit rerun model

Every widget interaction reruns the page. This is especially noticeable when:

- toggling "Show all categories"
- editing grouped rows
- moving between pages
- expanding large groups

This is the biggest reason the app feels clunky even when correctness is okay.

### `app.py` is doing too much

`app.py` currently mixes:

- CLI init
- session bootstrap
- derived-state caching
- filter logic
- group state logic
- row editor rendering
- save/reload behavior
- review state transitions

That makes it harder to optimize, harder to test, and easier for UI changes to break state behavior.

### Too much work happens per rerun

Even with some caching, the app still does heavy repeated work:

- recomputing derived state over the full dataframe
- building option summaries repeatedly
- rendering a lot of rows and controls
- computing group-level summaries from dataframes rather than a lighter indexed model

### Fingerprint grouping is convenient but blunt

Fingerprint groups are useful for repetitive mapping work, but they are not the same thing as review components or transaction decisions.

That leads to confusion when:

- one fingerprint spans many dates and many historical rows
- one fingerprint contains both active and settled rows
- a fingerprint maps to multiple legitimate categories by amount/context

## Immediate Technical Directions

### 1. Split data/state logic from UI rendering

Recommended split:

- `review_app/session.py`
  Session bootstrap and saved/base/current frame handling

- `review_app/derived.py`
  Primary state, row kind, blocker, save state, search state, group summaries

- `review_app/grouping.py`
  Fingerprint grouping and group-level metrics

- `review_app/views/row.py`
  Row view rendering

- `review_app/views/group.py`
  Group view rendering

This would make behavior easier to test without Streamlit.

### 2. Introduce a lightweight indexed view model

Instead of repeatedly slicing full dataframes in UI code, precompute:

- visible row ids
- visible group ids
- group -> visible row ids
- per-group dominant state
- per-group summaries

This should reduce repeated dataframe work and make grouped rendering more predictable.

### 3. Treat "historical settled" as a first-class concept

We now have `matched_cleared` and auto-settled `target_only` rows. That should be formalized rather than inferred ad hoc.

Possible next step:

- add an explicit `auto_settled_reason`
- make filters and summaries aware of that reason
- keep these rows out of normal review by default

### 4. Reduce widget count in grouped mode

A large part of the perceived slowness is simply how many widgets Streamlit needs to build.

Possible approaches:

- defer row details until expansion
- avoid building row-level editors for collapsed groups
- move some controls into modal/detail subviews
- paginate more aggressively inside groups

## Testing Gaps

We have decent unit coverage for data/state rules, but weak coverage for real interactive behavior and no performance budget.

Current strengths:

- schema loading/saving tests
- validation tests
- proposal-builder tests
- review reconciliation tests
- some Streamlit `AppTest` coverage

Current gaps:

- no browser-level timing measurements
- no regression tests for "interaction feels slow"
- no tests for number of rendered widgets/groups under common filters
- limited tests for save/resume after rebuild on realistic datasets

## Recommended Test Additions

### Browser simulator

Yes, we should add one.

Best candidate:

- `pytest` + Playwright

Why:

- can launch the real Streamlit app
- can measure time to first render and interaction latency
- can click expanders, toggles, filters, and save/resume paths

Useful first benchmarks:

- launch time on current Family reviewed CSV
- time to switch Grouped <-> Row mode
- time to toggle "Show all categories"
- time to expand a large fingerprint group
- time to apply a group action

### Non-browser perf probes

Also useful:

- benchmark derived-state computation on a representative 700-row CSV
- benchmark grouped summary generation
- count widgets rendered in default Grouped mode

These can be plain pytest benchmarks or timed scripts and will be easier to run in CI than full browser tests.

## Candidate Strategic Directions

### Option A: Keep Streamlit, simplify aggressively

Best if we want low migration cost.

Would require:

- splitting `app.py`
- shrinking rerender scope where possible
- reducing widget count
- relying more on precomputed view models

### Option B: Keep current data model, replace UI technology

Possible targets:

- a small React app
- a local Tauri/Electron shell if desktop matters

Benefits:

- much better control over incremental rendering
- easier fine-grained interaction performance
- clearer state architecture

Cost:

- significantly more engineering work
- more app infrastructure to maintain

### Option C: Hybrid

Keep Streamlit for now, but refactor the logic into UI-agnostic modules so that a future frontend rewrite is much cheaper.

This is the safest medium-term direction.

## Practical Recommendation

Short term:

1. Keep Streamlit for the current operational run.
2. Continue fixing correctness bugs that pollute the review surface.
3. Add browser-level performance tests on the Family review dataset.

Medium term:

1. Split `app.py` into data/state/view modules.
2. Introduce a real grouped view model.
3. Decide whether Streamlit remains good enough once the logic is cleaner.

## Changes Already Made During This Run

- hidden historical grouped rows no longer leak back into filtered groups
- settled rows are hidden by default
- transfer rows now support explicit "no category required"
- cleared exact matches become `matched_cleared`
- reconciled/cleared institutional `target_only` rows auto-settle instead of surfacing as invalid `create_source`
- rebuilt proposals can preserve new auto-settled rows when resuming from an older reviewed CSV
- generic `bit` defaults now point to payee `Bit` and category `Uncategorized`

## Open Questions For Follow-Up

- Should grouped mode group by fingerprint only, or support alternate groupings such as source transaction or connected component?
- Should ambiguous pairs auto-default to `keep_match` when only one candidate still aligns with current selections?
- Should reviewed decisions be stored as a smaller sidecar delta rather than full reviewed CSV snapshots?
- Is Streamlit still the right long-term UI once we add browser-level performance testing?
