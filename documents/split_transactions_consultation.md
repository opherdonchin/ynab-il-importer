# Split Transactions: Current Understanding and Fix Options

## Purpose

This note is meant to brief another agent on everything currently known about split transactions in this repo, especially in the institutional Family review flow.

The immediate trigger was a Family review row for `Tsomet Sfarim` on `2026-03-28` that appears in the review app as:

- payee: `Tsomet Sfarim`
- category: `Split`
- state: matched existing YNAB transaction

That raised the question: did our code create a split, or did YNAB already contain one?

## Short Answer

- The `Split` label is coming from an existing YNAB transaction.
- Our institutional import/review flow does not currently understand split detail.
- We currently flatten split parent transactions to category `Split` in the normal YNAB export used for matching.
- That means we can match to a YNAB split parent, but we cannot tell what the split categories actually were.
- This is a real modeling gap, not just a UI issue.

## Concrete Example

### Current Family proposal row

File:
- `data/paired/2026_04_01/family_proposed_transactions.csv`

Relevant row:
- transaction id `txn_361ea466bcc241cf`
- date `2026-03-28`
- fingerprint `tzomet sfarim`
- payee `Tsomet Sfarim`
- category `Split`
- `match_status=matched_auto`
- `decision_action=keep_match`

### Matching YNAB row

File:
- `data/derived/2026_04_01/family_ynab_api_norm.csv`

Relevant row:
- YNAB id `5009e023-a6c8-4a30-8fc1-9943c173112f`
- account `Bank Leumi`
- date `2026-03-28`
- payee `Tsomet Sfarim`
- category `Split`
- outflow `205.12`
- cleared `uncleared`
- approved `True`

So this is not being produced by the payee map. It already exists in YNAB as a split parent transaction.

## What the Payee Map Is Doing

File:
- `mappings/payee_map.csv`

Relevant rules for `צומת ספרים` include suggestions like:

- `Tsomet Sfarim / House and stuff`
- `Tzomet Sfarim / House and stuff`
- `Tsomet Sfarim / Birthdays`
- `Tzomet Sfarim / Gifts`

Important point:

- The map does not produce `Split`.
- `Split` is not a mapping output here.
- The map is not splitting transactions.

## How YNAB Split Transactions Are Represented in This Repo

There are currently two different YNAB dataframe builders with different behavior.

### 1. `transactions_to_dataframe(...)`

File:
- `src/ynab_il_importer/ynab_api.py`

This is the normal YNAB export path used by the institutional Family flow.

Behavior:

- emits one row per top-level YNAB transaction
- does not explode subtransactions
- preserves top-level fields like:
  - `payee_raw`
  - `category_raw`
  - `memo`
  - `import_id`
  - `matched_transaction_id`
  - `cleared`
  - `approved`

For a split parent transaction in YNAB:

- `category_raw` stays as `Split`
- child subtransaction categories are not exposed
- child amounts are not exposed

This is why the Family proposal can see `Tsomet Sfarim / Split` but cannot explain the split contents.

### 2. `category_transactions_to_dataframe(...)`

File:
- `src/ynab_il_importer/ynab_api.py`

This path does explode split transactions into subtransactions.

Behavior:

- if YNAB transaction has `subtransactions`, it emits one row per subtransaction
- populates:
  - `parent_ynab_id`
  - `is_subtransaction=True`
  - subtransaction amount
  - subtransaction category
- reuses the parent payee when subtransaction payee is blank

There is already test coverage proving this behavior:

- `tests/test_ynab_api.py`
- `tests/test_io_ynab_as_source.py`

Those tests show a split parent with category `Split` being exploded into child rows like:

- `Pilates`
- `Inflow: Ready to Assign`

So the repo already knows how to represent split detail, but only in some workflows.

## How the Current Institutional Flow Uses YNAB Data

The Family institutional review builder uses the normal top-level transaction export, not the split-aware subtransaction export.

Relevant path:

- `scripts/build_proposed_transactions.py`

High-level flow:

1. normalized bank/card source rows are prepared
2. YNAB rows are prepared via `pairing._prepare_ynab(...)`
3. candidate pairs are formed on account/date/amount, with lineage preferences
4. matched YNAB payee/category become the selected target values

Because the YNAB input is already flattened by the normal transaction dataframe:

- split parents enter the institutional matcher as ordinary target rows
- with category literally equal to `Split`

That means:

- the matcher can say "this source row matches an existing YNAB transaction"
- but it cannot say whether that match is semantically correct at the subcategory level

## What the Review App Currently Does With Split

The review app currently treats `Split` as just another category string.

There is no special split logic in the institutional review model:

- no split-specific blocker
- no split-specific label
- no parent/child inspection
- no split editor

So if a YNAB row says `category=Split`, the review app displays it as a category choice/value and lets it pass like any other matched row.

That is misleading because:

- `Split` is not a meaningful final category for our deterministic review logic
- it hides category allocation details the reviewer may actually need

## Current Limitations

### Limitation 1: Split parents are flattened in the institutional YNAB export

Effect:

- we lose subtransaction detail before matching even begins

### Limitation 2: Matching can accept a split parent as a normal resolved match

Effect:

- a row may appear "handled" because it matched an existing YNAB row
- but we still do not know whether the split details are appropriate

### Limitation 3: We cannot explain what a split consists of

For example, with `Tsomet Sfarim`:

- we know YNAB contains a split parent
- we do not know from the current institutional artifact whether it was split between:
  - `Birthdays`
  - `House and stuff`
  - `Gifts`
  - or something else

### Limitation 4: We do not have a split-aware upload/update strategy

Current upload/update assumptions are mostly about single-category transactions.

Open question:

- if a source row matches a split parent in YNAB, should we:
  - keep it
  - flag it
  - expand it
  - require manual confirmation

The code does not currently encode a deliberate policy.

### Limitation 5: Payee-map suggestions and split history can conflict

Example:

- payee map suggests `House and stuff`
- existing YNAB row is `Split`

Right now, existing YNAB match wins, which means `Split` can override an otherwise helpful concrete default.

## What We Do Know Reliably

- A YNAB split parent is represented at the top level with category `Split`.
- The YNAB API data model includes `subtransactions`.
- This repo already has working code to explode subtransactions in category-based source flows.
- The institutional Family flow is not using that split-aware representation.
- The `Tsomet Sfarim` case is a real example of the gap.

## Likely Root Cause

The repo evolved with at least two different YNAB data-consumption patterns:

- one for general transaction syncing/matching
- one for category-based YNAB-as-source workflows

Split-aware logic exists in the second path but was not carried into the institutional source-vs-YNAB target flow.

So this is probably not a bug in one line of code. It is more of an architectural mismatch:

- "top-level transaction matching" was treated as sufficient
- but split parents break that assumption

## Design Question We Need to Answer

In the institutional flow, what should a split parent mean?

Possible interpretations:

1. A split parent is acceptable as an already-handled transaction.
2. A split parent is inherently ambiguous and should require review.
3. A split parent should be expanded into child rows before matching.
4. A split parent should remain a parent row but expose child details in the UI.

The right answer depends on whether the goal is:

- safe rerunnable import
- full semantic review
- minimal workflow interruption
- future automation

## Fix Options

### Option A: Block or flag `Split` in institutional review

Smallest safe change.

Behavior:

- if matched YNAB category is `Split`, do not treat it as a normal resolved match
- surface a blocker or warning like:
  - `Existing YNAB split transaction`

Pros:

- easy to implement
- avoids false confidence
- makes the gap explicit immediately

Cons:

- does not explain the split
- still requires manual checking in YNAB
- not a complete model

Good if:

- we want immediate safety with low implementation risk

### Option B: Preserve split metadata on the target row

Middle-ground approach.

Behavior:

- keep one parent target row in the institutional proposal
- but enrich it with split metadata such as:
  - `target_is_split`
  - `target_parent_ynab_id`
  - `target_subtransaction_count`
  - maybe serialized child summary

Pros:

- low-to-medium complexity
- keeps current row model mostly intact
- gives reviewer context

Cons:

- still not a true split-aware matcher
- UI and validation logic would need special handling
- serialized summaries can get awkward

Good if:

- we want visibility without refactoring the whole review model yet

### Option C: Expand YNAB split parents into subtransaction target rows in institutional matching

Most principled data-model fix.

Behavior:

- institutional YNAB target preparation becomes split-aware
- a YNAB split parent emits child target rows
- matching operates against subtransactions rather than only parents

Pros:

- semantically accurate
- unifies treatment of split detail with existing category-source logic
- best long-term foundation for automation

Cons:

- bigger change
- matching logic needs redesign because one bank/card source row may match a parent amount while child rows only cover partial amounts
- could require parent-child grouping in review model

Important caveat:

- a single source transaction amount usually corresponds to the split parent total, not to any one child amount
- so naive expansion may make matching harder, not easier

Good if:

- we are willing to redesign matching/modeling around parent-child transaction structure

### Option D: Keep parent-level matching but add split-aware review actions

Behavior:

- matcher still matches to the parent row
- if the target is split, review app provides explicit status and maybe actions like:
  - `keep existing YNAB split`
  - `needs manual YNAB inspection`

Pros:

- preserves existing matching model
- reduces silent ambiguity
- probably easier than full split expansion

Cons:

- still does not deeply model split contents
- may still require out-of-band YNAB inspection

Good if:

- we want a practical workflow fix first

### Option E: Hybrid strategy

Recommended candidate approach.

Behavior:

1. detect split targets explicitly during institutional target prep
2. mark them with split metadata
3. treat split matches as a special review state, not normal matched rows
4. optionally fetch/expose child summaries for inspection
5. defer full split expansion unless later needed

Pros:

- safer immediately
- more informative
- lower risk than full model rewrite
- leaves room for future automation

Cons:

- still not a full split-native model

## Practical Near-Term Recommendation

If the goal is to keep the Family run moving while improving correctness:

1. Add explicit split detection in institutional target prep.
2. Stop treating `category=Split` as a normal resolved matched category.
3. Surface split matches in review as a special state or blocker.
4. Add child-summary metadata if the YNAB API payload already provides it.
5. Decide later whether full split expansion is worth the complexity.

This would let the workflow stop pretending a split is "just another category" while avoiding a major refactor during the operational run.

## Specific Repo Paths to Inspect

### Current top-level YNAB export

- `src/ynab_il_importer/ynab_api.py`
- function `transactions_to_dataframe(...)`

### Existing split-aware YNAB export

- `src/ynab_il_importer/ynab_api.py`
- function `category_transactions_to_dataframe(...)`

### Institutional builder

- `scripts/build_proposed_transactions.py`

### Pairing prep

- `src/ynab_il_importer/pairing.py`

### Current Tsomet Sfarim example

- `data/derived/2026_04_01/family_ynab_api_norm.csv`
- `data/paired/2026_04_01/family_proposed_transactions.csv`

### Relevant tests

- `tests/test_ynab_api.py`
- `tests/test_io_ynab_as_source.py`

## Suggested Questions for Another Agent

1. In YNAB API payloads, what exact fields are available on split parents and subtransactions in the transaction endpoint we already use?
2. Can we enrich `transactions_to_dataframe(...)` to preserve split metadata without changing row cardinality?
3. Should institutional matching ever auto-accept a match to a `Split` target?
4. If a source row matches a split parent total exactly, what is the safest review behavior?
5. Is there a clean parent-child review-row representation that works for both institutional and cross-budget workflows?
6. Would it be better to treat split targets as a dedicated `match_status` or as a normal match with a special blocker?

## Current Best Summary

Split transactions are partially understood by the repo but not by the institutional review flow.

The repo already knows how to explode split subtransactions in category-driven YNAB-as-source workflows. But the Family institutional matching path uses a top-level YNAB export that collapses split parents to category `Split`. As a result, split transactions can currently appear as normal matched rows even though we do not know their internal allocation. The safest next step is to detect and specially handle split targets instead of treating them as ordinary resolved matches.
