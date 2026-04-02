# Split Handling Implementation Plan

## Status

- Branch: `handle_splits`
- Current phase: Step 1 foundation work is starting
- Goal of this document: guide the staged implementation of split handling in a way that matches the actual repository structure

## Executive Summary

The broad direction is correct:

- authoritative transaction artifacts should move toward Parquet
- the canonical model should be one transaction with optional nested split lines
- Polars should become the main transformation layer
- PyArrow should define schemas and own Parquet interoperability

After reading the code, I would refine the approach in three important ways:

1. **Do not treat every existing CSV as equally authoritative.**
   The repo currently uses flat CSV for several different things: normalized source transactions, YNAB transaction exports, category-source exports, proposed review rows, reviewed app state, upload payloads, debug/helper files, and human-edited maps. Those should not all migrate together. The first Parquet migration should focus on **canonical transaction artifacts**, not every downstream report/work item.

2. **Keep the review app flat in Step 1.**
   The Streamlit app in `src/ynab_il_importer/review_app/` is deeply pandas- and flat-row-oriented. Forcing nested split-aware review state into that UI in Step 1 would make the migration much riskier than necessary. Step 1 should preserve the current flat review-row model and UI semantics.

3. **Exploded subtransaction exports already exist and should become named projections, not separate ad hoc models.**
   `src/ynab_il_importer/ynab_api.py` already has two shapes:
   - `transactions_to_dataframe(...)`: top-level transactions only
   - `category_transactions_to_dataframe(...)`: exploded subtransactions with `parent_ynab_id`
   
   Step 1 should unify those behind one canonical nested transaction artifact and expose both existing shapes as explicit projections.

## Current Code Reality

### Existing split-related behavior

- `src/ynab_il_importer/ynab_api.py`
  - `transactions_to_dataframe(...)` returns **top-level YNAB transactions** as flat pandas rows.
  - `category_transactions_to_dataframe(...)` returns **exploded subtransactions** as flat pandas rows and preserves split lineage via fields like `parent_ynab_id` and `is_subtransaction`.
- `scripts/io_ynab_as_source.py`
  - consumes `category_transactions_to_dataframe(...)`
  - reshapes those exploded rows into a category-source CSV
  - this path already discards parent/sibling split structure beyond what fits in its flat export
- `tests/test_ynab_api.py`
  - already asserts that category transactions explode subtransactions

### Places where flat pandas/CSV assumptions are baked in

- Parsers and normalizers:
  - `src/ynab_il_importer/io_leumi.py`
  - `src/ynab_il_importer/io_max.py`
  - `src/ynab_il_importer/io_ynab.py`
  - similar modules under `src/ynab_il_importer/`
- Matching and workflow builders:
  - `src/ynab_il_importer/pairing.py`
  - `src/ynab_il_importer/cross_budget_pairing.py`
  - `scripts/build_proposed_transactions.py`
  - `scripts/build_cross_budget_review_rows.py`
- Upload path:
  - `src/ynab_il_importer/upload_prep.py`
  - `scripts/prepare_ynab_upload.py`
- Review app:
  - `src/ynab_il_importer/review_app/io.py`
  - `src/ynab_il_importer/review_app/app.py`
  - `src/ynab_il_importer/review_app/state.py`
  - `src/ynab_il_importer/review_app/validation.py`
- CLI/helpers:
  - `src/ynab_il_importer/cli.py`
  - `src/ynab_il_importer/export.py`

### Observed architectural constraints

- There is currently **no Polars or PyArrow usage** in the repo.
- `pixi.toml` currently depends on `pandas`, not `polars` or `pyarrow`.
- `src/ynab_il_importer/export.py` centralizes CSV writing, but there is no equivalent artifact abstraction for Parquet.
- The review app currently treats flat review CSVs as the durable working artifact and resume state.
- Upload prep is explicitly single-category and flat; it does not model split payloads yet.

## Recommended Data Model Direction

### Canonical transaction artifact

The least-disruptive canonical model is:

- one row/object per transaction
- current familiar scalar transaction fields remain top-level where possible
- optional nested `splits` field for split transactions

This preserves compatibility with the current codebase because most current logic already expects scalar transaction fields with names like:

- `date`
- `secondary_date`
- `account_name`
- `source_account`
- `payee_raw`
- `category_raw`
- `memo`
- `outflow_ils`
- `inflow_ils`
- `txn_kind`
- `fingerprint`
- `import_id`
- `ynab_id`
- `cleared`
- `approved`

### Canonical schema recommendation

Rather than designing a highly abstract object model up front, define a schema that is mostly the current flat transaction vocabulary plus nested split support.

Recommended top-level transaction fields:

- artifact metadata
  - `artifact_kind`
  - `artifact_version`
  - `source_system`
- transaction identity
  - `transaction_id`
  - `ynab_id`
  - `import_id`
  - `parent_transaction_id`
- account identity
  - `account_id`
  - `account_name`
  - `source_account`
- timing
  - `date`
  - `secondary_date`
- amounts
  - `inflow_ils`
  - `outflow_ils`
  - optionally `signed_amount_ils`
- main classification fields
  - `payee_raw`
  - `category_raw`
  - `memo`
  - `txn_kind`
- lineage and matching helpers
  - `fingerprint`
  - `description_raw`
  - `description_clean`
  - `description_clean_norm`
  - `merchant_raw`
  - `ref`
  - `matched_transaction_id`
- YNAB state
  - `cleared`
  - `approved`
  - `is_subtransaction`
- nested split data
  - `splits`: `list<struct<...>>`

Recommended nested split element fields:

- `split_id`
- `parent_transaction_id`
- `ynab_subtransaction_id`
- `payee_raw`
- `category_raw`
- `memo`
- `inflow_ils`
- `outflow_ils`
- `import_id`
- `matched_transaction_id`

### Important refinement

For Step 1, the schema should be **split-capable**, not fully split-semantic. It is enough that the canonical artifact can store split children without losing them. Existing matching/review/upload logic may continue consuming flat projections during Step 1.

## Projection Strategy

The repo should explicitly distinguish three layers:

1. **Canonical nested transaction artifact**
   - authoritative
   - Parquet
   - schema-owned by PyArrow

2. **Flat compatibility projections**
   - derived from the canonical artifact
   - used by existing pairing, review-row building, and current upload prep
   - includes:
     - top-level YNAB transaction projection
     - exploded category-source projection
     - normalized source transaction projection

3. **Human-facing/debug projections**
   - optional CSV flat dumps
   - optional JSON nested dumps for split inspection

This keeps one source of truth while preserving current behavior.

## Recommended Module Changes

The initial suggestion to create separate schema/IO/projection modules is directionally good, but the repo does not need too many tiny modules at once. A pragmatic first cut would be:

- `src/ynab_il_importer/artifacts/transaction_schema.py`
  - PyArrow schema definitions
  - artifact version constants
- `src/ynab_il_importer/artifacts/transaction_io.py`
  - read/write canonical Parquet
  - optional CSV/JSON dump helpers
  - path/format detection helpers
- `src/ynab_il_importer/artifacts/transaction_projection.py`
  - canonical -> top-level flat transaction rows
  - canonical -> exploded category-source rows
  - canonical -> other compatibility projections

If that feels too fragmented during implementation, `transaction_io.py` and `transaction_projection.py` can be merged, but the schema definition should stay explicit.

## Step 1 Plan: Change Representation and Plumbing Without Changing Behavior

### Step 1 goal

Introduce a canonical Parquet transaction artifact with nested split support and migrate transaction-processing code toward Polars/PyArrow, while keeping current matching, review, and upload semantics functionally equivalent.

### Step 1 scope boundary

#### Move to authoritative Parquet in Step 1

- normalized bank/card transaction artifacts
- normalized YNAB transaction download artifacts
- normalized category-as-source artifacts

#### Keep flat compatibility outputs in Step 1

- proposed review rows from `scripts/build_proposed_transactions.py`
- reviewed review-app files in `src/ynab_il_importer/review_app/io.py`
- prepared upload CSVs from `scripts/prepare_ynab_upload.py`

#### Keep CSV as human-edited/control artifacts

- `mappings/payee_map.csv`
- category lists and other human-maintained tables
- small helper/debug files where plain text is the point

### Why the review artifact should stay flat in Step 1

This is the biggest correction to the initial proposal.

After reading `src/ynab_il_importer/review_app/app.py` and `src/ynab_il_importer/review_app/io.py`, I do **not** recommend making the review artifact nested or fully Polars-native in Step 1.

Recommended Step 1 review-app stance:

- keep the review-row schema flat
- keep current review semantics unchanged
- optionally add a Parquet mirror later if useful
- allow the app boundary to accept pandas, Polars, Arrow tables, or Parquet-derived flat data in Step 1
- convert once to pandas at the app boundary and continue using pandas internally in Step 1

That preserves the user-facing workflow while upstream transaction artifacts become future-ready.

### Review app dataframe-engine migration plan

The review app needs two separate decisions:

1. what the app accepts at its boundary
2. what the app uses internally for state, filtering, grouping, and validation

Those should migrate in different stages.

#### Step 1

- update the review app IO boundary to accept flat data from:
  - CSV path
  - pandas DataFrame
  - Polars DataFrame
  - Arrow table
- normalize that input once to pandas inside `review_app/io.py`
- keep the current app internals pandas-based

This gives the rest of the pipeline permission to migrate toward Polars earlier without forcing a simultaneous Streamlit rewrite.

#### Step 3

- still keep the UI internals pandas-based while adding split display
- use the Step 1 boundary adapter so the app can render Parquet-derived split-aware projections without changing its internal engine yet

#### Step 4 foundation

- migrate the app-internal state/model layer from pandas to Polars after split display requirements are proven
- do that migration before or alongside the split editor, because editable split state will be more complex than the current flat review rows

This is the stage where the app should fully move from pandas to Polars internally.

### Step 1 file-by-file change inventory

#### `pixi.toml`

- add `polars`
- add `pyarrow`
- keep `pandas` during migration because the review app and some scripts will still need it temporarily

#### `src/ynab_il_importer/ynab_api.py`

- factor out canonical YNAB transaction construction from raw API payloads
- preserve current public behaviors by making:
  - `transactions_to_dataframe(...)` a top-level flat projection wrapper
  - `category_transactions_to_dataframe(...)` an exploded subtransaction projection wrapper
- add helpers to emit canonical nested transaction artifacts before projecting

#### `scripts/io_ynab_as_source.py`

- stop treating the flat category-source CSV as the only real representation
- consume canonical YNAB transaction artifact or canonical constructor helpers
- write authoritative Parquet for the transaction artifact
- continue emitting the current flat category-source shape as a projection for compatibility
- explicitly document that Step 1 still preserves current split-blind category-source semantics

#### Parser/normalizer modules such as `src/ynab_il_importer/io_leumi.py`, `src/ynab_il_importer/io_max.py`, `src/ynab_il_importer/io_ynab.py`

- convert normalized outputs toward Polars or Arrow-backed tables
- keep current normalized column semantics intact
- route authoritative persistence through the new transaction artifact IO layer

#### `src/ynab_il_importer/pairing.py`

- do **not** make pairing split-aware in Step 1
- keep current matching logic conceptually unchanged
- migrate from pandas to Polars only if that can be done without changing semantics
- otherwise, load flat projections from Parquet and keep the algorithm stable first

Recommended implementation bias:

- change the input/output boundary before changing the matching logic

#### `src/ynab_il_importer/cross_budget_pairing.py`

- same stance as `pairing.py`
- keep current flat row semantics
- consume flat compatibility projections from canonical artifacts
- do not introduce split-aware matching yet

#### `scripts/build_proposed_transactions.py`

- replace direct authoritative CSV assumptions in `_load_csvs(...)` and related read paths
- introduce centralized artifact loading so transaction inputs can come from canonical Parquet
- keep the current flat `REVIEW_ROW_COLUMNS` schema in Step 1
- preserve current review-row semantics and matching behavior

#### `scripts/build_cross_budget_review_rows.py`

- same treatment as `build_proposed_transactions.py`
- move transaction loading to centralized artifact readers
- preserve flat review-row outputs

#### `src/ynab_il_importer/upload_prep.py`

- keep current single-category upload semantics
- keep payload construction flat in Step 1
- isolate payload-building logic behind a clearer interface so Step 2 can add split upload later

#### `scripts/prepare_ynab_upload.py`

- replace direct transaction-artifact CSV assumptions with centralized loaders
- continue to emit human-inspectable CSV/JSON upload artifacts in Step 1
- do not introduce split upload behavior yet

#### `src/ynab_il_importer/review_app/io.py`

- keep flat review-row load/save behavior in Step 1
- broaden the input boundary so the loader can accept pandas, Polars, Arrow, or CSV-path inputs
- if upstream proposed data comes from Parquet-derived flat projections, convert at the boundary
- keep legacy CSV translation support until the rest of the migration settles

#### `src/ynab_il_importer/review_app/app.py`, `state.py`, `validation.py`, `model.py`

- preserve existing flat-row behavior in Step 1
- avoid full Polars migration in the Streamlit layer
- only touch the app as needed to read the same flat review rows produced by the new artifact pipeline
- schedule the full internal pandas-to-Polars migration for Step 4 foundation, after split display is working

#### `src/ynab_il_importer/cli.py`

- inventory and replace direct authoritative `pd.read_csv(...)` transaction loading with centralized artifact loading
- keep convenience/report commands CSV-based when they truly operate on projections or user-edited tables

#### `src/ynab_il_importer/export.py`

- do not overload this existing CSV helper to mean “all artifact IO”
- either keep it as CSV/report writing only, or narrow it explicitly
- add new Parquet-capable artifact IO separately

### Step 1 migration sequence

1. Add dependencies and artifact modules.
2. Define canonical Arrow schema and artifact versioning.
3. Implement canonical Parquet read/write.
4. Add projection helpers that reproduce the current flat shapes.
5. Refactor YNAB download code to build canonical transaction artifacts and derive existing projections from them.
6. Refactor parser/normalizer outputs to persist through the artifact layer.
7. Replace direct authoritative transaction CSV reads in pairing/builder scripts with artifact loaders.
8. Preserve current proposed review rows as flat outputs.
9. Preserve current reviewed CSV workflow in the review app.
10. Run equivalence tests and golden-output comparisons before touching semantics.

### Step 1 testing plan

#### Golden output comparisons

Pick representative workflows and compare current flat outputs before/after migration:

- top-level YNAB transaction export
- category-source export from `scripts/io_ynab_as_source.py`
- pairing inputs
- proposed transaction review rows from `scripts/build_proposed_transactions.py`
- prepared upload rows from `scripts/prepare_ynab_upload.py`

Accept only non-semantic differences such as row order or dtype rendering where documented.

#### Schema tests

- validate Arrow schema structure explicitly
- ensure `splits` field exists and round-trips even when empty

#### Projection tests

- canonical transaction artifact -> top-level flat projection
- canonical transaction artifact -> exploded category-source projection
- canonical transaction artifact -> flat inputs expected by pairing/builders

#### Round-trip IO tests

- Parquet write/read preserves values
- split children survive round-trip
- CSV/JSON dumps are derived views, not authoritative stores

#### Review app compatibility tests

- verify Parquet-derived flat review rows still load into the app correctly
- if the app remains pandas-backed internally, test the conversion boundary explicitly

#### Non-regression tests for current split behavior

- existing top-level YNAB export behavior remains unchanged
- existing exploded subtransaction behavior remains unchanged
- upload path remains intentionally single-category only in Step 1

## Step 2 Plan: Standardize YNAB Download and Upload, Including Splits

### Step 2 goal

Make YNAB download and upload speak one coherent transaction model, including split transactions.

### Current reality to fix

- downloads are split between:
  - top-level flat transaction export
  - exploded category-source export
- upload prep only knows how to create non-split single-category transactions

### Step 2 implementation direction

- define one canonical YNAB boundary model based on the canonical transaction artifact
- unify download code so both existing export shapes are projections of the same underlying representation
- make upload preparation capable of reconstructing:
  - a normal single-line transaction payload
  - a split transaction payload with subtransactions

### Step 2 file focus

- `src/ynab_il_importer/ynab_api.py`
- `scripts/io_ynab_as_source.py`
- `src/ynab_il_importer/upload_prep.py`
- `scripts/prepare_ynab_upload.py`
- any API client/write helpers used to submit payloads

### Step 2 design tasks

- define how a canonical split transaction maps to YNAB API payload shape
- define how subtransaction IDs and parent IDs are preserved or regenerated
- decide how edited review data writes back into the canonical artifact before upload
- clarify whether upload should permit partial overwrite of existing split transactions or require full transaction replacement

### Step 2 tests

- top-level YNAB download round-trips to canonical artifact
- split YNAB download round-trips to canonical artifact
- canonical single transaction -> upload payload
- canonical split transaction -> upload payload
- upload prep preserves amounts and parent/child consistency

## Step 3 Plan: Display Split Transactions in the Review App

### Step 3 goal

Show split transactions clearly in the review app without enabling split editing yet.

### Current reality

- the app is flat-row-oriented
- current review cases assume scalar source/target payee/category fields
- grouped/fingerprint views operate on flat rows

### Step 3 implementation direction

Keep the current review flow, but add display-aware structure:

- folded transaction view
  - shows parent transaction summary
  - indicates that the transaction is split
  - shows count of split lines and amount total
- expanded transaction view
  - lists split lines beneath the parent
  - shows payee/category/amount/memo per split line

### Display requirements

- source side can show whether the source transaction is split
- target side can show whether the existing YNAB transaction is split
- ambiguous/matched/source-only states remain understandable at the parent transaction level
- grouped views should group by the parent transaction identity, not by independent split child rows unless explicitly exploded

### Metadata the UI will need

- parent transaction id
- split flag
- split count
- split amount totals
- split line list with payee/category/memo/amount
- enough lineage to show whether source and target are both split, only one is split, or neither is split

### Step 3 testing

- app loads transactions with and without splits
- folded and expanded rendering are stable
- grouped view does not duplicate or miscount split parents
- accept/review flow remains unchanged for non-editable split display

## Step 4 Plan: Add Split Transaction Editing

### Step 4 goal

Allow the review workflow to create, edit, and remove split structure.

### Minimum editor capabilities

- split a non-split transaction
- add split lines
- remove split lines
- edit split line payee
- edit split line category
- edit split line memo
- edit split line amount
- validate that split totals equal the parent amount
- remove split structure when appropriate

### Data model requirements

- review-state model must be able to hold in-progress edited split lines
- canonical artifact must distinguish:
  - original source state
  - current target state
  - reviewed/edited state destined for upload
- split edits must survive save/resume

### Validation rules

- sum of child inflow/outflow must equal parent total
- each child line must satisfy payee/category requirements as applicable
- transfer semantics remain valid at split-line level where allowed
- no upload proceeds with inconsistent split totals

### Write-back direction

- edited split state writes back to the authoritative transaction artifact
- flat review rows become a view or façade over that edited state rather than the full authoritative model

### Testing

- create split from non-split transaction
- edit existing split transaction
- remove split structure
- save/resume preserves edits
- upload payload reflects reviewed split edits exactly

## Major Risks

- The review app is already performance-sensitive and stateful. Pulling too much of the nested model into Streamlit too early could destabilize the workflow.
- Some scripts currently blur the line between authoritative artifacts and convenience CSVs. That must be untangled before the migration can stay coherent.
- Matching and cross-budget pairing are strongly flat and row-based today. Attempting split-aware matching before the representation layer is stable would multiply risk.
- YNAB split semantics may force decisions about how parent-vs-child matching should work earlier than expected.

## Recommended Open Questions Before Implementation

1. **Should Step 1 review artifacts stay CSV-authoritative, or should they become Parquet-backed with a pandas adapter?**
   My recommendation is: keep reviewed app files flat and CSV-authoritative in Step 1, then revisit after the core transaction artifacts are stable.

2. **Do we want one canonical schema with many optional fields, or a small family of related schemas?**
   My recommendation is: start with one canonical transaction schema plus optional fields and nested `splits`, because the existing code is already organized around a shared flat transaction vocabulary.

3. **Should parser modules move fully to Polars immediately, or can some emit pandas temporarily and canonicalize at the artifact boundary?**
   My recommendation is: allow temporary boundary conversion where needed, but ensure all authoritative persistence flows through the new Parquet artifact layer.

4. **Should proposed/review work items ever become the same artifact type as canonical transactions?**
   My recommendation is: no. Review rows are workflow artifacts derived from transactions, not the canonical transaction model itself.

5. **What is the exact policy for matching split parent transactions to non-split source transactions and vice versa?**
   This can wait until Step 2/3, but it should be called out early because it will affect matching semantics.

6. **How much performance work should be bundled into the migration?**
   My recommendation is: Step 1 should improve data-layer structure first and only take low-risk performance wins. UI performance work should be a separate tracked concern unless a migration change naturally simplifies it.

## Recommended First Implementation Slice

When implementation begins, the safest first slice is:

1. add `polars` and `pyarrow`
2. add canonical transaction schema + IO modules
3. refactor YNAB download construction behind canonical artifact + projections
4. add equivalence tests for current top-level and exploded YNAB exports

That creates the foundation for the rest of the migration without destabilizing the review workflow first.
