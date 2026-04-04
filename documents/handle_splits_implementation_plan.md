# Split Handling Implementation Plan

## Status

- Branch: `handle_splits`
- Current phase: Steps 1 and 2 are implemented; detailed Step 3 planning is in progress
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
   - used only where an active workflow still genuinely needs them
   - includes:
     - top-level transaction/debug projection where still useful
     - category/account extraction views
     - normalized source transaction projection

3. **Human-facing/debug projections**
   - optional CSV flat dumps
   - optional JSON nested dumps for split inspection

This keeps one source of truth while avoiding the trap of preserving stale flat exports just because they already exist.

## Recommended Module Changes

The initial suggestion to create separate schema/IO/projection modules is directionally good, but the repo does not need too many tiny modules at once. Only keep projection helpers that serve a live workflow purpose.

- `src/ynab_il_importer/artifacts/transaction_schema.py`
  - PyArrow schema definitions
  - artifact version constants
- `src/ynab_il_importer/artifacts/transaction_io.py`
  - read/write canonical Parquet
  - optional CSV/JSON dump helpers
  - path/format detection helpers
- `src/ynab_il_importer/artifacts/transaction_projection.py`
  - canonical -> only the specific projections/extractions that remain useful
  - this module can be narrowed or merged later if it becomes a grab bag of legacy helpers

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

Make the YNAB boundary coherent in both directions while trimming stale flat artifacts instead of preserving them automatically.

Concretely:

- one canonical YNAB transaction object should be downloaded and stored
- category and account-oriented views should be derived from that canonical object only when they still serve a functional purpose
- uploads should operate on canonical transactions, not row-by-row flat approximations
- Step 2 should support regular transaction upload, transfer upload, and new split creation
- Step 2 should not promise in-place modification of existing split composition where the official API does not support it

This is the stage where split transactions stop being passively preserved and start being handled deliberately at the YNAB API boundary.

### Step 2 scope boundary

#### In scope for Step 2

- standardizing YNAB download around the canonical transaction artifact
- standardizing YNAB upload payload construction around canonical transactions
- eliminating or demoting YNAB-facing downloads/reports that no longer serve an active workflow purpose
- defining category extraction as a deliberate operation on canonical transactions
- generalizing upload preparation from single-category rows to transaction-level payload assembly
- verifying split round-trip through the normal YNAB save response

#### Out of scope for Step 2

- changing review-app display behavior for splits
- adding split editing UI
- making matching or review semantics split-aware across the whole workflow
- changing bank/card parser semantics unrelated to YNAB round-trip behavior

The review app can remain flat and mostly split-blind here; Step 2 is about the YNAB-facing boundary and canonical artifact fidelity.

### API facts established before implementation

The official YNAB OpenAPI spec already resolves the main API questions:

- a split transaction is created by using parent `category_id = null` together with `subtransactions`
- split parent responses come back with `category_name = 'Split'`
- `TransactionDetail` responses include a `subtransactions` array with child amount/payee/category/memo fields
- updating `subtransactions` on an existing split transaction is explicitly not supported
- changing `date`, `amount`, or `category_id` on an existing split transaction is explicitly not supported
- `SaveSubTransaction.category_id` is nullable in the schema, but this repo does not need to depend on that looseness

Implications for this repo:

- Step 2 should support creating new split transactions
- Step 2 should not promise in-place edits to existing split composition
- Step 2 validation can require every split line to carry a category, using `Uncategorized` if needed
- split transfer lines should be treated as unsupported in Step 2

### Current code reality to fix

After Step 1, the canonical artifact can preserve split lines, but the YNAB-facing code still behaves like multiple inconsistent models.

#### Download side

In `src/ynab_il_importer/ynab_api.py` there are still two transaction download constructors:

- `transactions_to_dataframe(...)`
- `category_transactions_to_dataframe(...)`

That split should be removed. There should be one canonical transaction constructor, and category extraction should happen after that.

#### Category-as-source export discards family structure

In `scripts/io_ynab_as_source.py`:

- the script currently flattens category-matching rows out of YNAB transactions
- it emits a source-like CSV
- sibling and parent split structure is discarded after filtering

That is acceptable as a temporary bridge, but it should no longer define the YNAB boundary.

#### Uploads are still single-transaction, single-category only

In `src/ynab_il_importer/upload_prep.py`:

- upload prep is still row-oriented
- payload generation is still one reviewed row -> one YNAB payload
- there is no transaction-level assembly step

#### Existing API write wrappers are generic but not split-aware

In `src/ynab_il_importer/ynab_api.py`:

- `create_transactions(...)` and `update_transactions(...)` can technically send whatever YNAB accepts
- but there is no repo-level helper that constructs or validates YNAB split payloads before those calls

So the missing logic is not the HTTP wrapper. The missing logic is the canonical-to-YNAB payload transformation and the validation around it.

#### Verification is parent-only today

`upload_preflight(...)`, `summarize_upload_response(...)`, and `verify_upload_response(...)` in `src/ynab_il_importer/upload_prep.py` all assume:

- one prepared row corresponds to one uploaded transaction
- transfer/category checks happen only at the parent row level
- verification compares scalar parent fields only

That is too narrow once uploads are allowed to carry nested split data.

### Step 2 design principle

The simplest coherent Step 2 design is:

- use one canonical YNAB transaction constructor
- keep only the projections that still serve a current workflow purpose
- treat category extraction as a transformation of canonical transactions, not as a separate YNAB model
- make upload payload assembly transaction-based
- accept temporary review-boundary awkwardness if it keeps the API boundary clean

Step 2 should optimize for clean YNAB object flow, not for preserving every flat compatibility output.

### Downloads and reports: what to keep, demote, or remove

#### Keep

- canonical Parquet YNAB transaction download artifact
- `download_ynab_categories.py`
- JSON payload preview from `prepare_ynab_upload.py`

#### Keep only as transitional compatibility artifacts

- `io_ynab_as_source.py`
- flat category/account review extracts derived from canonical YNAB transactions where an existing flow still consumes them

#### Demote or remove by default in Step 2

- default flat CSV transaction dump from `download_ynab_api.py`
- default CSV upload preview from `prepare_ynab_upload.py`
- `category_transactions_to_dataframe(...)`

### Canonical YNAB boundary model

#### Canonical transaction as the shared contract

Step 2 should formally treat the canonical artifact from Step 1 as the YNAB boundary contract:

- one canonical row per YNAB parent transaction
- optional nested `splits` list

For YNAB-origin transactions specifically:

- `transaction_id` and `ynab_id` identify the parent transaction
- `parent_transaction_id` should remain the parent transaction id for the parent row
- each split line should preserve:
  - `split_id`
  - `ynab_subtransaction_id`
  - `parent_transaction_id`
  - payee/category/memo/amount fields

Useful refinements in Step 2:

- `is_split_transaction`
- `parent_category_is_split`
- stable split-line ordering
- explicit signed split amount
- upload intent metadata such as create/update/no-op

### Step 2 download strategy

#### Core direction

There should be one YNAB transaction constructor and one category extraction path layered on top of it.

Recommended direction:

- make `transactions_to_dataframe(...)` the single canonical YNAB download constructor name if we keep that name
- eliminate `category_transactions_to_dataframe(...)`
- add a general extraction helper that operates on canonical transactions rather than constructing a second YNAB model

That extraction helper should preserve canonical transaction objects, not explode them into unrelated child rows.

#### File-by-file Step 2 download changes

##### `src/ynab_il_importer/ynab_api.py`

- keep raw fetch functions as they are
- consolidate to one canonical transaction constructor
- make `transactions_to_dataframe(...)` mean the canonical download shape if that name is retained
- remove `category_transactions_to_dataframe(...)`
- add a general extraction helper for category-filtered and possibly account-filtered views

##### `src/ynab_il_importer/artifacts/transaction_projection.py`

- do not invest heavily in generalized split explosion helpers just to preserve legacy behavior
- keep this module only if it serves a clear purpose
- if its remaining job is small, collapse or narrow it

##### `scripts/download_ynab_api.py`

- make canonical Parquet the primary output
- make flat CSV optional rather than default if no active workflow requires it

##### `scripts/io_ynab_as_source.py`

- rebase it on canonical transaction extraction
- preserve it as a temporary bridge script only
- allow it to stay somewhat review-boundary-specific for now

##### `scripts/reconcile_cross_budget_balance.py`

- stop depending on `category_transactions_to_dataframe(...)`
- pull source-side category data from canonical transactions plus category extraction

### Step 2 upload strategy

#### Core direction

Uploads should assemble one payload per canonical parent transaction.

There should be a transaction-level upload assembly step that decides:

- regular transaction payload
- transfer transaction payload
- new split transaction payload
- unsupported operation on existing split transaction

#### Supported split behavior in Step 2

Step 2 should support:

- creating new split transactions
- verifying split transactions returned by YNAB

Step 2 should not support:

- in-place mutation of existing split composition
- split transfer lines
- uncategorized split lines at the repo workflow level

Even though `SaveSubTransaction.category_id` is nullable in the API schema, this repo should require every split line to have a category and use `Uncategorized` explicitly if needed.

### Step 2 file-by-file upload change inventory

##### `src/ynab_il_importer/upload_prep.py`

This is the main Step 2 upload refactor point.

The module should be reorganized conceptually into:

- readiness and validation helpers
- row-level compatibility preparation
- transaction-level upload grouping
- payload serialization
- preflight/verification

Recommended additions:

- a helper that groups reviewed rows into upload transaction units
- a helper that decides whether a group is:
  - regular
  - transfer
  - new split
  - unsupported existing-split edit
- a helper that serializes one upload transaction unit into a YNAB payload dict

Recommended compatibility rule:

- keep existing row-level helpers callable during transition
- add new transaction-level helpers beside them
- only retire the old direct row-to-payload path after the split-capable path is verified

##### `scripts/prepare_ynab_upload.py`

- keep JSON as the main payload preview
- make CSV preview optional rather than central
- surface unsupported existing-split-edit cases clearly in dry-run output

##### `src/ynab_il_importer/ynab_api.py`

- add explicit payload-shape helpers only if they truly belong at the API boundary
- otherwise keep the API client thin and let `upload_prep.py` own payload construction

My recommendation:

- keep HTTP wrappers thin
- keep payload construction in `upload_prep.py`
- add small validation helpers in `ynab_api.py` only if they encode stable YNAB-specific rules

### Step 2 create-vs-update policy

This is no longer an open question.

#### Regular transactions

- create: supported
- update: supported

#### New split transactions

- create: supported

#### Existing split transactions

- parent metadata updates may be technically possible in narrow cases, but Step 2 should not rely on that
- changing split composition in place is not supported by the API
- therefore Step 2 should treat reviewed changes to an existing split structure as unsupported

### Step 2 round-trip rules

The plan should make these round-trip guarantees explicit.

#### Download -> canonical

- every non-deleted YNAB parent transaction becomes one canonical parent row
- every non-deleted YNAB subtransaction becomes one nested split line under that parent

#### Canonical -> extracted review/source views

- account/category extractions stay tied to the parent transaction object
- the extraction may annotate which child lines matched a category, but it should not redefine the transaction model

#### Reviewed canonical -> upload payload

- regular canonical transaction becomes one regular parent payload
- new split canonical transaction becomes one payload with parent `category_id = null` and `subtransactions`
- attempted edit of existing split composition becomes an explicit unsupported outcome

#### Upload response -> verification

- parent identity should be checked by `(account_id, import_id)` or transaction id where applicable
- split verification should compare:
  - parent category name `Split`
  - parent amount
  - child count
  - child amount/category/payee/memo structure

Because the official response model includes `TransactionDetail.subtransactions`, Step 2 does not need to assume a post-upload refetch just to verify split creation.

### Step 2 resolved decisions

1. existing split child composition should be treated as non-updatable in Step 2
2. split parent uploads should use `category_id = null` and rely on YNAB’s split semantics
3. split transfer lines are out of scope for Step 2
4. Step 2 upload-capable split state can come from canonical artifacts and synthetic fixtures before Step 4 editing exists
5. split-create verification can use the standard save response because the official response model includes split child detail

### Step 2 migration sequence

1. consolidate YNAB download to one canonical constructor
2. remove `category_transactions_to_dataframe(...)`
3. add canonical category extraction that preserves parent transaction objects
4. rebase `io_ynab_as_source.py` and cross-budget reconciliation on that extraction path
5. add transaction-level upload assembly for regular transactions first
6. migrate the current regular upload path onto that transaction-level path
7. add new-split payload serialization and validation
8. add unsupported-operation handling for existing split edits
9. make JSON the primary dry-run artifact and demote flat CSV previews where they no longer serve a real purpose
10. verify round-trip behavior on representative regular and split examples before touching the review UI

### Step 2 testing plan

#### Download tests

- canonical YNAB constructor preserves nested split structure
- category extraction returns parent transactions with correct match annotations
- scripts that still need flat/debug views can derive them from canonical transactions without redefining the model

#### Upload serialization tests

- regular reviewed transaction -> one regular YNAB payload
- transfer reviewed transaction -> one transfer payload
- new split transaction -> one parent payload with expected `subtransactions`
- split line validation rejects missing categories at repo-workflow level
- existing split edit attempt is surfaced as unsupported

#### Response verification tests

- regular saved transaction verifies as before
- new split transaction verifies parent `Split` status and child structure

#### Script-level integration tests

- `scripts/download_ynab_api.py` writes canonical output and only writes flat CSV when explicitly requested, if that option remains
- `scripts/io_ynab_as_source.py` still serves the temporary bridge workflow without redefining the canonical model
- `scripts/prepare_ynab_upload.py` emits correct JSON for regular and new-split payload scenarios

#### Optional live smoke tests

It is acceptable during implementation to create clearly-labeled test transactions in the real YNAB budget and delete them immediately afterward.

If used, these live tests should:

- use an easy-to-search payee prefix such as `ZZ_SPLIT_TEST`
- record created transaction ids immediately
- delete them at the end of the test, even on partial success when possible

### Step 2 risks

- stale scripts may still assume the existence of flat CSV downloads that no longer need to be emitted by default
- the transitional cross-budget source flow may remain awkward until Step 3
- existing split edit attempts need to fail clearly rather than being silently mis-serialized

### Recommended first implementation slice for Step 2

When Step 2 implementation begins, the safest first slice is:

1. consolidate `ynab_api.py` to one canonical transaction constructor
2. remove `category_transactions_to_dataframe(...)`
3. add a category extraction helper that preserves parent transaction objects
4. migrate one real consumer of category extraction, ideally `reconcile_cross_budget_balance.py`
5. only after that, move `prepare_ynab_upload.py` and `upload_prep.py` onto transaction-level upload assembly

## Step 3 Plan: Display Split Transactions in the Review App

### Step 3 goal

Move the review app itself onto the canonical transaction representation, and use that refactor to enable correct display of split transactions in both folded and expanded forms.

Concretely:

- the app should load canonical review artifacts directly
- the app should save canonical review artifacts directly
- the app should work natively with canonical review rows:
  - one row per review transaction / parent transaction relation
  - nested `source_transaction` and `target_transaction` structs
  - nested split lists inside those transaction structs
- Polars should replace pandas as the main in-app table engine
- the visible review unit should remain the parent transaction, not an exploded split child row

Step 3 is therefore two tightly related pieces:

1. display split transactions properly
2. refactor the app’s internal representation away from flattened pandas rows and onto canonical Polars-backed review rows

### Step 3 scope boundary

#### In scope for Step 3

- canonical review artifacts remain the app’s true input and output
- the app’s internal representation moves from flattened pandas rows to canonical Polars-backed review rows
- the app displays split structure on source and target transactions
- cross-budget review shows why a parent transaction is in scope when the category match came from one or more split lines
- builders and review-schema metadata are extended where needed so the app has enough context to render split detail without inventing it
- the current decision/review workflow remains parent-row based

#### Out of scope for Step 3

- editing split structure
- changing upload semantics beyond what Step 2 already supports
- making matching algorithms split-aware in a new way
- replacing Streamlit

### Current reality after Step 2

The persisted artifacts are now close to the desired model, but the app internals are not.

- canonical review artifacts exist in Parquet via `src/ynab_il_importer/artifacts/review_schema.py`
- review rows now carry nested `source_transaction` and `target_transaction`
- `src/ynab_il_importer/review_app/io.py` still projects canonical review artifacts into a flat pandas shape
- `scripts/build_proposed_transactions.py` and `scripts/build_cross_budget_review_rows.py` already attach canonical source/target snapshots to each review row

Two review flows matter here:

- institutional review
  - source is bank/card-origin data
  - target is a YNAB transaction
- cross-budget review
  - source is itself a YNAB transaction or a category extraction from YNAB
  - target is another YNAB transaction

The main remaining Step 3 gap is display/context:

- the app still renders mostly scalar source/target fields and ignores nested split structure
- cross-budget extraction context is not explicit enough yet to tell the UI which split lines made a parent transaction relevant
- the app still assumes a flattened internal representation almost everywhere
- `app.py` currently computes and renders almost everything inline, so the internal-model refactor needs to happen carefully to avoid making the file even more monolithic

### Canonical internal representation

This is the key Step 3 change.

The app should no longer treat a flattened dataframe as its working transaction model.

The native in-app representation should be:

- one canonical review row per parent transaction relation
- top-level review metadata on that row
- nested `source_transaction` struct
- nested `target_transaction` struct
- nested split lists inside those transaction structs
- Polars as the main table/query/update layer

If the UI needs helper structures for searching, grouping, filtering, summary text, or widget defaults, those should be derived view/index data built from the canonical representation, not a flattened transaction model.

The right mental model is:

- canonical review table as source of truth
- lightweight derived view models for UI operations

rather than:

- flattened transaction rows as source of truth
- canonical objects merely attached for display decoration

### Canonical-through-app contract

- The app should consume the canonical review artifact, not a pre-flattened workflow export.
- The app should keep canonical records in memory as its main working model.
- The app should write canonical review artifacts directly from that model.
- Any derived helper structures should be disposable projections of the canonical in-memory model.

This is especially important because Step 4 will need to present and edit the canonical transaction directly. A flattened Step 3 model would add churn we would soon have to undo.

### Field classification

Step 3 implementation should explicitly classify app-facing fields into three buckets:

1. canonical nested transaction data
   - `source_transaction`
   - `target_transaction`
   - nested `splits`

2. review-control fields
   - selected payees/categories
   - decision action
   - reviewed
   - update maps
   - memo append

3. derived display/search helpers
   - split badges/counts
   - summary text
   - search text
   - source/target context labels
   - inference/progress tags that are purely display-oriented

This classification should drive both persistence and refactoring decisions:

- canonical nested transaction data and review-control fields are part of the real app model
- derived display/search helpers should be recomputed or regenerated from canonical state as needed

### Required review-artifact metadata additions

The current review artifact contains nested source/target transactions, but Step 3 will need a little more relationship context for correct display, especially in cross-budget flows.

Recommended additions to `src/ynab_il_importer/artifacts/review_schema.py`:

- source-side extraction context
  - `source_context_kind`
    - examples: `direct_source`, `ynab_category_extract`, `ynab_parent_category_match`, `ynab_split_category_match`
  - `source_context_category_id`
  - `source_context_category_name`
  - `source_context_matching_split_ids`
- optional target-side context if later needed for highlighting or explanation
  - `target_context_kind`
  - `target_context_matching_split_ids`

These fields should be canonical review-artifact fields, not ad hoc app-only state, because the builder is the right place to know why a row exists.

### Step 3 design direction

Keep the review flow parent-transaction-oriented and make both the data model and the display transaction-oriented:

- the review case should still be one parent transaction case
- split structure should appear as expandable detail within that case
- category extraction should be shown as context on the parent, not as a replacement object
- the internal code should reason over canonical review rows and canonical source/target transactions directly

### Step 3 app architecture target

Recommended in-app layers:

1. canonical review table
   - Polars dataframe
   - one row per review transaction relation
   - top-level review fields plus nested source/target transactions

2. derived indexed/view model
   - visible row ids
   - group membership
   - component membership
   - summary strings
   - search text
   - split badges/counts
   - source/target context text

3. widget state / edit buffer
   - user-edited payee/category/decision values
   - keyed by stable review row id, not dataframe position

Step 3 should introduce those layers explicitly instead of continuing to let one pandas dataframe do all three jobs at once.

### Current flattened assumptions that must change

This is the main refactor inventory for the canonical move.

#### `src/ynab_il_importer/review_app/io.py`

Current flat assumption:

- canonical artifacts are immediately projected to a flat pandas dataframe
- `_transaction_from_flat_row(...)` rebuilds canonical transactions from flattened scalar fields on save
- `project_review_artifact_to_flat_dataframe(...)` is the main app-facing load path

Required Step 3 change:

- loading should return canonical review rows directly, ideally as a Polars dataframe or a small canonical record layer built from it
- saving should update canonical review rows directly, not flatten and then rehydrate
- flatten/rebuild helpers should no longer be the primary app path
- if any compatibility projection survives, it should be debug-only, not the app’s working representation

#### `src/ynab_il_importer/review_app/app.py`

Current flat assumption:

- session state stores `df`, `df_original`, and `df_base` as pandas dataframes
- most rendering code pulls scalar values directly from flattened columns
- row/group navigation, summaries, and edits are all expressed in dataframe/column terms

Required Step 3 change:

- session state should store canonical review data, likely a Polars review table plus a lightweight derived index/view layer
- render helpers should read from canonical source/target transactions directly
- split display should operate on the nested split lists already in the canonical transactions
- edits should target stable canonical review rows, not a flat alias row model

#### `src/ynab_il_importer/review_app/state.py`

Current flat assumption:

- almost every helper expects a pandas dataframe with scalar columns such as:
  - `payee_selected`
  - `category_selected`
  - `fingerprint`
  - `source_row_id`
  - `target_row_id`
- filters, masks, and summary counts are built as pandas Series operations over those columns

Required Step 3 change:

- replace pandas/Series helpers with Polars expressions and/or canonical view-model helpers
- define state over canonical review rows rather than flattened transaction fields
- keep parent-row readiness/blocker semantics, but compute them from canonical review rows
- derive split-aware view metadata without flattening transactions

#### `src/ynab_il_importer/review_app/validation.py`

Current flat assumption:

- validation operates on pandas Series rows
- `_selected_value(...)` looks up scalar `*_selected` columns on flattened rows
- connectivity and contradiction logic is dataframe-column based

Required Step 3 change:

- validation should operate on canonical review records / Polars-backed row accessors
- decision validation should remain parent-row based
- connectivity can still rely on `source_row_id` and `target_row_id`, but through canonical review rows rather than flattened aliases
- reviewed-state application should mutate canonical review rows, not pandas copies

#### `src/ynab_il_importer/review_app/model.py`

Current flat assumption:

- helper logic mutates pandas dataframes directly
- fingerprint propagation writes straight to `payee_selected`, `category_selected`, `reviewed`, and `decision_action` columns

Required Step 3 change:

- rewrite propagation and competing-row-resolution helpers against canonical review rows
- keep fingerprint-based workflow behavior, but apply it through canonical row updates
- remove direct pandas mutation as the model-layer API

#### `src/ynab_il_importer/review_reconcile.py`

Current flat assumption:

- reconciliation between rebuilt proposals and reviewed state is keyed off flattened decision columns
- it copies scalar selected fields between pandas dataframes

Required Step 3 change:

- reconcile reviewed canonical rows directly
- preserve edited review decisions in canonical form
- avoid depending on flattened aliases as the long-term reviewed-state model

#### `scripts/build_proposed_transactions.py` and `scripts/build_cross_budget_review_rows.py`

Current flat assumption:

- builders already emit canonical nested source/target transactions, but the app-facing semantics are still described partly in flat-row terms

Required Step 3 change:

- keep one review row per parent transaction relation
- add the extra source/target context fields needed for canonical app rendering
- do not add app-specific flattened helper columns just to satisfy the UI

#### Tests

Current flat assumption:

- many review-app tests still assume flat pandas review rows are the main working model

Required Step 3 change:

- shift tests toward canonical review artifacts and canonical in-app state
- keep behavior-level assertions, but stop treating flat projection as the main contract

### Step 3 display model

Recommended row display modes:

- folded transaction view
  - shows parent transaction summary
  - indicates that the transaction is split
  - shows split count and total amount
  - shows whether the source-side category extraction matched:
    - the parent category
    - one or more child lines
- expanded transaction view
  - lists split lines beneath the parent
  - shows payee/category/amount/memo per split line
  - highlights which lines are relevant to the current review case

### Folded summary requirements

The row or group summary should remain readable even before expansion.

Recommended summary additions:

- a split badge on source and target sides independently
- split count for each side when non-zero
- short scope text for cross-budget source rows, for example:
  - `parent category match`
  - `2 split lines matched category`
- preserve the current primary-state/readiness emphasis so split display does not drown out review priority

### Display requirements

- source side can show whether the source transaction is split
- target side can show whether the existing YNAB transaction is split
- ambiguous/matched/source-only states remain understandable at the parent transaction level
- grouped views should group by the parent transaction identity, not by independent split child rows

More specifically:

- institutional review should show:
  - source transaction summary
  - target YNAB parent summary
  - expandable split detail on the target when present
- cross-budget review should show:
  - source YNAB parent summary
  - target YNAB parent summary
  - extraction context indicating why this source transaction is in scope for the current category-based workflow

### Expanded detail requirements

Expanded row detail should render split structure clearly but read-only:

- parent transaction summary first
- split section beneath it only when `splits` is non-empty
- one row per split line showing:
  - amount
  - payee
  - category
  - memo
  - split identifier where useful for debugging
- visual highlight for matched split lines in YNAB-as-source review when the source row was included because of child-line category matches
- clear fallback text when the transaction is not split

### Grouped-view behavior

Grouped/fingerprint view should continue to group parent review rows exactly as it does today.

Step 3 should not introduce split-line grouping.

Specific grouped-view rules:

- one parent review row remains one row in the group
- split lines are only visible inside the row expander
- group counts and primary-state summaries must not multiply because a parent has several split lines
- `Accept` / `Apply` actions stay at the parent-row level

### Metadata the UI will need

- parent transaction id
- split flag
- split count
- split amount totals
- split line list with payee/category/memo/amount
- enough lineage to show whether source and target are both split, only one is split, or neither is split
- extraction context for YNAB-as-source cases:
  - matched category name/id
  - whether the match came from the parent category or child lines
  - which child lines matched

### File-by-file Step 3 change inventory

#### `src/ynab_il_importer/artifacts/review_schema.py`

- add the explicit source/target display-context fields listed above
- keep `source_transaction` and `target_transaction` as canonical nested objects
- do not add editable split state yet

#### `scripts/build_proposed_transactions.py`

- keep emitting one review row per parent transaction relation
- populate any new source/target context metadata needed for display
- for institutional rows, populate enough context to show whether the source or target side is split, even if no special match highlighting is needed

#### `scripts/build_cross_budget_review_rows.py`

- this is the builder that most needs Step 3 metadata work
- when a source transaction is included because a split child matched the chosen category, record:
  - that the source context is a split-category match
  - which split ids matched
  - which category id/name drove inclusion
- when the parent category itself matched, record that distinctly

This prevents the app from needing to guess why a canonical parent transaction is on screen.

#### `src/ynab_il_importer/review_app/io.py`

- stop treating flat projection as the main app boundary
- load canonical review artifacts into the native in-app canonical representation
- save canonical review artifacts directly from the native in-app representation
- if helper projections are still useful for debugging, keep them clearly secondary
- preserve new source/target context metadata across save/resume
- ensure untouched rows round-trip without losing nested split structure or context annotations

#### `src/ynab_il_importer/review_app/app.py`

- add source/target transaction summary render helpers instead of continuing to inline every field
- add read-only split detail renderers for source and target transactions
- use canonical source/target transaction objects directly rather than rebuilding transaction views from scalar flat fields
- keep existing editing widgets and decision controls attached to the parent review row
- keep widget count under control by rendering split details only inside expanded rows
- replace pandas dataframe session state with canonical Polars-backed session state plus a derived view/index layer

Recommended refactor bias:

- move split-rendering helpers into small focused functions, even if they stay in `app.py` initially
- avoid mixing split-detail rendering with selection-widget mutation logic any more than necessary

#### `src/ynab_il_importer/review_app/state.py`

- replace pandas mask/Series logic with canonical Polars-backed state derivation
- keep primary-state logic parent-row based
- ignore split children for readiness/blocker semantics in Step 3
- add split-aware derived helpers only as view-model outputs, not as flattened transaction inputs

#### `src/ynab_il_importer/review_app/validation.py`

- replace pandas row validation with canonical review-row validation
- keep validation parent-row based for Step 3
- do not introduce split-line editing or split-balance validation yet
- ensure any new context/display fields are treated as context, not as decision semantics

#### `src/ynab_il_importer/review_app/model.py`

- replace dataframe-mutation helpers with canonical row/table mutation helpers
- keep transfer/category normalization logic, but decouple it from pandas APIs

#### `src/ynab_il_importer/review_reconcile.py`

- migrate from flat reviewed-row reconciliation to canonical reviewed-row reconciliation
- preserve occurrence-based matching, but on canonical review rows

#### `tests/test_review_io.py`

- add round-trip coverage for the new context fields
- add canonical load/save tests for the native app representation
- demote flat projection tests to compatibility coverage only if they still exist

#### `tests/test_build_proposed_transactions.py`

- add coverage that builder output preserves split source/target snapshots and any new context metadata

#### `tests/test_build_cross_budget_review_rows.py`

- add explicit coverage for:
  - parent-category source matches
  - split-child source matches
  - matching split ids/category context carried into the review artifact

#### `tests/test_review_app.py` and `tests/test_review_app_wrapper.py`

- add app-level tests that canonical review rows load, render, and remain reviewable
- keep tests focused on behavior, not Streamlit implementation details
- verify grouped mode does not multiply or miscount parent rows because of splits

### Step 3 internal representation

The app should no longer use a flattened pandas dataframe as its main internal representation.

Recommended internal representation:

1. canonical review table
   - Polars dataframe
   - one row per review transaction relation
   - top-level review fields such as:
     - `review_transaction_id`
     - `decision_action`
     - `reviewed`
     - `update_maps`
     - `source_row_id`
     - `target_row_id`
   - nested:
     - `source_transaction`
     - `target_transaction`
   - context fields such as:
     - `source_context_kind`
     - `source_context_category_name`
     - `source_context_matching_split_ids`

2. derived view/index structures
   - visible row ids
   - group membership
   - connected-component membership
   - search strings
   - summary labels
   - split badges/counts
   - context labels for source/target matching

3. widget/edit state
   - keyed by stable review row id
   - layered over canonical review rows rather than replacing them

This means Step 3 should not require new external callers or new flattened artifacts. It is a real internal refactor of the app.

### Step 3 save/resume rules

- saving from the app must write a canonical review artifact
- resuming must reconstruct the same nested source/target transactions and the same split-display context
- any flat compatibility CSV output should be secondary and debug-only if it survives at all
- if a user changes only payee/category/decision fields, the canonical source/target transaction snapshots should remain unchanged unless Step 4 later introduces explicit transaction editing

### Step 3 phased implementation plan

Because this is now a larger refactor, it should be done in explicit phases. In reality these phases are not four clean handoffs. They are four layers of work with different dependency shapes:

- `3A` is foundational and has to exist before the app can sensibly move inward.
- `3B` is the main refactor and is the real critical path now.
- `3C` can start before `3B` is finished, but it should not outrun `3B`; split display should be built on top of the canonical model as it becomes real, not on top of temporary flat scaffolding.
- `3D` is intentionally last and should only begin after `3B` and the useful parts of `3C` have stabilized.

So the reality of the plan is:

- `3A` is mostly in place, though a few edges may still get revisited as `3B` proceeds.
- `3B` is where the main work now sits.
- `3C` should continue in parallel only where it directly reinforces the canonical refactor.
- `3D` is cleanup and should stay deferred.

#### Phase 3A: establish the canonical app boundary

Purpose:

- stop treating flat projection as the primary app boundary
- load/save canonical review artifacts directly in a Polars-native path
- identify the minimum derived helper fields needed for app display and filtering

What completion means:

- canonical review artifacts are the real app input/output
- builders populate enough source/target context for split-aware display later
- `review_app/io.py` can load canonical artifacts natively and produce Polars review tables
- the app can carry canonical review data without immediately flattening it into the only working model

Reality right now:

- mostly complete
- may still reopen in small ways if later `3B`/`3C` work exposes missing context or a bad boundary choice

Completed work:

1. canonical review schema was extended with explicit source/target context fields
2. institutional and cross-budget builders now populate those context fields
3. `review_app/io.py` now has a canonical Polars load path
4. the app now keeps canonical review tables in session state and saves from the canonical object

Remaining work before `3A` can be treated as fully closed:

1. audit any remaining app load/reload/save edges that still quietly assume flat review rows first
2. finish demoting flat review projection from “default mental model” to “temporary derived helper”

#### Phase 3B: make app state, validation, and model canonical-aware

Purpose:

- remove flattened transaction representation from the app core
- replace pandas with Polars in the app’s main state/model/validation flow
- keep review semantics parent-row based while changing the substrate

What completion means:

- app state and derived state are driven primarily by canonical review tables plus lightweight helper/index data
- review semantics do not depend on flattened transaction columns as the authoritative model
- validation/model/state code no longer assumes flat pandas review rows are the core data structure

Reality right now:

- in progress
- this is the main Step 3 workstream and the main thing that should guide implementation order

Completed work:

1. `review_app/state.py` now has canonical/Polars helper derivations for split flags, counts, and display fields
2. app session state now carries canonical review tables alongside the editable view
3. canonical review data now drives search/filter text when available
4. summary construction has started moving onto canonical helper data

Remaining work:

1. refactor more core derived-state logic away from flat-only assumptions
2. refactor grouped metadata and/or component/precompute logic onto canonical/Polars-backed state
3. move `validation.py` away from depending on flattened transaction columns as the real substrate
4. move `model.py` away from direct flat-dataframe mutation assumptions
5. migrate `review_reconcile.py` and any remaining app-adjacent helpers that still assume flattened reviewed rows

This phase is not “after `3A` and before `3C`” in a strict sense. It is the core refactor that `3C` should progressively lean on.

Preferred order from here:

1. grouped metadata and component/precompute logic
2. validation/model layer
3. reconcile/resume path

#### Phase 3C: add split display

Purpose:

- use canonical source/target transactions to show folded and expanded split detail
- make cross-budget source context understandable when inclusion came from child split lines

What completion means:

- folded row/group summaries clearly indicate split status
- expanded views show parent transaction detail plus nested split lines cleanly
- source-side extraction context is understandable in cross-budget cases
- grouped mode remains transaction-row-oriented, not split-line-oriented

Reality right now:

- started lightly
- should continue only where it helps prove or exercise the canonical app model
- should not become a separate UI branch built on transitional flat assumptions

Completed work:

1. split/context badges and counts now appear in row/group summaries and row details
2. summary text can already fall back to canonical nested transaction and split content

Remaining work:

1. add explicit read-only split rendering blocks in `app.py`
2. render target-side split detail clearly in expanded rows
3. render source-side split/context detail clearly in expanded rows
4. show matching-split/context cues for cross-budget source extraction
5. verify grouped mode remains readable when split parents are visible

This phase is partly parallel to `3B`, but not independent of it. The intent is:

- do enough `3C` work to validate that the canonical transaction model really supports the UI we want
- avoid polishing split presentation on top of code that `3B` is about to replace

Preferred order from here:

1. target-side expanded split rendering
2. source-side expanded split/context rendering
3. folded-summary polish and grouped-mode cleanup

#### Phase 3D: cleanup after the canonical refactor

Purpose:

- remove or demote stale flat compatibility helpers where safe
- keep only the scalar fields that are true review controls or relation metadata
- leave the codebase ready for Step 4 split editing

What completion means:

- the canonical review object is unambiguously the app’s working model
- flat compatibility helpers are either gone or obviously secondary
- the codebase is ready for a split editor without another internal model rewrite

Reality right now:

- not started
- intentionally deferred until the canonical refactor is stable enough that cleanup will not just be rework

Planned work:

1. remove or demote no-longer-needed flat compatibility columns/helpers
2. simplify remaining app surfaces around the field classification above
3. only after the canonical refactor is stable, consider cleanup/decomposition inside `app.py`

### Step 3 testing

- app loads canonical review artifacts with and without split transactions
- canonical review artifacts round-trip through native canonical app load/save without losing split lines or display context
- canonical in-app state remains one row per review transaction relation
- canonical edit/review decisions persist through save/resume and reconcile
- folded summaries show split status without changing decision behavior
- expanded rendering shows split lines correctly on source and target sides
- grouped view does not duplicate or miscount split parents
- accept/review flow remains unchanged for non-editable split display
- cross-budget YNAB-as-source cases remain parent-transaction-oriented even when the source category match comes from a split child
- save/resume preserves nested split context and does not regress to flattened compatibility behavior

### Step 3 risks

- `app.py` is already large and performance-sensitive; combining a representation refactor with UI changes raises integration risk
- replacing pandas with Polars inside the app will touch a large amount of derived-state and validation logic
- cross-budget source extraction context is easy to under-specify; if the builder does not record why a parent row is in scope, the UI will be forced to guess
- the current parent-row validation logic could become harder to reason about if split display leaks into decision semantics too early
- Streamlit widget count could grow noticeably if split detail renders eagerly instead of only when expanded

### Recommended first implementation slice for Step 3

The safest first Step 3 slice is:

1. extend the canonical review schema with source extraction context
2. populate that context in `scripts/build_cross_budget_review_rows.py`
3. replace the app load/save path in `review_app/io.py` with a native canonical Polars path
4. refactor one contained logic layer, preferably `review_app/state.py`, off flattened pandas assumptions
5. only then add the first read-only split display, starting on the target side

## Step 4 Plan: Add Split Transaction Editing

### Step 4 goal

Allow the review workflow to create, edit, remove, review, save, resume, and upload
split structure while preserving the current parent-transaction-oriented matching model.

Step 4 should not redesign matching. It should extend the review model so that a single
review row can carry:

- the current source-side split snapshot
- the current target-side split snapshot
- the reviewed source-side split selection, if the user edits it
- the reviewed target-side split selection, if the user edits it

The app should remain mostly flat and dataframe-oriented. Split hierarchy should stay
confined to the split-list columns and to the editor/rendering paths that actually need it.

### Step 4 working assumptions established from the current codebase

- The canonical review artifact is already mostly flat in
  `src/ynab_il_importer/artifacts/review_schema.py`.
- Source and target split snapshots already exist as top-level nested columns:
  - `source_splits`
  - `target_splits`
- The review app already renders those columns read-only in
  `src/ynab_il_importer/review_app/app.py`.
- The upload path in `src/ynab_il_importer/upload_prep.py` already knows how to create
  split payloads, but it currently infers split upload units from grouped flat rows rather
  than from explicit reviewed split state on a single review row.
- The YNAB API does not support in-place editing of `subtransactions` on an existing
  split transaction, so Step 4 must distinguish between:
  - creating a new split transaction
  - keeping an existing split unchanged
  - replacing an existing split structure with a reviewed split structure
  - removing split structure from an existing split and replacing it with a non-split row

### Step 4 scope

In scope:

- target-side split editing for institutional review
- target-side split editing for cross-budget review where target creation/editing is allowed
- source-side split editing wherever the current workflow already allows source-side edits
- split creation from a non-split row
- split removal back to a non-split reviewed row
- split-line payee/category/memo/amount editing
- save/resume persistence for reviewed split edits
- upload-prep consumption of reviewed split edits
- validation and review blocking for inconsistent split edits

Out of scope:

- split-aware matching changes
- split-line-level grouping or case generation
- transfer subtransactions in split payloads
- automatic splitting suggestions
- rewriting the app around a deeply nested transaction relationship model

### Canonical review-artifact design for Step 4

Step 4 should keep the current flat review artifact but extend it with explicit reviewed
split-edit state. The key design principle is:

- current split snapshot columns remain factual
- reviewed split-edit columns carry the user’s intended edited state
- a small mode field tells the app and upload layer how to interpret the reviewed split columns

Recommended top-level additions per side:

- `source_split_mode`
- `target_split_mode`
- `source_splits_selected`
- `target_splits_selected`

Recommended mode values:

- `inherit`
  - use the current snapshot as-is
  - no reviewed split override is present
- `split`
  - use `*_splits_selected` as the reviewed split structure
- `unsplit`
  - ignore the current snapshot and treat the row as a reviewed non-split transaction

Why an explicit mode field is needed:

- `None` vs `[]` is not enough to distinguish:
  - untouched rows
  - reviewed split edits
  - explicit split removal
- the app, validator, and upload layer all need the same interpretation
- save/resume should preserve user intent even when the reviewed split list is empty or when
  a split is being removed

Recommended representation of `*_splits_selected`:

- reuse the existing `SPLIT_LINE_STRUCT` shape for now
- interpret its fields as the reviewed selected child-line values
- keep line amounts in the existing `inflow_ils` / `outflow_ils` shape
- keep `split_id` as the stable editor row id
- allow synthetic `split_id` values for newly created split lines

This keeps Step 4 low-risk because the app, IO, and upload layers can share one split-line
shape instead of inventing a second edited-split schema.

### Effective split semantics inside the app

The app should reason about three split views per side:

1. `current_splits`
   - factual snapshot from the source or target system
2. `selected_splits`
   - reviewed edited split state from `*_splits_selected`
3. `effective_splits`
   - the split structure currently being reviewed, derived from mode:
     - `inherit` -> current snapshot
     - `split` -> selected splits
     - `unsplit` -> no splits

The app should use `effective_splits` for:

- expanded split display
- split-count badges
- split-aware validation
- any upload-facing reviewed state

It should keep showing `current_splits` alongside `effective_splits` where that helps the
user understand what has changed.

### Review-app behavior requirements

The editor should support, at minimum:

- `Split transaction`
  - takes the current parent row and creates an initial editable split structure
- `Add split line`
  - appends a new editable split line
- `Remove split line`
  - removes one selected line
- `Remove split structure`
  - sets split mode to `unsplit`
- `Revert split edits`
  - returns split mode to `inherit` and clears `*_splits_selected`

The UI should preserve the current parent-level controls:

- parent payee selection
- parent category selection
- memo append
- decision action
- review / keep-open / grouped actions

But when split mode is `split`:

- the parent category should not be required for target-side upload
- the child-line categories should become the authoritative categorized state
- the parent payee should remain meaningful as the parent payee for YNAB payloads

### File-by-file Step 4 change inventory

#### `src/ynab_il_importer/artifacts/review_schema.py`

- add `source_split_mode`
- add `target_split_mode`
- add `source_splits_selected`
- add `target_splits_selected`
- keep snapshot split columns unchanged
- keep the rest of the review artifact flat

#### `src/ynab_il_importer/review_app/io.py`

- normalize the new mode fields
- round-trip `*_splits_selected`
- preserve explicit `[]` for reviewed selected split lists
- project the canonical artifact to the app-facing dataframe without losing split mode intent
- continue to be the explicit flattening boundary for the review app

#### `src/ynab_il_importer/review_app/state.py`

- extend the Polars data view with:
  - effective split counts
  - reviewed-split-present flags
  - split-mode flags
  - optional “has split edits” booleans
- add helper functions for:
  - deriving effective split lists from mode + current + selected
  - validating split totals
  - generating synthetic split ids where needed
- extend the row-edit mutation adapter to update split mode and selected split lists

#### `src/ynab_il_importer/review_app/validation.py`

- validate reviewed split state at the row level
- treat target split mode `split` as satisfying parent-category requirements through child lines
- block review when:
  - split totals do not match the parent amount
  - any required child category is missing
  - any required child payee/category combination is invalid
  - unsupported transfer split semantics appear
- keep component-level review rules parent-row-oriented

#### `src/ynab_il_importer/review_app/model.py`

- propagate split edits across related rows when the side being edited is shared
- make group/apply helpers preserve reviewed split state rather than overwriting it accidentally
- treat split edits as target-side reviewed values for fingerprint application only when that is
  unambiguous and intended

#### `src/ynab_il_importer/review_app/app.py`

- add split editor sections for source and target sides
- keep current read-only detail visible
- add reviewed/effective split display so the user can see edits before review
- keep split editing lazy and row-local so widget count stays manageable
- keep grouped mode parent-row-oriented; do not turn one review row into many UI rows

#### `src/ynab_il_importer/upload_prep.py`

- stop inferring split upload solely from grouped prepared rows
- instead, derive split upload from explicit reviewed target split state on each review row
- support:
  - unchanged non-split upload
  - reviewed split creation
  - reviewed split replacement
  - reviewed split removal back to a non-split upload
- continue to block unsupported split-transfer payloads

#### `src/ynab_il_importer/review_reconcile.py`

- preserve reviewed split-edit columns across rebuild/resume
- align rows by review identity without losing selected split state

#### Builder scripts

- `scripts/build_proposed_transactions.py`
- `scripts/build_cross_budget_review_rows.py`

Step 4 should populate the new split-edit columns with neutral defaults:

- `*_split_mode = inherit`
- `*_splits_selected = null`

The builders should not invent split edits. They should only populate the snapshot side.

### Validation rules for split editing

For any side in `split` mode:

- the sum of child signed amounts must equal the parent signed amount exactly
- each split line must have a stable `split_id`
- each split line must have:
  - payee if that side/workflow requires one
  - category unless the line is explicitly no-category-required
- empty split structures are invalid in `split` mode
- at least two lines should be required when creating a split from a non-split row

For `unsplit` mode:

- the selected split list is ignored
- parent payee/category rules revert to the ordinary non-split rules

For `inherit` mode:

- validation should operate on the current snapshot only where needed for display
- no split-edit validation should block review if the user has not changed split state

### Upload semantics for Step 4

Target-side upload should interpret reviewed split state as follows:

- `inherit`
  - unchanged existing target:
    - keep current target split structure if no target mutation is requested
  - create-target row:
    - behave as the current non-split or grouped-split path already does
- `split`
  - build upload `subtransactions` from `target_splits_selected`
  - parent `category_id` must be `null`
  - parent payee comes from the reviewed target payee
- `unsplit`
  - build a normal non-split payload from parent selected values

Existing target split transactions whose reviewed split structure changes should be treated as
“replace, not patch” operations in the upload plan, because the YNAB API does not support
editing `subtransactions` in place.

That means Step 4 should make the intended action explicit in upload prep, even if the final
delete/recreate wire-up remains a separate later upload refinement.

### Save/resume semantics

Save/resume must preserve:

- split mode
- selected split lines
- synthetic split ids
- parent selected payee/category values
- memo append
- review state

If a row is rebuilt from fresh source/target snapshots:

- reviewed split selections should remain attached to the review row
- snapshot split columns may change
- `inherit` mode should continue to mean “use the current fresh snapshot”
- `split` and `unsplit` should continue to mean “use the reviewed override”

### Testing strategy

Required coverage:

1. review schema round-trip
   - new split-edit columns persist through Parquet and CSV review save/load
2. review app state
   - create split from non-split row
   - add/remove line
   - revert to inherit
   - remove split structure
3. validation
   - mismatched totals block review
   - missing child category blocks review
   - valid reviewed split can be marked reviewed
4. save/resume
   - reviewed split edits survive save and reload
   - reviewed split edits survive rebuild/reconcile
5. upload prep
   - reviewed split state produces correct `subtransactions`
   - reviewed unsplit state produces normal non-split payload
   - unsupported split transfer is still blocked
6. app rendering
   - folded summary reflects effective split state
   - expanded split editor shows current and reviewed structure clearly

### Main risks

- making split editing implicit rather than explicit in the schema will create save/resume bugs
- overloading `None` / `[]` instead of using an explicit mode field will make split removal ambiguous
- letting split editing leak into matching semantics will enlarge Step 4 unnecessarily
- replacing too much of the current row editor at once could destabilize ordinary review flow
- upload replacement semantics for existing YNAB splits need to stay explicit and testable

### Recommended implementation sequence

1. extend the review schema and IO boundary with explicit reviewed split-edit fields
2. teach the app state/helpers to derive effective split state from mode + snapshot + selected
3. implement a target-side split editor first
4. wire validation and review blocking for target split edits
5. teach upload prep to consume reviewed target split edits
6. then add source-side split editing using the same editor/state machinery

This keeps the first Step 4 slice vertical and testable:

- schema
- app
- save/resume
- upload

before broader editor polish.
