# Split Handling Implementation Plan

## Status

- Branch: `handle_splits`
- Current phase: Step 1 is implemented and verified; detailed Step 2 planning is in progress
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

Make the YNAB boundary coherent in both directions:

- downloads should produce one canonical transaction model with optional nested split lines
- existing flat download/report shapes should become explicit projections of that model
- uploads should be able to create or update both regular transactions and split transactions from reviewed canonical state

This is the stage where split transactions stop being merely preserved and start being meaningfully round-trippable at the YNAB boundary.

### Step 2 scope boundary

#### In scope for Step 2

- standardizing YNAB download around the canonical transaction artifact
- standardizing YNAB upload payload construction around the same canonical model
- defining how split parent and split child information survives download -> canonical artifact -> upload payload
- generalizing upload preparation from single-category rows to transaction-level payload assembly
- preserving enough identifiers and lineage to distinguish:
  - regular transaction
  - split transaction
  - updated existing split transaction
  - newly created split transaction

#### Out of scope for Step 2

- changing review-app display behavior for splits
- adding split editing UI
- making matching or review semantics split-aware across the whole workflow
- changing bank/card parser semantics unrelated to YNAB round-trip behavior

The review app can remain flat and mostly split-blind here; Step 2 is about the YNAB-facing boundary and canonical artifact fidelity.

### Current code reality to fix

After Step 1, the codebase already preserves split lines in the canonical artifact, but the YNAB boundary is still asymmetrical.

#### Downloads currently have two shapes

In `src/ynab_il_importer/ynab_api.py`:

- `transactions_to_dataframe(...)`
  - returns top-level parent transactions only
  - preserves top-level category `Split` on split parents
- `category_transactions_to_dataframe(...)`
  - explodes subtransactions into separate flat rows
  - is category-centric rather than transaction-centric

Those are both derived from the same raw API payload, but today they still behave like separate ad hoc models rather than named projections of one boundary representation.

#### Category-as-source export discards family structure

In `scripts/io_ynab_as_source.py`:

- the script filters `category_transactions_to_dataframe(...)`
- it emits a flat source-like CSV keyed to category rows
- sibling and parent split structure is intentionally discarded after filtering

That behavior was acceptable in Step 1 because we were preserving current semantics, but Step 2 needs to define that flattening explicitly as a projection and make clear what is lost.

#### Uploads are still single-transaction, single-category only

In `src/ynab_il_importer/upload_prep.py`:

- `prepare_upload_transactions(...)` works row-by-row on flat reviewed rows
- `upload_payload_records(...)` always emits one payload dict per row
- each payload can have:
  - `payee_id` or `payee_name`
  - one `category_id`
  - no `subtransactions`

That means the upload path currently cannot express:

- a split transaction with child lines
- an edit to an existing split parent with revised child lines
- a parent row whose category is `Split` and whose real categories live only in children

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

That is too narrow for split payloads, where the parent may intentionally have category `Split` or empty category and the meaningful validation lives in child rows.

### Step 2 design principle

The least disruptive Step 2 design is:

- keep the **canonical transaction artifact** authoritative
- make **download projections** explicit views of that artifact
- make **upload payload assembly** a transaction-level operation, not a row-level operation

In other words:

- download should start with canonical parent transactions with nested `splits`
- upload should end with YNAB payload transactions with optional `subtransactions`
- any flat rows in between are compatibility or review views, not the YNAB boundary model itself

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

#### Important refinement for Step 2

The existing canonical schema is good enough to start Step 2, but it likely needs a small amount of YNAB-specific refinement before upload round-trip is fully reliable.

Likely additions or clarifications:

- top-level transaction metadata
  - whether the parent is a split transaction
  - whether the parent category is a real category or the YNAB sentinel `Split`
- split-line metadata
  - stable split-line ordering
  - explicit child signed amount, not only inflow/outflow pairs, if that simplifies payload construction
- upload lineage metadata
  - whether a reviewed transaction should be created, updated, or left unchanged

I would prefer adding those as carefully chosen canonical fields rather than inventing a separate upload-only schema unless code inspection during implementation proves that cleaner.

### Step 2 download strategy

#### Goal

All YNAB download shapes should come from the same canonical constructor.

#### Required download outputs

Step 2 should make these outputs explicit projections:

1. canonical nested YNAB transaction artifact
   - authoritative
   - parent transaction + nested split lines

2. top-level flat transaction projection
   - current `transactions_to_dataframe(...)` behavior
   - used by existing matching/reconciliation flows

3. exploded split-line/category projection
   - current `category_transactions_to_dataframe(...)` behavior
   - used by category-as-source flows

4. optional nested JSON/debug projection
   - useful for inspecting split fidelity during Step 2

#### File-by-file Step 2 download changes

##### `src/ynab_il_importer/ynab_api.py`

- keep raw fetch functions as they are
- strengthen `transactions_to_canonical_table(...)` so it becomes the one true download constructor
- refactor `transactions_to_dataframe(...)` into a wrapper around:
  - canonical table creation
  - top-level flat projection helper
- refactor `category_transactions_to_dataframe(...)` into a wrapper around:
  - canonical table creation
  - split-line explosion helper plus compatibility shaping

Recommended design bias:

- avoid duplicating split parsing logic across multiple dataframe builders
- make projection wrappers visibly depend on the canonical builder

##### `src/ynab_il_importer/artifacts/transaction_projection.py`

- likely needs one additional explicit projection for YNAB exploded category rows
- the existing `explode_split_lines(...)` is a good foundation, but it currently returns only split lines and not the unsplit-parent fallback rows needed to emulate current `category_transactions_to_dataframe(...)`
- Step 2 should add a projection helper that reproduces current category-source expectations exactly:
  - split parents explode into child rows
  - unsplit parents remain one row
  - parent and child lineage columns are preserved

##### `scripts/download_ynab_api.py`

- should continue writing:
  - canonical Parquet artifact
  - flat top-level CSV projection
- but Step 2 should make that relationship explicit in comments/tests and stop treating the flat CSV as an independent shape

##### `scripts/io_ynab_as_source.py`

- should derive category-source rows from the canonical YNAB artifact or a named projection helper rather than directly from the legacy flat constructor
- Step 2 should document and test what is intentionally lost in this projection:
  - parent category context
  - sibling split lines outside the selected category

That limitation can remain, but it needs to be named as a deliberate category projection rather than an accident.

### Step 2 upload strategy

#### Goal

Uploads should consume reviewed canonical transaction state and emit valid YNAB API payloads for both:

- regular transactions
- split transactions with `subtransactions`

#### Critical correction to the current upload model

The current row-level prepared upload dataframe is too low-level to be the real Step 2 upload boundary.

Step 2 should split upload work into two layers:

1. **row-level compatibility preparation**
   - continues to support current review/export/debug needs
   - may still produce flat prepared rows where useful

2. **transaction-level payload assembly**
   - groups reviewed state back into one parent transaction payload
   - decides whether the payload is regular or split
   - emits the exact YNAB API structure to send

This is the upload-side analogue of the Step 1 projection strategy.

#### Recommended transaction-level payload model

For a regular transaction payload:

- one parent transaction dict
- scalar amount/payee/category/memo fields
- no `subtransactions`

For a split transaction payload:

- one parent transaction dict
- parent amount equals the total signed amount of the child lines
- parent category should follow YNAB split semantics rather than forcing a normal category
- `subtransactions` list contains one dict per child line

Likely child payload fields:

- `amount`
- `payee_name` or `payee_id` where applicable
- `category_id`
- `memo`

The exact final field set should be verified against YNAB API behavior during implementation, but the plan should assume that subtransactions are created as a full child list attached to the parent.

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
  - split regular
  - split transfer, if YNAB even permits that shape
- a helper that serializes one upload transaction unit into a YNAB payload dict

Recommended compatibility rule:

- keep existing row-level helpers callable during transition
- add new transaction-level helpers beside them
- only retire the old direct row-to-payload path after the split-capable path is verified

##### `scripts/prepare_ynab_upload.py`

- should continue to write human-readable dry-run artifacts
- but Step 2 should distinguish between:
  - flat prepared inspection output
  - canonical or grouped transaction payload preview

Recommended output policy:

- keep the current CSV dry-run artifact for inspectability
- keep JSON as the authoritative preview of the actual payload to be posted
- if helpful, add an optional nested payload preview file rather than overloading the CSV

##### `src/ynab_il_importer/ynab_api.py`

- add explicit payload-shape helpers only if they truly belong at the API boundary
- otherwise keep the API client thin and let `upload_prep.py` own payload construction

My recommendation:

- keep HTTP wrappers thin
- keep payload construction in `upload_prep.py`
- add small validation helpers in `ynab_api.py` only if they encode stable YNAB-specific rules

### Step 2 update-vs-create policy

This needs to be explicit before implementation.

Recommended policy:

- `create_transactions(...)` is used for reviewed rows that represent new transactions to be created
- `update_transactions(...)` is used only when the reviewed canonical artifact clearly points at an existing YNAB parent transaction that should be modified

For split transactions specifically:

- parent transaction id should be the update identity
- split child updates should be treated as a whole-parent replacement operation, not a child-by-child patch unless YNAB behavior proves otherwise

Why this is the safer default:

- the repo already reasons mostly in terms of parent transactions
- it avoids trying to treat subtransactions as independently patchable domain objects before we have verified that model carefully

### Step 2 round-trip rules

The plan should make these round-trip guarantees explicit.

#### Download -> canonical

- every non-deleted YNAB parent transaction becomes one canonical parent row
- every non-deleted YNAB subtransaction becomes one nested split line under that parent

#### Canonical -> download projections

- top-level projection preserves current parent-row semantics
- exploded/category projection preserves current subtransaction-row semantics for category-based workflows

#### Reviewed canonical -> upload payload

- regular reviewed transaction becomes one regular parent payload
- split reviewed transaction becomes one parent payload with subtransactions

#### Upload response -> verification

- parent identity should be checked by `(account_id, import_id)` when possible
- split verification should compare:
  - parent amount
  - parent account/date/import id
  - child count
  - child amount totals
  - child categories/payees when available in the response

The current verification path is not enough for this and will need a split-capable companion.

### Step 2 open design questions to resolve during implementation

These are now specific enough that they should be called out before coding.

1. **How much child identity should we expect on update?**
   - If YNAB requires child ids for updating existing split children, the canonical artifact will need to preserve and round-trip them strictly.
   - If YNAB treats submitted `subtransactions` as replacement content, parent-level update may be simpler.

2. **What is the correct parent category representation for split uploads?**
   - We should not assume the parent category behaves like a normal category row.
   - The implementation should confirm whether the payload should omit parent category, send empty `category_id`, or rely on YNAB to derive `Split`.

3. **Can split lines use transfer semantics in the YNAB API?**
   - If yes, that needs explicit validation rules.
   - If no, the upload grouping logic should reject or defer those cases clearly.

4. **What reviewed artifact shape should feed split upload before Step 4 editing exists?**
   - In Step 2 alone, upload-capable split state may come only from canonical artifacts or synthetic tests, not the review app.
   - The plan should not overpromise split editing before the UI stages.

5. **How much response detail does YNAB actually return for created or updated split transactions?**
   - Verification quality will depend on this.
   - If the response is too shallow, Step 2 may need a post-upload fetch-and-verify path for split transactions.

### Step 2 migration sequence

1. Refactor YNAB download projections so both top-level and exploded views come from the canonical constructor.
2. Add explicit projection helpers for unsplit-parent fallback plus split-child explosion.
3. Add transaction-level upload assembly structures and helpers.
4. Keep existing regular-transaction payload behavior working through the new transaction-level path.
5. Add split payload serialization for canonical split transactions.
6. Extend preflight checks to detect invalid split payloads before execution.
7. Extend upload verification to reason about split responses or follow-up verification.
8. Update dry-run scripts to expose the new payload shape clearly without losing inspectability.
9. Run round-trip and golden tests on representative regular and split YNAB cases before touching review-app behavior.

### Step 2 testing plan

#### Download-side equivalence tests

- `transactions_to_dataframe(...)` still matches the current top-level flat shape when driven from canonical input
- `category_transactions_to_dataframe(...)` still matches the current exploded flat shape when driven from canonical input
- split parents and unsplit parents both project correctly

#### Canonical fidelity tests

- a downloaded split transaction round-trips:
  - raw API payload -> canonical table
  - canonical table -> projection
  - canonical table -> nested payload preview
- child line order and amounts are preserved

#### Upload serialization tests

- regular reviewed transaction -> one regular YNAB payload
- transfer reviewed transaction -> one transfer payload
- split transaction -> one parent payload with expected `subtransactions`
- split transaction totals reconcile exactly between parent and children

#### Preflight and validation tests

- reject split payloads whose child totals do not equal the parent total
- reject unsupported split child combinations if YNAB disallows them
- detect duplicate import ids or conflicting update identities at the parent level

#### Response verification tests

- regular saved transaction verifies as before
- split saved transaction verifies parent identity and child structure
- idempotent rerun logic still works for regular transactions and has a defined interpretation for split ones

#### Script-level integration tests

- `scripts/download_ynab_api.py` writes coherent canonical + flat outputs for split data
- `scripts/io_ynab_as_source.py` writes the expected category projection from canonical split data
- `scripts/prepare_ynab_upload.py` emits correct JSON for regular and split payload scenarios

### Step 2 risks

- YNAB API update semantics for split children may be stricter than the current code assumes.
- The current review workflow may not yet produce enough structured information to drive split uploads directly.
- Preflight and verification can easily become misleading if they keep treating split parents like regular rows.
- It will be tempting to drag split display or editing into this step; that should be resisted.

### Recommended first implementation slice for Step 2

When Step 2 implementation begins, the safest first slice is:

1. refactor `ynab_api.py` so both existing download dataframes are explicit projections from the canonical constructor
2. add projection tests proving that existing download behavior stays stable
3. add a transaction-level upload assembly helper for regular transactions only
4. migrate the existing regular upload path to use that helper before adding split payloads

That creates the download/upload backbone first, then adds split payload behavior on top of a cleaner regular-transaction path.

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
