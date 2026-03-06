# Streamlit Review UI Plan (Final Combined — Revised)

## Summary
Build a local Streamlit app that loads `outputs/proposed_transactions.csv`, allows **row‑wise selection** of `payee_selected` and `category_selected`, marks `update_map`, and saves a reviewed CSV for upload preparation. The UI optimizes for **fast, accurate upload readiness**. Mapping learning remains downstream and offline.

---

## Purpose & Philosophy
This app is a **transaction review / upload‑prep tool**, not a mapping workbench.

Primary user task:
1) inspect a transaction  
2) select payee  
3) select category  
4) optionally mark `update_map`  
5) move on

Guiding principles:
- explicitness over cleverness  
- stability over dynamism  
- CSV‑driven state, no hidden persistence  
- correctness and speed over model purity  

---

## Inputs

### Required
`outputs/proposed_transactions.csv`  
Required columns:
- `transaction_id`
- `date`
- `payee_options`
- `category_options`
- `payee_selected`
- `category_selected`
- `match_status`
- `update_map`
- `fingerprint`

### Optional (display if present)
- `outflow_ils`, `inflow_ils` (or `amount_ils`)
- `memo`, `merchant_raw`, `description_raw`, `description_clean`
- `source`, `account_name`, `txn_kind`, `note`

### Optional read‑only context
- `outputs/fingerprint_groups.csv`
- `outputs/matched_pairs.csv`  
Used only as hints later.

---

## Outputs

### Default save target
- `outputs/proposed_transactions_reviewed.csv` (atomic write)

### Optional save target
- Overwrite `outputs/proposed_transactions.csv` (toggleable)

### Explicitly out of scope
- Editing `mappings/payee_map.csv`
- Generating `outputs/map_updates.csv` inside the UI
- Uploading to YNAB

---

## Core Interaction Model

### Default workflow (Row‑wise)
- Separate payee/category selectors (deliberate)
- Manual overrides (friction‑free)
- Focus on unresolved rows

### Optional workflow (Grouped by fingerprint)
- **View switch inside the app**, not a startup fork
- Secondary accelerative view
- Allows “apply to all rows in group”

---

## UI Structure (Decision‑Complete)

### Top bar
- File path
- Reload button
- Save button
- Summary counts:
  - total rows
  - rows missing payee
  - rows missing category
  - unresolved rows
  - rows with `update_map=True`

### Sidebar
- View switch: `Row` / `Grouped`
- Filters:
  - `match_status`
  - unresolved only
  - missing payee only
  - missing category only
  - fingerprint substring
  - payee substring
  - memo/description substring
  - optional source/account filters

### Main pane
**Row view (default):**
- Paginated list (default 50 rows/page)
- Each row rendered as an **expander**:
  - summary line: date, amount, memo snippet, fingerprint, match_status
  - expanded area: full context + editable controls

**Grouped view (optional):**
- Group list by fingerprint
- Each fingerprint group as an expander:
  - group summary counts + options summary
  - “apply to all” controls inside
  - optional per‑row overrides inside group

---

## Editing Rules

### Payee & Category
- Dropdown from options (if available)
- Manual override field
- **Override wins if non‑empty**
- Manual override allowed even if not in options

### Validation (Hard)
A file is **not ready** unless BOTH:
- `payee_selected` is non‑empty
- `category_selected` is non‑empty

### Warnings (Soft)
- Selected values contain `;`
- `update_map=True` while payee/category missing
- Selected value not in suggested options
- Same fingerprint yields inconsistent selections

---

## Save Behavior
- Explicit Save button only
- Atomic write (temp + replace)
- Preserve row order
- Preserve untouched columns
- UTF‑8‑SIG
- Show success/failure, modified count, last save time

---

## Data Handling
- Parse option strings (`;`) only in memory
- Use `transaction_id` as stable key
- Preserve extra columns on roundtrip
- No hidden storage beyond CSV
- Session state: current df, filters, page, view mode, unsaved changes

---

## Milestones

### Milestone A — Core IO + Tests
- load/validate/save helpers
- unit tests for parsing, validation, roundtrip

### Milestone B — MVP Row‑wise UI
- row editor with payee/category selection
- manual overrides
- save/reload

### Milestone C — Filters + Readiness
- filters for unresolved rows
- readiness indicator (payee+category required)

### Milestone D — Phase 1.5 Accelerators
- apply to same fingerprint
- inconsistency warnings
- optional hints panel if files exist

### Milestone E — Grouped View Polish
- improve grouped view UX
- group‑level apply controls

---

## Implementation Structure

Suggested files:
- `src/ynab_il_importer/review/io.py`
- `src/ynab_il_importer/review/model.py`
- `src/ynab_il_importer/review/validation.py`
- `src/ynab_il_importer/review/app.py`

Core functions:
- `load_proposed_transactions(path)`
- `parse_option_string(value)`
- `resolve_selected_value(dropdown, override)`
- `validate_row(row) -> errors + warnings`
- `save_reviewed_transactions(df, path)`
- `apply_to_same_fingerprint(...)` (Phase 1.5)

---

## Testing

### Unit tests
- option parsing (empty, whitespace, duplicates)
- override resolution
- validation (missing payee/category)
- save/load roundtrip
- missing column handling
- update_map boolean handling

### Manual tests
1) load valid CSV  
2) missing column error  
3) edit/save/reload  
4) filter persistence  
5) readiness validation (payee+category)  
6) warnings for `;`  
7) bulk apply by fingerprint  
8) grouped view preserves edits  

---

## Assumptions & Defaults
- Current column names match the real file
- Default save target is `outputs/proposed_transactions_reviewed.csv`
- Default view is row‑wise; grouped view is optional inside app
- Payee + category are both required to be “ready”
