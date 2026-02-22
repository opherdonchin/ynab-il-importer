# ynab-il-importer — Project Context

## High-Level Goal

Build a robust, human-in-the-loop pipeline that:

1. Parses Israeli bank and credit card exports (Leumi, Max, etc.)
2. Extracts stable merchant identifiers (`merchant_raw`, `description_clean`)
3. Maps transactions to a canonical payee (`payee_canonical`)
4. Optionally assigns categories
5. Creates transactions in YNAB via API
6. Continuously improves through lightweight review and mapping updates

The system must support automation while remaining transparent and controllable.

---

## Core Design Principle

The **central artifact** of the system is a mapping table:

> (txn features) → payee_canonical (+ optional category)

Everything else (matching, hints, bootstrapping) supports building and refining this mapping.

---

## Current Architecture

### Stage 1: Parsing (Completed)

Raw bank/card exports → normalized transaction rows.

Each parsed transaction includes:

- `date` (purchase date; authoritative)
- `posting_date` (bank date; secondary)
- `txn_kind` (debit_card, bit, transfer, loan, other)
- `merchant_raw` (extracted merchant core)
- `description_clean`
- `description_clean_norm`
- `fingerprint_hash`
- `amount_ils`
- `account_name`
- `source`

Merchant extraction is rule-based and removes boilerplate.

Fingerprinting uses normalized merchant text and txn_kind.

---

### Stage 2: Payee Mapping (Current Focus)

We maintain:

## mappings/payee_map.csv  (Source of Truth)

Each row is a rule:

**Matching columns (wildcard if blank):**
- txn_kind
- fingerprint_hash
- description_clean_norm
- account_name
- source
- direction
- currency
- other optional discriminators

**Rule output:**
- payee_canonical
- category_target (optional)
- priority
- is_active
- notes

### Wildcard Semantics

If a rule column is blank:
→ It does NOT constrain matching.

More specific rules override more general ones.

---

### Rule Resolution

For a given transaction:

1. Find all matching rules.
2. Sort by:
   - priority (desc)
   - specificity (number of filled key columns)
3. If one clear winner → assign payee.
4. If tie → mark ambiguous.
5. If no rule → mark unmatched.

---

### Stage 3: Category Assignment

Two possible approaches:

- Rule-based (category_target in payee_map)
- Learned (historical payee → category mapping)
- Hybrid (preferred)

Currently category is optional in mapping rules.

---

## Bootstrapping Strategy

Historical YNAB data is:

- Noisy
- Inconsistent
- But valuable

We use it to generate:

- `suggested_payee_distribution`
- `suggested_category_distribution`

These are hints only.

The user decides canonical payee assignments.

---

## Regular Processing Pipeline

1. Parse bank/card exports
2. Apply payee_map rules
3. Flag unmatched or ambiguous
4. Review and update payee_map
5. Assign categories
6. Create YNAB transactions

---

## Key Design Decisions

- Mapping file is editable CSV (portable, version-controlled)
- Optional keys default blank (wildcards)
- fingerprint_hash is stable join key
- description_clean_norm is human-readable display key
- No reliance on YNAB file imports long-term (API preferred)
- No database dependency required

---

## Current State

- Parsing stable
- Merchant extraction working
- Fingerprinting stable
- Payee map scaffolding exists
- Candidate and preview CSVs generated
- Rule engine implemented
- No canonical payees defined yet

Next major milestone:

> Populate payee_map.csv and validate rule engine on real data.