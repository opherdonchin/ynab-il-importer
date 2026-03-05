# ynab-il-importer — Project Context

## Purpose

The goal of this project is to build a robust, repeatable, human-correctable
pipeline for importing Israeli bank and credit card transactions into YNAB.

The system must:

1. Parse Israeli financial exports (bank + card).
2. Normalize transactions into a unified schema.
3. Deduplicate against existing YNAB transactions.
4. Map transactions to canonical payee and category using a maintained mapping table.
5. Allow human review and correction.
6. Update both YNAB and the payee mapping based on review decisions.
7. Be safe to re-run (idempotent).

The system is intentionally deterministic and mapping-driven.
Machine learning is not part of v1.

---

# Architectural Principles

## 1. Fingerprint-Centric Mapping

Every normalized transaction has a stable:

    fingerprint

This fingerprint is the primary key used for mapping inference.
Fingerprints are derived from normalized text and optionally canonicalized via
`mappings/fingerprint_map.csv`.

---

## 2. Mapping Model

Mapping is defined by rules, primarily keyed by:

    fingerprint → (payee, category)

`payee_map.csv` is the source of truth.

Grain:

    One row = one rule that can emit a payee+category suggestion.

Columns:

- rule_id
- is_active
- priority
- txn_kind
- fingerprint
- description_clean_norm
- account_name
- source
- direction
- currency
- amount_bucket
- payee_canonical
- category_target
- notes

Mapping affects future inference only.
Past resolved transactions are never rewritten.

---

## 3. Transaction States

After mapping application, each transaction is classified:

- unmatched      (no mapping rows)
- defaulted      (single row OR single default)
- needs_choice   (multiple options, no default)

The system generates:

    outputs/proposed_transactions.csv

with:

- payee_options
- category_options
- payee_selected
- category_selected
- update_map flag

User review determines final selections.

---

## 4. Workflow Phases

### Phase 0 — Freeze Conventions
- Amount sign convention fixed.
- Date semantics fixed.

### Phase 1 — Normalize Sources
- Bank parsing complete.
- Credit card parsing must emit same normalized schema.

### Phase 2 — Bootstrap Mapping
- Download historical bank + card + YNAB.
- Match transactions.
- Generate initial payee_map with counts and suggested defaults.
- User curates payee_map.

### Phase 3 — Regular Processing
- Download new bank/card transactions.
- Download YNAB transactions for date range.
- Deduplicate.
- Apply payee_map.
- Emit proposed_transactions.

### Phase 4 — Review
- User edits payee/category.
- User marks update_map when desired.
- Category selection required before upload.

### Phase 5 — Update
- Append unique new (fingerprint, payee, category) rows to payee_map.
- Upload transactions via YNAB API.
- Ensure idempotency.

### Phase 6 — Steady State Loop
Repeat:
parse → dedupe → map → review → update → upload.

---

## 5. Idempotency

Uploads must be safe to re-run.

Preferred strategy:

- Deterministic import_id generation.
- Remove already-existing transactions before upload.

No duplicate uploads should occur.

---

## 6. Review UI

A local review UI (likely Streamlit) will:

- Display proposed_transactions.
- Provide dropdown selection for payee and category.
- Enforce category selection.
- Support update_map flag.
- Save edits to CSV.

The UI is not the source of truth.
CSV files remain authoritative and versionable.

---

## 7. Scope Boundaries (v1)

Not in scope for now:

- Automatic learning of mapping rules.
- Retrofitting historical transactions in YNAB.
- Complex rule precedence systems.
- Large-scale fingerprint algorithm redesign beyond the canonicalization map.

Focus: deterministic mapping + clean workflow.

---

## 8. Success Criteria (Milestone 1)

The system is considered successful when:

- ≥90% of new transactions default automatically.
- Review workload is small and clear.
- Upload is idempotent.
- payee_map grows cleanly over time.
- Credit card and bank are processed through identical workflow.

---

# Operational Philosophy

This project prioritizes:

- Explicitness over cleverness
- Stability over dynamism
- Auditability over automation
- CSV-driven state over hidden state
- Iterative improvement over premature generalization
