# Normalized Transaction Schema (Milestone 1)

All parsed sources should emit a unified normalized transaction table to support
downstream mapping, dedupe, and review.

## Core fields

- `source`: source label, lowercased (`bank` / `card`)
- `account_name`: logical account name used for pairing and dedupe
- `source_account`: raw account identifier from the source file (for debugging)
- `date`: authoritative transaction date
- `outflow_ils`: outflow amount in ILS
- `inflow_ils`: inflow amount in ILS
- `currency`: uppercase ISO-like code, default `ILS` when missing
- `txn_kind`: lowercased transaction kind (e.g., `expense`, `income`, `transfer`, `credit`)
- `merchant_raw`: raw merchant text when available
- `description_raw`: raw memo/description text from source
- `description_clean`: deterministic cleaned description used for mapping
- `description_clean_norm`: `normalize_text(description_clean)`
- `fingerprint`: `fingerprint_v0(description_clean_norm)`
- `fingerprint_hash`: `fingerprint_hash_v1(txn_kind, description_clean_norm)`
- `secondary_date`: posting/charge date when available

## Card-specific notes

- For card exports, `date` is taken from `תאריך עסקה` (transaction date).
- `secondary_date` is preserved from `תאריך חיוב` when available.
- Currency blanks are treated as `ILS`.
