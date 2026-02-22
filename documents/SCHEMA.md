# Normalized Transaction Schema (Milestone 1)

All parsed sources should emit a unified normalized transaction table to support
downstream mapping, dedupe, and review.

## Core fields

- `source`: source label, lowercased (`bank` / `card`)
- `account_name`: logical account name used for pairing and dedupe
- `date`: authoritative transaction date
- `amount_ils`: signed amount in ILS convention (outflows negative, inflows positive)
- `currency`: uppercase ISO-like code, default `ILS` when missing
- `txn_kind`: lowercased transaction kind (for cards in v1: `card`)
- `merchant_raw`: raw merchant text when available
- `description_raw`: raw memo/description text from source
- `description_clean`: deterministic cleaned description used for mapping
- `description_clean_norm`: `normalize_text(description_clean)`
- `fingerprint`: `fingerprint_v0(description_clean_norm)`
- `fingerprint_hash`: `fingerprint_hash_v1(txn_kind, description_clean_norm)`

## Card-specific notes

- For card exports, `date` is taken from `תאריך עסקה` (transaction date).
- `charge_date` is preserved from `תאריך חיוב` when available.
- Currency blanks are treated as `ILS`.
