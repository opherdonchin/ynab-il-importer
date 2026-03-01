# ynab-il-importer

Minimal ETL scaffold for importing Israeli bank/card exports into YNAB by learning payee/category hints from historical YNAB register data.

Directory convention:
- `data/derived/`: parsed/normalized snapshots of raw files (treated as local, not for commit).
- `outputs/`: generated review/labeling artifacts that are intended to be shared/committed.

- `documents/project_context.md` contains the file used as context for AI agents.
- `documents/plan.md` contains the current plan of action.

## Quickstart

1. Install environment:

```bash
pixi install
```

2. Put your source files in `data/raw/`:

- `data/raw/bank.xls`
- `data/raw/card.xlsx`
- `data/raw/ynab_register.csv`

3. Normalize source inputs:

```bash
pixi run python scripts/normalize_file.py \
  --format leumi \
  --in data/raw/Bankin.dat \
  --out data/derived/bank_normalized.csv

pixi run python scripts/normalize_file.py \
  --format max \
  --in data/raw/card.xlsx \
  --out data/derived/card_normalized.csv

pixi run python scripts/normalize_file.py \
  --format ynab \
  --in data/raw/ynab_register.csv \
  --out data/derived/ynab_normalized.csv
```

4. Build matched pairs:

```bash
pixi run python scripts/bootstrap_pairs.py \
  --bank data/derived/bank_normalized.csv \
  --card data/derived/card_normalized.csv \
  --ynab data/derived/ynab_normalized.csv \
  --out outputs/matched_pairs.csv
```

5. Build fingerprint groups for human labeling:

```bash
pixi run python scripts/build_groups.py \
  --pairs outputs/matched_pairs.csv \
  --out outputs/fingerprint_groups.csv
```

Expected outputs:

- `outputs/matched_pairs.csv`
- `outputs/fingerprint_groups.csv`

## Account Name Mapping

Account identity now comes from source files:

- Card exports: `4 ספרות אחרונות...` -> normalized as `xNNNN`
- Bank `.dat` exports: account number from the last field

Optional mapping to YNAB account names/IDs is read from:

- `mappings/account_name_map.csv`

CSV schema:

- `source` (`card` or `bank`; optional blank for global rows)
- `source_account` (for example `x1234` or bank account id from `.dat`)
- `source_account_label` (optional, human label)
- `ynab_account_name` (target YNAB account name)
- `ynab_account_id` (target YNAB account id; optional but recommended)

Behavior:

- If the mapping file is missing, parsed account names remain as in source files.
- If a parsed account is not found in the mapping file, it remains as in source files.
- In both cases, a warning is emitted with unmatched account names.

## Payee Map Rules

`mappings/payee_map.csv` is the source of truth for deterministic payee/category mapping.

Decision:
- Rules return `(payee_canonical, category_target)` in one row; `category_target` may be blank.
- Blank key fields are wildcards and do not constrain matching.
- Rule precedence is `priority DESC`, then specificity DESC, then `rule_id ASC`.
- If the top rules tie on `(priority, specificity)`, the result is `ambiguous`.

Important behavior:
- `description_clean_norm` is human-readable.
- `fingerprint` is the stable key used for matching when present.
- If a rule has both `fingerprint` and `description_clean_norm`, `fingerprint` wins.
- `direction` is derived from amount sign (`inflow` / `outflow` / `zero`) when not provided.
- `currency` defaults to `ILS` when missing in parsed inputs.

Build review outputs:

```bash
pixi run ynab-il build-payee-map \
  --parsed data/derived/bankin_fuller_parsed.csv \
  --matched-pairs outputs/matched_pairs.csv \
  --out-dir outputs/payee_map
```

Generated files:
- `payee_map_candidates.csv`: one row per `(txn_kind, fingerprint, description_clean_norm)`.
- `payee_map_applied_preview.csv`: parsed transactions + suggested payee/category and match status.

Candidate statuses:
- `matched_uniquely`: all rows for that key matched uniquely.
- `unmatched`: no rows for that key matched any active rule.
- `ambiguous`: mixed/noisy results (ambiguous tie or partial coverage).

Safe editing guidelines for `mappings/payee_map.csv`:
- Keep `rule_id` stable and unique.
- Use blanks in optional key columns for wildcard behavior.
- Increase `priority` to force a rule to win over a more specific lower-priority rule.
- Leave `category_target` blank when only payee is known.
