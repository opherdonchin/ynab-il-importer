# ynab-il-importer

Minimal ETL scaffold for importing Israeli bank/card exports into YNAB by learning payee/category hints from historical YNAB register data.

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
pixi run python scripts/normalize_inputs.py \
  --bank-in data/raw/bank.xls \
  --card-in data/raw/card.xlsx \
  --ynab-in data/raw/ynab_register.csv \
  --out-dir data/derived
```

4. Build matched pairs:

```bash
pixi run python scripts/bootstrap_pairs.py \
  --bank data/derived/bank_normalized.csv \
  --card data/derived/card_normalized.csv \
  --ynab data/derived/ynab_normalized.csv \
  --out data/derived/matched_pairs.csv
```

5. Build fingerprint groups for human labeling:

```bash
pixi run python scripts/build_groups.py \
  --pairs data/derived/matched_pairs.csv \
  --out data/derived/fingerprint_groups.csv
```

Expected outputs:

- `data/derived/matched_pairs.csv`
- `data/derived/fingerprint_groups.csv`

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
  --matched-pairs data/derived/matched_pairs_bankin_fuller.csv \
  --out-dir data/derived
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
