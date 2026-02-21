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
