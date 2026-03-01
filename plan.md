
2. Create project structure

* `src/ynab_il_importer/__init__.py`
* `src/ynab_il_importer/config.py`
* `src/ynab_il_importer/io_bank.py`
* `src/ynab_il_importer/io_card.py`
* `src/ynab_il_importer/io_ynab.py`
* `src/ynab_il_importer/normalize.py`
* `src/ynab_il_importer/fingerprint.py`
* `src/ynab_il_importer/pairing.py`
* `src/ynab_il_importer/export.py`
* `src/ynab_il_importer/cli.py`
* `scripts/bootstrap_pairs.py`
* `scripts/normalize_inputs.py`
* `scripts/build_groups.py`
* `tests/test_smoke.py`
* `data/.gitkeep`
* `data/raw/.gitkeep`
* `data/derived/.gitkeep`

3. Environment (use pixi)
   Create `pixi.toml`:

* Project name: `ynab-il-importer`
* Channels: `conda-forge`
* Platforms: `linux-64`, `osx-64`, `osx-arm64`, `win-64`
* Dependencies:

  * `python>=3.11`
  * `pandas>=2.2`
  * `openpyxl>=3.1`
  * `xlrd>=2.0`
  * `lxml>=5.0`  (bank “.xls” may be HTML)
  * `python-dateutil>=2.9`
  * `rapidfuzz>=3.6`
  * `pydantic>=2.6`
  * `typer>=0.12`
  * `rich>=13.7`
  * `pyyaml>=6.0`
* Dev dependencies:

  * `pytest>=8.0`
  * `ruff>=0.6`
  * `mypy>=1.10`
* Tasks:

  * `lint = "ruff check ."`
  * `fmt = "ruff format ."`
  * `test = "pytest -q"`
  * `smoke = "python -m ynab_il_importer.cli --help"`

4. CLI entrypoint
   In `src/ynab_il_importer/cli.py`, implement a Typer app with subcommands (skeletons that just print what they would do):

* `parse-bank --in PATH --out PATH`
* `parse-card --in PATH --out PATH`
* `parse-ynab --in PATH --out PATH`
* `match-pairs --bank PATH --card PATH --ynab PATH --out PATH`
* `build-groups --pairs PATH --out PATH`

Also add `pyproject.toml` with:

* build-system (setuptools)
* project metadata
* `tool.ruff` minimal config (line-length 100)
* `tool.pytest.ini_options` with `testpaths = ["tests"]`
* console script: `ynab-il = "ynab_il_importer.cli:app"`

5. Minimal parsers (just enough to start)
   Implement minimal, robust readers that return DataFrames with canonical columns:

* `io_bank.read_bank(path) -> df`:

  * Detect that the “.xls” may be HTML; try `pandas.read_html` first; fallback to `read_excel`.
  * Output columns:

    * `source="bank"`
    * `date` (use `תאריך` parsed to date)
    * `secondary_date` (use `תאריך ערך` if present)
    * `description_raw` (use `תיאור`)
    * `ref` (use `אסמכתא`)
    * `outflow_ils` (from `בחובה`)
    * `inflow_ils` (from `בזכות`)
* `io_card.read_card(path) -> df`:

  * Use `read_excel` and locate the header row by searching for `תאריך עסקה` in the sheet.
  * Output columns:

    * `source="card"`
    * `date` (use `תאריך עסקה`)
    * `secondary_date` (use `תאריך חיוב` if present)
    * `merchant_raw` (use `שם בית העסק`)
    * `description_raw` (merchant_raw + optional `הערות`)
    * `outflow_ils` / `inflow_ils` (derived from amount; spending goes to outflow)
    * `currency` (use `מטבע חיוב` if present)
* `io_ynab.read_ynab_register(path) -> df`:

  * Read CSV.
  * Output columns:

    * `source="ynab"`
    * `date`
    * `payee_raw`
    * `category_raw` (category name columns if present)
    * `outflow_ils`
    * `inflow_ils`
    * `memo`

6. Normalization + fingerprint v0

* `normalize.normalize_text(s)`: lowercase, strip punctuation, collapse whitespace, remove long digit runs.
* `fingerprint.fingerprint_v0(s)`: normalize_text + remove remaining standalone numbers + keep first 6 tokens joined by space (good enough v0).

7. Pairing v0 (amount+date)
   Implement `pairing.match_pairs(bank_df, card_df, ynab_df)`:

* Convert all `date` to date (no time).
* Build keys:

* `key = (date, outflow_ils, inflow_ils)`
* Inner-join bank↔ynab on key; card↔ynab on key; concatenate results with columns:

  * `date`
  * `outflow_ils`
  * `inflow_ils`
  * `raw_text` (bank description_raw or card description_raw)
  * `raw_norm`
  * `fingerprint_v0`
  * `ynab_payee_raw` (hint)
  * `ynab_category_raw`
  * `source_type` (`bank` / `card`)
  * `source_file`
  * `source_account`
  * `ynab_file`
  * `ynab_account`
* If duplicates (same key matches multiple rows), keep all but add `ambiguous_key=true`.

8. Group builder v0
   Implement `build_groups.py`:

* Read matched pairs CSV
* Group by `fingerprint_v0` and compute:

  * `count`
  * `example_raw_text` (most common raw_text)
  * `top_ynab_payees` (top 3 payee_raw with counts, as a single string)
  * `top_ynab_categories` (top 3 category_raw with counts)
  * `canonical_payee` (empty)
* Write `data/derived/fingerprint_groups.csv`

9. Smoke test

* `tests/test_smoke.py`:

  * import package
  * call fingerprint_v0 on a sample string
  * ensure CLI help runs

10. README quickstart (commands)
    Include:

* `pixi install`
* Put sample files in `data/raw/` as:

  * `bank.xls`
  * `card.xlsx`
  * `ynab_register.csv`
* Run:

  * `pixi run python scripts/normalize_inputs.py ...` (or call CLI equivalents)
  * `pixi run python scripts/bootstrap_pairs.py`
  * `pixi run python scripts/build_groups.py`
* Output: `data/derived/matched_pairs.csv` and `data/derived/fingerprint_groups.csv`

11. Commit

* `git add .`
* `git commit -m "Initial scaffolding: parsers, pairing v0, fingerprint groups, pixi env"`
