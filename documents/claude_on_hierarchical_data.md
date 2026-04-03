## Hierarchical Transactions: Revised Assessment

### The Actual Structure

```
Transaction
├── id, date, amount, account, memo        # always present
├── payee                                   # usually single, occasionally split
└── splits[]                               # present only when split
    ├── amount
    ├── category
    └── payee (optional override)
```

This is **sparse, shallow hierarchy** — not deep nesting. Most rows are flat; a minority have one level of children. This changes the recommendation substantially.

---

### Why the Previous Options Are Mostly Wrong Here

| Option | Problem |
|---|---|
| HDF5 / Zarr / NetCDF | Designed for dense numeric arrays — wrong abstraction entirely |
| Nested JSON per transaction | Works but makes aggregation and querying painful |
| Flat Parquet | Loses split structure unless you use list-of-struct columns |

---

### Appropriate Options

#### 1. **Two-table relational model** ← recommended
```
transactions: id, date, amount, account, memo, payee, category
splits:       id, transaction_id (FK), amount, category, payee
```
- Simple, queryable, standard
- A transaction with no splits is just a row in `transactions` with no children
- Aggregation, filtering, reporting all straightforward
- **File format:** SQLite (`.db`) for local use; Parquet pair for pipeline use

#### 2. **Single table with list-of-struct column (Parquet/Polars)**
```
transactions: id, date, amount, ..., splits: list[struct{amount, category, payee}]
```
- `splits` is null for most rows, a list for split transactions
- Good if you're working in Polars or PyArrow and want a single file
- Querying splits requires `explode()` — slightly awkward but manageable
- **File format:** `.parquet`

#### 3. **JSON per transaction**
- Natural fit for the schema, easy to read/write
- Fine for <10k transactions; becomes slow to parse and query at scale
- Loses easy aggregation
- **File format:** `.json` or `.jsonl` (one JSON object per line — better for streaming)

---

### Recommendation

**SQLite with two tables** is the right answer for your YNAB importer context:

- Matches how YNAB itself models splits (transactions + subtransactions)
- Queryable without loading everything into memory
- Easy to inspect with DBeaver or DB Browser for SQLite
- Python interface is stdlib (`sqlite3`) or lightweight ORM (`SQLAlchemy` core, `peewee`)
- Handles the 90%+ flat case with zero overhead

If you want a pure-file pipeline (no DB), use **two Parquet files** with the same schema — identical logic, just read with `polars` or `pandas`.

---

### Schema Sketch (SQLite)

```sql
CREATE TABLE transactions (
    id          TEXT PRIMARY KEY,   -- YNAB transaction id
    date        TEXT NOT NULL,
    amount      INTEGER NOT NULL,   -- milliunits, as YNAB uses
    account_id  TEXT NOT NULL,
    payee_id    TEXT,
    category_id TEXT,               -- NULL if split
    memo        TEXT,
    cleared     TEXT,
    approved    INTEGER
);

CREATE TABLE splits (
    id              TEXT PRIMARY KEY,
    transaction_id  TEXT NOT NULL REFERENCES transactions(id),
    amount          INTEGER NOT NULL,
    payee_id        TEXT,
    category_id     TEXT,
    memo            TEXT
);
```

This maps directly onto the YNAB API's `subtransactions` field, so import/export logic stays clean.