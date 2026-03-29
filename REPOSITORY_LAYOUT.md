# Repository Layout

## Source Of Truth

- `src/`: application code.
  - `src/ynab_il_importer/review_app/`: review-app package split into UI (`app.py`), state derivation (`state.py`), validation (`validation.py`), mutation helpers (`model.py`), and CSV I/O (`io.py`).
  - `src/ynab_il_importer/safe_types.py`: shared safe coercion helpers for CSV-backed booleans and similar fields.
- `scripts/`: command-line entry points and operational helpers.
- `tests/`: automated tests and small fixture data.
- `mappings/`: versioned mapping tables and profile-specific rule files.
- `documents/project_context.md`: durable project orientation.
- `documents/plan.md`: current execution plan.

## Documents

- `documents/archive/`: timestamped historical plan snapshots.
- `documents/drafts/`: active in-progress design notes that are not yet the main plan.
- `documents/handoffs/`: packaged review packets, manifests, and related handoff notes.

## Local Operational State

- `data/`: local raw inputs, normalized snapshots, paired artifacts, and saved reconciliation packets.
- `outputs/`: generated review/build outputs and review-app session state.
- `tmp/`: local scratch space for one-off checks.
- `tests_runtime/`: generated runtime files from tests.

These paths are intentionally treated as local working state rather than durable repository history.

## Retention Rules

- Put stable narrative documentation under `documents/`.
- Put active but provisional planning work under `documents/drafts/`.
- Put review packets and one-off handoff bundles under `documents/handoffs/<topic_or_date>/`.
- Keep versioned source-of-truth tables in `mappings/`.
- Keep durable operational evidence in `data/packets/` rather than in `outputs/`.
- Do not leave temporary exports, test sandboxes, or session logs at the repository root.
