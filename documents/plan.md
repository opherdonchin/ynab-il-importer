# Active Plan

## Workstream

Merged onto branch `aikido-workflow`.

Current focus:
- use the unified review-row model as the only persisted review format
- bootstrap Aikido cleanly from fresh YNAB exports
- move from reviewed Aikido backlog into upload, post-upload reconcile, and stable historical baselining

## Current Goal

Execute the Aikido bootstrap backlog from a coherent reviewed state:
- reviewed backlog rows are complete and upload artifacts are ready
- the next operational step is upload execution for the prepared Aikido backlog
- after upload, reconcile from a stable historical source baseline and carry that forward deterministically

## Settled Product Decisions

- Source and target are both editable.
- Persisted selected fields are side-specific only.
- Unsuffixed selected fields are removed from the review CSV.
- `decision_action` stores the row action or default suggestion.
- `reviewed` is the approval gate; reviewed rows cannot carry `No decision`.
- Institutional sources cannot use `create_source`, `delete_source`, or `delete_both`.
- `update_map` becomes `update_maps`.
- Chooser-based manual relinking is deferred.
- Review-app primary state language is:
  - `Fix`
  - `Decide`
  - `Settled`
- Choosing a substantive row action automatically resolves competing rows:
  - matching or create/delete actions auto-set competing rows to `ignore_row`
  - `ignore_row` itself does not propagate
- Upload prep may fall back hidden or missing target categories to live YNAB `Uncategorized`.

See `documents/decisions/` for the schema and design contract behind the unified review model.

## Current Code State

Done:
- unified review-row hard cutover merged into `aikido-workflow`
- YNAB export normalization now runs the shared fingerprint path
- fresh Aikido bootstrap exports normalized under `data/derived/aikido_bootstrap_2026_03_28/`
- Aikido categories rebuilt into `outputs/aikido/ynab_categories.csv`
- bootstrap matching artifacts built under `data/paired/aikido_bootstrap_2026_03_28/`
- Aikido payee map rebuilt from bootstrap matched pairs
- historical unresolved Aikido review rows isolated in:
  - `data/paired/aikido_bootstrap_2026_03_28/historical_unresolved_review_rows.csv`
- forward Aikido backlog review rows isolated in:
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_review_rows.csv`
- Aikido payee-map rules updated from review decisions, including trial-lesson handling and explicit reviewed-map corrections
- review app primary status language changed to `Fix / Decide / Settled`
- review app now shows a 3-color legend for those primary states
- review app filter set now matches triage needs more closely:
  - `State`
  - `Save status`
  - `Row kind`
  - `Action`
  - `Blocker`
  - `Suggestions`
  - `Map updates`
  - `Search`
- row review flow now:
  - marks reviewed explicitly
  - advances to the next row in Row view after successful review
  - supports `Accept all set decisions`
  - keeps non-review actions open in place
- row and group actions now auto-ignore competing rows instead of relying on manual propagation checkboxes
- review detail panels are shared between Row and Grouped views
- category refresh is aligned to the workflow profile and uses the live YNAB category file shape
- hidden categories are excluded from the review-app target category choices
- upload prep now:
  - honors `memo_append`
  - prepares only explicit reviewed `create_target` rows
  - falls back hidden or missing category names to YNAB `Uncategorized`
- Aikido reviewed backlog upload artifacts were prepared successfully:
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_upload.csv`
  - `data/paired/aikido_bootstrap_2026_03_28/backlog_upload.json`

Validated recently:
- focused review-app tests
- focused YNAB/fingerprint/payee-map tests
- Aikido payee-map validation
- upload-prep dry run for the reviewed Aikido backlog

## Aikido Bootstrap Snapshot

Fresh bootstrap window:
- Family Aikido source rows: `2024-01-02` through `2026-03-25`
- Aikido reconciled target history through `2025-10-18`

Historical reconciled bootstrap slice:
- matched pairs: `157`
- unresolved historical rows: `28`

Forward backlog slice:
- backlog review rows: `68`
- rows with suggested target payee/category from the rebuilt map: `30`
- reviewed backlog rows: `68`
- upload-prep rows prepared: `68`

## Next Steps

1. Execute upload from `data/paired/aikido_bootstrap_2026_03_28/backlog_upload.json`
2. Reconcile Aikido after upload and inspect remaining unresolved rows
3. Build historical source snapshots so reconcile can be propagated forward from a stable baseline
4. Review the Pilates historical-reconcile workflow and prior notes before locking the Aikido historical-baseline process
5. Review `data/paired/aikido_bootstrap_2026_03_28/historical_unresolved_review_rows.csv`

## Deferred

- chooser-based manual relinking UI
- broader sync execution for every non-`create_target` action
- richer `update_maps` ergonomics beyond the minimal explicit form
