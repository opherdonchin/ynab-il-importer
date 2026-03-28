# Active Plan

## Workstream

Merged onto branch `aikido-workflow`.

Current focus:
- use the unified review-row model as the only persisted review format
- bootstrap Aikido cleanly from fresh YNAB exports
- improve the review app so review state is easier to understand and act on

## Current Goal

Finish the Aikido bootstrap handoff so review can proceed from a clean baseline:
- rebuilt Aikido payee mapping from matched bootstrap pairs
- isolated historical unresolved cleanup from the forward bootstrap backlog
- make the review app communicate row/group state clearly

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
- review app primary status language changed to `Fix / Decide / Settled`
- review app now shows a 3-color legend for those primary states

Validated recently:
- focused review-app tests
- focused YNAB/fingerprint/payee-map tests
- Aikido payee-map validation

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

## Next Steps

1. Review `data/paired/aikido_bootstrap_2026_03_28/backlog_review_rows.csv`
2. Review `data/paired/aikido_bootstrap_2026_03_28/historical_unresolved_review_rows.csv`
3. Save reviewed outputs beside those files with `_reviewed` suffixes
4. Prepare upload artifacts from the reviewed backlog file
5. Reconcile post-upload and inspect any remaining unresolved Aikido rows

## Immediate UX Follow-Up

Next review-app improvement under consideration:
- simplify and improve the available filter dimensions so they match how users actually triage review work

## Deferred

- chooser-based manual relinking UI
- broader sync execution for every non-`create_target` action
- richer `update_maps` ergonomics beyond the minimal explicit form
