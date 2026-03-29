# Review App Workflow

This document describes the current review-app loop for unified review-row CSVs.

## Purpose

The review app is the human checkpoint between deterministic proposal building and any YNAB mutation. It should let us inspect, correct, save, rerun, and only then prepare uploads or sync actions.

## Inputs

Minimum inputs:
- proposed review CSV from a builder such as `scripts/build_proposed_transactions.py` or `scripts/build_cross_budget_review_rows.py`
- YNAB categories CSV from `scripts/download_ynab_categories.py` or `scripts/build_categories_from_ynab_snapshot.py`

Typical launch:

```bash
pixi run python scripts/review_app.py \
  --in data/paired/<date>/proposed_transactions.csv \
  --categories outputs/ynab_categories.csv
```

Resume a prior session:

```bash
pixi run python scripts/review_app.py --resume
```

## Core Review Model

Each row is a source/target review candidate. The app derives three primary states:
- `Fix`: the row is blocked by missing required fields or validation problems
- `Decide`: the row is valid enough to review but still needs a user decision
- `Settled`: the row is internally consistent and no longer needs active attention

Important contract points:
- `reviewed` is the approval gate
- `decision_action` stores the selected action
- source and target selected fields are side-specific
- competing rows are auto-resolved when a substantive action is chosen

For the authoritative design contract, see:
- `documents/decisions/unified_review_model_design.md`
- `documents/decisions/unified_review_model_schema.md`

## Typical Session

1. Load the proposed CSV and categories file.
2. Filter to rows that are not yet settled.
3. Inspect blocker, state, and suggestion information.
4. Edit selected payee/category fields or decision fields as needed.
5. Review rows individually or in grouped mode.
6. Save the reviewed CSV.
7. Rerun downstream prep or reconcile steps against the saved artifact.

## Views

Row view:
- best for line-by-line inspection and editing
- advances to the next row after successful review

Grouped view:
- groups related rows by fingerprint
- useful when many candidates share the same explanation or intended decision

## After Review

Common next steps for institutional import:

1. Prepare upload artifacts:

```bash
pixi run python scripts/prepare_ynab_upload.py \
  --in data/paired/<date>/proposed_transactions_reviewed.csv \
  --out data/paired/<date>/ynab_upload.csv \
  --ready-only \
  --skip-missing-accounts
```

2. Dry-run or execute sync/reconcile scripts as appropriate for the workflow.

Cross-budget workflows may instead feed the reviewed CSV into the relevant reconciliation flow rather than upload prep.

## Guardrails

- Reruns are expected and should be safe.
- Review CSVs are intended to be inspectable and versionable.
- Upload prep should only act on explicit reviewed decisions.
- Hidden inference is less important than deterministic artifacts a human can verify.

## Related Docs

- `documents/project_context.md`
- `documents/plan.md`
- `documents/hostile_audit_report.md`
