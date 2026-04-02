# Review App Workflow

This document describes the current review-app loop for unified review-row CSVs.

## Purpose

The review app is the human checkpoint between deterministic proposal building and any YNAB mutation. It should let us inspect, correct, save, rerun, and only then prepare uploads or sync actions.

## Inputs

Minimum inputs:
- proposed review CSV from a builder such as `scripts/build_proposed_transactions.py` or `scripts/build_cross_budget_review_rows.py`
- YNAB categories CSV from `scripts/download_ynab_categories.py` or `scripts/build_categories_from_ynab_snapshot.py`

Review CSV format note:
- the review loader expects unified review-row CSVs
- older institutional reviewed CSVs from before the unified cutover must be translated first with `scripts/translate_review_csv.py`
- the translator detects the format from the CSV columns and writes an explicit unified artifact such as `*_unified_v1.csv`

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

Translate a legacy reviewed CSV before reuse:

```bash
pixi run python scripts/translate_review_csv.py \
  --in data/paired/<old-date>/proposed_transactions_reviewed.csv \
  --out data/paired/<old-date>/proposed_transactions_reviewed_unified_v1.csv
```

## Core Review Model

Each row is a source/target review candidate. The app derives three primary states:
- `Fix`: the row is unresolved or blocked and still needs a decision or correction
- `Decide`: the row has a concrete default or chosen action and is waiting for approval
- `Settled`: the row is internally consistent and no longer needs active attention

Important contract points:
- `reviewed` is the approval gate
- `decision_action` stores the selected action
- source and target selected fields are side-specific
- competing rows are auto-resolved when a substantive action is chosen
- `None` in a selected category field means "no category required", not "missing category"
- institutional rows that were already exact-matched and have YNAB `cleared` or `reconciled` state are labeled `matched_cleared` and start out settled

For the authoritative design contract, see:
- `documents/decisions/unified_review_model_design.md`
- `documents/decisions/unified_review_model_schema.md`

## Typical Session

1. Load the proposed CSV and categories file.
2. Filter to rows that are not yet settled.
3. By default, leave `Matched cleared` hidden unless you are auditing prior settled work.
4. Inspect blocker, state, and suggestion information.
5. Edit selected payee/category fields or decision fields as needed.
6. Review rows individually or in grouped mode.
7. Save the reviewed CSV.
8. Rerun downstream prep or reconcile steps against the saved artifact.

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
