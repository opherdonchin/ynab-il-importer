# Aikido Baseline Reconciliation

## Purpose

This note records why the Aikido historical base is now good enough to carry forward, what was fixed live, and which artifact should be treated as the human-settled historical record.

## What Happened

The Aikido bootstrap backlog was reviewed, uploaded, and then checked again against fresh Family and Aikido YNAB exports from `2026-03-29`.

The post-upload dry-run reconcile showed:
- exact anchor month: `2025-09-01`
- full balance difference: `0.0`
- active forward window from `2025-11-01`: `68` matched, `0` unmatched source, `0` unmatched target, `0` ambiguous

That established that the balance baseline was already stable.

## Historical Concern

The historical unresolved rows were not an account-balance problem. They were mostly transaction-lineage mismatches that netted out inside the same month.

That meant:
- the monthly balances were trustworthy
- the historical line-by-line record still needed inspection before we could comfortably treat history as settled

## Live Fix

One historical row was a real structural mismatch:
- `2024-07-26`
- Family side had two source transactions
- Aikido side had one reconciled target transaction: `Miles Kessler -170`

That target transaction was fixed live in YNAB by turning it into:
- `Integral dojo -250`, memo `Seminar for 3`, category `*8* Seminars`
- plus a new `Ying Jin +80`, memo `Cash for Miles seminar`, category `*8* Seminars`

After that fix, the live historical compare improved from `157` to `159` matched pairs.

## Historical Pairing Policy

Using the live post-upload Aikido snapshot and a widened historical comparison window:
- `date-tolerance-days = 10`

the historical compare collapsed to:
- `170` matched pairs
- `0` unmatched source
- `0` unmatched target
- only one remaining ambiguity cluster: the March 2024 duplicate `Member Fees` case

That last cluster was then explicitly settled in the review artifact.

## Settled Artifact

Treat this file as the human-settled historical review record:

- `data/paired/aikido_cross_budget_live/history_review_rows_pre_2025_11_settled.csv`

It records the final March 2024 duplicate-fee decisions:
- `source_row_5945` -> `d315e5e7-ee89-4407-b4ba-be0d9763bd0e` as `keep_match`
- `source_row_5958` -> `b68ec802-5ab5-49d5-969c-962418b54547` as `keep_match`
- the competing rows are `ignore_row`

## Recommended Carry-Forward Baseline

Use the post-upload month report as the cached historical base and freeze it under the Pilates-style canonical name:

- `data/paired/aikido_cross_budget_live/anchored_reconcile_after_history_upload_month_report.csv`

Then run forward Aikido reconcile using:
- `--since 2025-11-01`
- `--source-month-report-in data/paired/aikido_cross_budget_live/anchored_reconcile_after_history_upload_month_report.csv`

## Practical Meaning

This does not claim that every historical step was originally entered perfectly.

It does mean:
- the account/category balance history is exact
- the one real broken structural row has been fixed live
- the remaining historical ambiguity has been explicitly settled in the review record
- we can now carry Aikido forward from a documented, comfortable baseline
