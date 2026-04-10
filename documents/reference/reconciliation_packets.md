# Reconciliation Packets

## Purpose

A reconciliation packet is a saved snapshot of one concrete closeout run.

Use it when you want:

- reproducible reruns without depending on live YNAB state
- a clean handoff bundle for later review
- durable evidence of which inputs, reports, and uploads belonged to one run

## Packet Shape

Packets live under:

```text
data/packets/<kind>/.../<window>/
```

Each packet contains:

- `inputs/`
- `artifacts/`
- `outputs/`
- `packet_manifest.json`
- `packet_summary.md`

Recommended roles:

- `inputs/`
  Raw statements, normalized Parquet inputs, YNAB snapshots.
- `artifacts/`
  Sync and reconcile reports, proposal artifacts, matched-pair artifacts.
- `outputs/`
  Reviewed artifacts, upload CSV/JSON, short human notes if needed.

## Packet Kinds

### Bank

Recommended inputs:

- raw bank statement
- normalized bank Parquet
- normalized YNAB Parquet snapshot

Recommended artifacts:

- bank sync report
- uncleared YNAB report
- bank reconcile report

### Card

Recommended inputs:

- current raw statement
- previous raw statement when used
- current normalized card Parquet
- previous normalized card Parquet when used
- normalized YNAB Parquet snapshot

Recommended artifacts:

- card sync report
- card reconcile report

### Cross-budget

The packet tooling still supports `cross_budget` for historical material, but cross-budget workflows are not part of the active repo path.

## Acquisition Methods

Prefer these flags so the manifest records provenance clearly:

- `--input-human ROLE=PATH`
  Original human-downloaded statements.
- `--input-agent ROLE=PATH`
  Direct YNAB downloads or other agent-fetched inputs.
- `--input-derived ROLE=PATH`
  Normalized Parquet inputs and other derived workflow inputs.
- `--artifact ROLE=PATH`
  Generated reports and proposal artifacts.
- `--output ROLE=PATH`
  Reviewed artifacts and upload payloads.

## Current Tool

Create packets with:

```bash
pixi run python scripts/package_reconciliation_packet.py ...
```

The script can:

- copy files into the packet directory
- write a manifest-only packet with original file references
- preserve key commands
- preserve short notes and explicit exception ids

## Minimal Example

```bash
pixi run python scripts/package_reconciliation_packet.py \
  --kind bank \
  --profile family \
  --account "Bank Leumi" \
  --window-start 2026-04-01 \
  --window-end 2026-04-30 \
  --input-human statement_raw=data/raw/2026_04_01/Bankin\ family.dat \
  --input-derived statement_norm=data/derived/2026_04_01/family_leumi_norm.parquet \
  --input-agent ynab_snapshot=data/derived/2026_04_01/family_ynab_api_norm.parquet \
  --artifact sync_report=data/paired/2026_04_01/family_family_bank_bank_sync_report.csv \
  --artifact reconcile_report=data/paired/2026_04_01/family_family_bank_bank_reconcile_report.csv \
  --output reviewed=data/paired/2026_04_01/family_proposed_transactions_reviewed.parquet
```

## Recommendation

Use packets for runs you may need to revisit. They are especially helpful once a run has:

- real uploads
- real sync/reconcile reports
- manual exceptions worth preserving
