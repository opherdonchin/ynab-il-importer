# Cross-Budget Review Packet Manifest

## Plans And Handoff

1. `documents/project_context.md`
2. `documents/handoffs/cross_budget_review_2026_03_25/plan_snapshot.md`
3. `documents/drafts/cross_budget_review_v2_plan.md`
4. `documents/handoffs/cross_budget_review_2026_03_25/handoff.md`
5. `documents/update_workflow.md`
6. `documents/review_app_workflow.md`
7. `documents/handoffs/cross_budget_review_2026_03_25/cross_budget_review_handoff_2026_03_25.zip` (local zip export)

## Current Matching / Proposal Code

1. `scripts/build_cross_budget_proposed.py`
2. `scripts/build_proposed_transactions.py`
3. `scripts/bootstrap_cross_budget_pairs.py`
4. `src/ynab_il_importer/cross_budget_pairing.py`
5. `src/ynab_il_importer/pairing.py`
6. `src/ynab_il_importer/cross_budget_reconciliation.py`
7. `src/ynab_il_importer/upload_prep.py`

## Current Review App Code

1. `src/ynab_il_importer/review_app/app.py`
2. `src/ynab_il_importer/review_app/io.py`
3. `src/ynab_il_importer/review_app/model.py`
4. `src/ynab_il_importer/review_app/state.py`
5. `src/ynab_il_importer/review_app/validation.py`

## Current Tests

1. `tests/test_cross_budget_pairing.py`
2. `tests/test_build_cross_budget_proposed.py`
3. `tests/test_cross_budget_reconciliation.py`
4. `tests/test_upload_prep.py`
5. `tests/test_review_app.py`
6. `tests/test_review_app_wrapper.py`

## Example Pilates Artifacts

1. `data/paired/pilates_cross_budget_live/proposed_transactions.csv`
2. `data/paired/pilates_cross_budget_live/family_direct_fix_candidates.csv`
3. `data/paired/pilates_cross_budget_live/pilates_direct_fix_candidates.csv`
4. `data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_post_user_fix_report.csv`
5. `data/paired/pilates_cross_budget_live/final_cross_budget_reconcile_post_user_fix_month_report.csv`

## Example Aikido Artifacts

1. `data/paired/2026_03_25_aikido/aikido_full_backlog_to_current_proposed_transactions.csv`
2. `data/paired/2026_03_25_aikido/aikido_full_backlog_to_current_unmatched_source.csv`
3. `data/paired/2026_03_25_aikido/aikido_cross_budget_reconcile_report.csv`
4. `data/paired/2026_03_25_aikido/aikido_status_2026_03_25.md`
