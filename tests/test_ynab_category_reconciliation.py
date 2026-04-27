import polars as pl
import pytest

import ynab_il_importer.ynab_category_reconciliation as category_reconciliation


def test_run_month_from_tag_requires_expected_format() -> None:
    assert category_reconciliation.run_month_from_tag("2026_04_12") == "2026-04-01"

    with pytest.raises(ValueError, match="YYYY_MM_DD"):
        category_reconciliation.run_month_from_tag("2026-04-12")


def test_select_review_rows_for_source_filters_reviewed_category_rows() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["keep-1", "drop-1", "drop-2"],
            "reviewed": [True, True, False],
            "source_present": [True, True, True],
            "source_context_kind": [
                "ynab_parent_category_match",
                "ynab_parent_category_match",
                "ynab_parent_category_match",
            ],
            "source_context_category_id": ["cat-aikido", "cat-other", "cat-aikido"],
            "source_context_category_name": ["Aikido", "Other", "Aikido"],
            "target_account": [
                "Personal In Leumi",
                "Personal In Leumi",
                "Personal In Leumi",
            ],
            "source_date": ["2026-03-01", "2026-03-02", "2026-03-03"],
            "source_payee_current": ["Tayo", "Nope", "Nope"],
        }
    )

    selected = category_reconciliation.select_review_rows_for_source(
        reviewed,
        source=category_reconciliation.CategoryReconcileSource(
            category_id="cat-aikido",
            category_name="",
            target_account_id="acc-target",
            target_account_name="Personal In Leumi",
        ),
    )

    assert selected["transaction_id"].to_list() == ["keep-1"]


def test_resolve_month_category_accepts_name_or_id() -> None:
    month_detail = {
        "categories": [
            {"id": "cat-aikido", "name": "Aikido", "deleted": False},
            {"id": "cat-other", "name": "Other", "deleted": False},
        ]
    }

    assert (
        category_reconciliation.resolve_month_category(
            month_detail,
            category_id="cat-aikido",
        )["name"]
        == "Aikido"
    )
    assert (
        category_reconciliation.resolve_month_category(
            month_detail,
            category_name="Aikido",
        )["id"]
        == "cat-aikido"
    )


def test_plan_category_account_reconciliation_resolves_create_target_uploads() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["create_target"],
            "source_date": ["2026-03-31"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [200.0],
            "inflow_ils": [0.0],
        }
    )
    prepared_units = pl.DataFrame(
        {
            "upload_transaction_id": ["review-1"],
            "import_id": ["YNAB:-200000:2026-03-31:1"],
            "existing_transaction_id": [""],
            "account_id": ["acc-target"],
        }
    )
    target_transactions = [
        {
            "id": "txn-created-1",
            "account_id": "acc-target",
            "import_id": "YNAB:-200000:2026-03-31:1",
            "amount": -200000,
            "date": "2026-03-31",
            "payee_name": "Tayo",
            "cleared": "cleared",
        }
    ]
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 500000,
        "cleared_balance": 500000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 500000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        prepared_units,
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is True
    assert result["update_count"] == 1
    assert result["updates"] == [{"id": "txn-created-1", "cleared": "reconciled"}]
    assert result["report"]["action"].tolist() == ["reconcile"]


def test_plan_category_account_reconciliation_accepts_already_reconciled_keep_match() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["keep_match"],
            "target_row_id": ["txn-live-1"],
            "source_date": ["2026-03-31"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [0.0],
            "inflow_ils": [150.0],
        }
    )
    target_transactions = [
        {
            "id": "txn-live-1",
            "account_id": "acc-target",
            "import_id": "",
            "amount": 150000,
            "date": "2026-03-31",
            "payee_name": "Tayo",
            "cleared": "reconciled",
        }
    ]
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 150000,
        "cleared_balance": 150000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 150000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        pl.DataFrame(),
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is True
    assert result["update_count"] == 0
    assert result["already_reconciled_count"] == 1
    assert result["report"]["action"].tolist() == ["already_reconciled"]


def test_plan_category_account_reconciliation_prefers_live_target_id_for_keep_match() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["keep_match"],
            "target_row_id": ["tgt-synthetic-1"],
            "target_transaction_id": ["txn-live-1"],
            "target_ynab_id": ["txn-live-1"],
            "source_date": ["2026-03-31"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [0.0],
            "inflow_ils": [150.0],
        }
    )
    target_transactions = [
        {
            "id": "txn-live-1",
            "account_id": "acc-target",
            "import_id": "",
            "amount": 150000,
            "date": "2026-03-31",
            "payee_name": "Tayo",
            "cleared": "reconciled",
        }
    ]
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 150000,
        "cleared_balance": 150000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 150000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        pl.DataFrame(),
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is True
    assert result["report"]["resolved_transaction_id"].tolist() == ["txn-live-1"]
    assert result["report"]["action"].tolist() == ["already_reconciled"]


def test_plan_category_account_reconciliation_blocks_when_uploaded_row_is_missing() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["create_target"],
            "source_date": ["2026-03-31"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [200.0],
            "inflow_ils": [0.0],
        }
    )
    prepared_units = pl.DataFrame(
        {
            "upload_transaction_id": ["review-1"],
            "import_id": ["YNAB:-200000:2026-03-31:1"],
            "existing_transaction_id": [""],
            "account_id": ["acc-target"],
        }
    )
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 500000,
        "cleared_balance": 500000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 500000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        prepared_units,
        target_transactions=[],
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is False
    assert "missing_uploaded_transaction_in_live_ynab" in result["reason"]
    assert result["report"]["action"].tolist() == ["blocked"]


def test_plan_category_account_reconciliation_skips_legacy_pre_run_missing_uploads() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["create_target"],
            "source_date": ["2024-01-04"],
            "source_payee_current": ["Legacy row"],
            "source_cleared": ["reconciled"],
            "outflow_ils": [200.0],
            "inflow_ils": [0.0],
        }
    )
    prepared_units = pl.DataFrame(
        {
            "upload_transaction_id": ["review-1"],
            "import_id": ["YNAB:-200000:2024-01-04:1"],
            "existing_transaction_id": [""],
            "account_id": ["acc-target"],
        }
    )
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 500000,
        "cleared_balance": 500000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 500000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        prepared_units,
        target_transactions=[],
        target_account=target_account,
        source_category=source_category,
        run_month="2026-04-01",
    )

    assert result["ok"] is True
    assert result["blocked_count"] == 0
    assert result["skipped_count"] == 1
    assert result["report"]["action"].tolist() == ["skipped"]
    assert result["report"]["reason"].tolist() == [
        "legacy_pre_run_source_row_without_live_import"
    ]


def test_plan_category_account_reconciliation_blocks_on_cleared_balance_mismatch() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["keep_match"],
            "target_row_id": ["txn-live-1"],
            "source_date": ["2026-03-31"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [0.0],
            "inflow_ils": [150.0],
        }
    )
    target_transactions = [
        {
            "id": "txn-live-1",
            "account_id": "acc-target",
            "import_id": "",
            "amount": 150000,
            "date": "2026-03-31",
            "payee_name": "Tayo",
            "cleared": "reconciled",
        }
    ]
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 500000,
        "cleared_balance": 400000,
        "uncleared_balance": 0,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 500000}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        pl.DataFrame(),
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is False
    assert "category/cleared balance mismatch" in result["reason"]
    assert result["cleared_parity_ok"] is False


def test_plan_category_account_reconciliation_projects_uncleared_reconciles_into_parity() -> None:
    reviewed = pl.DataFrame(
        {
            "transaction_id": ["review-1"],
            "decision_action": ["keep_match"],
            "target_transaction_id": ["txn-live-1"],
            "target_ynab_id": ["txn-live-1"],
            "source_date": ["2026-04-06"],
            "source_payee_current": ["Tayo"],
            "outflow_ils": [0.0],
            "inflow_ils": [1070.0],
        }
    )
    target_transactions = [
        {
            "id": "txn-live-1",
            "account_id": "acc-target",
            "import_id": "",
            "amount": 1070000,
            "date": "2026-04-06",
            "payee_name": "Tayo",
            "cleared": "uncleared",
        }
    ]
    target_account = {
        "id": "acc-target",
        "name": "Personal In Leumi",
        "balance": 12171310,
        "cleared_balance": 11101310,
        "uncleared_balance": 1070000,
    }
    source_category = {"id": "cat-aikido", "name": "Aikido", "balance": 12171310}

    result = category_reconciliation.plan_category_account_reconciliation(
        reviewed,
        pl.DataFrame(),
        target_transactions=target_transactions,
        target_account=target_account,
        source_category=source_category,
    )

    assert result["ok"] is True
    assert result["update_count"] == 1
    assert result["cleared_parity_ok"] is True
    assert result["uncleared_zero_ok"] is True
    assert result["projected_target_account_cleared_balance_ils"] == 12171.31
    assert result["projected_target_account_uncleared_balance_ils"] == 0.0
