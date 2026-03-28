from __future__ import annotations

import pandas as pd

import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.validation as review_validation


def _review_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_validate_row_blocks_reviewed_no_decision_and_institutional_source_mutation() -> None:
    row = pd.Series(
        {
            "workflow_type": "institutional",
            "decision_action": "create_source",
            "reviewed": True,
            "source_payee_selected": "",
            "source_category_selected": "",
            "target_payee_selected": "Cafe",
            "target_category_selected": "Food",
            "update_maps": "fingerprint_add_source",
        }
    )

    errors, warnings = review_validation.validate_row(row)

    assert "create_source is not allowed for institutional sources" in errors
    assert "missing source payee" in errors
    assert "missing source category" in errors
    assert warnings == []


def test_review_component_errors_catch_unresolved_and_conflicting_rows() -> None:
    df = _review_rows(
        [
            {
                "source_row_id": "s1",
                "target_row_id": "t1",
                "workflow_type": "cross_budget",
                "decision_action": "keep_match",
                "reviewed": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
            {
                "source_row_id": "s1",
                "target_row_id": "t2",
                "workflow_type": "cross_budget",
                "decision_action": review_validation.NO_DECISION,
                "reviewed": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
            {
                "source_row_id": "s1",
                "target_row_id": "t3",
                "workflow_type": "cross_budget",
                "decision_action": "delete_source",
                "reviewed": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
        ]
    )

    errors = review_validation.review_component_errors(df, 0)

    assert "connected rows still contain No decision" in errors
    assert "row 1: reviewed row cannot have No decision" in errors
    assert "source transaction s1 is both matched and deleted" in errors


def test_load_save_roundtrip_uses_side_specific_selected_fields(tmp_path) -> None:
    src = tmp_path / "review.csv"
    pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "memo",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "update_maps": "fingerprint_add_source",
                "decision_action": "create_target",
                "fingerprint": "fp1",
                "workflow_type": "institutional",
                "source_payee_selected": "Cafe source",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "reviewed": "TRUE",
                "source_present": "TRUE",
                "target_present": "",
            }
        ]
    ).to_csv(src, index=False, encoding="utf-8-sig")

    loaded = review_io.load_proposed_transactions(src)
    assert loaded.loc[0, "payee_selected"] == "Cafe"
    assert loaded.loc[0, "category_selected"] == "Food"
    assert loaded.loc[0, "update_maps"] == "fingerprint_add_source"
    assert bool(loaded.loc[0, "reviewed"]) is True

    out = tmp_path / "reviewed.csv"
    review_io.save_reviewed_transactions(loaded, out)
    saved = pd.read_csv(out, dtype="string").fillna("")

    assert saved.loc[0, "target_payee_selected"] == "Cafe"
    assert saved.loc[0, "target_category_selected"] == "Food"
    assert saved.loc[0, "update_maps"] == "fingerprint_add_source"
    assert saved.loc[0, "reviewed"] == "TRUE"
    assert "payee_selected" not in saved.columns
    assert "category_selected" not in saved.columns
    assert "update_map" not in saved.columns


def test_summary_counts_and_filters_follow_new_decision_action_rules() -> None:
    df = _review_rows(
        [
            {
                "match_status": "source_only",
                "target_payee_selected": "",
                "target_category_selected": "",
                "decision_action": "create_target",
                "reviewed": False,
                "update_maps": "",
            },
            {
                "match_status": "ambiguous",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "decision_action": review_validation.NO_DECISION,
                "reviewed": False,
                "update_maps": "payee_add_fingerprint",
            },
            {
                "match_status": "matched_auto",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "decision_action": "keep_match",
                "reviewed": True,
                "update_maps": "",
            },
        ]
    )

    counts = review_state.summary_counts(df)
    reviewed_only = review_state.apply_filters(
        df,
        {
            "match_status": ["source_only", "ambiguous", "matched_auto"],
            "reviewed_mode": "reviewed",
            "unresolved_only": False,
        },
    )

    assert counts == {
        "total": 3,
        "missing_payee": 1,
        "missing_category": 1,
        "unresolved": 2,
        "update_maps": 1,
    }
    assert reviewed_only.index.tolist() == [2]


def test_related_rows_mask_can_expand_by_source_and_target() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1"},
            {"source_row_id": "s1", "target_row_id": "t2"},
            {"source_row_id": "s3", "target_row_id": "t1"},
            {"source_row_id": "s4", "target_row_id": "t4"},
        ]
    )

    mask = review_state.related_rows_mask(df, 0, include_source=True, include_target=True)

    assert mask.tolist() == [True, True, True, False]
