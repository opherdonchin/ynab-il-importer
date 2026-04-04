from __future__ import annotations

from pathlib import Path
from typing import Any

import math
import pandas as pd
import polars as pl

import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
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


def test_parse_option_string_ignores_nan_values() -> None:
    assert review_model.parse_option_string(math.nan) == []


def test_validate_row_accepts_plain_mapping() -> None:
    errors, warnings = review_validation.validate_row(
        {
            "workflow_type": "cross_budget",
            "decision_action": "create_target",
            "reviewed": False,
            "source_payee_selected": "Cafe",
            "source_category_selected": "Food",
            "target_payee_selected": "Cafe",
            "target_category_selected": "Food",
            "payee_options": "Cafe;Bakery",
            "category_options": "Food;Dining",
            "update_maps": "",
        }
    )

    assert errors == []
    assert warnings == []


def test_normalize_decision_action_scalar_defaults_blank_to_no_decision() -> None:
    assert review_validation.normalize_decision_action("") == review_validation.NO_DECISION
    assert review_validation.normalize_decision_action("  keep_match  ") == "keep_match"


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


def test_blocker_series_marks_missing_reviewed_payee() -> None:
    df = _review_rows(
        [
            {
                "workflow_type": "cross_budget",
                "decision_action": "create_target",
                "reviewed": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "",
                "target_category_selected": "Food",
                "update_maps": "",
            }
        ]
    )

    blockers = review_validation.blocker_series(df)

    assert blockers.tolist() == ["Missing payee"]


def test_blocker_series_is_none_for_settled_consistent_rows() -> None:
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
            }
        ]
    )

    blockers = review_validation.blocker_series(df)

    assert blockers.tolist() == ["None"]


def test_allowed_decision_actions_accepts_plain_mapping() -> None:
    actions = review_validation.allowed_decision_actions(
        {
            "workflow_type": "institutional",
            "source_present": False,
            "target_present": True,
        }
    )

    assert actions == [review_validation.NO_DECISION, "delete_target", "ignore_row"]


def test_most_common_value_accepts_polars_series() -> None:
    series = pl.Series(["Cafe", "Bakery", "Cafe", ""])

    assert review_state.most_common_value(series) == "Cafe"


def test_grouped_row_indices_accepts_polars_frame() -> None:
    df = pl.DataFrame({"fingerprint": ["fp-a", "fp-b", "fp-a", ""]})

    fingerprints, group_indices = review_state.grouped_row_indices(df)

    assert fingerprints == ["fp-a", "fp-b"]
    assert group_indices == {"fp-a": [0, 2], "fp-b": [1]}


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


def test_save_reviewed_transactions_prefers_side_specific_target_columns(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "memo": "memo",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "fingerprint": "fp1",
                "workflow_type": "institutional",
                "source_payee_selected": "",
                "source_category_selected": "",
                "target_payee_selected": "Target Payee",
                "target_category_selected": "Target Category",
                "payee_selected": "",
                "category_selected": "",
                "reviewed": True,
                "source_present": True,
                "target_present": False,
            }
        ]
    )

    out = tmp_path / "reviewed.csv"
    review_io.save_reviewed_transactions(df, out)
    saved = pd.read_csv(out, dtype="string").fillna("")

    assert saved.loc[0, "target_payee_selected"] == "Target Payee"
    assert saved.loc[0, "target_category_selected"] == "Target Category"


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


def test_filtered_row_indices_follow_series_filters_without_dataframe_masks() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "keep",
                "match_status": "source_only",
                "decision_action": "create_target",
                "reviewed": False,
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
            {
                "transaction_id": "hide",
                "match_status": "ambiguous",
                "decision_action": review_validation.NO_DECISION,
                "reviewed": False,
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "payee_add_fingerprint",
            },
        ]
    )

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)
    row_kind_series = review_state.row_kind_series(df)
    action_series = review_state.action_series(df)
    save_state = pd.Series(["Unsaved", "Saved"], index=df.index, dtype="string")
    suggestion_series = review_state.suggestion_series(df)
    map_update_series = review_state.map_update_filter_series(df)
    search_text = pd.Series(["keep me", "hide me"], index=df.index, dtype="string")

    indices = review_state.filtered_row_indices(
        df.index,
        primary_state=["Decide"],
        row_kind=["Source only", "Ambiguous"],
        action_filter=["create_target"],
        save_status=["Unsaved", "Saved"],
        blocker_filter=["None"],
        suggestion_filter=["No suggestions", "Has suggestions"],
        map_update_filter=["Has update_maps", "No update_maps"],
        primary_state_series=primary_state_series,
        row_kind_series=row_kind_series,
        action_series=action_series,
        save_state=save_state,
        blocker_series=blocker_series,
        suggestion_series=suggestion_series,
        map_update_series=map_update_series,
        search_query="keep",
        search_text=search_text,
    )

    assert indices == [0]


def test_state_matrix_counts_accepts_series_inputs() -> None:
    primary_state = pd.Series(["Fix", "Fix", "Settled"], dtype="string")
    save_state = pd.Series(["Unsaved", "Saved", "Saved"], dtype="string")

    counts = review_state.state_matrix_counts(primary_state, save_state)

    assert counts == {
        "Fix / Unsaved": 1,
        "Fix / Saved": 1,
        "Settled / Saved": 1,
    }


def test_primary_state_series_maps_fix_decide_and_settled() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "fix",
                "decision_action": "create_target",
                "reviewed": True,
                "target_payee_selected": "",
                "target_category_selected": "Food",
            },
            {
                "transaction_id": "decide",
                "decision_action": "create_target",
                "reviewed": False,
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
            },
            {
                "transaction_id": "settled",
                "decision_action": "keep_match",
                "reviewed": True,
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
            },
        ]
    )
    blockers = review_validation.blocker_series(df)

    states = review_state.primary_state_series(df, blockers)

    assert states.tolist() == ["Fix", "Decide", "Settled"]


def test_allowed_decision_actions_allow_source_mutation_for_cross_budget_target_only() -> None:
    actions = review_validation.allowed_decision_actions(
        pd.Series(
            {
                "workflow_type": "cross_budget",
                "source_present": False,
                "target_present": True,
            }
        )
    )

    assert actions == [review_validation.NO_DECISION, "create_source", "delete_target", "ignore_row"]


def test_apply_review_state_rejects_reviewed_no_decision() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "t1",
                "source_row_id": "s1",
                "target_row_id": "t1",
                "workflow_type": "cross_budget",
                "decision_action": review_validation.NO_DECISION,
                "reviewed": False,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            }
        ]
    )

    updated, errors = review_validation.apply_review_state(df, [0], reviewed=True)

    assert updated["reviewed"].tolist() == [False]
    assert errors == [
        "connected rows still contain No decision",
        "row 0: reviewed row cannot have No decision",
    ]


def test_apply_review_state_reuses_provided_component_map(monkeypatch) -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "t1",
                "source_row_id": "s1",
                "target_row_id": "t1",
                "workflow_type": "cross_budget",
                "decision_action": "keep_match",
                "reviewed": False,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
            {
                "transaction_id": "t2",
                "source_row_id": "s1",
                "target_row_id": "t2",
                "workflow_type": "cross_budget",
                "decision_action": "ignore_row",
                "reviewed": False,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
        ]
    )
    component_map = review_validation.precompute_components(df)

    def fail(_: pd.DataFrame) -> dict[object, int]:
        raise AssertionError("precompute_components should not be called")

    monkeypatch.setattr(review_validation, "precompute_components", fail)

    updated, errors = review_validation.apply_review_state(
        df,
        [0],
        reviewed=True,
        component_map=component_map,
    )

    assert errors == []
    assert updated["reviewed"].tolist() == [True, True]


def test_apply_competing_row_resolution_ignores_conflicts() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1", "decision_action": "keep_match"},
            {"source_row_id": "s1", "target_row_id": "t2", "decision_action": review_validation.NO_DECISION},
            {"source_row_id": "s3", "target_row_id": "t1", "decision_action": review_validation.NO_DECISION},
        ]
    )

    touched = review_model.apply_competing_row_resolution(df, [0])

    assert touched == [1, 2]
    assert df["decision_action"].tolist() == ["keep_match", "ignore_row", "ignore_row"]


def test_uncategorized_mask_detects_uncategorized_label() -> None:
    df = _review_rows(
        [
            {"transaction_id": "t1", "target_category_selected": "Uncategorized"},
            {"transaction_id": "t2", "target_category_selected": "Food"},
        ]
    )

    mask = review_state.uncategorized_mask(df)

    assert mask.tolist() == [True, False]


def test_blocker_series_allows_reviewed_uncategorized_row() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "t1",
                "workflow_type": "institutional",
                "decision_action": "create_target",
                "reviewed": True,
                "source_payee_selected": "bit",
                "source_category_selected": "",
                "target_payee_selected": "Bit",
                "target_category_selected": "Uncategorized",
                "update_maps": "",
                "source_present": True,
                "target_present": False,
            }
        ]
    )

    blockers = review_validation.blocker_series(df)
    states = review_state.primary_state_series(df, blockers)

    assert blockers.tolist() == ["None"]
    assert states.tolist() == ["Settled"]


def test_search_text_series_contains_payee_and_memo() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "t1",
                "memo": "weekly groceries",
                "target_payee_selected": "Cafe Roma",
            }
        ]
    )

    search_text = review_state.search_text_series(df)

    assert "weekly groceries" in search_text.iloc[0]
    assert "cafe roma" in search_text.iloc[0]


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


def test_related_row_indices_accept_polars_and_preserve_order() -> None:
    df = pl.DataFrame(
        {
            "source_row_id": ["s1", "s1", "s3", "s4"],
            "target_row_id": ["t1", "t2", "t1", "t4"],
        }
    )

    indices = review_state.related_row_indices(df, 0, include_source=True, include_target=True)

    assert indices == [0, 1, 2]


def test_precompute_components_single_component() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1"},
            {"source_row_id": "s1", "target_row_id": "t2"},
            {"source_row_id": "s3", "target_row_id": "t2"},
        ]
    )

    component_map = review_validation.precompute_components(df)

    assert set(component_map.values()) == {0}


def test_precompute_components_two_components() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1"},
            {"source_row_id": "s1", "target_row_id": "t2"},
            {"source_row_id": "s3", "target_row_id": "t3"},
        ]
    )

    component_map = review_validation.precompute_components(df)

    assert component_map[0] == component_map[1]
    assert component_map[2] != component_map[0]


def test_apply_row_edit_propagates_to_related_indices() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1", "source_payee_selected": "", "target_payee_selected": "", "target_category_selected": ""},
            {"source_row_id": "s1", "target_row_id": "t2", "source_payee_selected": "", "target_payee_selected": "", "target_category_selected": ""},
            {"source_row_id": "s3", "target_row_id": "t1", "source_payee_selected": "", "target_payee_selected": "", "target_category_selected": ""},
        ]
    )

    review_state.apply_row_edit(
        df,
        0,
        source_payee="Source Cafe",
        target_payee="Target Cafe",
        target_category="Food",
    )

    assert df["source_payee_selected"].tolist() == ["Source Cafe", "Source Cafe", ""]
    assert df["target_payee_selected"].tolist() == ["Target Cafe", "", "Target Cafe"]
    assert df["target_category_selected"].tolist() == ["Food", "", "Food"]


def test_precompute_components_accepts_polars_review_table() -> None:
    df = pl.DataFrame(
        {
            "source_row_id": ["s1", "s1", "s3"],
            "target_row_id": ["t1", "t2", "t2"],
        }
    )

    component_map = review_validation.precompute_components(df)

    assert set(component_map.keys()) == {0, 1, 2}
    assert set(component_map.values()) == {0}


def test_precompute_component_errors_propagates() -> None:
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
                "source_row_id": "s3",
                "target_row_id": "t3",
                "workflow_type": "cross_budget",
                "decision_action": "keep_match",
                "reviewed": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "update_maps": "",
            },
        ]
    )

    component_map = review_validation.precompute_components(df)
    component_errors = review_validation.precompute_component_errors(df, component_map)

    assert component_errors[component_map[0]] == component_errors[component_map[1]]
    assert "connected rows still contain No decision" in component_errors[component_map[0]]
    assert component_errors[component_map[2]] == []


def test_precompute_component_errors_accepts_polars_review_table() -> None:
    df = pl.DataFrame(
        {
            "source_row_id": ["s1", "s1", "s3"],
            "target_row_id": ["t1", "t2", "t3"],
            "workflow_type": ["cross_budget", "cross_budget", "cross_budget"],
            "decision_action": ["keep_match", review_validation.NO_DECISION, "keep_match"],
            "reviewed": [True, True, True],
            "source_payee_selected": ["Cafe", "Cafe", "Cafe"],
            "source_category_selected": ["Food", "Food", "Food"],
            "target_payee_selected": ["Cafe", "Cafe", "Cafe"],
            "target_category_selected": ["Food", "Food", "Food"],
            "update_maps": ["", "", ""],
        }
    )

    component_map = review_validation.precompute_components(df)
    component_errors = review_validation.precompute_component_errors(df, component_map)

    assert "connected rows still contain No decision" in component_errors[component_map[0]]
    assert component_errors[component_map[2]] == []


def test_blocker_series_with_components_returns_series_and_component_map() -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1", "decision_action": "keep_match"},
            {"source_row_id": "s1", "target_row_id": "t2", "decision_action": "ignore_row"},
            {"source_row_id": "s3", "target_row_id": "t3", "decision_action": "keep_match"},
        ]
    )

    blockers, component_map = review_validation.blocker_series_with_components(df)

    assert blockers.index.tolist() == df.index.tolist()
    assert component_map[0] == component_map[1]
    assert component_map[2] != component_map[0]


def test_blocker_series_with_components_uses_supplied_component_map(monkeypatch) -> None:
    df = _review_rows(
        [
            {"source_row_id": "s1", "target_row_id": "t1", "decision_action": "keep_match"},
            {"source_row_id": "s1", "target_row_id": "t2", "decision_action": "ignore_row"},
        ]
    )
    component_map = {0: 7, 1: 7}

    def fail(_: pd.DataFrame | pl.DataFrame) -> dict[Any, int]:
        raise AssertionError("precompute_components should not be called")

    monkeypatch.setattr(review_validation, "precompute_components", fail)

    blockers, reused_map = review_validation.blocker_series_with_components(
        df,
        component_map=component_map,
    )

    assert blockers.index.tolist() == df.index.tolist()
    assert reused_map == component_map


def test_derive_inference_tags_marks_missing_rows() -> None:
    df = _review_rows(
        [
            {
                "transaction_id": "t1",
                "match_status": "matched_auto",
                "target_payee_selected": "",
                "target_category_selected": "",
            },
            {
                "transaction_id": "t2",
                "match_status": "ambiguous",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
            },
        ]
    )

    inferred = review_state.derive_inference_tags(df)

    assert inferred.tolist() == ["missing", "ambiguous"]


def test_initial_inference_tags_aligns_by_transaction_occurrence() -> None:
    base = _review_rows(
        [
            {
                "transaction_id": "dup",
                "match_status": "ambiguous",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
            },
            {
                "transaction_id": "dup",
                "match_status": "matched_auto",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
            },
        ]
    )
    current = base.copy()
    current.loc[1, "match_status"] = "source_only"

    inferred = review_state.initial_inference_tags(current, base)

    assert inferred.tolist() == ["ambiguous", "matched_auto"]


def test_canonical_review_helpers_derive_split_and_display_fields() -> None:
    df = pl.DataFrame(
        {
            "review_transaction_id": ["row-1", "row-2"],
            "source_transaction": [
                {
                    "transaction_id": "src-1",
                    "account_name": "Family Leumi",
                    "source_account": "Family Leumi",
                    "date": "2026-03-01",
                    "payee_raw": "Salary Liya",
                    "category_raw": "Split",
                    "splits": [
                        {
                            "split_id": "sub-1",
                            "category_raw": "Pilates",
                            "outflow_ils": 100.0,
                            "inflow_ils": 0.0,
                        }
                    ],
                },
                None,
            ],
            "target_transaction": [
                {
                    "transaction_id": "tgt-1",
                    "account_name": "In Family",
                    "source_account": "In Family",
                    "date": "2026-03-01",
                    "payee_raw": "Transfer : In Family",
                    "category_raw": "",
                    "splits": [],
                },
                {
                    "transaction_id": "tgt-2",
                    "account_name": "In Family",
                    "source_account": "In Family",
                    "date": "2026-03-02",
                    "payee_raw": "Manual Split",
                    "category_raw": "Split",
                    "splits": [
                        {
                            "split_id": "sub-2",
                            "category_raw": "Food",
                            "outflow_ils": 50.0,
                            "inflow_ils": 0.0,
                        },
                        {
                            "split_id": "sub-3",
                            "category_raw": "Pets",
                            "outflow_ils": 25.0,
                            "inflow_ils": 0.0,
                        },
                    ],
                },
            ],
        }
    )

    augmented = review_state.canonical_review_helpers(df)
    rows = augmented.to_dicts()

    assert rows[0]["source_is_split"] is True
    assert rows[0]["source_split_count"] == 1
    assert rows[0]["source_display_payee"] == "Salary Liya"
    assert rows[0]["source_display_category"] == "Split"
    assert rows[0]["source_display_account"] == "Family Leumi"
    assert rows[1]["source_is_split"] is False
    assert rows[1]["source_split_count"] == 0
    assert rows[1]["target_is_split"] is True
    assert rows[1]["target_split_count"] == 2
    assert rows[1]["target_display_payee"] == "Manual Split"


def test_canonical_search_text_series_includes_context_and_split_text() -> None:
    df = pl.DataFrame(
        {
            "review_transaction_id": ["txn-1"],
            "payee_options": ["Cafe;Bakery"],
            "category_options": ["Food;Dining"],
            "match_status": ["ambiguous"],
            "decision_action": ["keep_match"],
            "update_maps": ["payee_add_fingerprint"],
            "source_context_kind": ["ynab_split_category_match"],
            "source_context_category_name": ["Food"],
            "source_context_matching_split_ids": ["split-1"],
            "target_context_kind": [""],
            "target_context_matching_split_ids": [""],
            "memo_append": ["note"],
            "source_transaction": [
                {
                    "payee_raw": "Cafe source",
                    "category_raw": "Food",
                    "account_name": "Source account",
                    "date": "2026-03-01",
                    "memo": "source memo",
                    "splits": [
                        {
                            "split_id": "split-1",
                            "payee_raw": "Split payee",
                            "category_raw": "Split category",
                            "memo": "split memo",
                        }
                    ],
                }
            ],
            "target_transaction": [
                {
                    "payee_raw": "Cafe target",
                    "category_raw": "Dining",
                    "account_name": "Target account",
                    "date": "2026-03-01",
                    "memo": "target memo",
                    "splits": None,
                }
            ],
        }
    )

    text = review_state.canonical_search_text_series(df)

    assert "ynab_split_category_match" in text.iloc[0]
    assert "split-1" in text.iloc[0]
    assert "split payee" in text.iloc[0]
    assert "target account" in text.iloc[0]
