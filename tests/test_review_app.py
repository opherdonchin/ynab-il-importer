from __future__ import annotations

from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

import ynab_il_importer.review_app.app as review_app
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.validation as review_validation


def _row(
    *,
    transaction_id: str,
    fingerprint: str = "fp1",
    source_row_id: str = "s1",
    target_row_id: str = "t1",
    match_status: str = "ambiguous",
    source_present: bool = True,
    target_present: bool = True,
    decision_action: str = "No decision",
    reviewed: bool = False,
    workflow_type: str = "cross_budget",
    target_category: str = "Food",
    update_maps: str = "",
) -> dict[str, object]:
    return {
        "transaction_id": transaction_id,
        "date": "2026-03-01",
        "account_name": "Account 1",
        "outflow_ils": "10",
        "inflow_ils": "0",
        "memo": f"memo-{transaction_id}",
        "fingerprint": fingerprint,
        "payee_options": "Cafe;Grocer",
        "category_options": "Food;Dining",
        "match_status": match_status,
        "workflow_type": workflow_type,
        "source_row_id": source_row_id,
        "target_row_id": target_row_id,
        "source_present": "TRUE" if source_present else "",
        "target_present": "TRUE" if target_present else "",
        "source_payee_selected": "Cafe",
        "source_category_selected": "Food",
        "target_payee_selected": "Cafe",
        "target_category_selected": target_category,
        "decision_action": decision_action,
        "update_maps": update_maps,
        "reviewed": "TRUE" if reviewed else "",
    }


def _write_review_rows(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _write_categories(path: Path) -> None:
    pd.DataFrame(
        [
            {"category_group": "Living", "category_name": "Food"},
            {"category_group": "Living", "category_name": "Dining"},
            {"category_group": "Internal Master Category", "category_name": "Uncategorized"},
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")


def _build_app_test(
    tmp_path: Path,
    *,
    proposed_rows: list[dict[str, object]],
    reviewed_rows: list[dict[str, object]] | None = None,
    resume: bool = False,
) -> tuple[AppTest, Path]:
    proposed = tmp_path / "proposed.csv"
    reviewed = tmp_path / "proposed_reviewed.csv"
    categories = tmp_path / "categories.csv"

    _write_review_rows(proposed, proposed_rows)
    _write_categories(categories)
    if reviewed_rows is not None:
        _write_review_rows(reviewed, reviewed_rows)

    argv = ["app.py", "--in", str(proposed), "--categories", str(categories)]
    if resume:
        argv.append("--resume")

    script = (
        "import sys\n"
        f"sys.path.insert(0, r\"{Path('src').resolve()}\")\n"
        f"sys.argv = {argv!r}\n"
        "from ynab_il_importer.review_app.app import main\n"
        "main()\n"
    )
    app = AppTest.from_string(script, default_timeout=30)
    return app, reviewed


def _find_selectbox(container, prefix: str):
    return next(widget for widget in container.selectbox if str(widget.key).startswith(prefix))


def _find_button(container, key_fragment: str):
    return next(widget for widget in container.button if key_fragment in str(widget.key))


def _find_button_by_label(container, label: str):
    return next(widget for widget in container.button if widget.label == label)


def _find_multiselect_by_label(container, label: str):
    return next(widget for widget in container.multiselect if widget.label == label)


def _show_all_primary_states(app: AppTest) -> None:
    _find_multiselect_by_label(app.sidebar, "State").set_value(["Fix", "Decide", "Settled"])
    _find_multiselect_by_label(app.sidebar, "Save status").set_value(["Unsaved", "Saved"])


def test_apply_to_same_fingerprint_respects_eligible_mask() -> None:
    df = pd.DataFrame(
        {
            "fingerprint": ["fp1", "fp1", "fp2"],
            "target_payee_selected": ["A", "A", "B"],
            "target_category_selected": ["C", "C", "D"],
            "update_maps": ["", "", ""],
            "reviewed": [False, False, False],
        }
    )
    eligible_mask = pd.Series([True, False, True], index=df.index)

    review_model.apply_to_same_fingerprint(
        df,
        "fp1",
        payee="X",
        category="Y",
        eligible_mask=eligible_mask,
    )

    assert df.loc[0, "target_payee_selected"] == "X"
    assert df.loc[0, "target_category_selected"] == "Y"
    assert df.loc[1, "target_payee_selected"] == "A"
    assert df.loc[1, "target_category_selected"] == "C"


def test_changed_mask_aligns_duplicate_transaction_ids() -> None:
    base = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "target_payee_selected": ["A", "A"],
            "target_category_selected": ["C", "C"],
            "decision_action": ["keep_match", "keep_match"],
            "update_maps": ["", ""],
            "reviewed": [False, False],
        }
    )
    current = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "target_payee_selected": ["A", "A"],
            "target_category_selected": ["C", "D"],
            "decision_action": ["keep_match", "keep_match"],
            "update_maps": ["", ""],
            "reviewed": [False, False],
        }
    )

    changed = review_state.changed_mask(current, base)

    assert changed.tolist() == [False, True]


def test_changed_mask_marks_rows_missing_from_baseline() -> None:
    base = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "target_payee_selected": ["A", "B"],
            "target_category_selected": ["C", "D"],
            "decision_action": ["keep_match", "keep_match"],
            "update_maps": ["", ""],
            "reviewed": [False, False],
        }
    )
    current = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2", "t3"],
            "target_payee_selected": ["A", "B", "C"],
            "target_category_selected": ["C", "D", "E"],
            "decision_action": ["keep_match", "keep_match", "create_target"],
            "update_maps": ["", "", ""],
            "reviewed": [False, False, False],
        }
    )

    changed = review_state.changed_mask(current, base)

    assert changed.tolist() == [False, False, True]


def test_allowed_decision_actions_block_source_mutation_for_institutional() -> None:
    actions = review_validation.allowed_decision_actions(
        pd.Series(
            {
                "workflow_type": "institutional",
                "source_present": False,
                "target_present": True,
            }
        )
    )

    assert actions == [review_validation.NO_DECISION, "delete_target", "ignore_row"]


def test_app_row_save_persists_side_specific_fields_and_review_state(tmp_path: Path) -> None:
    app, reviewed_path = _build_app_test(
        tmp_path,
        proposed_rows=[_row(transaction_id="t1", decision_action="keep_match")],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "target_category_select_0").set_value("Dining")
    _find_button_by_label(row, "Mark reviewed").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df.loc[0, "target_category_selected"] == "Dining"
    assert session_df.loc[0, "category_selected"] == "Dining"
    assert session_df.loc[0, "decision_action"] == "keep_match"
    assert bool(session_df.loc[0, "reviewed"]) is True

    _find_button_by_label(app.sidebar, "Save").click()
    app.run()

    saved = pd.read_csv(reviewed_path, dtype="string").fillna("")
    assert saved.loc[0, "target_category_selected"] == "Dining"
    assert saved.loc[0, "decision_action"] == "keep_match"
    assert saved.loc[0, "reviewed"] == "TRUE"
    assert "category_selected" not in saved.columns
    assert "payee_selected" not in saved.columns


def test_next_row_cursor_advances_and_keeps_last_page_when_done() -> None:
    indices = ["row-a", "row-b", "row-c"]

    assert review_app._next_row_cursor(indices, "row-a", 2) == ("row-b", 1)
    assert review_app._next_row_cursor(indices, "row-b", 2) == ("row-c", 2)
    assert review_app._next_row_cursor(indices, "row-c", 2) == (None, 2)


def test_mark_reviewed_opens_next_row_in_row_view(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="x1", decision_action="keep_match"),
            _row(transaction_id="t2", source_row_id="s2", target_row_id="x2", decision_action="create_target"),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_button_by_label(row, "Mark reviewed").click()
    app.run()

    session_df = app.session_state["df"]
    assert bool(session_df.loc[0, "reviewed"]) is True
    assert app.session_state["expanded_row_id"] == 1


def test_app_review_blocked_for_cascaded_component_with_no_decision_rows(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="t1", decision_action="No decision"),
            _row(transaction_id="t2", source_row_id="s1", target_row_id="t2", decision_action="No decision"),
            _row(transaction_id="t3", source_row_id="s3", target_row_id="t2", decision_action="No decision"),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "decision_action_0").set_value("keep_match")
    _find_button_by_label(row, "Mark reviewed").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df.loc[0, "decision_action"] == "keep_match"
    assert session_df.loc[1, "decision_action"] == "ignore_row"
    assert session_df.loc[2, "decision_action"] == review_validation.NO_DECISION
    assert bool(session_df.loc[0, "reviewed"]) is False
    assert bool(session_df.loc[1, "reviewed"]) is False
    assert bool(session_df.loc[2, "reviewed"]) is False


def test_app_create_target_auto_ignores_same_source_rows(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="", target_present=False),
            _row(transaction_id="t2", source_row_id="s1", target_row_id="", target_present=False),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "decision_action_0").set_value("create_target")
    _find_button_by_label(row, "Apply without review").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["create_target", "ignore_row"]
    assert session_df["reviewed"].tolist() == [False, False]
    assert app.session_state["expanded_row_id"] == 0


def test_app_keep_match_auto_ignores_same_source_and_target_rows(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="x1"),
            _row(transaction_id="t2", source_row_id="s1", target_row_id="x2"),
            _row(transaction_id="t3", source_row_id="s3", target_row_id="x1"),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "decision_action_0").set_value("keep_match")
    _find_button_by_label(row, "Apply without review").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["keep_match", "ignore_row", "ignore_row"]


def test_app_ignore_row_does_not_propagate(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="x1"),
            _row(transaction_id="t2", source_row_id="s1", target_row_id="x2"),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "decision_action_0").set_value("ignore_row")
    _find_button_by_label(row, "Apply without review").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["ignore_row", review_validation.NO_DECISION]


def test_mark_reviewed_reviews_auto_ignored_competing_rows(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="", target_present=False),
            _row(transaction_id="t2", source_row_id="s1", target_row_id="", target_present=False),
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_selectbox(row, "decision_action_0").set_value("create_target")
    _find_button_by_label(row, "Mark reviewed").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["create_target", "ignore_row"]
    assert session_df["reviewed"].tolist() == [True, True]


def test_accept_all_set_decisions_reviews_only_rows_with_actions(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(transaction_id="t1", source_row_id="s1", target_row_id="x1", decision_action="keep_match"),
            _row(
                transaction_id="t2",
                source_row_id="s2",
                target_row_id="x2",
                decision_action="create_target",
                target_present=False,
            ),
            _row(
                transaction_id="t3",
                source_row_id="s3",
                target_row_id="x3",
                decision_action=review_validation.NO_DECISION,
            ),
        ],
    )

    app.run()
    _find_button_by_label(app.sidebar, "Accept all set decisions").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["reviewed"].tolist() == [True, True, False]


def test_primary_state_series_keeps_no_decision_open_and_component_conflicts_fix() -> None:
    df = pd.DataFrame(
        [
            _row(transaction_id="draft", decision_action=review_validation.NO_DECISION),
            _row(
                transaction_id="matched",
                source_row_id="shared-source",
                target_row_id="target-a",
                decision_action="keep_match",
                reviewed=True,
            ),
            _row(
                transaction_id="deleted",
                source_row_id="shared-source",
                target_row_id="target-b",
                decision_action="delete_source",
                reviewed=True,
            ),
        ]
    )
    df["reviewed"] = review_validation.normalize_flag_series(df["reviewed"])

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)

    assert blocker_series.tolist() == [
        "No decision",
        "Contradiction in component",
        "Contradiction in component",
    ]
    assert primary_state_series.tolist() == ["Decide", "Fix", "Fix"]


def test_apply_row_filters_supports_action_blocker_suggestions_map_updates_and_search() -> None:
    df = pd.DataFrame(
        [
            _row(
                transaction_id="create-target",
                match_status="source_only",
                target_present=False,
                decision_action="create_target",
            ),
            _row(
                transaction_id="ignored",
                match_status="ambiguous",
                decision_action="ignore_row",
                update_maps="fingerprint_add_source",
            ),
        ]
    )
    df["reviewed"] = review_validation.normalize_flag_series(df["reviewed"])

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)
    row_kind_series = review_state.row_kind_series(df)
    action_series = review_state.action_series(df)
    save_state = pd.Series(["Unsaved", "Saved"], index=df.index, dtype="string")
    suggestion_series = review_state.suggestion_series(df)
    map_update_series = review_state.map_update_filter_series(df)
    search_text = review_state.search_text_series(df)

    filtered = review_state.apply_row_filters(
        df,
        primary_state=["Decide"],
        row_kind=["Source only"],
        action_filter=["create_target"],
        save_status=["Unsaved"],
        blocker_filter=["None"],
        suggestion_filter=["Has suggestions"],
        map_update_filter=["No update_maps"],
        primary_state_series=primary_state_series,
        row_kind_series=row_kind_series,
        action_series=action_series,
        save_state=save_state,
        blocker_series=blocker_series,
        suggestion_series=suggestion_series,
        map_update_series=map_update_series,
        search_query="memo-create-target",
        search_text=search_text,
    )

    assert filtered["transaction_id"].tolist() == ["create-target"]
