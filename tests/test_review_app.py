from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
from streamlit.testing.v1 import AppTest

from ynab_il_importer.artifacts.review_schema import REVIEW_SCHEMA
import ynab_il_importer.review_app.app as review_app
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.io as review_io
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


def _build_canonical_app_test(
    tmp_path: Path,
    *,
    records: list[dict[str, object]],
) -> tuple[AppTest, Path]:
    proposed = tmp_path / "proposed.parquet"
    reviewed = tmp_path / "proposed_reviewed.parquet"
    categories = tmp_path / "categories.csv"

    table = pa.Table.from_pylist(records, schema=REVIEW_SCHEMA)
    review_io.save_review_artifact(table, proposed)
    _write_categories(categories)

    argv = ["app.py", "--in", str(proposed), "--categories", str(categories)]
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


def _markdown_values(container) -> list[str]:
    return [str(widget.value) for widget in getattr(container, "markdown", [])]


def _caption_values(container) -> list[str]:
    return [str(widget.value) for widget in getattr(container, "caption", [])]


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

    df = review_model.apply_to_same_fingerprint(
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


def test_apply_to_same_fingerprint_accepts_polars_frame() -> None:
    df = pl.DataFrame(
        {
            "fingerprint": ["fp1", "fp1", "fp2"],
            "target_payee_selected": ["A", "A", "B"],
            "target_category_selected": ["C", "C", "D"],
            "update_maps": ["", "", ""],
            "reviewed": [False, False, False],
        }
    )
    eligible_mask = pl.Series([True, False, True])

    updated = review_model.apply_to_same_fingerprint(
        df,
        "fp1",
        payee="X",
        category="Y",
        eligible_mask=eligible_mask,
    )

    assert isinstance(updated, pl.DataFrame)
    assert updated["target_payee_selected"].to_list() == ["X", "A", "B"]
    assert updated["target_category_selected"].to_list() == ["Y", "C", "D"]


def test_default_row_kind_selection_hides_matched_cleared_by_default() -> None:
    assert review_app._default_row_kind_selection(
        ["Matched", "Matched cleared", "Source only"]
    ) == ["Matched", "Source only"]


def test_default_primary_state_selection_hides_settled_by_default() -> None:
    assert review_app._default_primary_state_selection(
        ["Fix", "Decide", "Settled"]
    ) == ["Fix", "Decide"]


def test_format_category_label_special_cases_no_category_required() -> None:
    assert (
        review_app._format_category_label(review_model.NO_CATEGORY_REQUIRED, {})
        == "None (no category required)"
    )


def test_canonical_review_bundle_preserves_flat_splits_and_aligns_helpers() -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "memo",
                "fingerprint": "fp1",
                "payee_options": "Cafe",
                "category_options": "Food",
                "match_status": "source_only",
                "workflow_type": "institutional",
                "source_present": True,
                "target_present": False,
                "source_payee_selected": "Cafe",
                "source_category_selected": "",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "decision_action": "create_target",
                "update_maps": "",
                "reviewed": False,
                "source_transaction_id": "src-1",
                "source_source_system": "bank",
                "source_account": "Account 1",
                "source_date": "2026-03-01",
                "source_payee_current": "Cafe source",
                "source_category_current": "",
                "source_memo": "memo",
                "source_fingerprint": "fp1",
                "source_splits": [
                    {
                        "split_id": "sub-1",
                        "parent_transaction_id": "src-1",
                        "inflow_ils": 0.0,
                        "outflow_ils": 6.0,
                        "payee_raw": "Cafe source",
                        "category_id": "cat-food",
                        "category_raw": "Food",
                        "memo": "split memo",
                        "matched_transaction_id": "",
                    }
                ],
            }
        ],
        index=[42],
    )

    bundle = review_app._canonical_review_bundle(df)

    assert bundle["table"] is not None
    assert bundle["table"].row(0, named=True)["source_splits"][0]["split_id"] == "sub-1"
    helpers = bundle["helpers"]
    assert helpers is not None
    assert helpers["source_split_count"].to_list() == [1]
    assert helpers["source_is_split"].to_list() == [True]
    helper_lookup = bundle["helper_lookup"]
    assert helper_lookup is not None
    assert int(helper_lookup[42]["source_split_count"]) == 1
    assert bool(helper_lookup[42]["source_is_split"]) is True


def test_split_summary_suffix_reports_source_and_target_counts() -> None:
    helper_row = pd.Series({"source_split_count": 2, "target_split_count": 1})

    assert review_app._split_summary_suffix(helper_row) == " | Src split 2 | Tgt split 1"


def test_pick_summary_text_falls_back_to_canonical_transaction_data() -> None:
    row = pd.Series(
        {
            "memo": "",
            "fingerprint": "",
            "source_splits": [
                {
                    "memo": "split memo",
                    "payee_raw": "",
                    "category_raw": "",
                }
            ],
            "target_splits": None,
        }
    )

    assert review_app._pick_summary_text(row) == "split memo"


def test_summary_date_and_account_prefer_canonical_helpers() -> None:
    row = pd.Series({"date": "2026-03-01", "account_name": "Flat account"})
    helper_row = pd.Series(
        {
            "source_display_date": "2026-03-02",
            "target_display_date": "",
            "source_display_account": "",
            "target_display_account": "Canonical account",
        }
    )

    assert review_app._summary_date(row, helper_row) == "2026-03-02"
    assert review_app._summary_account(row, helper_row) == "Canonical account"


def test_source_context_caption_explains_split_category_match() -> None:
    row = pd.Series(
        {
            "source_context_kind": "ynab_split_category_match",
            "source_context_category_name": "Food",
            "source_context_matching_split_ids": "split-1;split-2",
        }
    )

    caption = review_app._source_context_caption(row)

    assert "one or more YNAB split lines match category Food" in caption
    assert "split-1;split-2" in caption


def test_split_caption_lines_marks_matching_split_ids() -> None:
    splits = [
        {
            "split_id": "split-1",
            "outflow_ils": 12.5,
            "inflow_ils": 0.0,
            "payee_raw": "Cafe",
            "category_raw": "Food",
            "memo": "beans",
        },
        {
            "split_id": "split-2",
            "outflow_ils": 0.0,
            "inflow_ils": 9.0,
            "payee_raw": "Refund",
            "category_raw": "Ready to Assign",
            "memo": "",
        },
    ]

    lines = review_app._split_caption_lines(splits, matching_split_ids="split-2")

    assert lines[0].startswith("Split split-1: -12.5")
    assert lines[1].startswith("Matching split split-2: +9")


def test_grouped_row_indices_only_include_filtered_rows() -> None:
    filtered = pd.DataFrame(
        {
            "fingerprint": ["fp-a", "fp-b", "fp-a"],
        },
        index=[10, 20, 30],
    )

    fingerprints, group_indices = review_app._grouped_row_indices(filtered)

    assert fingerprints == ["fp-a", "fp-b"]
    assert group_indices == {"fp-a": [10, 30], "fp-b": [20]}


def test_app_renders_canonical_split_detail_from_parquet(tmp_path: Path) -> None:
    records = [
        {
            "artifact_kind": "review_artifact",
            "artifact_version": "review_v3",
            "review_transaction_id": "demo-split",
            "source": "cross_budget",
            "account_name": "Family Leumi",
            "date": "2026-03-02",
            "outflow_ils": 140.0,
            "inflow_ils": 0.0,
            "memo": "books and gifts",
            "fingerprint": "bookstore combo",
            "workflow_type": "cross_budget",
            "relation_kind": "source_target_pair",
            "match_status": "ambiguous",
            "match_method": "category_extract",
            "payee_options": "Bookstore Combo",
            "category_options": "Books;Gifts",
            "update_maps": "",
            "decision_action": review_validation.NO_DECISION,
            "reviewed": False,
            "memo_append": "",
            "source_present": True,
            "target_present": True,
            "source_row_id": "src-split-1",
            "target_row_id": "tgt-split-1",
            "source_account": "Shared Budget",
            "target_account": "Family Leumi",
            "source_date": "2026-03-02",
            "target_date": "2026-03-02",
            "source_memo": "books and gifts",
            "target_memo": "house and groceries",
            "source_fingerprint": "bookstore combo",
            "target_fingerprint": "mega store",
            "source_bank_txn_id": "",
            "source_card_txn_id": "",
            "source_card_suffix": "",
            "source_secondary_date": "",
            "source_ref": "",
            "source_source_system": "ynab",
            "source_transaction_id": "src-split-1",
            "source_ynab_id": "src-split-1",
            "source_import_id": "",
            "source_parent_transaction_id": "src-split-1",
            "source_account_id": "acct-shared",
            "source_payee_current": "Bookstore Combo",
            "source_category_id": "",
            "source_category_current": "Split",
            "source_description_raw": "books and gifts",
            "source_description_clean": "books and gifts",
            "source_merchant_raw": "Bookstore Combo",
            "source_cleared": "uncleared",
            "source_approved": True,
            "source_is_subtransaction": False,
            "source_context_kind": "ynab_split_category_match",
            "source_context_category_id": "cat-books",
            "source_context_category_name": "Books",
            "source_context_matching_split_ids": "split-book-1",
            "source_payee_selected": "Bookstore Combo",
            "source_category_selected": "Books",
            "target_context_kind": "",
            "target_context_matching_split_ids": "split-house-1",
            "target_payee_selected": "Mega Store",
            "target_category_selected": "House and stuff",
            "target_source_system": "ynab",
            "target_transaction_id": "tgt-split-1",
            "target_ynab_id": "tgt-split-1",
            "target_import_id": "",
            "target_parent_transaction_id": "tgt-split-1",
            "target_account_id": "acct-family",
            "target_secondary_date": "",
            "target_payee_current": "Mega Store",
            "target_category_id": "",
            "target_category_current": "Split",
            "target_description_raw": "house and groceries",
            "target_description_clean": "house and groceries",
            "target_merchant_raw": "Mega Store",
            "target_ref": "",
            "target_cleared": "uncleared",
            "target_approved": False,
            "target_is_subtransaction": False,
            "source_splits": [
                {
                    "split_id": "split-book-1",
                    "parent_transaction_id": "src-split-1",
                    "ynab_subtransaction_id": "split-book-1",
                    "payee_raw": "Bookstore Combo",
                    "category_id": "",
                    "category_raw": "Books",
                    "memo": "novel",
                    "inflow_ils": 0.0,
                    "outflow_ils": 90.0,
                    "import_id": "",
                    "matched_transaction_id": "",
                },
                {
                    "split_id": "split-gift-1",
                    "parent_transaction_id": "src-split-1",
                    "ynab_subtransaction_id": "split-gift-1",
                    "payee_raw": "Bookstore Combo",
                    "category_id": "",
                    "category_raw": "Gifts",
                    "memo": "card",
                    "inflow_ils": 0.0,
                    "outflow_ils": 50.0,
                    "import_id": "",
                    "matched_transaction_id": "",
                },
            ],
            "target_splits": [
                {
                    "split_id": "split-house-1",
                    "parent_transaction_id": "tgt-split-1",
                    "ynab_subtransaction_id": "split-house-1",
                    "payee_raw": "Mega Store",
                    "category_id": "",
                    "category_raw": "House and stuff",
                    "memo": "supplies",
                    "inflow_ils": 0.0,
                    "outflow_ils": 120.0,
                    "import_id": "",
                    "matched_transaction_id": "",
                }
            ],
        }
    ]

    app, _ = _build_canonical_app_test(tmp_path, records=records)

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    markdown_text = "\n".join(_markdown_values(row))
    caption_text = "\n".join(_caption_values(row))

    assert "Source split detail" in markdown_text
    assert "Target split detail" in markdown_text
    assert "Matching split split-book-1" in caption_text
    assert "Matching split split-house-1" in caption_text


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
    assert int(app.session_state["scroll_to_top_nonce"]) == 1


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


def test_group_accept_reviews_set_decisions_without_overwriting_them(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                fingerprint="fp-group",
                source_row_id="s1",
                target_row_id="x1",
                decision_action="keep_match",
            ),
            _row(
                transaction_id="t2",
                fingerprint="fp-group",
                source_row_id="s2",
                target_row_id="",
                target_present=False,
                decision_action="create_target",
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    group = app.expander[0]
    _find_button_by_label(group, "Accept set decisions in group (2)").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["keep_match", "create_target"]
    assert session_df["reviewed"].tolist() == [True, True]


def test_group_accept_uses_live_group_row_decisions(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                fingerprint="fp-group-live",
                source_row_id="s1",
                target_row_id="x1",
                decision_action=review_validation.NO_DECISION,
            ),
            _row(
                transaction_id="t2",
                fingerprint="fp-group-live",
                source_row_id="s1",
                target_row_id="x2",
                decision_action=review_validation.NO_DECISION,
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    _find_selectbox(app, "decision_action_0").set_value("keep_match")
    app.run()
    _find_selectbox(app, "decision_action_1").set_value("ignore_row")
    app.run()

    group = app.expander[0]
    _find_button_by_label(group, "Accept set decisions in group (2)").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["keep_match", "ignore_row"]
    assert session_df["reviewed"].tolist() == [True, True]


def test_group_accept_resolves_competing_rows_for_ambiguous_group(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                fingerprint="fp-amb",
                source_row_id="s1",
                target_row_id="x1",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
            ),
            _row(
                transaction_id="t2",
                fingerprint="fp-amb",
                source_row_id="s1",
                target_row_id="x2",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    _find_selectbox(app, "decision_action_0").set_value("keep_match")
    app.run()

    group = app.expander[0]
    _find_button_by_label(group, "Accept set decisions in group (1)").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["keep_match", "ignore_row"]
    assert session_df["reviewed"].tolist() == [True, True]


def test_group_accept_button_disabled_until_group_has_chosen_row_decisions(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                fingerprint="fp-disabled",
                source_row_id="s1",
                target_row_id="x1",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
            ),
            _row(
                transaction_id="t2",
                fingerprint="fp-disabled",
                source_row_id="s1",
                target_row_id="x2",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    group = app.expander[0]
    accept_button = _find_button_by_label(group, "Accept set decisions in group (0)")
    assert accept_button.disabled is True


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


def test_accept_all_set_decisions_uses_staged_values_and_skips_blocked_components(
    tmp_path: Path,
) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                source_row_id="s1",
                target_row_id="x1",
                decision_action=review_validation.NO_DECISION,
            ),
            _row(
                transaction_id="t2",
                source_row_id="",
                target_row_id="x2",
                source_present=False,
                target_present=True,
                workflow_type="institutional",
                decision_action="create_source",
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    _find_selectbox(app, "decision_action_0").set_value("keep_match")
    app.run()

    _find_button_by_label(app.sidebar, "Accept all set decisions").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].tolist() == ["keep_match", "create_source"]
    assert session_df["reviewed"].tolist() == [True, False]
    assert any(
        "Accepted 1 rows in memory. Blocked 1 rows" in str(element.value)
        for element in app.error
    )


def test_primary_state_series_treats_no_decision_as_fix_and_component_conflicts_fix() -> None:
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
    assert primary_state_series.tolist() == ["Fix", "Fix", "Fix"]


def test_transfer_uncategorized_is_not_treated_as_fix() -> None:
    df = pd.DataFrame(
        [
            {
                "transaction_id": "transfer-row",
                "date": "2026-03-01",
                "account_name": "Bank Leumi",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "cash move",
                "fingerprint": "transfer cash",
                "payee_options": "Transfer : Cash",
                "category_options": "",
                "match_status": "matched_cleared",
                "workflow_type": "institutional",
                "source_row_id": "s1",
                "target_row_id": "t1",
                "source_present": "TRUE",
                "target_present": "TRUE",
                "source_payee_selected": "cash move",
                "source_category_selected": "",
                "target_payee_selected": "Transfer : Cash",
                "target_category_selected": "Uncategorized",
                "decision_action": "keep_match",
                "update_maps": "",
                "reviewed": "TRUE",
            }
        ]
    )
    df["reviewed"] = review_validation.normalize_flag_series(df["reviewed"])

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)

    assert blocker_series.tolist() == ["None"]
    assert primary_state_series.tolist() == ["Settled"]


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
