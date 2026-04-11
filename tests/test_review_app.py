from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest
from streamlit.testing.v1 import AppTest

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


def _txn(
    *,
    transaction_id: str,
    payee: str,
    category: str,
    category_id: str = "cat-food",
    amount: float = 10.0,
    memo: str = "memo",
    splits: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "artifact_kind": "transaction",
        "artifact_version": "transaction_v1",
        "source_system": "ynab",
        "transaction_id": transaction_id,
        "ynab_id": transaction_id,
        "import_id": "",
        "parent_transaction_id": transaction_id,
        "account_id": "acct-1",
        "account_name": "Account 1",
        "source_account": "Account 1",
        "date": "2026-03-01",
        "secondary_date": "",
        "inflow_ils": 0.0,
        "outflow_ils": amount,
        "signed_amount_ils": -amount,
        "payee_raw": payee,
        "category_id": category_id,
        "category_raw": category,
        "memo": memo,
        "txn_kind": "",
        "fingerprint": f"fp-{transaction_id}",
        "description_raw": "",
        "description_clean": "",
        "description_clean_norm": "",
        "merchant_raw": "",
        "ref": "",
        "matched_transaction_id": "",
        "cleared": "uncleared",
        "approved": False,
        "is_subtransaction": False,
        "splits": splits,
    }


def _canonical_record(
    *,
    review_transaction_id: str,
    source_current: dict[str, object],
    target_current: dict[str, object],
    source_original: dict[str, object] | None = None,
    target_original: dict[str, object] | None = None,
    decision_action: str = review_validation.NO_DECISION,
    reviewed: bool = False,
    changed: bool = False,
    target_present: bool = True,
) -> dict[str, object]:
    return {
        "artifact_kind": "review_artifact",
        "artifact_version": "review_v4",
        "review_transaction_id": review_transaction_id,
        "workflow_type": "cross_budget",
        "relation_kind": "source_target_pair",
        "match_status": "ambiguous",
        "match_method": "",
        "payee_options": "Cafe;Grocer",
        "category_options": "Food;Dining;Uncategorized",
        "update_maps": "",
        "decision_action": decision_action,
        "reviewed": reviewed,
        "changed": changed,
        "memo_append": "",
        "source_present": True,
        "target_present": target_present,
        "source_row_id": "s1",
        "target_row_id": "t1" if target_present else "",
        "target_account": "Account 1",
        "source_context_kind": "",
        "source_context_category_id": "",
        "source_context_category_name": "",
        "source_context_matching_split_ids": "",
        "source_payee_selected": str(source_current.get("payee_raw", "")),
        "source_category_selected": str(source_current.get("category_raw", "")),
        "target_context_kind": "",
        "target_context_matching_split_ids": "",
        "target_payee_selected": str(target_current.get("payee_raw", "")),
        "target_category_selected": str(target_current.get("category_raw", "")),
        "source_current": source_current,
        "target_current": target_current,
        "source_original": source_original or source_current,
        "target_original": target_original or target_current,
    }


def _write_review_rows(path: Path, rows: list[dict[str, object]]) -> None:
    review_io.save_review_artifact(pd.DataFrame(rows), path)


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
    proposed = tmp_path / "proposed.parquet"
    reviewed = tmp_path / "proposed_reviewed.parquet"
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

    review_io.save_review_artifact(pd.DataFrame(records), proposed)
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


def _load_demo_builder_module():
    module_path = Path("scripts/build_review_app_demo.py").resolve()
    spec = importlib.util.spec_from_file_location("build_review_app_demo", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    _find_multiselect_by_label(app.sidebar, "State").set_value(
        ["Needs fix", "Needs decision", "Needs review", "Settled"]
    )
    _find_multiselect_by_label(app.sidebar, "Save status").set_value(["Unsaved", "Saved"])


def test_apply_to_same_fingerprint_respects_eligible_mask() -> None:
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

    df = review_model.apply_to_same_fingerprint(
        df,
        "fp1",
        payee="X",
        category="Y",
        eligible_mask=eligible_mask,
    )

    assert df["target_payee_selected"].to_list() == ["X", "A", "B"]
    assert df["target_category_selected"].to_list() == ["Y", "C", "D"]


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


def test_apply_to_same_fingerprint_propagates_memo_append() -> None:
    df = pl.DataFrame(
        {
            "fingerprint": ["fp1", "fp1", "fp2"],
            "memo_append": ["", "", ""],
            "target_payee_selected": ["A", "A", "B"],
            "target_category_selected": ["C", "C", "D"],
            "update_maps": ["", "", ""],
            "reviewed": [False, False, False],
        }
    )

    updated = review_model.apply_to_same_fingerprint(
        df,
        "fp1",
        memo_append="shared memo",
    )

    assert updated["memo_append"].to_list() == ["shared memo", "shared memo", ""]


def test_apply_to_indices_only_updates_selected_rows() -> None:
    df = pl.DataFrame(
        {
            "fingerprint": ["fp1", "fp1", "fp1"],
            "memo_append": ["", "", ""],
            "target_payee_selected": ["A", "B", "C"],
            "target_category_selected": ["Food", "Food", "Food"],
            "update_maps": ["", "", ""],
            "reviewed": [False, False, False],
        }
    )

    updated = review_model.apply_to_indices(
        df,
        [0, 2],
        payee="Transfer : In Family",
        memo_append="loan sync",
    )

    assert updated["target_payee_selected"].to_list() == [
        "Transfer : In Family",
        "B",
        "Transfer : In Family",
    ]
    assert updated["memo_append"].to_list() == ["loan sync", "", "loan sync"]


def test_apply_group_edits_in_memory_only_updates_selected_group_rows() -> None:
    df = pl.DataFrame(
        {
            "fingerprint": ["transfer in family", "transfer in family", "transfer in family"],
            "target_payee_selected": ["Transfer : In Family", "Transfer : In Family", "Transfer : In Family"],
            "target_category_selected": ["", "", ""],
            "memo_append": ["", "", ""],
            "update_maps": ["", "", ""],
            "decision_action": [review_validation.NO_DECISION, review_validation.NO_DECISION, review_validation.NO_DECISION],
            "reviewed": [False, False, False],
            "source_row_id": ["", "", ""],
            "target_row_id": ["t1", "t2", "t3"],
        }
    )

    updated, affected = review_app._apply_group_edits_in_memory(
        df,
        group_indices=[0, 2],
        category="Loan Paydown",
    )

    assert affected == [0, 2]
    assert updated["target_category_selected"].to_list() == ["Loan Paydown", "", "Loan Paydown"]


def test_default_row_kind_selection_hides_matched_cleared_by_default() -> None:
    assert review_app._default_row_kind_selection(
        ["Matched", "Matched cleared", "Source only"]
    ) == ["Matched", "Source only"]


def test_default_primary_state_selection_hides_settled_by_default() -> None:
    assert review_app._default_primary_state_selection(
        ["Needs fix", "Needs decision", "Needs review", "Settled"]
    ) == ["Needs fix", "Needs decision", "Needs review"]


def test_preserve_expansion_context_sets_group_and_row_targets() -> None:
    review_app.st.session_state.clear()

    review_app._preserve_expansion_context(idx=7, group_fingerprint="fp-1")
    assert review_app.st.session_state["expanded_group_fp"] == "fp-1"
    assert review_app.st.session_state["expanded_group_row_id"] == 7

    review_app._preserve_expansion_context(idx=3)
    assert review_app.st.session_state["expanded_row_id"] == 3


def test_augment_with_account_budget_metadata_derives_transfer_budget_flags() -> None:
    review_app.st.session_state.clear()
    review_app.st.session_state["account_budget_lookup"] = {
        "bank leumi": True,
        "in family": True,
        "loan": False,
    }
    df = pl.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "target_account": "Bank Leumi",
                "source_account": "Bank Leumi",
                "target_payee_selected": "Transfer : Loan",
                "source_payee_selected": "Transfer : In Family",
            }
        ]
    )

    enriched = review_app._augment_with_account_budget_metadata(df)
    row = enriched.row(0, named=True)

    assert row["target_account_on_budget"] is True
    assert row["source_account_on_budget"] is True
    assert row["target_transfer_account_on_budget"] is False
    assert row["source_transfer_account_on_budget"] is True


def test_augment_with_account_budget_metadata_keeps_missing_source_side_blank() -> None:
    review_app.st.session_state.clear()
    review_app.st.session_state["account_budget_lookup"] = {
        "leumi loan 64370054": False,
        "in family": True,
    }
    df = pl.DataFrame(
        [
            {
                "account_name": "Leumi loan 64370054",
                "target_account": "Leumi loan 64370054",
                "source_account": "",
                "source_present": False,
                "target_present": True,
                "target_payee_selected": "Transfer : In Family",
                "source_payee_selected": "",
            }
        ]
    )

    enriched = review_app._augment_with_account_budget_metadata(df)
    row = enriched.row(0, named=True)

    assert row["target_account_on_budget"] is False
    assert row["source_account_on_budget"] is None
    assert row["target_transfer_account_on_budget"] is True
    assert row["source_transfer_account_on_budget"] is None


def test_clear_split_editor_state_removes_modal_buffer_and_widget_keys() -> None:
    review_app.st.session_state.clear()
    review_app.st.session_state["_split_editor"] = {"idx": 1}
    review_app.st.session_state["_split_editor_payee_0"] = "Cafe"
    review_app.st.session_state["_split_editor_category_0"] = "Food"
    review_app.st.session_state["_split_editor_amount_0"] = "-10"
    review_app.st.session_state["unrelated_key"] = "keep"

    review_app._clear_split_editor_state()

    assert "_split_editor" not in review_app.st.session_state
    assert "_split_editor_payee_0" not in review_app.st.session_state
    assert "_split_editor_category_0" not in review_app.st.session_state
    assert "_split_editor_amount_0" not in review_app.st.session_state
    assert review_app.st.session_state["unrelated_key"] == "keep"


def test_format_category_label_special_cases_no_category_required() -> None:
    assert (
        review_app._format_category_label(review_model.NO_CATEGORY_REQUIRED, {})
        == "None (no category required)"
    )


def test_canonical_review_bundle_preserves_flat_splits_and_aligns_helpers() -> None:
    df = pl.DataFrame(
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
    assert int(helper_lookup[0]["source_split_count"]) == 1
    assert bool(helper_lookup[0]["source_is_split"]) is True


def test_split_summary_suffix_reports_source_and_target_counts() -> None:
    helper_row = pd.Series({"source_split_count": 2, "target_split_count": 1})

    assert review_app._split_summary_suffix(helper_row) == " | Src split 2 | Tgt split 1"


def test_split_category_summary_lists_unique_categories() -> None:
    assert review_app._split_category_summary(
        [
            {"category_raw": "Books"},
            {"category_raw": "Gifts"},
            {"category_raw": "Books"},
        ]
    ) == "Books —; Gifts —"


def test_split_category_summary_accepts_array_backed_split_payloads() -> None:
    splits = np.array(
        [
            {
                "split_id": "split-1",
                "category_raw": "Books",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
            },
            {
                "split_id": "split-2",
                "category_raw": "Gifts",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
            },
        ],
        dtype=object,
    )

    assert review_app._split_category_summary(splits) == "Books -12; Gifts -10"


def test_split_editor_amount_text_formats_numeric_and_blank_values() -> None:
    assert review_app._split_editor_amount_text(-12.0) == "-12"
    assert review_app._split_editor_amount_text("0.50") == "0.5"
    assert review_app._split_editor_amount_text("-") == ""


def test_collect_split_editor_lines_reads_from_session_state() -> None:
    review_app.st.session_state.clear()
    review_app.st.session_state["_split_editor"] = {
        "lines": [
            {"_line_id": 0, "split_id": "split-1", "payee_raw": "Cafe", "category_raw": "Food", "memo": "beans", "amount_ils": "-12"},
            {"_line_id": 1, "split_id": "split-2", "payee_raw": "Gift shop", "category_raw": "Gifts", "memo": "card", "amount_ils": "-10"},
        ],
    }
    review_app.st.session_state["_split_editor_payee_0"] = "Updated Cafe"
    review_app.st.session_state["_split_editor_category_0"] = "Dining"
    review_app.st.session_state["_split_editor_amount_0"] = "-11.5"

    result = review_app._collect_split_editor_lines()

    assert result == [
        {"split_id": "split-1", "payee_raw": "Updated Cafe", "category_raw": "Dining", "memo": "beans", "amount_ils": "-11.5"},
        {"split_id": "split-2", "payee_raw": "Gift shop", "category_raw": "Gifts", "memo": "card", "amount_ils": "-10"},
    ]


def test_target_split_editor_rows_accept_array_backed_split_payloads() -> None:
    row = pd.Series(
        {
            "target_splits": np.array(
                [
                    {
                        "split_id": "split-1",
                        "payee_raw": "Cafe Roma",
                        "category_raw": "Going out",
                        "memo": "coffee",
                        "inflow_ils": 0.0,
                        "outflow_ils": 12.0,
                    },
                    {
                        "split_id": "split-2",
                        "payee_raw": "Cafe Roma",
                        "category_raw": "Gifts",
                        "memo": "tip",
                        "inflow_ils": 0.0,
                        "outflow_ils": 10.0,
                    },
                ],
                dtype=object,
            ),
            "target_current_transaction": {
                "payee_raw": "Cafe Roma",
                "category_raw": "Split",
                "memo": "morning coffee",
                "inflow_ils": 0.0,
                "outflow_ils": 22.0,
            },
            "target_payee_selected": "Cafe Roma",
            "target_category_selected": "Split",
            "target_memo": "morning coffee",
            "memo": "morning coffee",
            "inflow_ils": 0.0,
            "outflow_ils": 22.0,
            "target_present": True,
        }
    )

    assert review_app._target_split_editor_rows(row) == [
        {
            "split_id": "split-1",
            "payee_raw": "Cafe Roma",
            "category_raw": "Going out",
            "memo": "coffee",
            "amount_ils": -12.0,
        },
        {
            "split_id": "split-2",
            "payee_raw": "Cafe Roma",
            "category_raw": "Gifts",
            "memo": "tip",
            "amount_ils": -10.0,
        },
    ]


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


def test_target_split_editor_rows_seed_existing_split_lines() -> None:
    row = pd.Series(
        {
            "target_splits": [
                {
                    "split_id": "split-1",
                    "payee_raw": "Cafe",
                    "category_raw": "Food",
                    "memo": "beans",
                    "inflow_ils": 0.0,
                    "outflow_ils": 12.5,
                }
            ]
        }
    )

    rows = review_app._target_split_editor_rows(row)

    assert rows == [
        {
            "split_id": "split-1",
            "payee_raw": "Cafe",
            "category_raw": "Food",
            "memo": "beans",
            "amount_ils": -12.5,
        }
    ]


def test_target_split_editor_rows_seed_prefilled_and_blank_lines_for_create_split() -> None:
    row = pd.Series(
        {
            "inflow_ils": 0.0,
            "outflow_ils": 10.0,
            "target_payee_selected": "Target Cafe",
            "target_category_selected": "Food",
            "target_memo": "memo",
            "target_present": False,
            "source_present": True,
        }
    )

    rows = review_app._target_split_editor_rows(row)

    assert rows[0]["payee_raw"] == "Target Cafe"
    assert rows[0]["category_raw"] == "Food"
    assert rows[0]["amount_ils"] == -10.0
    assert rows[1] == {
        "split_id": "",
        "payee_raw": "",
        "category_raw": "",
        "memo": "",
        "amount_ils": 0.0,
    }


def test_app_create_split_opens_modal_with_seeded_lines_and_cancel_clears_it(
    tmp_path: Path,
) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[_row(transaction_id="t1", decision_action="keep_match")],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_button_by_label(row, "Create split").click()
    app.run()

    assert app.session_state["_split_editor"]["idx"] == 0
    editor_lines = app.session_state["_split_editor"]["lines"]
    assert [
        {k: v for k, v in line.items() if k != "_line_id"} for line in editor_lines
    ] == [
        {
            "split_id": "",
            "payee_raw": "Cafe",
            "category_raw": "Food",
            "memo": "memo-t1",
            "amount_ils": -10.0,
        },
        {
            "split_id": "",
            "payee_raw": "",
            "category_raw": "",
            "memo": "",
            "amount_ils": 0.0,
        },
    ]

    _find_button_by_label(app, "Cancel").click()
    app.run()

    assert "_split_editor" not in app.session_state
    assert app.session_state["expanded_row_id"] == 0


def test_app_edit_split_opens_modal_with_committed_split_lines(tmp_path: Path) -> None:
    source_txn = _txn(transaction_id="src-1", payee="Cafe", category="Food")
    target_txn = _txn(
        transaction_id="tgt-1",
        payee="Parent Payee",
        category="Split",
        category_id="",
        memo="Parent memo",
        splits=[
            {
                "split_id": "split-1",
                "parent_transaction_id": "tgt-1",
                "ynab_subtransaction_id": "",
                "payee_raw": "Split Payee 1",
                "category_id": "cat-food",
                "category_raw": "Food",
                "memo": "beans",
                "inflow_ils": 0.0,
                "outflow_ils": 6.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
            {
                "split_id": "split-2",
                "parent_transaction_id": "tgt-1",
                "ynab_subtransaction_id": "",
                "payee_raw": "Split Payee 2",
                "category_id": "cat-dining",
                "category_raw": "Dining",
                "memo": "lunch",
                "inflow_ils": 0.0,
                "outflow_ils": 4.0,
                "import_id": "",
                "matched_transaction_id": "",
            },
        ],
    )
    app, _ = _build_canonical_app_test(
        tmp_path,
        records=[
            _canonical_record(
                review_transaction_id="review-1",
                source_current=source_txn,
                target_current=target_txn,
            )
        ],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_button_by_label(row, "Edit split").click()
    app.run()

    assert app.session_state["_split_editor"]["idx"] == 0
    editor_lines = app.session_state["_split_editor"]["lines"]
    assert [
        {k: v for k, v in line.items() if k != "_line_id"} for line in editor_lines
    ] == [
        {
            "split_id": "split-1",
            "payee_raw": "Split Payee 1",
            "category_raw": "Food",
            "memo": "beans",
            "amount_ils": -6.0,
        },
        {
            "split_id": "split-2",
            "payee_raw": "Split Payee 2",
            "category_raw": "Dining",
            "memo": "lunch",
            "amount_ils": -4.0,
        },
    ]


def test_app_save_split_without_changes_closes_modal_and_keeps_row_stable(
    tmp_path: Path,
) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[_row(transaction_id="t1", decision_action="keep_match")],
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    _show_all_primary_states(app)
    app.run()

    row = app.expander[0]
    _find_button_by_label(row, "Create split").click()
    app.run()
    _find_button_by_label(app, "Save split").click()
    app.run()

    session_df = app.session_state["df"]
    assert "_split_editor" not in app.session_state
    assert app.session_state["expanded_row_id"] == 0
    row = session_df.row(0, named=True)
    assert row["target_splits"] is None
    assert row["target_payee_selected"] == "Cafe"
    assert row["target_category_selected"] == "Food"
    assert row["target_current_transaction"]["payee_raw"] == "Cafe"
    assert row["target_current_transaction"]["category_raw"] == "Food"


def test_grouped_row_indices_only_include_filtered_rows() -> None:
    filtered = pl.DataFrame(
        {
            "_row_pos": [10, 14, 19],
            "fingerprint": ["fp-a", "fp-b", "fp-a"],
        }
    )

    fingerprints, group_indices = review_app._grouped_row_indices(filtered)

    assert fingerprints == ["fp-a", "fp-b"]
    assert group_indices == {"fp-a": [10, 19], "fp-b": [14]}


def test_require_groupable_review_rows_rejects_blank_fingerprint() -> None:
    df = pl.DataFrame({"fingerprint": ["fp-1", ""]})

    with pytest.raises(ValueError, match="blank fingerprint values"):
        review_app._require_groupable_review_rows(df)


def test_demo_builder_emits_review_v4_records_with_target_split() -> None:
    demo_builder = _load_demo_builder_module()

    records = demo_builder.demo_review_records()

    assert all(record["artifact_version"] == "review_v4" for record in records)
    assert any(
        isinstance(record.get("target_current"), dict)
        and isinstance(record["target_current"].get("splits"), list)
        and len(record["target_current"]["splits"]) > 1
        for record in records
    )


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
                        "outflow_ils": 140.0,
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
    assert "Source split detail" not in markdown_text
    assert "Target split detail" not in markdown_text
    assert "Split</td><td>Books -90; Gifts -50" in markdown_text
    assert "Split</td><td>House and stuff -140" in markdown_text


def test_changed_mask_aligns_duplicate_transaction_ids() -> None:
    base = pl.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "target_payee_selected": ["A", "A"],
            "target_category_selected": ["C", "C"],
            "decision_action": ["keep_match", "keep_match"],
            "update_maps": ["", ""],
            "reviewed": [False, False],
        }
    )
    current = pl.DataFrame(
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

    assert changed.to_list() == [False, True]


def test_changed_mask_marks_rows_missing_from_baseline() -> None:
    base = pl.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "target_payee_selected": ["A", "B"],
            "target_category_selected": ["C", "D"],
            "decision_action": ["keep_match", "keep_match"],
            "update_maps": ["", ""],
            "reviewed": [False, False],
        }
    )
    current = pl.DataFrame(
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

    assert changed.to_list() == [False, False, True]


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

    assert actions == [review_validation.NO_DECISION, "update_target", "delete_target", "ignore_row"]


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
    _find_button_by_label(row, "Accept row").click()
    app.run()

    session_df = app.session_state["df"]
    row = session_df.row(0, named=True)
    assert row["target_category_selected"] == "Dining"
    assert row["category_selected"] == "Dining"
    assert row["decision_action"] == "keep_match"
    assert bool(row["reviewed"]) is True

    _find_button_by_label(app.sidebar, "Save").click()
    app.run()

    saved = review_io.load_review_artifact(reviewed_path).to_pandas()
    assert saved.loc[0, "target_category_selected"] == "Dining"
    assert saved.loc[0, "decision_action"] == "keep_match"
    assert bool(saved.loc[0, "reviewed"]) is True
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
    _find_button_by_label(row, "Accept row").click()
    app.run()

    session_df = app.session_state["df"]
    assert bool(session_df.row(0, named=True)["reviewed"]) is True
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
    _find_button_by_label(row, "Accept row").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "ignore_row", review_validation.NO_DECISION]
    assert session_df["reviewed"].to_list() == [False, False, False]


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
    _find_button_by_label(row, "Apply edits").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["create_target", "ignore_row"]
    assert session_df["reviewed"].to_list() == [False, False]
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
    _find_button_by_label(row, "Apply edits").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "ignore_row", "ignore_row"]


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
    _find_button_by_label(row, "Apply edits").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["ignore_row", review_validation.NO_DECISION]


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
    _find_button_by_label(row, "Accept row").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["create_target", "ignore_row"]
    assert session_df["reviewed"].to_list() == [True, True]


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
    _find_button_by_label(group, "Accept group").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "create_target"]
    assert session_df["reviewed"].to_list() == [True, True]


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
    _find_button_by_label(group, "Accept group").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "ignore_row"]
    assert session_df["reviewed"].to_list() == [True, True]


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
    _find_button_by_label(group, "Accept group").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "ignore_row"]
    assert session_df["reviewed"].to_list() == [True, True]


def test_group_accept_uses_group_widget_values_without_separate_apply(tmp_path: Path) -> None:
    app, _ = _build_app_test(
        tmp_path,
        proposed_rows=[
            _row(
                transaction_id="t1",
                fingerprint="fp-group-widgets",
                source_row_id="s1",
                target_row_id="x1",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
                target_category="Uncategorized",
            ),
            _row(
                transaction_id="t2",
                fingerprint="fp-group-widgets",
                source_row_id="s2",
                target_row_id="x2",
                match_status="ambiguous",
                decision_action=review_validation.NO_DECISION,
                target_category="Uncategorized",
            ),
        ],
    )

    app.run()
    _show_all_primary_states(app)
    app.run()

    group = app.expander[0]
    _find_selectbox(group, "group_category_").set_value("Living / Dining")
    app.run()
    group = app.expander[0]
    _find_selectbox(group, "group_decision_").set_value("ignore_row")
    app.run()

    group = app.expander[0]
    _find_button_by_label(group, "Accept group").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["target_category_selected"].to_list() == ["Dining", "Dining"]
    assert session_df["decision_action"].to_list() == ["ignore_row", "ignore_row"]
    assert session_df["reviewed"].to_list() == [True, True]


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
    accept_button = _find_button_by_label(group, "Accept group")
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
    _find_button_by_label(app.sidebar, "Accept all reviewable rows").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["reviewed"].to_list() == [True, True, False]


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

    _find_button_by_label(app.sidebar, "Accept all reviewable rows").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df["decision_action"].to_list() == ["keep_match", "create_source"]
    assert session_df["reviewed"].to_list() == [True, False]
    assert any(
        "Accepted 1 rows in memory. Blocked 1 rows" in str(element.value)
        for element in app.error
    )


def test_primary_state_series_treats_no_decision_as_needs_decision_and_component_conflicts_as_needs_fix() -> None:
    df = pl.DataFrame(
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
    df = df.with_columns(review_validation.normalize_flag_series(df["reviewed"]).alias("reviewed"))

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)

    assert blocker_series.to_list() == [
        "Decision required",
        "Contradiction in component",
        "Contradiction in component",
    ]
    assert primary_state_series.to_list() == ["Needs decision", "Needs fix", "Needs fix"]


def test_transfer_uncategorized_is_not_treated_as_fix() -> None:
    df = pl.DataFrame(
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
    df = df.with_columns(review_validation.normalize_flag_series(df["reviewed"]).alias("reviewed"))

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)

    assert blocker_series.to_list() == ["None"]
    assert primary_state_series.to_list() == ["Settled"]


def test_off_budget_transfer_without_category_is_treated_as_fix() -> None:
    df = pl.DataFrame(
        [
            {
                "transaction_id": "transfer-row",
                "date": "2026-03-01",
                "account_name": "Bank Leumi",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": "loan payment",
                "fingerprint": "transfer loan",
                "payee_options": "Transfer : Loan",
                "category_options": "",
                "match_status": "source_only",
                "workflow_type": "institutional",
                "source_row_id": "s1",
                "target_row_id": "",
                "source_present": "TRUE",
                "target_present": "FALSE",
                "source_payee_selected": "loan payment",
                "source_category_selected": "",
                "target_payee_selected": "Transfer : Loan",
                "target_category_selected": "None",
                "target_account_on_budget": True,
                "target_transfer_account_on_budget": False,
                "decision_action": "create_target",
                "update_maps": "",
                "reviewed": "FALSE",
            }
        ]
    )
    df = df.with_columns(
        review_validation.normalize_flag_series(df["reviewed"]).alias("reviewed")
    )

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)

    assert blocker_series.to_list() == ["Missing category"]
    assert primary_state_series.to_list() == ["Needs fix"]


def test_apply_row_filters_supports_action_blocker_suggestions_map_updates_and_search() -> None:
    df = pl.DataFrame(
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
    df = df.with_columns(review_validation.normalize_flag_series(df["reviewed"]).alias("reviewed"))

    blocker_series = review_validation.blocker_series(df)
    primary_state_series = review_state.primary_state_series(df, blocker_series)
    row_kind_series = review_state.row_kind_series(df)
    action_series = review_state.action_series(df)
    save_state = pl.Series(["Unsaved", "Saved"], dtype=pl.Utf8)
    suggestion_series = review_state.suggestion_series(df)
    map_update_series = review_state.map_update_filter_series(df)
    search_text = review_state.search_text_series(df)

    filtered = review_state.apply_row_filters(
        df,
        primary_state=["Needs review"],
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

    assert filtered["transaction_id"].to_list() == ["create-target"]
