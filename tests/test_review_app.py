from __future__ import annotations

from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

import ynab_il_importer.review_app.app as review_app
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state


def _make_rows(*, category_selected: str) -> list[dict[str, str]]:
    return [
        {
            "transaction_id": "t1",
            "date": "2025-01-01",
            "account_name": "Account 1",
            "outflow_ils": "10",
            "inflow_ils": "0",
            "memo": "memo1",
            "fingerprint": "fp1",
            "payee_options": "A;B",
            "category_options": "C;D",
            "payee_selected": "A",
            "category_selected": category_selected,
            "match_status": "ambiguous",
            "update_map": "",
        },
        {
            "transaction_id": "t2",
            "date": "2025-01-02",
            "account_name": "Account 1",
            "outflow_ils": "11",
            "inflow_ils": "0",
            "memo": "memo2",
            "fingerprint": "fp1",
            "payee_options": "A;B",
            "category_options": "C;D",
            "payee_selected": "A",
            "category_selected": category_selected,
            "match_status": "ambiguous",
            "update_map": "",
        },
    ]


def _write_proposed(path: Path, rows: list[dict[str, str]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _write_categories(path: Path) -> None:
    pd.DataFrame(
        [
            {"category_group": "G", "category_name": "C"},
            {"category_group": "G", "category_name": "D"},
            {"category_group": "Internal Master Category", "category_name": "Uncategorized"},
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")


def _build_app_test(
    tmp_path: Path,
    *,
    proposed_rows: list[dict[str, str]],
    reviewed_rows: list[dict[str, str]] | None = None,
    resume: bool = False,
) -> tuple[AppTest, Path, Path]:
    proposed = tmp_path / "proposed.csv"
    reviewed = tmp_path / "proposed_reviewed.csv"
    categories = tmp_path / "categories.csv"

    _write_proposed(proposed, proposed_rows)
    _write_categories(categories)
    if reviewed_rows is not None:
        _write_proposed(reviewed, reviewed_rows)

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
    return app, proposed, reviewed


def _find_selectbox(container, key: str):
    return next(
        widget for widget in container.selectbox if str(widget.key).startswith(key)
    )


def _find_button(container, key: str):
    return next(widget for widget in container.button if key in str(widget.key))


def _find_button_by_label(container, label: str):
    return next(widget for widget in container.button if widget.label == label)


def _sidebar_markdown_text(app: AppTest) -> str:
    return "\n".join(markdown.value for markdown in app.sidebar.markdown)


def _group_markdown_text(app: AppTest, index: int = 0) -> str:
    return "\n".join(markdown.value for markdown in app.expander[index].markdown)


def test_apply_to_same_fingerprint_respects_eligible_mask() -> None:
    df = pd.DataFrame(
        {
            "fingerprint": ["fp1", "fp1", "fp2"],
            "payee_selected": ["A", "A", "B"],
            "category_selected": ["C", "C", "D"],
            "update_map": [False, False, False],
            "reviewed": [False, False, False],
        }
    )
    eligible_mask = pd.Series([True, False, True], index=df.index)

    review_model.apply_to_same_fingerprint(
        df,
        "fp1",
        payee="X",
        category="Y",
        update_map=True,
        reviewed=True,
        eligible_mask=eligible_mask,
    )

    assert df.loc[0, "payee_selected"] == "X"
    assert df.loc[0, "category_selected"] == "Y"
    assert bool(df.loc[0, "reviewed"]) is True
    assert df.loc[1, "payee_selected"] == "A"
    assert df.loc[1, "category_selected"] == "C"
    assert bool(df.loc[1, "reviewed"]) is False


def test_format_category_label_uses_group_slash_name() -> None:
    assert (
        review_app._format_category_label("Uncategorized", {"Uncategorized": "Internal"})
        == "Internal / Uncategorized"
    )


def test_changed_mask_aligns_duplicate_transaction_ids() -> None:
    base = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "payee_selected": ["A", "A"],
            "category_selected": ["C", "C"],
        }
    )
    current = pd.DataFrame(
        {
            "transaction_id": ["dup", "dup"],
            "payee_selected": ["A", "A"],
            "category_selected": ["C", "D"],
        }
    )

    changed = review_state.changed_mask(current, base)

    assert changed.tolist() == [False, True]


def test_grouped_row_save_updates_counts_and_persists_to_file(tmp_path: Path) -> None:
    app, _, reviewed = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
    )

    app.run()
    app.sidebar.checkbox[0].uncheck()
    app.run()

    assert any("Account 1" in expander.label for expander in app.expander)
    group = app.expander[0]
    _find_selectbox(group, "category_select_0").set_value("D")
    _find_button(group, "FormSubmitter:row_form_0").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df.loc[0, "category_selected"] == "D"
    assert bool(session_df.loc[0, "reviewed"]) is True
    assert "**Updated:** 1" in _sidebar_markdown_text(app)
    assert "Unsaved: 1" in _group_markdown_text(app)
    assert "Changed: 1" in _group_markdown_text(app)

    _find_button_by_label(app.sidebar, "Save").click()
    app.run()

    saved = pd.read_csv(reviewed, dtype="string").fillna("")
    assert saved.loc[0, "category_selected"] == "D"
    assert saved.loc[0, "reviewed"] == "TRUE"
    assert "**Saved:** 1" in _sidebar_markdown_text(app)
    app.sidebar.checkbox[0].uncheck()
    app.run()
    assert "Saved: 1" in _group_markdown_text(app)
    assert "Unsaved: 1" not in _group_markdown_text(app)


def test_group_apply_does_not_overwrite_reviewed_rows(tmp_path: Path) -> None:
    app, _, reviewed = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
    )

    app.run()
    app.sidebar.checkbox[0].uncheck()
    app.run()

    group = app.expander[0]
    assert any(str(widget.key).startswith("group_payee_override_") for widget in group.text_input)
    assert not any(
        str(widget.key).startswith("group_payee_override_") for widget in group.selectbox
    )
    _find_selectbox(group, "category_select_0").set_value("D")
    _find_button(group, "FormSubmitter:row_form_0").click()
    app.run()

    group = app.expander[0]
    _find_selectbox(group, "group_category_fp1").set_value("C")
    _find_button(group, "group_apply_fp1").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df.loc[0, "category_selected"] == "D"
    assert session_df.loc[1, "category_selected"] == "C"
    assert bool(session_df.loc[0, "reviewed"]) is True
    assert bool(session_df.loc[1, "reviewed"]) is True

    _find_button_by_label(app.sidebar, "Save").click()
    app.run()

    saved = pd.read_csv(reviewed, dtype="string").fillna("")
    assert saved.loc[0, "category_selected"] == "D"
    assert saved.loc[1, "category_selected"] == "C"


def test_category_toggle_exposes_all_categories(tmp_path: Path) -> None:
    app, _, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected=""),
    )

    app.run()
    app.sidebar.radio[0].set_value("Row")
    app.run()
    app.sidebar.checkbox[0].uncheck()
    app.run()

    row = app.expander[0]
    category_select = _find_selectbox(row, "category_select_0")
    assert "Uncategorized" not in category_select.options

    show_all = next(
        widget for widget in row.checkbox if str(widget.key).startswith("show_all_categories_0")
    )
    show_all.check()
    app.run()

    row = app.expander[0]
    category_select = _find_selectbox(row, "category_select_0")
    assert "Internal Master Category / Uncategorized" in category_select.options


def test_group_category_toggle_exposes_all_categories(tmp_path: Path) -> None:
    app, _, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected=""),
    )

    app.run()
    app.sidebar.checkbox[0].uncheck()
    app.run()

    group = app.expander[0]
    group_category = _find_selectbox(group, "group_category_fp1")
    assert "Internal Master Category / Uncategorized" not in group_category.options

    show_all = next(
        widget
        for widget in group.checkbox
        if str(widget.key).startswith("group_show_all_categories_fp1")
    )
    show_all.check()
    app.run()

    group = app.expander[0]
    group_category = _find_selectbox(group, "group_category_fp1")
    assert "Internal Master Category / Uncategorized" in group_category.options


def test_sidebar_save_action_defaults_to_save(tmp_path: Path) -> None:
    app, _, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
    )

    app.run()

    assert app.sidebar.selectbox[0].value == "Save"
    assert app.sidebar.button[2].label == "Save"


def test_categories_load_on_startup_when_resuming(tmp_path: Path) -> None:
    reviewed_rows = _make_rows(category_selected="C")
    app, _, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
        reviewed_rows=reviewed_rows,
        resume=True,
    )

    app.run()

    assert "Uncategorized" in app.session_state["category_list"]


def test_resume_keeps_original_source_path(tmp_path: Path) -> None:
    reviewed_rows = _make_rows(category_selected="D")
    app, proposed, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
        reviewed_rows=reviewed_rows,
        resume=True,
    )

    app.run()

    assert Path(app.session_state["source_path"]) == proposed
    assert app.session_state["df"].loc[0, "category_selected"] == "D"


def test_grouped_row_save_works_while_resumed(tmp_path: Path) -> None:
    reviewed_rows = _make_rows(category_selected="")
    app, _, reviewed = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
        reviewed_rows=reviewed_rows,
        resume=True,
    )

    app.run()

    group = app.expander[0]
    _find_selectbox(group, "category_select_0").set_value("D")
    _find_button(group, "FormSubmitter:row_form_0").click()
    app.run()

    session_df = app.session_state["df"]
    assert session_df.loc[0, "category_selected"] == "D"
    assert bool(session_df.loc[0, "reviewed"]) is True

    _find_button_by_label(app.sidebar, "Save").click()
    app.run()

    saved = pd.read_csv(reviewed, dtype="string").fillna("")
    assert saved.loc[0, "category_selected"] == "D"
    assert saved.loc[0, "reviewed"] == "TRUE"


def test_reload_original_clears_stale_widget_state(tmp_path: Path) -> None:
    app, proposed, _ = _build_app_test(
        tmp_path,
        proposed_rows=_make_rows(category_selected="C"),
    )

    app.run()
    app.sidebar.checkbox[0].uncheck()
    app.run()

    group = app.expander[0]
    _find_selectbox(group, "category_select_0").set_value("D")
    _find_button(group, "FormSubmitter:row_form_0").click()
    app.run()

    _find_button_by_label(app.sidebar, "Reload original").click()
    app.run()

    assert Path(app.session_state["source_path"]) == proposed
    assert app.session_state["df"].loc[0, "category_selected"] == "C"
    app.sidebar.checkbox[0].uncheck()
    app.run()
    group = app.expander[0]
    assert _find_selectbox(group, "category_select_0").value == "C"
