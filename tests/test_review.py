import pandas as pd

import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.review_app.state as review_state
import ynab_il_importer.review_app.validation as review_validation


def test_parse_option_string() -> None:
    assert review_model.parse_option_string("") == []
    assert review_model.parse_option_string("a") == ["a"]
    assert review_model.parse_option_string("a; b; a;") == ["a", "b"]


def test_resolve_selected_value() -> None:
    assert review_model.resolve_selected_value("a", "") == "a"
    assert review_model.resolve_selected_value("a", "override") == "override"


def test_validate_row_errors_and_warnings() -> None:
    row = pd.Series(
        {
            "payee_selected": "",
            "category_selected": "",
            "update_map": True,
            "payee_options": "A;B",
            "category_options": "C",
        }
    )
    errors, warnings = review_validation.validate_row(row)
    assert "missing payee" in errors
    assert "missing category" in errors
    assert "update_map set while payee/category missing" in warnings


def test_load_save_roundtrip(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "transaction_id": ["t1", "t2"],
            "date": ["2024-01-01", "2024-01-02"],
            "payee_options": ["A;B", ""],
            "category_options": ["C", ""],
            "payee_selected": ["A", ""],
            "category_selected": ["C", ""],
            "match_status": ["unique", "none"],
            "update_map": ["TRUE", ""],
            "fingerprint": ["fp1", "fp2"],
        }
    )
    src = tmp_path / "proposed.csv"
    df.to_csv(src, index=False, encoding="utf-8-sig")

    loaded = review_io.load_proposed_transactions(src)
    assert loaded["update_map"].tolist() == [True, False]

    out = tmp_path / "reviewed.csv"
    review_io.save_reviewed_transactions(loaded, out)
    saved = pd.read_csv(out, dtype="string").fillna("")
    assert saved["update_map"].tolist() == ["TRUE", ""]


def test_transfer_payee_does_not_require_category() -> None:
    row = pd.Series(
        {
            "payee_selected": "Transfer : Cash",
            "category_selected": "",
            "update_map": False,
            "payee_options": "Transfer : Cash",
            "category_options": "",
        }
    )

    errors, warnings = review_validation.validate_row(row)

    assert "missing category" not in errors
    assert warnings == []


def test_transfer_rows_are_not_unresolved_without_category() -> None:
    df = pd.DataFrame(
        {
            "payee_selected": ["Transfer : Planned Liya", "Cafe"],
            "category_selected": ["", ""],
            "update_map": [False, False],
            "match_status": ["ambiguous", "none"],
        }
    )

    counts = review_state.summary_counts(df)
    filtered = review_state.apply_filters(
        df, {"match_status": ["ambiguous", "none"], "unresolved_only": True}
    )

    assert counts["missing_category"] == 1
    assert counts["unresolved"] == 1
    assert filtered["payee_selected"].tolist() == ["Cafe"]
