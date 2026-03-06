import pandas as pd

from ynab_il_importer.review.io import load_proposed_transactions, save_reviewed_transactions
from ynab_il_importer.review.model import parse_option_string, resolve_selected_value
from ynab_il_importer.review.validation import validate_row


def test_parse_option_string() -> None:
    assert parse_option_string("") == []
    assert parse_option_string("a") == ["a"]
    assert parse_option_string("a; b; a;") == ["a", "b"]


def test_resolve_selected_value() -> None:
    assert resolve_selected_value("a", "") == "a"
    assert resolve_selected_value("a", "override") == "override"


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
    errors, warnings = validate_row(row)
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

    loaded = load_proposed_transactions(src)
    assert loaded["update_map"].tolist() == [True, False]

    out = tmp_path / "reviewed.csv"
    save_reviewed_transactions(loaded, out)
    saved = pd.read_csv(out, dtype="string").fillna("")
    assert saved["update_map"].tolist() == ["TRUE", ""]
