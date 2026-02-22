import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.rules import apply_payee_map_rules
from ynab_il_importer.rules import normalize_payee_map_rules


def _rules(rows: list[dict[str, object]]) -> pd.DataFrame:
    return normalize_payee_map_rules(pd.DataFrame(rows))


def test_wildcard_blank_fields_match_any_context() -> None:
    rules = _rules(
        [
            {
                "rule_id": "r1",
                "fingerprint": "supermarket",
                "payee_canonical": "Supermarket",
            }
        ]
    )
    tx = pd.DataFrame(
        [
            {"fingerprint": "supermarket", "source": "bank", "account_name": "A", "amount_ils": -20},
            {"fingerprint": "supermarket", "source": "card", "account_name": "B", "amount_ils": -30},
        ]
    )
    out = apply_payee_map_rules(tx, rules)
    assert out["match_status"].tolist() == ["unique", "unique"]
    assert out["match_rule_id"].tolist() == ["r1", "r1"]


def test_specificity_wins_when_priority_equal() -> None:
    rules = _rules(
        [
            {"rule_id": "r1", "priority": 0, "fingerprint": "bit", "payee_canonical": "BIT Generic"},
            {
                "rule_id": "r2",
                "priority": 0,
                "fingerprint": "bit",
                "source": "bank",
                "payee_canonical": "BIT Bank",
            },
        ]
    )
    tx = pd.DataFrame([{"fingerprint": "bit", "source": "bank", "amount_ils": -50}])
    out = apply_payee_map_rules(tx, rules)
    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "match_rule_id"] == "r2"
    assert out.loc[0, "payee_canonical_suggested"] == "BIT Bank"


def test_priority_wins_over_specificity() -> None:
    rules = _rules(
        [
            {
                "rule_id": "r1",
                "priority": 0,
                "fingerprint": "rent",
                "source": "bank",
                "payee_canonical": "Landlord A",
            },
            {
                "rule_id": "r2",
                "priority": 10,
                "fingerprint": "rent",
                "payee_canonical": "Landlord B",
            },
        ]
    )
    tx = pd.DataFrame([{"fingerprint": "rent", "source": "bank", "amount_ils": -1000}])
    out = apply_payee_map_rules(tx, rules)
    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "match_rule_id"] == "r2"
    assert out.loc[0, "payee_canonical_suggested"] == "Landlord B"


def test_ambiguous_when_top_priority_and_specificity_tie() -> None:
    rules = _rules(
        [
            {
                "rule_id": "a_rule",
                "priority": 2,
                "fingerprint": "same",
                "source": "bank",
                "payee_canonical": "Payee A",
            },
            {
                "rule_id": "b_rule",
                "priority": 2,
                "fingerprint": "same",
                "source": "bank",
                "payee_canonical": "Payee B",
            },
        ]
    )
    tx = pd.DataFrame([{"fingerprint": "same", "source": "bank", "amount_ils": -5}])
    out = apply_payee_map_rules(tx, rules)
    assert out.loc[0, "match_status"] == "ambiguous"
    assert out.loc[0, "match_rule_id"] == "a_rule;b_rule"
    assert out.loc[0, "payee_canonical_suggested"] == ""


def test_blank_category_target_stays_unassigned() -> None:
    rules = _rules(
        [
            {
                "rule_id": "r1",
                "fingerprint": "coffee",
                "payee_canonical": "Cafe",
                "category_target": "",
            }
        ]
    )
    tx = pd.DataFrame([{"fingerprint": "coffee", "amount_ils": -15}])
    out = apply_payee_map_rules(tx, rules)
    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "payee_canonical_suggested"] == "Cafe"
    assert out.loc[0, "category_target_suggested"] == ""
