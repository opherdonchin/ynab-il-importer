import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.rules as rules_mod


def _rules(rows: list[dict[str, object]]) -> pd.DataFrame:
    return rules_mod.normalize_payee_map_rules(pd.DataFrame(rows))


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
            {"fingerprint": "supermarket", "source": "bank", "account_name": "A", "outflow_ils": 20, "inflow_ils": 0},
            {"fingerprint": "supermarket", "source": "card", "account_name": "B", "outflow_ils": 30, "inflow_ils": 0},
        ]
    )
    out = rules_mod.apply_payee_map_rules(tx, rules)
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
    tx = pd.DataFrame([{"fingerprint": "bit", "source": "bank", "outflow_ils": 50, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
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
    tx = pd.DataFrame([{"fingerprint": "rent", "source": "bank", "outflow_ils": 1000, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
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
    tx = pd.DataFrame([{"fingerprint": "same", "source": "bank", "outflow_ils": 5, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
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
    tx = pd.DataFrame([{"fingerprint": "coffee", "outflow_ils": 15, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "payee_canonical_suggested"] == "Cafe"
    assert out.loc[0, "category_target_suggested"] == ""


def test_exact_amount_bucket_matches_only_exact_value() -> None:
    rules = _rules(
        [
            {
                "rule_id": "exact",
                "fingerprint": "transfer",
                "amount_bucket": "=6300",
                "payee_canonical": "Transfer : Planned Liya",
            }
        ]
    )
    tx = pd.DataFrame(
        [
            {"fingerprint": "transfer", "outflow_ils": 6300, "inflow_ils": 0},
            {"fingerprint": "transfer", "outflow_ils": 6299.99, "inflow_ils": 0},
        ]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "payee_canonical_suggested"] == "Transfer : Planned Liya"
    assert out.loc[1, "match_status"] == "none"


def test_card_suffix_disambiguates_transfer_rules() -> None:
    rules = _rules(
        [
            {
                "rule_id": "generic",
                "fingerprint": "לאומי ויזה",
                "source": "bank",
                "payee_canonical": "Transfer : Generic Card",
            },
            {
                "rule_id": "specific",
                "fingerprint": "לאומי ויזה",
                "source": "bank",
                "card_suffix": "7195",
                "payee_canonical": "Transfer : Liya X7195",
            },
        ]
    )
    tx = pd.DataFrame(
        [{"fingerprint": "לאומי ויזה", "source": "bank", "card_suffix": "7195", "outflow_ils": 10, "inflow_ils": 0}]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out.loc[0, "match_status"] == "unique"
    assert out.loc[0, "match_rule_id"] == "specific"
    assert out.loc[0, "payee_canonical_suggested"] == "Transfer : Liya X7195"
