import sys
from pathlib import Path

import polars as pl
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.rules as rules_mod


def _rules(rows: list[dict[str, object]]) -> pl.DataFrame:
    return rules_mod.normalize_payee_map_rules(pl.DataFrame(rows))


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
    tx = pl.DataFrame(
        [
            {"fingerprint": "supermarket", "source": "bank", "account_name": "A", "outflow_ils": 20, "inflow_ils": 0},
            {"fingerprint": "supermarket", "source": "card", "account_name": "B", "outflow_ils": 30, "inflow_ils": 0},
        ]
    )
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out["match_status"].to_list() == ["unique", "unique"]
    assert out["match_rule_id"].to_list() == ["r1", "r1"]


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
    tx = pl.DataFrame([{"fingerprint": "bit", "source": "bank", "outflow_ils": 50, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out[0, "match_status"] == "unique"
    assert out[0, "match_rule_id"] == "r2"
    assert out[0, "payee_canonical_suggested"] == "BIT Bank"


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
    tx = pl.DataFrame([{"fingerprint": "rent", "source": "bank", "outflow_ils": 1000, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out[0, "match_status"] == "unique"
    assert out[0, "match_rule_id"] == "r2"
    assert out[0, "payee_canonical_suggested"] == "Landlord B"


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
    tx = pl.DataFrame([{"fingerprint": "same", "source": "bank", "outflow_ils": 5, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out[0, "match_status"] == "ambiguous"
    assert out[0, "match_rule_id"] == "a_rule;b_rule"
    assert out[0, "payee_canonical_suggested"] == ""


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
    tx = pl.DataFrame([{"fingerprint": "coffee", "outflow_ils": 15, "inflow_ils": 0}])
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out[0, "match_status"] == "unique"
    assert out[0, "payee_canonical_suggested"] == "Cafe"
    assert out[0, "category_target_suggested"] == ""


def test_none_category_target_is_preserved_as_explicit_no_category() -> None:
    rules = _rules(
        [
            {
                "rule_id": "r1",
                "fingerprint": "transfer",
                "payee_canonical": "Transfer : Cash",
                "category_target": review_model.NO_CATEGORY_REQUIRED,
            }
        ]
    )
    tx = pl.DataFrame([{"fingerprint": "transfer", "outflow_ils": 15, "inflow_ils": 0}])

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out[0, "match_status"] == "unique"
    assert out[0, "payee_canonical_suggested"] == "Transfer : Cash"
    assert out[0, "category_target_suggested"] == review_model.NO_CATEGORY_REQUIRED


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
    tx = pl.DataFrame(
        [
            {"fingerprint": "transfer", "outflow_ils": 6300, "inflow_ils": 0},
            {"fingerprint": "transfer", "outflow_ils": 6299.99, "inflow_ils": 0},
        ]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out[0, "match_status"] == "unique"
    assert out[0, "payee_canonical_suggested"] == "Transfer : Planned Liya"
    assert out[1, "match_status"] == "none"


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
    tx = pl.DataFrame(
        [{"fingerprint": "לאומי ויזה", "source": "bank", "card_suffix": "7195", "outflow_ils": 10, "inflow_ils": 0}]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out[0, "match_status"] == "unique"
    assert out[0, "match_rule_id"] == "specific"
    assert out[0, "payee_canonical_suggested"] == "Transfer : Liya X7195"


def test_specific_account_rule_beats_generic_facebook_payee_default() -> None:
    rules = _rules(
        [
            {
                "rule_id": "generic",
                "priority": 0,
                "fingerprint": "paypal facebook עסקת חו",
                "source": "card",
                "direction": "outflow",
                "payee_canonical": "Facebook",
            },
            {
                "rule_id": "specific",
                "priority": 10,
                "fingerprint": "paypal facebook עסקת חו",
                "account_name": "Opher x9922",
                "source": "card",
                "direction": "outflow",
                "payee_canonical": "Facebook",
                "category_target": "Aikido",
            },
        ]
    )
    tx = pl.DataFrame(
        [
            {
                "fingerprint": "paypal facebook עסקת חו",
                "account_name": "Opher x9922",
                "source": "card",
                "outflow_ils": 568,
                "inflow_ils": 0,
            },
            {
                "fingerprint": "paypal facebook עסקת חו",
                "account_name": "Liya X7195",
                "source": "card",
                "outflow_ils": 120,
                "inflow_ils": 0,
            },
        ]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert out["match_rule_id"].to_list() == ["specific", "generic"]
    assert out["payee_canonical_suggested"].to_list() == ["Facebook", "Facebook"]
    assert out["category_target_suggested"].to_list() == ["Aikido", ""]


def test_load_payee_map_canonicalizes_fingerprint_keys_with_fingerprint_map(
    tmp_path: Path,
) -> None:
    payee_map_path = tmp_path / "payee_map.csv"
    fingerprint_map_path = tmp_path / "fingerprint_map.csv"

    pd.DataFrame(
        [
            {
                "rule_id": "adrian_rule",
                "is_active": True,
                "priority": 10,
                "txn_kind": "",
                "fingerprint": "אדריאן דדון",
                "description_clean_norm": "",
                "account_name": "",
                "source": "",
                "direction": "",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": "Adrian Sports Trainer",
                "category_target": "Chugim",
                "notes": "",
                "card_suffix": "",
            }
        ],
        columns=rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(payee_map_path, index=False, encoding="utf-8-sig")

    fingerprint_map_path.write_text(
        "\n".join(
            [
                "rule_id,is_active,priority,pattern,canonical_text,notes",
                "adrian,TRUE,0,אדריאן,Adrian Sports Trainer,",
            ]
        ),
        encoding="utf-8",
    )

    rules = rules_mod.load_payee_map(
        payee_map_path,
        fingerprint_map_path=fingerprint_map_path,
    )
    tx = pl.DataFrame(
        [{"fingerprint": "adrian sports trainer", "outflow_ils": 297, "inflow_ils": 0}]
    )

    out = rules_mod.apply_payee_map_rules(tx, rules)

    assert rules[0, "fingerprint"] == "adrian sports trainer"
    assert out[0, "match_status"] == "unique"
    assert out[0, "payee_canonical_suggested"] == "Adrian Sports Trainer"
