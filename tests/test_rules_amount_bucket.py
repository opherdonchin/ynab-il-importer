import pandas as pd

from ynab_il_importer.rules import apply_payee_map_rules, normalize_payee_map_rules


def test_amount_bucket_rules() -> None:
    rules = pd.DataFrame(
        [
            {
                "rule_id": "gas_hi",
                "is_active": True,
                "priority": 0,
                "fingerprint": "yellow",
                "amount_bucket": ">=150",
                "payee_canonical": "Yellow",
                "category_target": "Gas",
            },
            {
                "rule_id": "gas_lo",
                "is_active": True,
                "priority": 0,
                "fingerprint": "yellow",
                "amount_bucket": "<150",
                "payee_canonical": "Gas Food",
                "category_target": "Groceries",
            },
        ]
    )
    rules = normalize_payee_map_rules(rules)

    tx = pd.DataFrame(
        [
            {
                "fingerprint": "yellow",
                "outflow_ils": 200.0,
                "inflow_ils": 0.0,
                "date": "2024-01-01",
            },
            {
                "fingerprint": "yellow",
                "outflow_ils": 100.0,
                "inflow_ils": 0.0,
                "date": "2024-01-02",
            },
        ]
    )
    out = apply_payee_map_rules(tx, rules)
    assert out.loc[0, "payee_canonical_suggested"] == "Yellow"
    assert out.loc[1, "payee_canonical_suggested"] == "Gas Food"
