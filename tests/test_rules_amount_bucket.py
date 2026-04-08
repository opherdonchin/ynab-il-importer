import polars as pl

import ynab_il_importer.rules as rules_mod


def test_amount_bucket_rules() -> None:
    rules = pl.DataFrame(
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
    rules = rules_mod.normalize_payee_map_rules(rules)

    tx = pl.DataFrame(
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
    out = rules_mod.apply_payee_map_rules(tx, rules)
    assert out[0, "payee_canonical_suggested"] == "Yellow"
    assert out[1, "payee_canonical_suggested"] == "Gas Food"
