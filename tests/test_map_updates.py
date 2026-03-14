import pandas as pd

import ynab_il_importer.map_updates as map_updates


def test_build_map_update_candidates_dedupes_changed_reviewed_rows() -> None:
    base = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "coffee",
                "payee_selected": "Coffee Shop",
                "category_selected": "Eating Out",
            },
            {
                "transaction_id": "t2",
                "fingerprint": "coffee",
                "payee_selected": "Coffee Shop",
                "category_selected": "Eating Out",
            },
        ]
    )
    current = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "coffee",
                "payee_selected": "Cafe",
                "category_selected": "Dining",
                "reviewed": True,
                "update_map": False,
                "memo": "row one",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
            },
            {
                "transaction_id": "t2",
                "fingerprint": "coffee",
                "payee_selected": "Cafe",
                "category_selected": "Dining",
                "reviewed": True,
                "update_map": False,
                "memo": "row two",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
            },
        ]
    )

    actual = map_updates.build_map_update_candidates(current, base)

    assert len(actual) == 1
    assert actual.loc[0, "fingerprint"] == "coffee"
    assert actual.loc[0, "payee_canonical"] == "Cafe"
    assert actual.loc[0, "category_target"] == "Dining"
    assert actual.loc[0, "count"] == 2
    assert actual.loc[0, "rule_id"].startswith("candidate_")


def test_build_map_update_candidates_includes_update_map_even_if_unchanged() -> None:
    base = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "cash move",
                "payee_selected": "Transfer : Cash",
                "category_selected": "",
            }
        ]
    )
    current = pd.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "cash move",
                "payee_selected": "Transfer : Cash",
                "category_selected": "",
                "reviewed": True,
                "update_map": True,
                "memo": "transfer row",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 50.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
            }
        ]
    )

    actual = map_updates.build_map_update_candidates(current, base)

    assert len(actual) == 1
    assert actual.loc[0, "payee_canonical"] == "Transfer : Cash"
    assert actual.loc[0, "category_target"] == ""
    assert "update_map=TRUE" in actual.loc[0, "notes"]
