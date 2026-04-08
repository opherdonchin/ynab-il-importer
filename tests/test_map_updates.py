from __future__ import annotations

import polars as pl

import ynab_il_importer.map_updates as map_updates


def test_build_map_update_candidates_dedupes_changed_reviewed_rows() -> None:
    base = pl.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "coffee",
                "target_payee_selected": "Coffee Shop",
                "target_category_selected": "Eating Out",
                "source_present": True,
            },
            {
                "transaction_id": "t2",
                "fingerprint": "coffee",
                "target_payee_selected": "Coffee Shop",
                "target_category_selected": "Eating Out",
                "source_present": True,
            },
        ]
    )
    current = pl.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "coffee",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Dining",
                "reviewed": True,
                "update_maps": "",
                "memo": "row one",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
                "source_present": True,
            },
            {
                "transaction_id": "t2",
                "fingerprint": "coffee",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Dining",
                "reviewed": True,
                "update_maps": "",
                "memo": "row two",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
                "source_present": True,
            },
        ]
    )

    actual = map_updates.build_map_update_candidates(current, base)

    assert len(actual) == 1
    row = actual.row(0, named=True)
    assert row["fingerprint"] == "coffee"
    assert row["payee_canonical"] == "Cafe"
    assert row["category_target"] == "Dining"
    assert row["count"] == 2
    assert row["rule_id"].startswith("candidate_")


def test_build_map_update_candidates_includes_explicit_update_maps_even_if_unchanged() -> None:
    base = pl.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "cash move",
                "target_payee_selected": "Transfer : Cash",
                "target_category_selected": "",
                "source_present": True,
            }
        ]
    )
    current = pl.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "cash move",
                "target_payee_selected": "Transfer : Cash",
                "target_category_selected": "",
                "reviewed": True,
                "update_maps": "fingerprint_add_source;payee_limit_fingerprint",
                "memo": "transfer row",
                "account_name": "Bank Leumi",
                "source": "bank",
                "txn_kind": "",
                "outflow_ils": 50.0,
                "inflow_ils": 0.0,
                "currency": "ILS",
                "source_present": True,
            }
        ]
    )

    actual = map_updates.build_map_update_candidates(current, base)

    assert len(actual) == 1
    row = actual.row(0, named=True)
    assert row["payee_canonical"] == "Transfer : Cash"
    assert row["category_target"] == ""
    assert "update_maps=fingerprint_add_source;payee_limit_fingerprint" in row["notes"]
