from __future__ import annotations

import time

import pandas as pd

import ynab_il_importer.review_app.app as review_app
import ynab_il_importer.review_app.validation as review_validation


def make_review_df(n: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    pair_count = max(1, n // 2)
    for pair_idx in range(pair_count):
        rows.append(
            {
                "transaction_id": f"tx-{pair_idx}-a",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": f"memo-{pair_idx}-a",
                "fingerprint": f"fp-{pair_idx}",
                "payee_options": "Cafe;Grocer",
                "category_options": "Food;Dining",
                "match_status": "ambiguous",
                "workflow_type": "cross_budget",
                "source_row_id": f"source-{pair_idx}",
                "target_row_id": f"target-{pair_idx}-a",
                "source_present": True,
                "target_present": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "decision_action": "keep_match",
                "update_maps": "",
                "reviewed": False,
            }
        )
        rows.append(
            {
                "transaction_id": f"tx-{pair_idx}-b",
                "account_name": "Account 1",
                "date": "2026-03-01",
                "outflow_ils": "10",
                "inflow_ils": "0",
                "memo": f"memo-{pair_idx}-b",
                "fingerprint": f"fp-{pair_idx}",
                "payee_options": "Cafe;Grocer",
                "category_options": "Food;Dining",
                "match_status": "ambiguous",
                "workflow_type": "cross_budget",
                "source_row_id": f"source-{pair_idx}",
                "target_row_id": f"target-{pair_idx}-b",
                "source_present": True,
                "target_present": True,
                "source_payee_selected": "Cafe",
                "source_category_selected": "Food",
                "target_payee_selected": "Cafe",
                "target_category_selected": "Food",
                "decision_action": "ignore_row",
                "update_maps": "",
                "reviewed": False,
            }
        )
    return pd.DataFrame(rows[:n])


def test_blocker_series_with_components_smoke_500_rows() -> None:
    df = make_review_df(500)

    started = time.perf_counter()
    blocker_series, component_map = review_validation.blocker_series_with_components(df)
    duration = time.perf_counter() - started

    assert len(blocker_series) == len(df)
    assert blocker_series.dtype == "string"
    assert len(component_map) == len(df)
    assert duration < 10


def test_cached_derived_state_skips_recompute_when_generation_is_unchanged(monkeypatch) -> None:
    df = make_review_df(500)
    cache: dict[str, object] = {"_df_generation": 0}
    call_count = 0
    original = review_validation.precompute_components

    def wrapped(frame: pd.DataFrame) -> dict[object, int]:
        nonlocal call_count
        call_count += 1
        return original(frame)

    monkeypatch.setattr(review_validation, "precompute_components", wrapped)

    review_app._get_cached_derived_state(cache, df, None, None)
    review_app._get_cached_derived_state(cache, df, None, None)

    assert call_count == 1

    cache["_df_generation"] = 1
    review_app._get_cached_derived_state(cache, df, None, None)

    assert call_count == 2
