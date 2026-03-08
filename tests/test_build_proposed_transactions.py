import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_proposed_transactions.py"
SPEC = importlib.util.spec_from_file_location("build_proposed_transactions", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
build_proposed_transactions = importlib.util.module_from_spec(SPEC)
sys.modules["build_proposed_transactions"] = build_proposed_transactions
SPEC.loader.exec_module(build_proposed_transactions)


def test_dedupe_source_overlaps_drops_matching_card_rows() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "memo": "bank row",
            },
            {
                "source": "card",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "memo": "card row",
            },
            {
                "source": "card",
                "date": "2025-12-12",
                "outflow_ils": 18.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "memo": "other card row",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert len(deduped) == 2
    assert deduped["memo"].tolist() == ["bank row", "other card row"]


def test_dedupe_source_overlaps_preserves_extra_bank_rows() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "memo": "bank 1",
            },
            {
                "source": "bank",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "memo": "bank 2",
            },
            {
                "source": "card",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "memo": "card 1",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert deduped["memo"].tolist() == ["bank 1", "bank 2"]


def test_dedupe_sources_handles_non_range_index(monkeypatch: pytest.MonkeyPatch) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "memo": "matched",
            },
            {
                "account_name": "Bank Leumi",
                "date": "2025-12-13",
                "outflow_ils": 30.0,
                "inflow_ils": 0.0,
                "memo": "keep",
            },
        ],
        index=[0, 2],
    )
    ynab_df = pd.DataFrame([{"dummy": 1}])
    pairs = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
            }
        ]
    )

    monkeypatch.setattr(build_proposed_transactions.pairing, "match_pairs", lambda *_: pairs)

    deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.equals(pairs)
    assert deduped["memo"].tolist() == ["keep"]
