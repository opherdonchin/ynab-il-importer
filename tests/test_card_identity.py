from __future__ import annotations

import pytest

import ynab_il_importer.card_identity as card_identity


def test_make_card_txn_id_is_deterministic_and_versioned() -> None:
    value = card_identity.make_card_txn_id(
        source="card",
        source_account="x9922",
        card_suffix="9922",
        date="2026-03-09",
        secondary_date="2026-04-10",
        outflow_ils=546.79,
        inflow_ils=0.0,
        description_raw="מחשני השוק האורגים ח'",
        max_sheet="עסקאות במועד החיוב",
        max_txn_type="רגילה",
        max_original_amount=546.79,
        max_original_currency="ILS",
    )

    assert value == card_identity.make_card_txn_id(
        source="card",
        source_account="x9922",
        card_suffix="9922",
        date="2026-03-09",
        secondary_date="2026-04-10",
        outflow_ils=546.79,
        inflow_ils=0.0,
        description_raw="מחשני השוק האורגים ח'",
        max_sheet="עסקאות במועד החיוב",
        max_txn_type="רגילה",
        max_original_amount=546.79,
        max_original_currency="ILS",
    )
    assert value.startswith("CARD:V1:")
    assert card_identity.parse_card_txn_id(value)["version"] == "V1"


def test_parse_card_txn_id_rejects_unknown_versions() -> None:
    with pytest.raises(ValueError, match="Unsupported card_txn_id version"):
        card_identity.parse_card_txn_id("CARD:V2:1234567890abcdef12345678")


def test_append_card_txn_id_marker_rejects_conflicting_marker() -> None:
    with pytest.raises(ValueError, match="conflicting"):
        card_identity.append_card_txn_id_marker(
            "[ynab-il card_txn_id=CARD:V1:1234567890abcdef12345678]",
            "CARD:V1:abcdef1234567890abcdef12",
        )
