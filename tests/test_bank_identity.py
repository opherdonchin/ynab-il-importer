from __future__ import annotations

import pytest

import ynab_il_importer.bank_identity as bank_identity


def test_make_bank_txn_id_is_deterministic_and_versioned() -> None:
    value = bank_identity.make_bank_txn_id(
        source="bank",
        source_account="123456",
        date="2026-03-01",
        secondary_date="2026-03-02",
        outflow_ils=10.5,
        inflow_ils=0,
        ref="0042",
        description_raw="ACME STORE",
    )

    assert value == bank_identity.make_bank_txn_id(
        source="bank",
        source_account="123456",
        date="2026-03-01",
        secondary_date="2026-03-02",
        outflow_ils=10.5,
        inflow_ils=0,
        ref="0042",
        description_raw="ACME STORE",
    )
    assert value.startswith("BANK:V1:")
    assert bank_identity.parse_bank_txn_id(value)["version"] == "V1"


def test_parse_bank_txn_id_rejects_unknown_versions() -> None:
    with pytest.raises(ValueError, match="Unsupported bank_txn_id version"):
        bank_identity.parse_bank_txn_id("BANK:V2:1234567890abcdef12345678")


def test_make_bank_txn_id_distinguishes_same_date_amount_and_ref_by_description() -> None:
    first = bank_identity.make_bank_txn_id(
        source="bank",
        source_account="123456",
        date="2026-03-01",
        secondary_date="2026-03-01",
        outflow_ils=50,
        inflow_ils=0,
        ref="9999",
        description_raw="TRANSFER TO A",
    )
    second = bank_identity.make_bank_txn_id(
        source="bank",
        source_account="123456",
        date="2026-03-01",
        secondary_date="2026-03-01",
        outflow_ils=50,
        inflow_ils=0,
        ref="9999",
        description_raw="TRANSFER TO B",
    )

    assert first != second


def test_append_bank_txn_id_marker_rejects_conflicting_marker() -> None:
    with pytest.raises(ValueError, match="conflicting"):
        bank_identity.append_bank_txn_id_marker(
            "[ynab-il bank_txn_id=BANK:V1:1234567890abcdef12345678]",
            "BANK:V1:abcdef1234567890abcdef12",
        )
