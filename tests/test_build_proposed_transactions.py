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
                "account_key": "Bank Leumi",
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


def test_dedupe_sources_retains_ambiguous_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Opher x9922",
                "date": "2026-02-25",
                "outflow_ils": 421.43,
                "inflow_ils": 0.0,
                "memo": "passport fee",
            }
        ]
    )
    ynab_df = pd.DataFrame([{"dummy": 1}])
    pairs = pd.DataFrame(
        [
            {
                "account_key": "Opher x9922",
                "account_name": "Opher x9922",
                "date": "2026-02-25",
                "outflow_ils": 421.43,
                "inflow_ils": 0.0,
                "ambiguous_key": True,
            },
            {
                "account_key": "Opher x9922",
                "account_name": "Opher x9922",
                "date": "2026-02-25",
                "outflow_ils": 421.43,
                "inflow_ils": 0.0,
                "ambiguous_key": True,
            },
        ]
    )

    monkeypatch.setattr(build_proposed_transactions.pairing, "match_pairs", lambda *_: pairs)

    with pytest.warns(UserWarning, match="Retaining 1 source rows with ambiguous YNAB"):
        deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.equals(pairs)
    assert deduped["memo"].tolist() == ["passport fee"]


def test_dedupe_sources_drops_exact_import_id_matches_before_weak_pairing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Opher x9922",
                "date": "2026-02-25",
                "outflow_ils": 421.43,
                "inflow_ils": 0.0,
                "fingerprint": "ds passport",
                "description_raw": "DS-11 PASSPORT FEES",
                "memo": "passport fee 1",
            },
            {
                "account_name": "Opher x9922",
                "date": "2026-02-25",
                "outflow_ils": 421.43,
                "inflow_ils": 0.0,
                "fingerprint": "ds passport",
                "description_raw": "DS-11 PASSPORT FEES",
                "memo": "passport fee 2",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "account_name": "Opher x9922",
                "import_id": "YNAB:-421430:2026-02-25:1",
            }
        ]
    )

    monkeypatch.setattr(
        build_proposed_transactions.pairing,
        "match_pairs",
        lambda source, *_: pd.DataFrame() if len(source) == 1 else pd.DataFrame([{"bad": 1}]),
    )

    with pytest.warns(UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"):
        deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.empty
    assert deduped["memo"].tolist() == ["passport fee 2"]


def test_dedupe_sources_prefers_account_ids_for_exact_import_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi Alias",
                "ynab_account_id": "acc-bank",
                "date": "2026-02-25",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "delek",
                "description_raw": "DELEK",
                "memo": "should drop",
            },
            {
                "account_name": "Bank Leumi Alias",
                "ynab_account_id": "acc-bank",
                "date": "2026-02-25",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "delek",
                "description_raw": "DELEK",
                "memo": "should keep",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "account_id": "acc-bank",
                "account_name": "Bank Leumi",
                "import_id": "YNAB:-25000:2026-02-25:1",
            }
        ]
    )

    monkeypatch.setattr(
        build_proposed_transactions.pairing,
        "match_pairs",
        lambda source, *_: pd.DataFrame() if len(source) == 1 else pd.DataFrame([{"bad": 1}]),
    )

    with pytest.warns(UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"):
        deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.empty
    assert deduped["memo"].tolist() == ["should keep"]


def test_dedupe_sources_drops_exact_card_txn_id_matches_before_weak_pairing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Opher x9922",
                "date": "2026-03-09",
                "outflow_ils": 120.0,
                "inflow_ils": 0.0,
                "fingerprint": "merchant a",
                "description_raw": "MERCHANT A",
                "card_txn_id": "CARD:V1:1234567890abcdef12345678",
                "memo": "drop me",
            },
            {
                "account_name": "Opher x9922",
                "date": "2026-03-09",
                "outflow_ils": 120.0,
                "inflow_ils": 0.0,
                "fingerprint": "merchant b",
                "description_raw": "MERCHANT B",
                "card_txn_id": "",
                "memo": "keep me",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "account_name": "Opher x9922",
                "import_id": "CARD:V1:1234567890abcdef12345678",
            }
        ]
    )

    monkeypatch.setattr(
        build_proposed_transactions.pairing,
        "match_pairs",
        lambda source, *_: pd.DataFrame() if len(source) == 1 else pd.DataFrame([{"bad": 1}]),
    )

    with pytest.warns(UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"):
        deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.empty
    assert deduped["memo"].tolist() == ["keep me"]


def test_dedupe_sources_retains_lineage_conflict_after_exact_import_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2026-03-12",
                "outflow_ils": 7.0,
                "inflow_ils": 0.0,
                "fingerprint": "dabbah",
                "bank_txn_id": "BANK:V1:111111111111111111111111",
                "memo": "drop me",
            },
            {
                "account_name": "Bank Leumi",
                "date": "2026-03-12",
                "outflow_ils": 7.0,
                "inflow_ils": 0.0,
                "fingerprint": "ikea",
                "bank_txn_id": "BANK:V1:222222222222222222222222",
                "memo": "keep me",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "import_id": "BANK:V1:111111111111111111111111",
            }
        ]
    )
    pairs = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2026-03-12",
                "outflow_ils": 7.0,
                "inflow_ils": 0.0,
                "ynab_import_id": "BANK:V1:111111111111111111111111",
                "ynab_fingerprint": "dabbah",
                "ambiguous_key": False,
            }
        ]
    )

    monkeypatch.setattr(build_proposed_transactions.pairing, "match_pairs", lambda *_: pairs)

    with pytest.warns(UserWarning) as record:
        deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    messages = [str(item.message) for item in record]
    assert any("exact import_id" in message for message in messages)
    assert any("lineage conflict" in message for message in messages)
    assert matched_pairs.equals(pairs)
    assert deduped["memo"].tolist() == ["keep me"]


def test_dedupe_sources_drops_fingerprint_conflict_without_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_df = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2026-03-12",
                "outflow_ils": 7.0,
                "inflow_ils": 0.0,
                "fingerprint": "ikea",
                "memo": "keep me",
            }
        ]
    )
    ynab_df = pd.DataFrame([{"account_name": "Bank Leumi", "import_id": ""}])
    pairs = pd.DataFrame(
        [
            {
                "account_name": "Bank Leumi",
                "date": "2026-03-12",
                "outflow_ils": 7.0,
                "inflow_ils": 0.0,
                "ynab_import_id": "",
                "ynab_fingerprint": "dabbah",
                "ambiguous_key": False,
            }
        ]
    )

    monkeypatch.setattr(build_proposed_transactions.pairing, "match_pairs", lambda *_: pairs)

    deduped, matched_pairs = build_proposed_transactions._dedupe_sources(source_df, ynab_df)

    assert matched_pairs.equals(pairs)
    assert deduped.empty


def test_dedupe_source_overlaps_matches_immediate_debit_on_secondary_date() -> None:
    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Bank Leumi",
                "date": "2025-12-28",
                "secondary_date": "2026-01-01",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "fingerprint": "lime",
                "card_suffix": "0849",
                "memo": "bank row",
            },
            {
                "source": "card",
                "account_name": "Bank Leumi",
                "date": "2025-12-30",
                "secondary_date": "2026-01-01",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "fingerprint": "lime",
                "card_suffix": "0849",
                "max_txn_type": "חיוב עסקות מיידי",
                "memo": "card row",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert deduped["memo"].tolist() == ["bank row"]


def test_apply_default_selections_defaults_unmatched_rows() -> None:
    tx = pd.DataFrame(
        [
            {
                "fingerprint": "local cafe",
                "payee_options": "",
                "payee_selected": "",
                "category_options": "",
                "category_selected": "",
                "match_status": "none",
            }
        ]
    )

    actual = build_proposed_transactions.proposed_defaults.apply_default_selections(
        tx, only_unreviewed=False
    )

    assert actual.loc[0, "payee_selected"] == "local cafe"
    assert actual.loc[0, "payee_options"] == "local cafe"
    assert actual.loc[0, "category_selected"] == "Uncategorized"
    assert actual.loc[0, "category_options"] == "Uncategorized"
    assert actual.loc[0, "match_status"] == "none"


def test_apply_default_selections_preserves_existing_choices() -> None:
    tx = pd.DataFrame(
        [
            {
                "fingerprint": "local cafe",
                "payee_options": "Cafe A; Cafe B",
                "payee_selected": "",
                "category_options": "Eating Out",
                "category_selected": "",
                "match_status": "ambiguous",
            }
        ]
    )

    actual = build_proposed_transactions.proposed_defaults.apply_default_selections(
        tx, only_unreviewed=False
    )

    assert actual.loc[0, "payee_selected"] == ""
    assert actual.loc[0, "payee_options"] == "Cafe A; Cafe B"
    assert actual.loc[0, "category_selected"] == ""
    assert actual.loc[0, "category_options"] == "Eating Out"
