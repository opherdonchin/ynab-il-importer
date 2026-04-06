import importlib.util
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import ynab_il_importer.review_app.model as review_model
from ynab_il_importer.artifacts.transaction_io import write_flat_transaction_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_proposed_transactions.py"
SPEC = importlib.util.spec_from_file_location("build_proposed_transactions", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
build_proposed_transactions = importlib.util.module_from_spec(SPEC)
sys.modules["build_proposed_transactions"] = build_proposed_transactions
SPEC.loader.exec_module(build_proposed_transactions)


def _write_payee_map(path: Path) -> None:
    rows = [
        {
            "rule_id": "coffee_1",
            "is_active": True,
            "priority": 0,
            "txn_kind": "",
            "fingerprint": "coffee shop",
            "description_clean_norm": "",
            "account_name": "",
            "source": "",
            "direction": "",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Coffee Shop",
            "category_target": "Eating Out",
            "notes": "",
            "card_suffix": "",
        }
    ]
    pd.DataFrame(rows, columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS).to_csv(
        path, index=False, encoding="utf-8-sig"
    )


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


def test_load_csvs_prefers_sidecar_parquet(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    flat_df = pl.DataFrame(
        {
            "source": ["bank"],
            "account_name": ["Family Leumi"],
            "source_account": ["Family Leumi"],
            "date": ["2026-03-01"],
            "txn_kind": ["expense"],
            "merchant_raw": ["Mega Pet"],
            "description_clean": ["Mega Pet"],
            "description_raw": ["Mega Pet Pet Food"],
            "description_clean_norm": ["mega pet"],
            "fingerprint": ["mega-pet-parquet"],
            "outflow_ils": [90.0],
            "inflow_ils": [0.0],
            "bank_txn_id": ["BANK:1"],
        }
    )
    write_flat_transaction_artifacts(
        flat_df,
        csv_path,
        artifact_kind="normalized_source_transaction",
        source_system="bank",
    )
    csv_path.write_text(
        "fingerprint,outflow_ils,inflow_ils\nmega-pet-csv,90.0,0.0\n",
        encoding="utf-8",
    )

    loaded = build_proposed_transactions._load_csvs([csv_path])

    assert loaded.loc[0, "fingerprint"] == "mega-pet-parquet"


def test_load_canonical_transaction_input_requires_sidecar_parquet(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "family_ynab_api_norm.csv"
    csv_path.write_text(
        "source,account_name,date,payee_raw,category_raw,fingerprint,outflow_ils,inflow_ils\n"
        "ynab,Bank Leumi,2026-03-28,Tsomet Sfarim,Split,tsomet sfarim,205.12,0.0\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="Canonical parquet sidecar required for normalized transaction input"
    ):
        build_proposed_transactions._load_canonical_transaction_input(csv_path)


def test_build_options_from_applied_uses_candidate_rule_ids() -> None:
    rules = build_proposed_transactions.rules_mod.normalize_payee_map_rules(
        pd.DataFrame(
            [
                {
                    "rule_id": "r1",
                    "is_active": True,
                    "priority": 0,
                    "txn_kind": "",
                    "fingerprint": "coffee shop",
                    "description_clean_norm": "",
                    "account_name": "",
                    "source": "",
                    "direction": "",
                    "currency": "",
                    "amount_bucket": "",
                    "payee_canonical": "Coffee Shop",
                    "category_target": "Eating Out",
                    "notes": "",
                    "card_suffix": "",
                },
                {
                    "rule_id": "r2",
                    "is_active": True,
                    "priority": 0,
                    "txn_kind": "",
                    "fingerprint": "coffee shop",
                    "description_clean_norm": "",
                    "account_name": "",
                    "source": "",
                    "direction": "",
                    "currency": "",
                    "amount_bucket": "",
                    "payee_canonical": "Cafe Nero",
                    "category_target": "Eating Out",
                    "notes": "",
                    "card_suffix": "",
                },
            ]
        )
    )
    applied = pd.DataFrame(
        [{"match_candidate_rule_ids": "r2;r1"}, {"match_candidate_rule_ids": ""}]
    )

    options = build_proposed_transactions._build_options_from_applied(applied, rules)

    assert options.loc[0, "payee_options"] == "Cafe Nero; Coffee Shop"
    assert options.loc[0, "category_options"] == "Eating Out"
    assert options.loc[1, "payee_options"] == ""


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

    with pytest.warns(
        UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"
    ):
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

    with pytest.warns(
        UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"
    ):
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

    with pytest.warns(
        UserWarning, match="Dropping 1 source rows matched to YNAB by exact import_id"
    ):
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


def test_build_review_rows_emits_institutional_statuses(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "groceries",
                "description_raw": "Groceries",
            },
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-02",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "fingerprint": "coffee shop",
                "description_raw": "Coffee Shop",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Groceries",
                "category_raw": "Food",
                "fingerprint": "groceries",
                "memo": "existing",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
            {
                "ynab_id": "ynab-target-only",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-03",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "payee_raw": "Manual Cash",
                "category_raw": "Cash",
                "fingerprint": "manual cash",
                "memo": "manual",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": False,
            },
        ]
    )

    review_rows, pairs = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    assert len(pairs) == 1
    assert set(review_rows["match_status"].tolist()) == {
        "matched_auto",
        "source_only",
        "target_only",
    }

    source_only = review_rows.loc[review_rows["match_status"] == "source_only"].iloc[0]
    assert source_only["target_payee_selected"] == "Coffee Shop"
    assert source_only["target_category_selected"] == "Eating Out"
    assert source_only["decision_action"] == "create_target"
    assert source_only["workflow_type"] == "institutional"

    matched = review_rows.loc[review_rows["match_status"] == "matched_auto"].iloc[0]
    assert bool(matched["reviewed"]) is False
    assert matched["target_payee_current"] == "Groceries"
    assert matched["decision_action"] == "keep_match"

    target_only = review_rows.loc[review_rows["match_status"] == "target_only"].iloc[0]
    assert target_only["target_payee_current"] == "Manual Cash"
    assert target_only["source"] == "ynab"
    assert target_only["decision_action"] == "No decision"
    assert bool(target_only["reviewed"]) is False


def test_build_review_rows_marks_cleared_exact_matches_as_settled(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "groceries",
                "description_raw": "Groceries",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Groceries",
                "category_raw": "Food",
                "fingerprint": "groceries",
                "memo": "existing",
                "import_id": "BANK:V1:abc",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            }
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    matched = review_rows.iloc[0]
    assert matched["match_status"] == "matched_cleared"
    assert matched["relation_kind"] == "matched_cleared_pair"
    assert bool(matched["reviewed"]) is True
    assert matched["source_source_system"] == "bank"
    assert matched["source_payee_current"] == "Groceries"
    assert matched["target_source_system"] == "ynab"
    assert matched["target_splits"] is None


def test_build_review_rows_normalizes_transfer_uncategorized_to_explicit_none(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "card payment",
                "description_raw": "Card payment",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Transfer : Cash",
                "category_raw": "Uncategorized",
                "fingerprint": "card payment",
                "memo": "existing",
                "import_id": "BANK:V1:abc",
                "matched_transaction_id": "",
                "cleared": "cleared",
                "approved": True,
            }
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    matched = review_rows.iloc[0]
    assert matched["target_category_current"] == "Uncategorized"
    assert matched["target_category_selected"] == review_model.NO_CATEGORY_REQUIRED
    assert matched["category_options"] == review_model.NO_CATEGORY_REQUIRED


def test_build_review_rows_auto_settles_target_only_transfer_counterparts(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "groceries",
                "description_raw": "Groceries",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Groceries",
                "category_raw": "Food",
                "fingerprint": "groceries",
                "memo": "existing",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
            {
                "ynab_id": "ynab-transfer",
                "account_id": "acc-2",
                "account_name": "Card",
                "date": "2025-01-03",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "payee_raw": "Transfer : Checking",
                "category_raw": "Uncategorized",
                "fingerprint": "transfer checking",
                "memo": "payment",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    target_only = review_rows.loc[
        review_rows["target_payee_selected"] == "Transfer : Checking"
    ].iloc[0]
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "ignore_row"
    assert bool(target_only["reviewed"]) is True
    assert target_only["relation_kind"] == "target_only_transfer_counterpart"
    assert target_only["target_category_selected"] == review_model.NO_CATEGORY_REQUIRED


def test_build_review_rows_auto_settles_reconciled_target_only_rows(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "groceries",
                "description_raw": "Groceries",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Groceries",
                "category_raw": "Food",
                "fingerprint": "groceries",
                "memo": "existing",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
            {
                "ynab_id": "ynab-old",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2024-12-30",
                "outflow_ils": 15.0,
                "inflow_ils": 0.0,
                "payee_raw": "Manual Cash",
                "category_raw": "Cash",
                "fingerprint": "manual cash",
                "memo": "old reconciled",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "reconciled",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    target_only = review_rows.loc[review_rows["target_payee_selected"] == "Manual Cash"].iloc[0]
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "ignore_row"
    assert bool(target_only["reviewed"]) is True
    assert target_only["relation_kind"] == "target_only_cleared"


def test_build_review_rows_auto_settles_manual_target_only_rows(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "fingerprint": "groceries",
                "description_raw": "Groceries",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-match",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-01",
                "outflow_ils": 40.0,
                "inflow_ils": 0.0,
                "payee_raw": "Groceries",
                "category_raw": "Food",
                "fingerprint": "groceries",
                "memo": "existing",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
            {
                "ynab_id": "ynab-manual",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2024-12-31",
                "outflow_ils": 15.0,
                "inflow_ils": 0.0,
                "payee_raw": "Manual Cash",
                "category_raw": "Cash",
                "fingerprint": "manual cash",
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    target_only = review_rows.loc[review_rows["target_payee_selected"] == "Manual Cash"].iloc[0]
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "ignore_row"
    assert bool(target_only["reviewed"]) is True
    assert target_only["relation_kind"] == "target_only_manual"


def test_build_review_rows_emits_institutional_ambiguous_candidates(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            {
                "source": "bank",
                "account_name": "Checking",
                "date": "2025-01-02",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "fingerprint": "coffee shop",
                "description_raw": "Coffee Shop",
            }
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-a",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-02",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "payee_raw": "Cafe A",
                "category_raw": "Eating Out",
                "fingerprint": "coffee shop",
                "memo": "a",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": False,
            },
            {
                "ynab_id": "ynab-b",
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2025-01-02",
                "outflow_ils": 12.0,
                "inflow_ils": 0.0,
                "payee_raw": "Cafe B",
                "category_raw": "Eating Out",
                "fingerprint": "coffee shop",
                "memo": "b",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": False,
            },
        ]
    )

    review_rows, pairs = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    assert len(pairs) == 2
    ambiguous = review_rows.loc[review_rows["match_status"] == "ambiguous"].copy()
    assert len(ambiguous) == 2
    assert set(ambiguous["relation_kind"].tolist()) == {"ambiguous_candidate"}
    assert len(set(ambiguous["source_row_id"].tolist())) == 1
    assert set(ambiguous["target_payee_current"].tolist()) == {"Cafe A", "Cafe B"}


def test_institutional_candidate_pairs_prefer_exact_lineage_and_clear_false_ambiguity() -> None:
    prepared_source = pd.DataFrame(
        [
            {
                "source_row_id": "src-1",
                "source_lineage_id": "BANK:V1:a",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "source_row_id": "src-2",
                "source_lineage_id": "BANK:V1:b",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
        ]
    )
    prepared_target = pd.DataFrame(
        [
            {
                "target_row_id": "tgt-1",
                "ynab_import_id": "BANK:V1:a",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-2",
                "ynab_import_id": "BANK:V1:b",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-3",
                "ynab_import_id": "CARD:V1:c",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-4",
                "ynab_import_id": "CARD:V1:d",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
        ]
    )

    pairs = build_proposed_transactions._institutional_candidate_pairs(
        prepared_source,
        prepared_target,
    )

    assert set(pairs["source_row_id"].tolist()) == {"src-1", "src-2"}
    assert set(pairs["target_row_id"].tolist()) == {"tgt-1", "tgt-2"}
    assert pairs["ambiguous_key"].tolist() == [False, False]


def test_institutional_candidate_pairs_prefer_exact_memo_lineage_markers() -> None:
    prepared_source = pd.DataFrame(
        [
            {
                "source_row_id": "src-1",
                "source_lineage_id": "BANK:V1:111111111111111111111111",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            }
        ]
    )
    prepared_target = pd.DataFrame(
        [
            {
                "target_row_id": "tgt-bank",
                "ynab_import_id": "YNAB:-58000:2026-03-03:1",
                "target_memo": "[ynab-il bank_txn_id=BANK:V1:111111111111111111111111]",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            },
            {
                "target_row_id": "tgt-card",
                "ynab_import_id": "CARD:V1:c",
                "target_memo": "ROASTERS",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            },
        ]
    )

    pairs = build_proposed_transactions._institutional_candidate_pairs(
        prepared_source,
        prepared_target,
    )

    assert pairs["target_row_id"].tolist() == ["tgt-bank"]
    assert pairs["ambiguous_key"].tolist() == [False]


def test_institutional_candidate_pairs_prefer_exact_import_over_memo_lineage_marker() -> None:
    prepared_source = pd.DataFrame(
        [
            {
                "source_row_id": "src-1",
                "source_lineage_id": "CARD:V1:111111111111111111111111",
                "account_key": "Opher x9922",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            }
        ]
    )
    prepared_target = pd.DataFrame(
        [
            {
                "target_row_id": "tgt-memo",
                "ynab_import_id": "YNAB:-58000:2026-03-03:1",
                "target_memo": "[ynab-il card_txn_id=CARD:V1:111111111111111111111111]",
                "account_key": "Opher x9922",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            },
            {
                "target_row_id": "tgt-import",
                "ynab_import_id": "CARD:V1:111111111111111111111111",
                "target_memo": "FACEBOOK",
                "account_key": "Opher x9922",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -58.0,
            },
        ]
    )

    pairs = build_proposed_transactions._institutional_candidate_pairs(
        prepared_source,
        prepared_target,
    )

    assert pairs["target_row_id"].tolist() == ["tgt-import"]
    assert pairs["ambiguous_key"].tolist() == [False]
