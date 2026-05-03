import importlib.util
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import ynab_il_importer.review_app.model as review_model
from ynab_il_importer.artifacts.transaction_io import write_flat_transaction_artifacts

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_proposed_transactions.py"
SPEC = importlib.util.spec_from_file_location(
    "build_proposed_transactions", SCRIPT_PATH
)
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
    pd.DataFrame(
        rows, columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS
    ).to_csv(path, index=False, encoding="utf-8-sig")


def _write_fingerprint_map(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "rule_id": "coffee",
                "is_active": True,
                "priority": 0,
                "pattern": "בית קפה",
                "canonical_text": "coffee shop",
                "notes": "",
            }
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")


def _canonical_source_polars(df: pd.DataFrame) -> pl.DataFrame:
    return _canonical_transaction_polars(
        df,
        source_system="bank",
        artifact_kind="normalized_source_transaction",
    )


def _canonical_target_polars(df: pd.DataFrame) -> pl.DataFrame:
    return _canonical_transaction_polars(
        df,
        source_system="ynab",
        artifact_kind="ynab_transaction",
    )


def _canonical_transaction_polars(
    df: pd.DataFrame,
    *,
    source_system: str,
    artifact_kind: str,
) -> pl.DataFrame:
    out = df.copy()
    for field in build_proposed_transactions.TRANSACTION_SCHEMA:
        if field.name in out.columns:
            continue
        if pa.types.is_boolean(field.type):
            out[field.name] = False
        elif pa.types.is_floating(field.type):
            out[field.name] = 0.0
        elif pa.types.is_list(field.type):
            out[field.name] = None
        else:
            out[field.name] = ""
    out["artifact_kind"] = out["artifact_kind"].replace("", artifact_kind)
    out["artifact_version"] = out["artifact_version"].replace("", "transaction_v1")
    out["source_system"] = out["source_system"].replace("", source_system)
    return pl.from_pandas(out)


def test_dedupe_source_overlaps_drops_matching_card_rows() -> None:
    source_df = pl.DataFrame(
        [
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "description_raw": "מצפור פארק החורשות0849- בכרטיס המסתיים",
                "memo": "bank row",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "description_raw": "מצפור פארק החורשות",
                "memo": "card row",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 18.0,
                "inflow_ils": 0.0,
                "fingerprint": "מצפור פארק החורשות",
                "description_raw": "מצפור פארק החורשות",
                "memo": "other card row",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert len(deduped) == 2
    assert deduped["memo"].to_list() == ["bank row", "other card row"]


def test_dedupe_duplicate_source_transaction_ids_keeps_first_row() -> None:
    source_df = pl.DataFrame(
        [
            {
                "transaction_id": "CARD:V1:duplicate",
                "source_system": "card",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-04-03",
                "outflow_ils": 60.0,
                "inflow_ils": 0.0,
                "fingerprint": "charge a",
                "description_raw": "April charge current",
                "memo": "current row",
            },
            {
                "transaction_id": "CARD:V1:duplicate",
                "source_system": "card",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-04-03",
                "outflow_ils": 60.0,
                "inflow_ils": 0.0,
                "fingerprint": "charge a",
                "description_raw": "April charge previous",
                "memo": "previous row",
            },
            {
                "transaction_id": "CARD:V1:unique",
                "source_system": "card",
                "account_name": "Liya X7195",
                "source_account": "x7195",
                "date": "2026-04-04",
                "outflow_ils": 70.0,
                "inflow_ils": 0.0,
                "fingerprint": "charge b",
                "description_raw": "Other charge",
                "memo": "unique row",
            },
        ]
    )

    with pytest.warns(
        UserWarning,
        match="Dropping 1 duplicate source rows matched on canonical transaction_id",
    ):
        deduped = build_proposed_transactions._dedupe_duplicate_source_transaction_ids(
            source_df
        )

    assert deduped["memo"].to_list() == ["current row", "unique row"]


def test_pairs_artifact_frame_drops_object_transaction_columns() -> None:
    pairs = pl.DataFrame(
        {
            "fingerprint": ["coffee shop"],
            "ynab_payee_raw": ["Coffee Shop"],
            "source_transaction": [{"transaction_id": "src-1"}],
            "target_transaction": [{"transaction_id": "tgt-1"}],
        },
        schema={
            "fingerprint": pl.String,
            "ynab_payee_raw": pl.String,
            "source_transaction": pl.Object,
            "target_transaction": pl.Object,
        },
    )

    artifact = build_proposed_transactions._pairs_artifact_frame(pairs)

    assert artifact.columns == ["fingerprint", "ynab_payee_raw"]


def test_dedupe_source_overlaps_preserves_extra_bank_rows() -> None:
    source_df = pl.DataFrame(
        [
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "description_raw": "merchant0849- בכרטיס המסתיים",
                "memo": "bank 1",
            },
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "description_raw": "merchant0849- בכרטיס המסתיים",
                "memo": "bank 2",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "secondary_date": "",
                "date": "2025-12-12",
                "outflow_ils": 25.0,
                "inflow_ils": 0.0,
                "fingerprint": "fp",
                "description_raw": "merchant",
                "memo": "card 1",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert deduped["memo"].to_list() == ["bank 1", "bank 2"]


def test_load_source_inputs_requires_parquet_inputs(tmp_path: Path) -> None:
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

    with pytest.raises(ValueError, match="Canonical transaction input must be parquet"):
        build_proposed_transactions._load_source_inputs([csv_path])


def test_build_target_suggestions_canonicalizes_payee_map_fingerprints(
    tmp_path: Path,
) -> None:
    payee_map_path = tmp_path / "payee_map.csv"
    fingerprint_map_path = tmp_path / "fingerprint_map.csv"
    pd.DataFrame(
        [
            {
                "rule_id": "coffee_1",
                "is_active": True,
                "priority": 0,
                "txn_kind": "",
                "fingerprint": "בית קפה",
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
        ],
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(payee_map_path, index=False, encoding="utf-8-sig")
    _write_fingerprint_map(fingerprint_map_path)

    transactions = pl.DataFrame(
        [
            {
                "transaction_id": "t1",
                "fingerprint": "coffee shop",
                "source": "card",
                "account_name": "Account",
                "date": "2026-04-01",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "description_raw": "בית קפה",
                "memo": "בית קפה",
            }
        ]
    )

    out = build_proposed_transactions.build_target_suggestions(
        transactions,
        map_path=payee_map_path,
        fingerprint_map_path=fingerprint_map_path,
    )

    assert out[0, "payee_selected"] == "Coffee Shop"
    assert out[0, "category_selected"] == "Eating Out"


def test_build_options_from_applied_uses_candidate_rule_ids() -> None:
    rules = build_proposed_transactions.rules_mod.normalize_payee_map_rules(
        pl.DataFrame(
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
    ).to_pandas()
    applied = pd.DataFrame(
        [{"match_candidate_rule_ids": "r2;r1"}, {"match_candidate_rule_ids": ""}]
    )

    options = build_proposed_transactions._build_options_from_applied(applied, rules)

    assert options.loc[0, "payee_options"] == "Cafe Nero; Coffee Shop"
    assert options.loc[0, "category_options"] == "Eating Out"
    assert options.loc[1, "payee_options"] == ""


def test_dedupe_source_overlaps_matches_immediate_debit_on_secondary_date() -> None:
    source_df = pl.DataFrame(
        [
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2025-12-28",
                "secondary_date": "2026-01-01",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "fingerprint": "lime",
                "description_raw": "LIME*RIDE0849- בכרטיס המסתיים",
                "memo": "bank row",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "date": "2025-12-30",
                "secondary_date": "2026-01-01",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "fingerprint": "lime",
                "description_raw": "LIME*RIDE",
                "max_txn_type": "חיוב עסקות מיידי",
                "memo": "card row",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 1 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert deduped["memo"].to_list() == ["bank row"]


def test_dedupe_source_overlaps_collapses_four_way_linked_duplicates() -> None:
    source_df = pl.DataFrame(
        [
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-03-03",
                "secondary_date": "",
                "outflow_ils": 200.0,
                "inflow_ils": 0.0,
                "fingerprint": "bit",
                "description_raw": "BIT- ב0849- בכרטיס המסתיים ב14:29  03/03/26",
                "memo": "bank 1",
            },
            {
                "source_system": "bank",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-03-03",
                "secondary_date": "",
                "outflow_ils": 200.0,
                "inflow_ils": 0.0,
                "fingerprint": "bit",
                "description_raw": "BIT- ב0849- בכרטיס המסתיים ב14:30  03/03/26",
                "memo": "bank 2",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "date": "2026-03-03",
                "secondary_date": "",
                "outflow_ils": 200.0,
                "inflow_ils": 0.0,
                "fingerprint": "bit",
                "description_raw": "BIT | למי: נעה גן צבי",
                "memo": "card 1",
            },
            {
                "source_system": "card",
                "account_name": "Bank Leumi",
                "source_account": "x0849",
                "date": "2026-03-03",
                "secondary_date": "",
                "outflow_ils": 200.0,
                "inflow_ils": 0.0,
                "fingerprint": "bit",
                "description_raw": "BIT | למי: נבו פולק",
                "memo": "card 2",
            },
        ]
    )

    with pytest.warns(UserWarning, match="Dropping 2 bank/card overlap rows"):
        deduped = build_proposed_transactions._dedupe_source_overlaps(source_df)

    assert deduped["memo"].to_list() == ["bank 1", "bank 2"]


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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert len(pairs) == 1
    assert set(review_rows["match_status"].to_list()) == {
        "matched_auto",
        "source_only",
        "target_only",
    }

    source_only = review_rows.filter(pl.col("match_status") == "source_only").row(
        0, named=True
    )
    assert source_only["target_payee_selected"] == "Coffee Shop"
    assert source_only["target_category_selected"] == "Eating Out"
    assert source_only["decision_action"] == "create_target"
    assert source_only["workflow_type"] == "institutional"

    matched = review_rows.filter(pl.col("match_status") == "matched_auto").row(
        0, named=True
    )
    assert bool(matched["reviewed"]) is False
    assert matched["target_payee_current"] == "Groceries"
    assert matched["decision_action"] == "keep_match"

    target_only = review_rows.filter(pl.col("match_status") == "target_only").row(
        0, named=True
    )
    assert target_only["target_payee_current"] == "Manual Cash"
    assert target_only["source"] == "ynab"
    assert target_only["decision_action"] == "No decision"
    assert bool(target_only["reviewed"]) is False


def test_build_review_rows_skips_reconciled_exact_matches_by_default(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert review_rows.is_empty()


def test_build_review_rows_can_include_reconciled_exact_matches(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
        include_reconciled_ynab=True,
    )

    matched = review_rows.row(0, named=True)
    assert matched["match_status"] == "matched_cleared"
    assert matched["relation_kind"] == "matched_cleared_pair"
    assert bool(matched["reviewed"]) is False
    assert matched["source_source_system"] == "bank"
    assert matched["source_payee_current"] == "Groceries"
    assert matched["target_source_system"] == "ynab"
    assert matched["target_splits"] is None


def test_build_review_rows_preserves_transfer_uncategorized_without_budget_context(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
        include_reconciled_ynab=True,
    )

    matched = review_rows.row(0, named=True)
    assert matched["target_category_current"] == "Uncategorized"
    assert matched["target_category_selected"] == "Uncategorized"
    assert matched["category_options"] == "Uncategorized"


def test_build_review_rows_leaves_target_only_transfer_counterparts_for_explicit_decision(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    target_only = review_rows.filter(
        pl.col("target_payee_selected") == "Transfer : Checking"
    ).row(0, named=True)
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "No decision"
    assert bool(target_only["reviewed"]) is False
    assert target_only["relation_kind"] == "target_only_transfer_counterpart"
    assert target_only["target_category_selected"] == "Uncategorized"


def test_build_review_rows_hides_internal_in_scope_transfer_counterparts(
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
                "ynab_id": "ynab-bank-transfer",
                "account_id": "acc-bank",
                "account_name": "Bank Leumi",
                "date": "2025-01-03",
                "outflow_ils": 20.0,
                "inflow_ils": 0.0,
                "payee_raw": "Transfer : Opher x9922",
                "category_raw": "Uncategorized",
                "fingerprint": "transfer opher x9922",
                "memo": "card payment",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
            {
                "ynab_id": "ynab-card-transfer",
                "account_id": "acc-card",
                "account_name": "Opher x9922",
                "date": "2025-01-03",
                "outflow_ils": 0.0,
                "inflow_ils": 20.0,
                "payee_raw": "Transfer : Bank Leumi",
                "category_raw": "Uncategorized",
                "fingerprint": "transfer bank leumi",
                "memo": "card payment",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
        allowed_target_accounts=["Checking", "Bank Leumi", "Opher x9922"],
    )

    transfer_rows = review_rows.filter(
        pl.col("target_payee_current").is_in(
            ["Transfer : Opher x9922", "Transfer : Bank Leumi"]
        )
    )
    assert transfer_rows.is_empty()


def test_build_review_rows_applies_payee_map_to_target_only_transfer_rows(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
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
        },
        {
            "rule_id": "transfer_bank_leumi_us_money",
            "is_active": True,
            "priority": 10,
            "txn_kind": "",
            "fingerprint": "transfer bank leumi",
            "description_clean_norm": "",
            "account_name": "US Money",
            "source": "ynab",
            "direction": "outflow",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Transfer : Bank Leumi",
            "category_target": "Ready to Assign",
            "notes": "manual target_only transfer default",
            "card_suffix": "",
        },
    ]
    pd.DataFrame(
        rows,
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

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
                "ynab_id": "ynab-transfer-us",
                "account_id": "acc-us",
                "account_name": "US Money",
                "date": "2025-01-03",
                "outflow_ils": 100.0,
                "inflow_ils": 0.0,
                "payee_raw": "Transfer : Bank Leumi",
                "category_raw": "Uncategorized",
                "fingerprint": "transfer bank leumi",
                "memo": "family transfer",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            }
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    target_only = review_rows.filter(
        (pl.col("target_account") == "US Money")
        & (pl.col("target_payee_current") == "Transfer : Bank Leumi")
    ).row(0, named=True)

    assert target_only["match_status"] == "target_only"
    assert target_only["target_payee_selected"] == "Transfer : Bank Leumi"
    assert target_only["target_category_selected"] == "Ready to Assign"
    assert "Ready to Assign" in str(target_only["category_options"])


def test_build_review_rows_applies_card_suffix_transfer_rule_to_leumi_visa_source_only_row(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    rows = [
        {
            "rule_id": "leumi_visa_7195",
            "is_active": True,
            "priority": 0,
            "txn_kind": "",
            "fingerprint": "לאומי ויזה",
            "description_clean_norm": "",
            "account_name": "",
            "source": "bank",
            "direction": "outflow",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Transfer : Liya X7195",
            "category_target": "None",
            "notes": "",
            "card_suffix": "7195",
        },
        {
            "rule_id": "leumi_visa_5898",
            "is_active": True,
            "priority": 0,
            "txn_kind": "",
            "fingerprint": "לאומי ויזה",
            "description_clean_norm": "",
            "account_name": "",
            "source": "bank",
            "direction": "outflow",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Transfer : Opher X5898",
            "category_target": "None",
            "notes": "",
            "card_suffix": "5898",
        },
    ]
    pd.DataFrame(
        rows,
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    source_df = pd.DataFrame(
        [
            {
                "transaction_id": "BANK:V1:e20b1e2745f03414b076c305",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-04-10",
                "secondary_date": "2026-04-10",
                "outflow_ils": 1251.08,
                "inflow_ils": 0.0,
                "payee_raw": "לאומי ויזה",
                "fingerprint": "לאומי ויזה",
                "description_raw": "לאומי ויזה",
                "ref": "0425898",
            },
            {
                "transaction_id": "BANK:V1:match",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-04-09",
                "secondary_date": "2026-04-09",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "payee_raw": "Unrelated",
                "fingerprint": "unrelated",
                "description_raw": "Unrelated",
                "ref": "0000001",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-unrelated",
                "account_id": "acc-1",
                "account_name": "Bank Leumi",
                "date": "2026-04-09",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "payee_raw": "Unrelated",
                "category_raw": "Misc",
                "fingerprint": "unrelated",
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            }
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    row = review_rows.filter(pl.col("source_ref") == "0425898").row(0, named=True)
    assert row["match_status"] == "source_only"
    assert row["target_payee_selected"] == "Transfer : Opher X5898"
    assert "Transfer : Opher X5898" in str(row["payee_options"])


def test_build_review_rows_applies_description_based_transfer_rule_to_leumi_gold_inflow(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    rows = [
        {
            "rule_id": "leumi_gold_transfer_us_money",
            "is_active": True,
            "priority": 10,
            "txn_kind": "",
            "fingerprint": "",
            "description_clean_norm": "העברת זהב",
            "account_name": "",
            "source": "bank",
            "direction": "inflow",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Transfer : US Money",
            "category_target": "Ready to Assign",
            "notes": "",
            "card_suffix": "",
        },
    ]
    pd.DataFrame(
        rows,
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    source_df = pd.DataFrame(
        [
            {
                "transaction_id": "BANK:V1:gold-transfer",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-04-12",
                "secondary_date": "2026-04-12",
                "outflow_ils": 0.0,
                "inflow_ils": 27187.52,
                "payee_raw": "העברת זהב",
                "fingerprint": "זהב",
                "description_raw": 'OPHER DONCHIN,P1629664,BEZALEL 8  6 BEE :"העברת זהב העברה מאת',
                "description_clean": "העברת זהב",
                "merchant_raw": "העברת זהב",
                "ref": "3041322",
                "source_system": "bank",
            },
            {
                "transaction_id": "BANK:V1:match",
                "account_name": "Bank Leumi",
                "source_account": "67833011333622",
                "date": "2026-04-09",
                "secondary_date": "2026-04-09",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "payee_raw": "Unrelated",
                "fingerprint": "unrelated",
                "description_raw": "Unrelated",
                "description_clean": "Unrelated",
                "merchant_raw": "Unrelated",
                "ref": "0000001",
                "source_system": "bank",
            },
        ]
    )
    ynab_df = pd.DataFrame(
        [
            {
                "ynab_id": "ynab-unrelated",
                "account_id": "acc-1",
                "account_name": "Bank Leumi",
                "date": "2026-04-09",
                "outflow_ils": 10.0,
                "inflow_ils": 0.0,
                "payee_raw": "Unrelated",
                "category_raw": "Misc",
                "fingerprint": "unrelated",
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            }
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    row = review_rows.filter(pl.col("source_ref") == "3041322").row(0, named=True)
    assert row["match_status"] == "source_only"
    assert row["target_payee_selected"] == "Transfer : US Money"
    assert row["target_category_selected"] == "Ready to Assign"
    assert "Transfer : US Money" in str(row["payee_options"])


def test_build_review_rows_skips_reconciled_target_only_rows_by_default(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert review_rows.filter(
        pl.col("target_payee_selected") == "Manual Cash"
    ).is_empty()


def test_build_review_rows_can_include_reconciled_target_only_rows(
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
        include_reconciled_ynab=True,
    )

    target_only = review_rows.filter(
        pl.col("target_payee_selected") == "Manual Cash"
    ).row(0, named=True)
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "No decision"
    assert bool(target_only["reviewed"]) is False
    assert target_only["relation_kind"] == "target_only_cleared"


def test_build_review_rows_skips_reconciled_ambiguous_matches_by_default(
    tmp_path: Path,
) -> None:
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
                "cleared": "reconciled",
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
                "cleared": "reconciled",
                "approved": False,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert review_rows.is_empty()


def test_build_review_rows_filters_target_scope_to_allowed_accounts(
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
                "ynab_id": "ynab-offscope",
                "account_id": "acc-2",
                "account_name": "US Money",
                "date": "2025-01-03",
                "outflow_ils": 100.0,
                "inflow_ils": 0.0,
                "payee_raw": "Transfer : Checking",
                "category_raw": "Uncategorized",
                "fingerprint": "transfer checking",
                "memo": "legacy transfer",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "uncleared",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
        allowed_target_accounts=["Checking"],
    )

    assert review_rows.height == 1
    row = review_rows.row(0, named=True)
    assert row["target_account"] == "Checking"
    assert row["match_status"] == "matched_auto"


def test_build_review_rows_skips_reconciled_transfer_counterparts_by_default(
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
                "account_id": "acc-1",
                "account_name": "Checking",
                "date": "2024-12-30",
                "outflow_ils": 15.0,
                "inflow_ils": 0.0,
                "payee_raw": "Transfer : Savings",
                "category_raw": "",
                "fingerprint": "transfer savings",
                "memo": "",
                "import_id": "",
                "matched_transaction_id": "",
                "cleared": "reconciled",
                "approved": True,
            },
        ]
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert review_rows.filter(
        pl.col("target_payee_current") == "Transfer : Savings"
    ).is_empty()


def test_build_review_rows_leaves_manual_target_only_rows_for_explicit_decision(tmp_path: Path) -> None:
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    target_only = review_rows.filter(
        pl.col("target_payee_selected") == "Manual Cash"
    ).row(0, named=True)
    assert target_only["match_status"] == "target_only"
    assert target_only["decision_action"] == "No decision"
    assert bool(target_only["reviewed"]) is False
    assert target_only["relation_kind"] == "target_only_manual"


def test_build_review_rows_emits_institutional_ambiguous_candidates(
    tmp_path: Path,
) -> None:
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
        _canonical_source_polars(source_df),
        _canonical_target_polars(ynab_df),
        map_path=map_path,
    )

    assert len(pairs) == 2
    ambiguous = review_rows.filter(pl.col("match_status") == "ambiguous")
    assert len(ambiguous) == 2
    assert set(ambiguous["relation_kind"].to_list()) == {"ambiguous_candidate"}
    assert len(set(ambiguous["source_row_id"].to_list())) == 1
    assert set(ambiguous["target_payee_current"].to_list()) == {"Cafe A", "Cafe B"}


def test_institutional_candidate_pairs_prefer_exact_lineage_and_clear_false_ambiguity() -> (
    None
):
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
                "target_memo": "",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-2",
                "ynab_import_id": "BANK:V1:b",
                "target_memo": "",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-3",
                "ynab_import_id": "CARD:V1:c",
                "target_memo": "",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
            {
                "target_row_id": "tgt-4",
                "ynab_import_id": "CARD:V1:d",
                "target_memo": "",
                "account_key": "Bank Leumi",
                "date_key": pd.Timestamp("2026-03-03").date(),
                "amount_key": -200.0,
            },
        ]
    )

    pairs = build_proposed_transactions._institutional_candidate_pairs(
        pl.from_pandas(prepared_source),
        pl.from_pandas(prepared_target),
    )

    assert set(pairs["source_row_id"].to_list()) == {"src-1", "src-2"}
    assert set(pairs["target_row_id"].to_list()) == {"tgt-1", "tgt-2"}
    assert pairs["ambiguous_key"].to_list() == [False, False]


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
        pl.from_pandas(prepared_source),
        pl.from_pandas(prepared_target),
    )

    assert pairs["target_row_id"].to_list() == ["tgt-bank"]
    assert pairs["ambiguous_key"].to_list() == [False]


def test_institutional_candidate_pairs_prefer_exact_import_over_memo_lineage_marker() -> (
    None
):
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
        pl.from_pandas(prepared_source),
        pl.from_pandas(prepared_target),
    )

    assert pairs["target_row_id"].to_list() == ["tgt-import"]
    assert pairs["ambiguous_key"].to_list() == [False]


def test_prepare_review_source_rows_uses_canonical_transaction_id_for_lineage() -> None:
    source_df = _canonical_source_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "BANK:V1:a",
                    "account_name": "Bank Leumi",
                    "source_account": "67833011333622",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "BIT",
                    "fingerprint": "bit",
                    "description_raw": "BIT 14:29",
                    "ref": "0031429",
                },
                {
                    "transaction_id": "BANK:V1:b",
                    "account_name": "Bank Leumi",
                    "source_account": "67833011333622",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "BIT",
                    "fingerprint": "bit",
                    "description_raw": "BIT 14:30",
                    "ref": "0031430",
                },
            ]
        )
    )
    target_df = _canonical_target_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "826669af-13cd-43c8-a554-1735494e5417",
                    "ynab_id": "826669af-13cd-43c8-a554-1735494e5417",
                    "import_id": "BANK:V1:a",
                    "account_name": "Bank Leumi",
                    "account_id": "acct-1",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Subject payment",
                    "category_raw": "University",
                    "memo": "BIT 14:29",
                    "fingerprint": "subject payment",
                },
                {
                    "transaction_id": "ebdf99f3-6779-4a9d-ac88-457ddd712591",
                    "ynab_id": "ebdf99f3-6779-4a9d-ac88-457ddd712591",
                    "import_id": "BANK:V1:b",
                    "account_name": "Bank Leumi",
                    "account_id": "acct-1",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Subject payment",
                    "category_raw": "University",
                    "memo": "BIT 14:30",
                    "fingerprint": "subject payment",
                },
                {
                    "transaction_id": "2468a7c8-e261-492d-9612-670c5cefaeba",
                    "ynab_id": "2468a7c8-e261-492d-9612-670c5cefaeba",
                    "import_id": "CARD:V1:c",
                    "account_name": "Bank Leumi",
                    "account_id": "acct-1",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Subject",
                    "category_raw": "University",
                    "memo": "BIT | recipient c",
                    "fingerprint": "subject",
                },
                {
                    "transaction_id": "bfeee590-3d3c-4c67-bc01-f967042c1fad",
                    "ynab_id": "bfeee590-3d3c-4c67-bc01-f967042c1fad",
                    "import_id": "YNAB:-200000:2026-03-03:2",
                    "account_name": "Bank Leumi",
                    "account_id": "acct-1",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Subject payment",
                    "category_raw": "University",
                    "memo": "BIT | recipient d",
                    "fingerprint": "subject payment",
                },
            ]
        )
    )

    prepared_source = build_proposed_transactions._prepare_review_source_rows(source_df)
    prepared_target = build_proposed_transactions._prepare_review_target_rows(target_df)
    pairs = build_proposed_transactions._institutional_candidate_pairs(
        prepared_source,
        prepared_target,
    )

    assert prepared_source["source_lineage_id"].to_list() == ["BANK:V1:a", "BANK:V1:b"]
    assert set(pairs["ynab_import_id"].to_list()) == {"BANK:V1:a", "BANK:V1:b"}
    assert pairs.height == 2
    assert pairs["ambiguous_key"].to_list() == [False, False]


def test_prepare_review_source_rows_tags_bank_debit_memo_with_card_suffix() -> None:
    source_df = _canonical_source_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "BANK:V1:bit-a",
                    "account_name": "Bank Leumi",
                    "source_account": "67833011333622",
                    "date": "2026-03-03",
                    "outflow_ils": 200.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "BIT",
                    "merchant_raw": "BIT",
                    "description_clean": "BIT",
                    "fingerprint": "bit",
                    "description_raw": "BIT- ב0849- בכרטיס המסתיים ב14:29  03/03/26",
                    "ref": "0031429",
                }
            ]
        )
    )

    prepared = build_proposed_transactions._prepare_review_source_rows(source_df)
    row = prepared.row(0, named=True)

    assert row["source_card_suffix"] == "0849"
    assert row["source_memo"] == "BIT [card x0849]"


def test_prepare_review_source_rows_marks_ynab_category_split_context() -> None:
    source_df = _canonical_transaction_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "split-1",
                    "ynab_id": "split-1",
                    "account_name": "Personal In Leumi",
                    "source_account": "Family Leumi",
                    "date": "2026-04-01",
                    "outflow_ils": 350.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Aikido Dojo",
                    "category_id": "cat-aikido",
                    "category_raw": "Aikido",
                    "memo": "April class",
                    "fingerprint": "aikido dojo",
                    "is_subtransaction": True,
                    "ref": "parent-1",
                }
            ]
        ),
        source_system="ynab_category",
        artifact_kind="normalized_source_transaction",
    )

    prepared = build_proposed_transactions._prepare_review_source_rows(source_df)
    row = prepared.row(0, named=True)

    assert row["source_context_kind"] == "ynab_split_category_match"
    assert row["source_context_category_id"] == "cat-aikido"
    assert row["source_context_category_name"] == "Aikido"
    assert row["source_context_matching_split_ids"] == "split-1"


def test_build_review_rows_preserves_ynab_category_source_context(tmp_path: Path) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = _canonical_transaction_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "family-parent-1",
                    "ynab_id": "family-parent-1",
                    "account_name": "Personal In Leumi",
                    "source_account": "Family Leumi",
                    "date": "2026-04-01",
                    "outflow_ils": 350.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Aikido Dojo",
                    "category_id": "cat-aikido",
                    "category_raw": "Aikido",
                    "memo": "April class",
                    "fingerprint": "aikido dojo",
                    "is_subtransaction": False,
                    "ref": "family-parent-1",
                }
            ]
        ),
        source_system="ynab_category",
        artifact_kind="normalized_source_transaction",
    )
    ynab_df = _canonical_target_polars(
        pd.DataFrame(
            [
                {
                    "ynab_id": "ynab-match",
                    "account_id": "acc-1",
                    "account_name": "Personal In Leumi",
                    "date": "2026-04-01",
                    "outflow_ils": 350.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Aikido Dojo",
                    "category_raw": "Aikido",
                    "fingerprint": "aikido dojo",
                    "memo": "",
                }
            ]
        )
    )

    review_rows, _ = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    row = review_rows.filter(pl.col("match_status") == "matched_auto").row(0, named=True)
    assert row["source_context_kind"] == "ynab_parent_category_match"
    assert row["source_context_category_id"] == "cat-aikido"
    assert row["source_context_category_name"] == "Aikido"


def test_build_review_rows_hides_reconciled_ynab_category_match_by_generated_import_id(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = _canonical_transaction_polars(
        pd.DataFrame(
            [
                {
                    "transaction_id": "family-parent-2",
                    "ynab_id": "family-parent-2",
                    "account_name": "Personal In Leumi",
                    "source_account": "Family Leumi",
                    "date": "2026-04-29",
                    "outflow_ils": 735.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Facebook",
                    "category_id": "cat-aikido",
                    "category_raw": "Aikido",
                    "memo": "Facebook",
                    "fingerprint": "facebook",
                    "import_id": "",
                    "is_subtransaction": False,
                    "ref": "family-parent-2",
                }
            ]
        ),
        source_system="ynab_category",
        artifact_kind="normalized_source_transaction",
    )
    ynab_df = _canonical_target_polars(
        pd.DataFrame(
            [
                {
                    "ynab_id": "ynab-match",
                    "account_id": "acc-1",
                    "account_name": "Personal In Leumi",
                    "date": "2026-05-01",
                    "outflow_ils": 735.0,
                    "inflow_ils": 0.0,
                    "payee_raw": "Facebook",
                    "category_raw": "Marketing",
                    "fingerprint": "facebook",
                    "memo": "",
                    "import_id": "YNAB:-735000:2026-04-29:1",
                    "cleared": "reconciled",
                }
            ]
        )
    )

    review_rows, pairs = build_proposed_transactions.build_review_rows(
        source_df,
        ynab_df,
        map_path=map_path,
    )

    assert pairs.height == 1
    assert pairs["ynab_import_id"].to_list() == ["YNAB:-735000:2026-04-29:1"]
    assert review_rows.is_empty()


def test_apply_review_target_suggestions_uses_current_pilates_bank_transfer_alias(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    pd.DataFrame(
        [
            {
                "rule_id": "pilates_leumi_alias",
                "is_active": True,
                "priority": 10,
                "txn_kind": "",
                "fingerprint": "pilates leumi",
                "description_clean_norm": "",
                "account_name": "",
                "source": "",
                "direction": "",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": "Transfer : Bank Leumi 225237",
                "category_target": "",
                "notes": "",
                "card_suffix": "",
            }
        ],
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    relations = pl.DataFrame(
        [
            {
                "transaction_id": "family-transfer-1",
                "source": "ynab",
                "account_name": "In Family",
                "date": "2026-04-10",
                "outflow_ils": 0.0,
                "inflow_ils": 5000.0,
                "memo": "Transfer : Pilates Leumi",
                "fingerprint": "pilates leumi",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "institutional",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": "source-row-1",
                "target_row_id": "",
                "source_account": "Family Leumi",
                "target_account": "In Family",
                "source_date": "2026-04-10",
                "target_date": "",
                "source_payee_current": "Transfer : Pilates Leumi",
                "target_payee_current": "",
                "source_category_current": "Pilates",
                "target_category_current": "",
                "source_memo": "Transfer : Pilates Leumi",
                "target_memo": "",
                "source_fingerprint": "pilates leumi",
                "target_fingerprint": "",
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": "",
                "source_ref": "family-transfer-1",
                "source_context_kind": "ynab_parent_category_match",
                "source_context_category_id": "cat-pilates",
                "source_context_category_name": "Pilates",
                "source_context_matching_split_ids": "",
                "source_payee_selected": "Transfer : Pilates Leumi",
                "source_category_selected": "Pilates",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": {"transaction_id": "family-transfer-1"},
                "target_transaction": None,
            }
        ]
    )

    review_rows = build_proposed_transactions._apply_review_target_suggestions(
        relations,
        map_path=map_path,
    )

    row = review_rows.row(0, named=True)
    assert row["target_payee_selected"] == "Transfer : Bank Leumi 225237"
    assert row["target_category_selected"] == ""


def test_apply_review_target_suggestions_uses_canonical_account_name_for_rules(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    pd.DataFrame(
        [
            {
                "rule_id": "facebook_aikido",
                "is_active": True,
                "priority": 10,
                "txn_kind": "",
                "fingerprint": "paypal facebook עסקת חו",
                "description_clean_norm": "",
                "account_name": "Opher x9922",
                "source": "card",
                "direction": "outflow",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": "Facebook",
                "category_target": "Aikido",
                "notes": "",
                "card_suffix": "",
            }
        ],
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    relations = pl.DataFrame(
        [
            {
                "transaction_id": "facebook-1",
                "source": "card",
                "account_name": "Opher x9922",
                "date": "2026-04-10",
                "outflow_ils": 735.0,
                "inflow_ils": 0.0,
                "memo": "PAYPAL *FACEBOOK",
                "fingerprint": "paypal facebook עסקת חו",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "institutional",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": "source-row-1",
                "target_row_id": "",
                "source_account": "x9922",
                "target_account": "",
                "source_date": "2026-04-10",
                "target_date": "",
                "source_payee_current": "PAYPAL *FACEBOOK",
                "target_payee_current": "",
                "source_category_current": "",
                "target_category_current": "",
                "source_memo": "PAYPAL *FACEBOOK",
                "target_memo": "",
                "source_fingerprint": "paypal facebook עסקת חו",
                "target_fingerprint": "",
                "source_bank_txn_id": "",
                "source_card_txn_id": "CARD:1",
                "source_card_suffix": "9922",
                "source_secondary_date": "",
                "source_ref": "",
                "source_context_kind": "",
                "source_context_category_id": "",
                "source_context_category_name": "",
                "source_context_matching_split_ids": "",
                "source_payee_selected": "",
                "source_category_selected": "",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": {"transaction_id": "facebook-1"},
                "target_transaction": None,
            }
        ]
    )

    review_rows = build_proposed_transactions._apply_review_target_suggestions(
        relations,
        map_path=map_path,
    )

    row = review_rows.row(0, named=True)
    assert row["target_payee_selected"] == "Facebook"
    assert row["target_category_selected"] == "Aikido"


def test_apply_review_target_suggestions_falls_back_to_source_payee_when_target_unknown(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    pd.DataFrame(
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    relations = pl.DataFrame(
        [
            {
                "transaction_id": "family-backlog-1",
                "source": "ynab",
                "account_name": "In Family",
                "date": "2024-07-09",
                "outflow_ils": 350.0,
                "inflow_ils": 0.0,
                "memo": "Boaz",
                "fingerprint": "boaz",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "institutional",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": "source-row-1",
                "target_row_id": "",
                "source_account": "In Family",
                "target_account": "In Family",
                "source_date": "2024-07-09",
                "target_date": "",
                "source_payee_current": "Boaz",
                "target_payee_current": "",
                "source_category_current": "Pilates",
                "target_category_current": "",
                "source_memo": "Boaz",
                "target_memo": "",
                "source_fingerprint": "boaz",
                "target_fingerprint": "",
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": "",
                "source_ref": "family-backlog-1",
                "source_context_kind": "ynab_parent_category_match",
                "source_context_category_id": "cat-pilates",
                "source_context_category_name": "Pilates",
                "source_context_matching_split_ids": "",
                "source_payee_selected": "Boaz",
                "source_category_selected": "Pilates",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": {"transaction_id": "family-backlog-1"},
                "target_transaction": None,
            }
        ]
    )

    review_rows = build_proposed_transactions._apply_review_target_suggestions(
        relations,
        map_path=map_path,
    )

    row = review_rows.row(0, named=True)
    assert row["target_payee_selected"] == "Boaz"
    assert row["target_category_selected"] == ""


def test_apply_review_target_suggestions_materializes_singleton_payee_option_only(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "payee_map.csv"
    pd.DataFrame(
        [
            {
                "rule_id": "adrian_1",
                "is_active": True,
                "priority": 0,
                "txn_kind": "",
                "fingerprint": "adrian sports trainer",
                "description_clean_norm": "",
                "account_name": "",
                "source": "",
                "direction": "",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": "Adrian Dado",
                "category_target": "Stuff I Forgot to Budget For",
                "notes": "",
                "card_suffix": "",
            },
            {
                "rule_id": "adrian_2",
                "is_active": True,
                "priority": 0,
                "txn_kind": "",
                "fingerprint": "adrian sports trainer",
                "description_clean_norm": "",
                "account_name": "",
                "source": "",
                "direction": "",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": "Adrian Dado",
                "category_target": "Adrian 570 NIS 28th",
                "notes": "",
                "card_suffix": "",
            },
        ],
        columns=build_proposed_transactions.rules_mod.PAYEE_MAP_COLUMNS,
    ).to_csv(map_path, index=False, encoding="utf-8-sig")

    relations = pl.DataFrame(
        [
            {
                "transaction_id": "family-backlog-2",
                "source": "ynab",
                "account_name": "In Family",
                "date": "2025-08-30",
                "outflow_ils": 0.0,
                "inflow_ils": 300.0,
                "memo": "Adrian Sports Trainer",
                "fingerprint": "adrian sports trainer",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "institutional",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": "source-row-2",
                "target_row_id": "",
                "source_account": "In Family",
                "target_account": "In Family",
                "source_date": "2025-08-30",
                "target_date": "",
                "source_payee_current": "Adrian Sports Trainer",
                "target_payee_current": "",
                "source_category_current": "Pilates",
                "target_category_current": "",
                "source_memo": "Adrian Sports Trainer",
                "target_memo": "",
                "source_fingerprint": "adrian sports trainer",
                "target_fingerprint": "",
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": "",
                "source_ref": "family-backlog-2",
                "source_context_kind": "ynab_split_category_match",
                "source_context_category_id": "cat-pilates",
                "source_context_category_name": "Pilates",
                "source_context_matching_split_ids": "",
                "source_payee_selected": "Adrian Sports Trainer",
                "source_category_selected": "Pilates",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": {"transaction_id": "family-backlog-2"},
                "target_transaction": None,
            }
        ]
    )

    review_rows = build_proposed_transactions._apply_review_target_suggestions(
        relations,
        map_path=map_path,
    )

    row = review_rows.row(0, named=True)
    assert row["target_payee_selected"] == "Adrian Dado"
    assert row["target_category_selected"] == ""
