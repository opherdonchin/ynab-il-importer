from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.rules as rules_mod


SCRIPT_PATH = ROOT / "scripts" / "build_cross_budget_review_rows.py"
SPEC = importlib.util.spec_from_file_location("build_cross_budget_review_rows_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
build_cross_budget_review_rows = importlib.util.module_from_spec(SPEC)
sys.modules["build_cross_budget_review_rows_script"] = build_cross_budget_review_rows
SPEC.loader.exec_module(build_cross_budget_review_rows)


def _source_row(*, row_id: str, date: str, payee_raw: str, fingerprint: str, account_name: str = "Family Leumi", inflow_ils: float = 0.0, outflow_ils: float = 100.0) -> dict[str, object]:
    return {
        "source": "ynab",
        "ynab_id": row_id,
        "account_name": account_name,
        "date": date,
        "payee_raw": payee_raw,
        "category_raw": "Pilates",
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "txn_kind": "expense" if outflow_ils else "credit",
        "fingerprint": fingerprint,
        "memo": payee_raw,
    }


def _target_row(*, row_id: str, date: str, payee_raw: str, fingerprint: str, inflow_ils: float = 0.0, outflow_ils: float = 100.0, account_name: str = "In Family") -> dict[str, object]:
    return {
        "source": "ynab",
        "ynab_id": row_id,
        "account_id": f"acc-{row_id}",
        "account_name": account_name,
        "date": date,
        "payee_raw": payee_raw,
        "category_raw": "Pilates Expense",
        "outflow_ils": outflow_ils,
        "inflow_ils": inflow_ils,
        "txn_kind": "expense" if outflow_ils else "credit",
        "fingerprint": fingerprint,
        "memo": payee_raw,
        "import_id": "",
        "matched_transaction_id": "",
        "cleared": "uncleared",
        "approved": False,
    }


def _write_payee_map(path: Path) -> None:
    rows = [
        {
            "rule_id": "rent_1",
            "is_active": True,
            "priority": 0,
            "txn_kind": "",
            "fingerprint": "office rent",
            "description_clean_norm": "",
            "account_name": "",
            "source": "",
            "direction": "",
            "currency": "",
            "amount_bucket": "",
            "payee_canonical": "Office Rent",
            "category_target": "Pilates Expenses",
            "notes": "",
            "card_suffix": "",
        }
    ]
    pd.DataFrame(rows, columns=rules_mod.PAYEE_MAP_COLUMNS).to_csv(path, index=False, encoding="utf-8-sig")


def _runtime_dir(name: str) -> Path:
    path = ROOT / "tests_runtime" / name
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_build_review_rows_emits_matched_source_only_and_target_only() -> None:
    tmp_path = _runtime_dir("cross_budget_review_rows_main")
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            _source_row(row_id="source-existing", date="2025-11-01", payee_raw="Existing Client", fingerprint="existing client", inflow_ils=100.0, outflow_ils=0.0),
            _source_row(row_id="source-new", date="2025-11-02", payee_raw="Office Rent", fingerprint="office rent"),
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(row_id="target-existing", date="2025-11-01", payee_raw="Existing Client", fingerprint="existing client", inflow_ils=100.0, outflow_ils=0.0),
            _target_row(row_id="target-orphan", date="2025-11-03", payee_raw="Manual Pilates", fingerprint="manual pilates"),
        ]
    )

    review_rows, result = build_cross_budget_review_rows.build_review_rows(
        source_df,
        target_df,
        source_category="Pilates",
        target_account="In Family",
        map_path=map_path,
        date_tolerance_days=0,
    )

    assert len(result.matched_pairs_df) == 1
    assert set(review_rows["match_status"].tolist()) == {"matched_auto", "source_only", "target_only"}

    source_only = review_rows.loc[review_rows["match_status"] == "source_only"].iloc[0]
    assert source_only["decision_action"] == "create_target"
    assert source_only["payee_selected"] == "Office Rent"
    assert source_only["category_selected"] == "Pilates Expenses"
    assert source_only["target_account"] == "In Family"

    matched = review_rows.loc[review_rows["match_status"] == "matched_auto"].iloc[0]
    assert bool(matched["reviewed"]) is True
    assert matched["payee_selected"] == "Existing Client"

    target_only = review_rows.loc[review_rows["match_status"] == "target_only"].iloc[0]
    assert target_only["target_payee_current"] == "Manual Pilates"
    assert target_only["decision_action"] == ""


def test_build_review_rows_ignores_zero_amount_source_rows() -> None:
    tmp_path = _runtime_dir("cross_budget_review_rows_zero")
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            _source_row(row_id="source-zero", date="2026-03-13", payee_raw="paypal facebook", fingerprint="paypal facebook", inflow_ils=0.0, outflow_ils=0.0),
            _source_row(row_id="source-real", date="2026-03-14", payee_raw="Office Rent", fingerprint="office rent"),
        ]
    )
    target_df = pd.DataFrame(columns=list(_target_row(row_id="unused", date="2026-03-13", payee_raw="unused", fingerprint="unused").keys()))

    review_rows, _ = build_cross_budget_review_rows.build_review_rows(
        source_df,
        target_df,
        source_category="Pilates",
        target_account="In Family",
        map_path=map_path,
        date_tolerance_days=0,
    )

    assert len(review_rows) == 1
    assert review_rows.loc[0, "payee_selected"] == "Office Rent"
    assert review_rows.loc[0, "match_status"] == "source_only"


def test_build_review_rows_skips_suggestions_for_blank_fingerprint() -> None:
    tmp_path = _runtime_dir("cross_budget_review_rows_blank_fingerprint")
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-no-fingerprint",
                date="2026-03-14",
                payee_raw="Manual Family Row",
                fingerprint="",
            )
        ]
    )
    target_df = pd.DataFrame(columns=list(_target_row(row_id="unused", date="2026-03-13", payee_raw="unused", fingerprint="unused").keys()))

    review_rows, _ = build_cross_budget_review_rows.build_review_rows(
        source_df,
        target_df,
        source_category="Pilates",
        target_account="In Family",
        map_path=map_path,
        date_tolerance_days=0,
    )

    assert len(review_rows) == 1
    assert review_rows.loc[0, "match_status"] == "source_only"
    assert review_rows.loc[0, "payee_selected"] == ""
    assert review_rows.loc[0, "category_selected"] == ""


def test_build_review_rows_expands_ambiguous_rows_into_candidate_relations() -> None:
    tmp_path = _runtime_dir("cross_budget_review_rows_ambiguous_source_repeat")
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-ambiguous",
                date="2025-11-02",
                payee_raw="Family Cash",
                fingerprint="family cash",
                inflow_ils=120.0,
                outflow_ils=0.0,
            )
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-a",
                date="2025-11-01",
                payee_raw="Manual Pilates A",
                fingerprint="manual pilates a",
                inflow_ils=120.0,
                outflow_ils=0.0,
            ),
            _target_row(
                row_id="target-b",
                date="2025-11-03",
                payee_raw="Manual Pilates B",
                fingerprint="manual pilates b",
                inflow_ils=120.0,
                outflow_ils=0.0,
            ),
        ]
    )

    review_rows, result = build_cross_budget_review_rows.build_review_rows(
        source_df,
        target_df,
        source_category="Pilates",
        target_account="In Family",
        map_path=map_path,
        date_tolerance_days=1,
    )

    assert len(result.ambiguous_matches_df) == 1
    ambiguous = review_rows.loc[review_rows["match_status"] == "ambiguous"].copy()
    assert len(ambiguous) == 2
    assert set(ambiguous["relation_kind"].tolist()) == {"ambiguous_candidate"}
    assert set(ambiguous["source_row_id"].tolist()) == {"source-ambiguous"}
    assert set(ambiguous["target_row_id"].tolist()) == {"target-a", "target-b"}
    assert set(ambiguous["target_payee_current"].tolist()) == {"Manual Pilates A", "Manual Pilates B"}


def test_build_review_rows_preserves_repeated_target_candidates() -> None:
    tmp_path = _runtime_dir("cross_budget_review_rows_ambiguous_target_repeat")
    map_path = tmp_path / "payee_map.csv"
    _write_payee_map(map_path)

    source_df = pd.DataFrame(
        [
            _source_row(
                row_id="source-a",
                date="2025-11-01",
                payee_raw="Family Payment A",
                fingerprint="family payment a",
                inflow_ils=120.0,
                outflow_ils=0.0,
            ),
            _source_row(
                row_id="source-b",
                date="2025-11-03",
                payee_raw="Family Payment B",
                fingerprint="family payment b",
                inflow_ils=120.0,
                outflow_ils=0.0,
            ),
        ]
    )
    target_df = pd.DataFrame(
        [
            _target_row(
                row_id="target-shared",
                date="2025-11-02",
                payee_raw="Manual Shared",
                fingerprint="manual shared",
                inflow_ils=120.0,
                outflow_ils=0.0,
            )
        ]
    )

    review_rows, result = build_cross_budget_review_rows.build_review_rows(
        source_df,
        target_df,
        source_category="Pilates",
        target_account="In Family",
        map_path=map_path,
        date_tolerance_days=1,
    )

    assert len(result.ambiguous_matches_df) == 2
    ambiguous = review_rows.loc[review_rows["match_status"] == "ambiguous"].copy()
    assert len(ambiguous) == 2
    assert set(ambiguous["source_row_id"].tolist()) == {"source-a", "source-b"}
    assert set(ambiguous["target_row_id"].tolist()) == {"target-shared"}
    assert set(ambiguous["target_payee_current"].tolist()) == {"Manual Shared"}
