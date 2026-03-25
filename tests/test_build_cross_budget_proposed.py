from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pandas as pd

import ynab_il_importer.rules as rules_mod


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "build_cross_budget_proposed.py"
SPEC = importlib.util.spec_from_file_location("build_cross_budget_proposed_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
build_cross_budget_proposed = importlib.util.module_from_spec(SPEC)
sys.modules["build_cross_budget_proposed_script"] = build_cross_budget_proposed
SPEC.loader.exec_module(build_cross_budget_proposed)


def _source_row(
    *,
    row_id: str,
    date: str,
    payee_raw: str,
    fingerprint: str,
    account_name: str = "Family Leumi",
    inflow_ils: float = 0.0,
    outflow_ils: float = 100.0,
) -> dict[str, object]:
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


def _target_row(
    *,
    row_id: str,
    date: str,
    payee_raw: str,
    fingerprint: str,
    inflow_ils: float = 0.0,
    outflow_ils: float = 100.0,
) -> dict[str, object]:
    return {
        "source": "ynab",
        "ynab_id": row_id,
        "account_id": f"acc-{row_id}",
        "account_name": "In Family",
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
    pd.DataFrame(rows, columns=rules_mod.PAYEE_MAP_COLUMNS).to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )


def _runtime_dir(name: str) -> Path:
    path = ROOT / "tests_runtime" / name
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_main_excludes_matched_rows_and_rewrites_account(monkeypatch) -> None:
    tmp_path = _runtime_dir("build_cross_budget_proposed_main")
    source_path = tmp_path / "family.csv"
    target_path = tmp_path / "pilates.csv"
    map_path = tmp_path / "payee_map.csv"
    out_path = tmp_path / "proposed.csv"
    pairs_out = tmp_path / "matched.csv"
    unmatched_source_out = tmp_path / "unmatched_source.csv"
    unmatched_target_out = tmp_path / "unmatched_target.csv"
    ambiguous_out = tmp_path / "ambiguous.csv"

    pd.DataFrame(
        [
            _source_row(
                row_id="source-existing",
                date="2025-11-01",
                payee_raw="Existing Client",
                fingerprint="existing client",
                inflow_ils=100.0,
                outflow_ils=0.0,
            ),
            _source_row(
                row_id="source-new",
                date="2025-11-02",
                payee_raw="Office Rent",
                fingerprint="office rent",
                outflow_ils=100.0,
                inflow_ils=0.0,
            ),
        ]
    ).to_csv(source_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            _target_row(
                row_id="target-existing",
                date="2025-11-01",
                payee_raw="Existing Client",
                fingerprint="existing client",
                inflow_ils=100.0,
                outflow_ils=0.0,
            )
        ]
    ).to_csv(target_path, index=False, encoding="utf-8-sig")
    _write_payee_map(map_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cross_budget_proposed.py",
            "--source",
            str(source_path),
            "--ynab",
            str(target_path),
            "--source-category",
            "Pilates",
            "--target-account",
            "In Family",
            "--map",
            str(map_path),
            "--out",
            str(out_path),
            "--pairs-out",
            str(pairs_out),
            "--unmatched-source-out",
            str(unmatched_source_out),
            "--unmatched-target-out",
            str(unmatched_target_out),
            "--ambiguous-out",
            str(ambiguous_out),
        ],
    )

    build_cross_budget_proposed.main()

    proposed = pd.read_csv(out_path).fillna("")
    matched = pd.read_csv(pairs_out).fillna("")
    unmatched_source = pd.read_csv(unmatched_source_out).fillna("")

    assert len(proposed) == 1
    assert proposed.loc[0, "account_name"] == "In Family"
    assert proposed.loc[0, "source_account"] == "Family Leumi"
    assert proposed.loc[0, "payee_selected"] == "Office Rent"
    assert proposed.loc[0, "category_selected"] == "Pilates Expenses"
    assert len(matched) == 1
    assert len(unmatched_source) == 1


def test_ambiguous_rows_are_not_proposed(monkeypatch) -> None:
    tmp_path = _runtime_dir("build_cross_budget_proposed_ambiguous")
    source_path = tmp_path / "family.csv"
    target_path = tmp_path / "pilates.csv"
    map_path = tmp_path / "payee_map.csv"
    out_path = tmp_path / "proposed.csv"
    ambiguous_out = tmp_path / "ambiguous.csv"

    pd.DataFrame(
        [
            _source_row(
                row_id="source-1",
                date="2025-11-01",
                payee_raw="Office Rent",
                fingerprint="office rent",
            )
        ]
    ).to_csv(source_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            _target_row(
                row_id="target-1",
                date="2025-11-01",
                payee_raw="Office Rent",
                fingerprint="office rent",
            ),
            _target_row(
                row_id="target-2",
                date="2025-11-01",
                payee_raw="Office Rent",
                fingerprint="office rent",
            ),
        ]
    ).to_csv(target_path, index=False, encoding="utf-8-sig")
    _write_payee_map(map_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cross_budget_proposed.py",
            "--source",
            str(source_path),
            "--ynab",
            str(target_path),
            "--source-category",
            "Pilates",
            "--target-account",
            "In Family",
            "--map",
            str(map_path),
            "--out",
            str(out_path),
            "--ambiguous-out",
            str(ambiguous_out),
        ],
    )

    build_cross_budget_proposed.main()

    proposed = pd.read_csv(out_path).fillna("")
    ambiguous = pd.read_csv(ambiguous_out).fillna("")

    assert proposed.empty
    assert len(ambiguous) == 1


def test_zero_amount_source_rows_are_not_proposed(monkeypatch) -> None:
    tmp_path = _runtime_dir("build_cross_budget_proposed_zero_amount")
    source_path = tmp_path / "family.csv"
    target_path = tmp_path / "aikido.csv"
    map_path = tmp_path / "payee_map.csv"
    out_path = tmp_path / "proposed.csv"

    pd.DataFrame(
        [
            _source_row(
                row_id="source-zero",
                date="2026-03-13",
                payee_raw="paypal facebook",
                fingerprint="paypal facebook",
                inflow_ils=0.0,
                outflow_ils=0.0,
            ),
            _source_row(
                row_id="source-real",
                date="2026-03-14",
                payee_raw="Office Rent",
                fingerprint="office rent",
                inflow_ils=0.0,
                outflow_ils=100.0,
            ),
        ]
    ).to_csv(source_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(columns=list(_target_row(row_id="unused", date="2026-03-13", payee_raw="unused", fingerprint="unused").keys())).to_csv(
        target_path,
        index=False,
        encoding="utf-8-sig",
    )
    _write_payee_map(map_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cross_budget_proposed.py",
            "--source",
            str(source_path),
            "--ynab",
            str(target_path),
            "--source-category",
            "Pilates",
            "--target-account",
            "In Family",
            "--map",
            str(map_path),
            "--out",
            str(out_path),
        ],
    )

    build_cross_budget_proposed.main()

    proposed = pd.read_csv(out_path).fillna("")

    assert len(proposed) == 1
    assert proposed.loc[0, "payee_selected"] == "Office Rent"
    assert proposed.loc[0, "outflow_ils"] == 100.0


def test_main_handles_truly_empty_target_file(monkeypatch) -> None:
    tmp_path = _runtime_dir("build_cross_budget_proposed_empty_target")
    source_path = tmp_path / "family.csv"
    target_path = tmp_path / "aikido.csv"
    map_path = tmp_path / "payee_map.csv"
    out_path = tmp_path / "proposed.csv"

    pd.DataFrame(
        [
            _source_row(
                row_id="source-real",
                date="2026-03-14",
                payee_raw="Office Rent",
                fingerprint="office rent",
                inflow_ils=0.0,
                outflow_ils=100.0,
            ),
        ]
    ).to_csv(source_path, index=False, encoding="utf-8-sig")
    target_path.write_text("", encoding="utf-8")
    _write_payee_map(map_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cross_budget_proposed.py",
            "--source",
            str(source_path),
            "--ynab",
            str(target_path),
            "--source-category",
            "Pilates",
            "--target-account",
            "In Family",
            "--map",
            str(map_path),
            "--out",
            str(out_path),
        ],
    )

    build_cross_budget_proposed.main()

    proposed = pd.read_csv(out_path).fillna("")

    assert len(proposed) == 1
    assert proposed.loc[0, "payee_selected"] == "Office Rent"
