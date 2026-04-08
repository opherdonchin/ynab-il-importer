from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import polars as pl

import ynab_il_importer.rules as rules


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "bootstrap_payee_map.py"
SPEC = importlib.util.spec_from_file_location("bootstrap_payee_map_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
bootstrap_payee_map = importlib.util.module_from_spec(SPEC)
sys.modules["bootstrap_payee_map_script"] = bootstrap_payee_map
SPEC.loader.exec_module(bootstrap_payee_map)


def test_build_bootstrap_rules_uses_amount_bucket_for_multi_outcome_fingerprint() -> None:
    pairs = pd.DataFrame(
        [
            {
                "fingerprint": "isshin aikido",
                "ynab_payee_raw": "Member Fees",
                "ynab_category_raw": "Ready to Assign",
                "signed_amount": 275.0,
            },
            {
                "fingerprint": "isshin aikido",
                "ynab_payee_raw": "Introductory class",
                "ynab_category_raw": "Ready to Assign",
                "signed_amount": 50.0,
            },
            {
                "fingerprint": "facebook",
                "ynab_payee_raw": "Facebook",
                "ynab_category_raw": "*11* Marketing",
                "signed_amount": -326.25,
            },
            {
                "fingerprint": "facebook",
                "ynab_payee_raw": "Facebook",
                "ynab_category_raw": "*11* Marketing",
                "signed_amount": -289.15,
            },
        ]
    )

    actual = bootstrap_payee_map._build_bootstrap_rules(pairs)

    assert list(actual.loc[actual["fingerprint"] == "facebook", "amount_bucket"]) == [""]

    isshin_rules = actual.loc[actual["fingerprint"] == "isshin aikido"].reset_index(drop=True)
    assert isshin_rules["amount_bucket"].tolist() == ["=50", "=275"]

    tx = pd.DataFrame(
        [
            {
                "fingerprint": "isshin aikido",
                "payee_raw": "Isshin Aikido",
                "description_clean_norm": "isshin aikido",
                "inflow_ils": 50.0,
                "outflow_ils": 0.0,
                "txn_kind": "credit",
                "source": "ynab",
                "account_name": "Bank Leumi",
                "currency": "ILS",
                "amount_bucket": "",
            },
            {
                "fingerprint": "isshin aikido",
                "payee_raw": "Isshin Aikido",
                "description_clean_norm": "isshin aikido",
                "inflow_ils": 275.0,
                "outflow_ils": 0.0,
                "txn_kind": "credit",
                "source": "ynab",
                "account_name": "Bank Leumi",
                "currency": "ILS",
                "amount_bucket": "",
            },
            {
                "fingerprint": "facebook",
                "payee_raw": "Facebook",
                "description_clean_norm": "facebook",
                "inflow_ils": 0.0,
                "outflow_ils": 326.25,
                "txn_kind": "expense",
                "source": "ynab",
                "account_name": "Opher X5898",
                "currency": "ILS",
                "amount_bucket": "",
            },
        ]
    )

    applied = rules.apply_payee_map_rules(
        pl.from_pandas(tx),
        rules.normalize_payee_map_rules(pl.from_pandas(actual)),
    )

    assert applied["match_status"].to_list() == ["unique", "unique", "unique"]
    assert applied["payee_canonical_suggested"].to_list() == [
        "Introductory class",
        "Member Fees",
        "Facebook",
    ]
