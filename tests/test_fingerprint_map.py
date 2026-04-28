import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.fingerprint as fingerprint


def test_load_fingerprint_map_expands_and_sorts(tmp_path: Path) -> None:
    map_path = tmp_path / "fingerprint_map.csv"
    map_path.write_text(
        "\n".join(
            [
                "rule_id,is_active,priority,pattern,canonical_text,notes",
                "r1,TRUE,0,foo|bar,foo corp,",
                "r2,TRUE,10,baz,baz corp,",
            ]
        ),
        encoding="utf-8",
    )

    rules = fingerprint.load_fingerprint_map(map_path)
    assert rules[0]["rule_id"] == "r2"
    assert set(r["pattern"] for r in rules) == {"foo", "bar", "baz"}


def test_apply_fingerprints_uses_map_and_logs(tmp_path: Path) -> None:
    map_path = tmp_path / "fingerprint_map.csv"
    map_path.write_text(
        "\n".join(
            [
                "rule_id,is_active,priority,pattern,canonical_text,notes",
                "r1,TRUE,1,super pharm|superpharm,super pharm,",
            ]
        ),
        encoding="utf-8",
    )
    rules = fingerprint.load_fingerprint_map(map_path)

    df = pd.DataFrame(
        [
            {"description_clean": "Super Pharm Tel Aviv 1234", "source": "card"},
            {"description_clean": "Other Vendor", "source": "card"},
        ]
    )
    log_path = tmp_path / "fingerprint_log.csv"
    out = fingerprint.apply_fingerprints(df, map_rules=rules, log_path=log_path)

    assert out.loc[0, "fingerprint"].startswith("super pharm")
    assert out.loc[1, "fingerprint"].startswith("other vendor")
    assert log_path.exists()

    log_df = pd.read_csv(log_path, dtype="string", keep_default_na=False)
    assert "run_id" in log_df.columns
    assert log_df.loc[0, "matched_rule_id"] == "r1"
    assert log_df.loc[1, "matched_rule_id"] == ""


def test_apply_fingerprints_collapses_paypal_facebook_variants() -> None:
    rules = fingerprint.load_fingerprint_map(ROOT / "mappings" / "fingerprint_map.csv")

    df = pd.DataFrame(
        [
            {
                "description_clean": "PAYPAL FACEBOOK 35314369001 IE חיוב עסקת חו ל בשח",
                "source": "card",
            }
        ]
    )

    out = fingerprint.apply_fingerprints(df, map_rules=rules, use_fingerprint_map=True)

    assert out.loc[0, "fingerprint"] == "paypal facebook עסקת חו"
