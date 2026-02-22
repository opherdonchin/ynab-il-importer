import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.account_map import apply_account_name_map


def test_apply_account_name_map_warns_when_map_missing() -> None:
    df = pd.DataFrame({"account_name": ["x1234", "x5678"], "amount_ils": [-10, -20]})
    with pytest.warns(UserWarning, match="Unmatched account names: x1234, x5678"):
        out = apply_account_name_map(df, source="card", account_map_path="mappings/does_not_exist.csv")
    assert out["account_name"].tolist() == ["x1234", "x5678"]


def test_apply_account_name_map_maps_known_and_warns_unknown(tmp_path: Path) -> None:
    map_path = tmp_path / "account_name_map.csv"
    pd.DataFrame(
        [
            {"source": "card", "source_account": "x1234", "ynab_account_name": "Family Visa"},
        ]
    ).to_csv(map_path, index=False)

    df = pd.DataFrame({"account_name": ["x1234", "x9999"], "amount_ils": [-10, -20]})
    with pytest.warns(UserWarning, match="Unmatched account names: x9999"):
        out = apply_account_name_map(df, source="card", account_map_path=map_path)
    assert out["account_name"].tolist() == ["Family Visa", "x9999"]
