from __future__ import annotations

import pandas as pd


TRUE_VALUES = {"1", "true", "t", "yes", "y"}


def normalize_flag_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip().str.lower()
    return text.isin(TRUE_VALUES)
