from __future__ import annotations

import math

import pandas as pd

from ynab_il_importer.safe_types import normalize_flag_series


def test_normalize_flag_series_string_false() -> None:
    series = normalize_flag_series(pd.Series(["False"]))

    assert series.tolist() == [False]


def test_normalize_flag_series_string_true() -> None:
    series = normalize_flag_series(pd.Series(["True"]))

    assert series.tolist() == [True]


def test_normalize_flag_series_string_zero() -> None:
    series = normalize_flag_series(pd.Series(["0"]))

    assert series.tolist() == [False]


def test_normalize_flag_series_empty() -> None:
    series = normalize_flag_series(pd.Series([""]))

    assert series.tolist() == [False]


def test_normalize_flag_series_nan() -> None:
    series = normalize_flag_series(pd.Series([math.nan]))

    assert series.tolist() == [False]


def test_normalize_flag_series_bool_true() -> None:
    series = normalize_flag_series(pd.Series([True]))

    assert series.tolist() == [True]
