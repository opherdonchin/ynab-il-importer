import pandas as pd

from ynab_il_importer.fingerprint import fingerprint_v0
from ynab_il_importer.normalize import normalize_text


def _series_or_default(
    df: pd.DataFrame, col: str, default: str | float = ""
) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def _pick_raw_text(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            series = df[col].astype("string").fillna("").str.strip()
            if (series != "").any():
                return series
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _prepare_source(
    df: pd.DataFrame, raw_candidates: list[str], pair_source: str
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "account_name",
                "date",
                "amount_ils",
                "raw_text",
                "pair_source",
                "account_key",
                "date_key",
                "amount_key",
            ]
        )

    account_name = (
        _series_or_default(df, "account_name").astype("string").fillna("").str.strip()
    )
    prepared = pd.DataFrame(
        {
            "account_name": account_name,
            "date": pd.to_datetime(
                _series_or_default(df, "date"), errors="coerce"
            ).dt.date,
            "amount_ils": pd.to_numeric(
                _series_or_default(df, "amount_ils", 0.0), errors="coerce"
            ).round(2),
            "raw_text": _pick_raw_text(df, raw_candidates),
            "pair_source": pair_source,
        }
    )
    prepared["account_key"] = prepared["account_name"]
    prepared["date_key"] = prepared["date"]
    prepared["amount_key"] = prepared["amount_ils"]
    return prepared.dropna(subset=["date_key", "amount_key", "account_key"])


def _prepare_ynab(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "account_key",
                "date_key",
                "amount_key",
                "ynab_payee_raw",
                "ynab_category_raw",
            ]
        )

    prepared = pd.DataFrame(
        {
            "account_key": _series_or_default(df, "account_name")
            .astype("string")
            .fillna("")
            .str.strip(),
            "date_key": pd.to_datetime(
                _series_or_default(df, "date"), errors="coerce"
            ).dt.date,
            "amount_key": pd.to_numeric(
                _series_or_default(df, "amount_ils", 0.0), errors="coerce"
            ).round(2),
            "ynab_payee_raw": _series_or_default(df, "payee_raw")
            .astype("string")
            .fillna(""),
            "ynab_category_raw": _series_or_default(df, "category_raw")
            .astype("string")
            .fillna(""),
        }
    )
    return prepared.dropna(subset=["account_key", "date_key", "amount_key"])


def _join_pairs(source_df: pd.DataFrame, ynab_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty or ynab_df.empty:
        return pd.DataFrame(
            columns=[
                "account_name",
                "date",
                "amount_ils",
                "raw_text",
                "ynab_payee_raw",
                "ynab_category_raw",
                "pair_source",
                "account_key",
                "date_key",
                "amount_key",
            ]
        )

    joined = source_df.merge(
        ynab_df, on=["account_key", "date_key", "amount_key"], how="inner"
    )
    return joined[
        [
            "account_name",
            "date",
            "amount_ils",
            "raw_text",
            "ynab_payee_raw",
            "ynab_category_raw",
            "pair_source",
            "account_key",
            "date_key",
            "amount_key",
        ]
    ]


def match_pairs(
    bank_df: pd.DataFrame, card_df: pd.DataFrame, ynab_df: pd.DataFrame
) -> pd.DataFrame:
    ynab_prepared = _prepare_ynab(ynab_df)
    ynab_prepared.head()

    bank_pairs = _join_pairs(
        _prepare_source(
            bank_df,
            ["description_clean", "merchant_raw", "description_raw"],
            "bank-ynab",
        ),
        ynab_prepared,
    )
    card_pairs = _join_pairs(
        _prepare_source(
            card_df,
            ["description_clean", "description_raw", "merchant_raw"],
            "card-ynab",
        ),
        ynab_prepared,
    )

    pairs = pd.concat([bank_pairs, card_pairs], ignore_index=True)
    if pairs.empty:
        return pd.DataFrame(
            columns=[
                "account_name",
                "date",
                "amount_ils",
                "raw_text",
                "raw_norm",
                "fingerprint_v0",
                "ynab_payee_raw",
                "ynab_category_raw",
                "pair_source",
                "ambiguous_key",
            ]
        )

    pairs["raw_norm"] = pairs["raw_text"].map(normalize_text)
    pairs["fingerprint_v0"] = pairs["raw_text"].map(fingerprint_v0)

    key_counts = (
        pairs.groupby(["account_key", "date_key", "amount_key"], dropna=False)
        .size()
        .rename("_key_count")
        .reset_index()
    )
    pairs = pairs.merge(
        key_counts, on=["account_key", "date_key", "amount_key"], how="left"
    )
    pairs["ambiguous_key"] = pairs["_key_count"].fillna(0).astype(int) > 1

    return pairs[
        [
            "account_name",
            "date",
            "amount_ils",
            "raw_text",
            "raw_norm",
            "fingerprint_v0",
            "ynab_payee_raw",
            "ynab_category_raw",
            "pair_source",
            "ambiguous_key",
        ]
    ]
