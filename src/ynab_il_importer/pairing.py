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
    df: pd.DataFrame, raw_candidates: list[str], source_type: str
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "source_type",
                "source_file",
                "source_account",
                "account_name",
                "date",
                "outflow_ils",
                "inflow_ils",
                "raw_text",
                "account_key",
                "date_key",
                "amount_key",
            ]
        )

    account_name = (
        _series_or_default(df, "account_name").astype("string").fillna("").str.strip()
    )
    source_account = (
        _series_or_default(df, "source_account").astype("string").fillna("").str.strip()
    )
    outflow_ils = pd.to_numeric(
        _series_or_default(df, "outflow_ils", 0.0), errors="coerce"
    ).fillna(0.0).round(2)
    inflow_ils = pd.to_numeric(
        _series_or_default(df, "inflow_ils", 0.0), errors="coerce"
    ).fillna(0.0).round(2)
    if "outflow_ils" not in df.columns and "inflow_ils" not in df.columns:
        amount = pd.to_numeric(
            _series_or_default(df, "amount_ils", 0.0), errors="coerce"
        ).fillna(0.0).round(2)
        outflow_ils = amount.where(amount < 0, 0.0).abs().round(2)
        inflow_ils = amount.where(amount > 0, 0.0).round(2)
    prepared = pd.DataFrame(
        {
            "source_type": str(source_type).strip(),
            "source_file": _series_or_default(df, "source_file")
            .astype("string")
            .fillna("")
            .str.strip(),
            "source_account": source_account,
            "account_name": account_name,
            "date": pd.to_datetime(
                _series_or_default(df, "date"), errors="coerce"
            ).dt.date,
            "outflow_ils": outflow_ils,
            "inflow_ils": inflow_ils,
            "raw_text": _pick_raw_text(df, raw_candidates),
        }
    )
    prepared["account_key"] = prepared["account_name"]
    prepared["date_key"] = prepared["date"]
    prepared["amount_key"] = (prepared["inflow_ils"] - prepared["outflow_ils"]).round(2)
    return prepared.dropna(subset=["date_key", "amount_key", "account_key"])


def _prepare_ynab(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "account_key",
                "date_key",
                "amount_key",
                "ynab_file",
                "ynab_account",
                "ynab_payee_raw",
                "ynab_category_raw",
            ]
        )

    ynab_account = (
        _series_or_default(df, "account_name").astype("string").fillna("").str.strip()
    )
    outflow_ils = pd.to_numeric(
        _series_or_default(df, "outflow_ils", 0.0), errors="coerce"
    ).fillna(0.0).round(2)
    inflow_ils = pd.to_numeric(
        _series_or_default(df, "inflow_ils", 0.0), errors="coerce"
    ).fillna(0.0).round(2)
    if "outflow_ils" not in df.columns and "inflow_ils" not in df.columns:
        amount = pd.to_numeric(
            _series_or_default(df, "amount_ils", 0.0), errors="coerce"
        ).fillna(0.0).round(2)
        outflow_ils = amount.where(amount < 0, 0.0).abs().round(2)
        inflow_ils = amount.where(amount > 0, 0.0).round(2)
    prepared = pd.DataFrame(
        {
            "account_key": ynab_account,
            "date_key": pd.to_datetime(
                _series_or_default(df, "date"), errors="coerce"
            ).dt.date,
            "amount_key": (inflow_ils - outflow_ils).round(2),
            "ynab_file": _series_or_default(df, "ynab_file")
            .astype("string")
            .fillna("")
            .str.strip(),
            "ynab_account": ynab_account,
            "ynab_outflow_ils": outflow_ils,
            "ynab_inflow_ils": inflow_ils,
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
                "source_type",
                "source_file",
                "source_account",
                "account_name",
                "date",
                "outflow_ils",
                "inflow_ils",
                "raw_text",
                "ynab_file",
                "ynab_account",
                "ynab_outflow_ils",
                "ynab_inflow_ils",
                "ynab_payee_raw",
                "ynab_category_raw",
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
            "source_type",
            "source_file",
            "source_account",
            "account_name",
            "date",
            "outflow_ils",
            "inflow_ils",
            "raw_text",
            "ynab_file",
            "ynab_account",
            "ynab_outflow_ils",
            "ynab_inflow_ils",
            "ynab_payee_raw",
            "ynab_category_raw",
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
            "bank",
        ),
        ynab_prepared,
    )
    card_pairs = _join_pairs(
        _prepare_source(
            card_df,
            ["description_clean", "description_raw", "merchant_raw"],
            "card",
        ),
        ynab_prepared,
    )

    pairs = pd.concat([bank_pairs, card_pairs], ignore_index=True)
    if pairs.empty:
        return pd.DataFrame(
            columns=[
                "source_type",
                "source_file",
                "source_account",
                "account_name",
                "date",
                "outflow_ils",
                "inflow_ils",
                "raw_text",
                "raw_norm",
                "fingerprint_v0",
                "ynab_file",
                "ynab_account",
                "ynab_outflow_ils",
                "ynab_inflow_ils",
                "ynab_payee_raw",
                "ynab_category_raw",
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
            "source_type",
            "source_file",
            "source_account",
            "account_name",
            "date",
            "outflow_ils",
            "inflow_ils",
            "raw_text",
            "raw_norm",
            "fingerprint_v0",
            "ynab_file",
            "ynab_account",
            "ynab_outflow_ils",
            "ynab_inflow_ils",
            "ynab_payee_raw",
            "ynab_category_raw",
            "ambiguous_key",
        ]
    ]
