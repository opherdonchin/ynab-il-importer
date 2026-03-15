import pandas as pd

import ynab_il_importer.normalize as normalize


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


BANK_RAW_CANDIDATES = ["description_clean", "merchant_raw", "description_raw"]
CARD_RAW_CANDIDATES = ["description_clean", "description_raw", "merchant_raw"]
DEFAULT_RAW_CANDIDATES = ["description_clean", "merchant_raw", "description_raw", "raw_text"]


def _pick_raw_text_by_source(
    df: pd.DataFrame, source_series: pd.Series
) -> pd.Series:
    result = pd.Series([""] * len(df), index=df.index, dtype="string")

    def _fill(mask: pd.Series, candidates: list[str]) -> None:
        if not mask.any():
            return
        result.loc[mask] = _pick_raw_text(df.loc[mask], candidates)

    normalized_source = source_series.astype("string").fillna("").str.strip().str.lower()
    _fill(normalized_source == "bank", BANK_RAW_CANDIDATES)
    _fill(normalized_source == "card", CARD_RAW_CANDIDATES)

    remaining = result == ""
    if remaining.any():
        result.loc[remaining] = _pick_raw_text(df.loc[remaining], DEFAULT_RAW_CANDIDATES)
    return result


def _prepare_source(df: pd.DataFrame) -> pd.DataFrame:
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
                "fingerprint",
                "account_key",
                "date_key",
                "amount_key",
            ]
        )

    source_type = (
        _series_or_default(df, "source").astype("string").fillna("").str.strip().str.lower()
    )
    source_type = source_type.where(source_type != "", "source")
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
            "source_type": source_type,
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
            "raw_text": _pick_raw_text_by_source(df, source_type),
            "fingerprint": _series_or_default(df, "fingerprint")
            .astype("string")
            .fillna("")
            .str.strip(),
        }
    )
    if (prepared["fingerprint"] == "").any():
        raise ValueError("Source data missing fingerprint values; run fingerprinting first.")
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
                "ynab_account_id",
                "ynab_account",
                "ynab_payee_raw",
                "ynab_category_raw",
                "ynab_fingerprint",
                "ynab_id",
                "ynab_import_id",
                "ynab_matched_transaction_id",
                "ynab_cleared",
                "ynab_approved",
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
            "ynab_account_id": _series_or_default(df, "account_id")
            .astype("string")
            .fillna("")
            .str.strip(),
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
            "ynab_fingerprint": _series_or_default(df, "fingerprint")
            .astype("string")
            .fillna("")
            .str.strip(),
            "ynab_id": _series_or_default(df, "ynab_id").astype("string").fillna("").str.strip(),
            "ynab_import_id": _series_or_default(df, "import_id")
            .astype("string")
            .fillna("")
            .str.strip(),
            "ynab_matched_transaction_id": _series_or_default(df, "matched_transaction_id")
            .astype("string")
            .fillna("")
            .str.strip(),
            "ynab_cleared": _series_or_default(df, "cleared").astype("string").fillna("").str.strip(),
            "ynab_approved": _series_or_default(df, "approved").astype("string").fillna("").str.strip(),
        }
    )
    prepared["account_key"] = prepared["ynab_account"]
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
                "fingerprint",
                "ynab_file",
                "ynab_account_id",
                "ynab_account",
                "ynab_outflow_ils",
                "ynab_inflow_ils",
                "ynab_payee_raw",
                "ynab_category_raw",
                "ynab_fingerprint",
                "ynab_id",
                "ynab_import_id",
                "ynab_matched_transaction_id",
                "ynab_cleared",
                "ynab_approved",
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
            "fingerprint",
            "ynab_file",
            "ynab_account_id",
            "ynab_account",
            "ynab_outflow_ils",
            "ynab_inflow_ils",
            "ynab_payee_raw",
            "ynab_category_raw",
            "ynab_fingerprint",
            "ynab_id",
            "ynab_import_id",
            "ynab_matched_transaction_id",
            "ynab_cleared",
            "ynab_approved",
            "account_key",
            "date_key",
            "amount_key",
        ]
    ]


def match_pairs(source_df: pd.DataFrame, ynab_df: pd.DataFrame) -> pd.DataFrame:
    ynab_prepared = _prepare_ynab(ynab_df)
    ynab_prepared.head()

    pairs = _join_pairs(_prepare_source(source_df), ynab_prepared)
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
                "fingerprint",
                "ynab_file",
                "ynab_account_id",
                "ynab_account",
                "ynab_outflow_ils",
                "ynab_inflow_ils",
                "ynab_payee_raw",
                "ynab_category_raw",
                "ynab_fingerprint",
                "ynab_id",
                "ynab_import_id",
                "ynab_matched_transaction_id",
                "ynab_cleared",
                "ynab_approved",
                "ambiguous_key",
            ]
        )

    pairs["raw_norm"] = pairs["raw_text"].map(normalize.normalize_text)

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
            "fingerprint",
            "ynab_file",
            "ynab_account_id",
            "ynab_account",
            "ynab_outflow_ils",
            "ynab_inflow_ils",
            "ynab_payee_raw",
            "ynab_category_raw",
            "ynab_fingerprint",
            "ynab_id",
            "ynab_import_id",
            "ynab_matched_transaction_id",
            "ynab_cleared",
            "ynab_approved",
            "ambiguous_key",
        ]
    ]
