from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

import ynab_il_importer.normalize as normalize


@dataclass(frozen=True, slots=True)
class CrossBudgetMatchResult:
    matched_pairs_df: pd.DataFrame
    unmatched_source_df: pd.DataFrame
    unmatched_target_df: pd.DataFrame
    ambiguous_matches_df: pd.DataFrame


def _finalize_unmatched(df: pd.DataFrame, *, status: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["status"] = status
    if "date_key" in out.columns:
        out["date"] = pd.to_datetime(out["date_key"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def _series_or_default(
    df: pd.DataFrame, column: str, default: str | float = ""
) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _pick_text(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    text = pd.Series([""] * len(df), index=df.index, dtype="string")
    for column in candidates:
        if column not in df.columns:
            continue
        series = df[column].astype("string").fillna("").str.strip()
        text = text.where(text != "", series)
    return text


def _merged_series_or_default(
    df: pd.DataFrame, column: str, default: str | float = ""
) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _signed_amount(values: pd.DataFrame | pd.Series) -> pd.Series | float:
    if isinstance(values, pd.Series):
        inflow = float(pd.to_numeric(pd.Series([values.get("inflow_ils", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        outflow = float(pd.to_numeric(pd.Series([values.get("outflow_ils", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        return round(inflow - outflow, 2)

    inflow = pd.to_numeric(_series_or_default(values, "inflow_ils", 0.0), errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(_series_or_default(values, "outflow_ils", 0.0), errors="coerce").fillna(0.0)
    return (inflow - outflow).round(2)


def _classify_row_kind(payee_text: str, txn_kind: str) -> str:
    payee = str(payee_text or "").strip().lower()
    kind = str(txn_kind or "").strip().lower()
    if kind == "transfer":
        return "transfer_like"
    if payee.startswith("transfer :") or payee.startswith("transfer:"):
        return "transfer_like"
    if payee.startswith("loan "):
        return "transfer_like"
    return "ordinary"


def _classify_source_row_kind(row: pd.Series) -> str:
    return _classify_row_kind(
        payee_text=row.get("payee_raw", ""),
        txn_kind=row.get("txn_kind", ""),
    )


def _classify_target_row_kind(row: pd.Series) -> str:
    return _classify_row_kind(
        payee_text=row.get("payee_raw", ""),
        txn_kind=row.get("txn_kind", ""),
    )


def _text_key(df: pd.DataFrame) -> pd.Series:
    preferred = _pick_text(
        df,
        [
            "fingerprint",
            "description_clean_norm",
            "description_clean",
            "description_raw",
            "memo",
            "payee_raw",
        ],
    )
    return preferred.map(normalize.normalize_text)


def _source_row_id(df: pd.DataFrame) -> pd.Series:
    ynab_id = _series_or_default(df, "ynab_id").astype("string").fillna("").str.strip()
    generated = pd.Series(
        [f"source_row_{idx}" for idx in df.index],
        index=df.index,
        dtype="string",
    )
    return ynab_id.where(ynab_id != "", generated)


def _target_row_id(df: pd.DataFrame) -> pd.Series:
    ynab_id = _series_or_default(df, "ynab_id").astype("string").fillna("").str.strip()
    generated = pd.Series(
        [f"target_row_{idx}" for idx in df.index],
        index=df.index,
        dtype="string",
    )
    return ynab_id.where(ynab_id != "", generated)


def prepare_cross_budget_source(
    source_df: pd.DataFrame,
    *,
    source_category: str | None = None,
) -> pd.DataFrame:
    if source_df is None or source_df.empty:
        return pd.DataFrame()

    required = {"date", "payee_raw", "outflow_ils", "inflow_ils", "fingerprint", "account_name"}
    missing = sorted(required - set(source_df.columns))
    if missing:
        raise ValueError(f"Cross-budget source data missing columns: {missing}")

    prepared = source_df.copy()
    if source_category:
        category_text = _series_or_default(prepared, "category_raw").astype("string").fillna("").str.strip()
        prepared = prepared.loc[category_text == str(source_category).strip()].copy()
        if prepared.empty:
            return prepared

    prepared["date_key"] = pd.to_datetime(_series_or_default(prepared, "date"), errors="coerce").dt.normalize()
    prepared["signed_amount"] = _signed_amount(prepared)
    prepared["row_kind"] = prepared.apply(_classify_source_row_kind, axis=1)
    prepared["source_account"] = (
        _series_or_default(prepared, "source_account")
        .astype("string")
        .fillna("")
        .str.strip()
    )
    fallback_account = (
        _series_or_default(prepared, "account_name")
        .astype("string")
        .fillna("")
        .str.strip()
    )
    prepared["source_account"] = prepared["source_account"].where(
        prepared["source_account"] != "",
        fallback_account,
    )
    prepared["raw_text"] = _pick_text(
        prepared,
        ["description_raw", "memo", "payee_raw", "description_clean"],
    )
    prepared["text_key"] = _text_key(prepared)
    prepared["source_row_id"] = _source_row_id(prepared)
    prepared["source_file"] = (
        _series_or_default(prepared, "source_file")
        .astype("string")
        .fillna("")
        .str.strip()
    )
    return prepared.dropna(subset=["date_key"])


def prepare_cross_budget_target(
    target_df: pd.DataFrame,
    *,
    target_account: str,
) -> pd.DataFrame:
    if target_df is None or target_df.empty:
        return pd.DataFrame()

    required = {"date", "payee_raw", "outflow_ils", "inflow_ils", "fingerprint", "account_name"}
    missing = sorted(required - set(target_df.columns))
    if missing:
        raise ValueError(f"Cross-budget target data missing columns: {missing}")

    prepared = target_df.copy()
    account_text = (
        _series_or_default(prepared, "account_name")
        .astype("string")
        .fillna("")
        .str.strip()
    )
    prepared = prepared.loc[account_text == str(target_account).strip()].copy()
    if prepared.empty:
        return prepared

    prepared["date_key"] = pd.to_datetime(_series_or_default(prepared, "date"), errors="coerce").dt.normalize()
    prepared["signed_amount"] = _signed_amount(prepared)
    prepared["row_kind"] = prepared.apply(_classify_target_row_kind, axis=1)
    prepared["raw_text"] = _pick_text(
        prepared,
        ["memo", "payee_raw", "description_raw", "description_clean"],
    )
    prepared["text_key"] = _text_key(prepared)
    prepared["target_row_id"] = _target_row_id(prepared)
    prepared["target_file"] = (
        _series_or_default(prepared, "target_file")
        .astype("string")
        .fillna("")
        .str.strip()
    )
    return prepared.dropna(subset=["date_key"])


def _pairs_from_edges(edges: pd.DataFrame, *, match_type: str) -> pd.DataFrame:
    if edges.empty:
        return pd.DataFrame()

    pairs = edges.copy()
    pairs["match_type"] = match_type
    pairs["raw_norm"] = _merged_series_or_default(
        pairs, "raw_text_source"
    ).map(normalize.normalize_text)
    pairs["ambiguous_key"] = False
    return pd.DataFrame(
        {
            "source_type": _merged_series_or_default(pairs, "source_source"),
            "source_file": _merged_series_or_default(pairs, "source_file"),
            "source_account": _merged_series_or_default(pairs, "source_account"),
            "account_name": _merged_series_or_default(pairs, "account_name_source"),
            "date": _merged_series_or_default(pairs, "date_source"),
            "outflow_ils": _merged_series_or_default(pairs, "outflow_ils_source", 0.0),
            "inflow_ils": _merged_series_or_default(pairs, "inflow_ils_source", 0.0),
            "raw_text": _merged_series_or_default(pairs, "raw_text_source"),
            "raw_norm": pairs["raw_norm"],
            "fingerprint": _merged_series_or_default(pairs, "fingerprint_source"),
            "ynab_file": _merged_series_or_default(pairs, "target_file"),
            "ynab_account_id": _merged_series_or_default(pairs, "account_id_target"),
            "ynab_account": _merged_series_or_default(pairs, "account_name_target"),
            "ynab_outflow_ils": _merged_series_or_default(pairs, "outflow_ils_target", 0.0),
            "ynab_inflow_ils": _merged_series_or_default(pairs, "inflow_ils_target", 0.0),
            "ynab_payee_raw": _merged_series_or_default(pairs, "payee_raw_target"),
            "ynab_category_raw": _merged_series_or_default(pairs, "category_raw_target"),
            "ynab_fingerprint": _merged_series_or_default(pairs, "fingerprint_target"),
            "ynab_id": _merged_series_or_default(pairs, "ynab_id_target"),
            "ynab_import_id": _merged_series_or_default(pairs, "import_id_target"),
            "ynab_matched_transaction_id": _merged_series_or_default(
                pairs, "matched_transaction_id_target"
            ),
            "ynab_cleared": _merged_series_or_default(pairs, "cleared_target"),
            "ynab_approved": _merged_series_or_default(pairs, "approved_target"),
            "ambiguous_key": False,
            "match_type": match_type,
            "date_gap_days": _merged_series_or_default(pairs, "date_gap_days", 0),
            "source_row_id": _merged_series_or_default(pairs, "source_row_id"),
            "target_row_id": _merged_series_or_default(pairs, "target_row_id"),
            "signed_amount": _merged_series_or_default(pairs, "signed_amount", 0.0),
            "row_kind": _merged_series_or_default(pairs, "row_kind"),
            "source_memo": _merged_series_or_default(pairs, "memo_source"),
            "ynab_memo": _merged_series_or_default(pairs, "memo_target"),
            "source_payee_raw": _merged_series_or_default(pairs, "payee_raw_source"),
            "source_category_raw": _merged_series_or_default(pairs, "category_raw_source"),
        }
    )


def _exact_bucket_counts(df: pd.DataFrame, row_id_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date_key", "signed_amount", "row_kind", f"{row_id_col}_count"])
    return (
        df.groupby(["date_key", "signed_amount", "row_kind"], dropna=False)[row_id_col]
        .size()
        .reset_index(name=f"{row_id_col}_count")
    )


def _build_ambiguous_bucket_rows(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    *,
    reason: str,
) -> pd.DataFrame:
    if source_df.empty and target_df.empty:
        return pd.DataFrame()

    source = source_df.copy()
    target = target_df.copy()
    source["date_iso"] = source["date_key"].dt.strftime("%Y-%m-%d")
    target["date_iso"] = target["date_key"].dt.strftime("%Y-%m-%d")

    buckets: list[dict[str, Any]] = []
    keys = set(
        zip(
            source.get("date_key", pd.Series(dtype="datetime64[ns]")),
            source.get("signed_amount", pd.Series(dtype="float64")),
            source.get("row_kind", pd.Series(dtype="string")),
        )
    )
    keys.update(
        zip(
            target.get("date_key", pd.Series(dtype="datetime64[ns]")),
            target.get("signed_amount", pd.Series(dtype="float64")),
            target.get("row_kind", pd.Series(dtype="string")),
        )
    )

    for date_key, signed_amount, row_kind in sorted(
        keys,
        key=lambda value: (
            value[0].isoformat() if pd.notna(value[0]) else "",
            float(value[1]) if pd.notna(value[1]) else 0.0,
            str(value[2]),
        ),
    ):
        source_bucket = source.loc[
            (source["date_key"] == date_key)
            & (source["signed_amount"] == signed_amount)
            & (source["row_kind"] == row_kind)
        ]
        target_bucket = target.loc[
            (target["date_key"] == date_key)
            & (target["signed_amount"] == signed_amount)
            & (target["row_kind"] == row_kind)
        ]
        if source_bucket.empty and target_bucket.empty:
            continue
        buckets.append(
            {
                "status": "ambiguous",
                "reason": reason,
                "date": date_key.strftime("%Y-%m-%d") if pd.notna(date_key) else "",
                "signed_amount": signed_amount,
                "row_kind": row_kind,
                "source_count": len(source_bucket),
                "target_count": len(target_bucket),
                "source_row_ids": "; ".join(source_bucket["source_row_id"].astype("string").tolist()),
                "target_row_ids": "; ".join(target_bucket["target_row_id"].astype("string").tolist()),
                "source_payees": "; ".join(source_bucket["payee_raw"].astype("string").tolist()),
                "target_payees": "; ".join(target_bucket["payee_raw"].astype("string").tolist()),
            }
        )
    return pd.DataFrame(buckets)


def _candidate_edges(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    *,
    date_tolerance_days: int,
) -> pd.DataFrame:
    if source_df.empty or target_df.empty:
        return pd.DataFrame()

    edges = source_df.merge(
        target_df,
        on=["signed_amount", "row_kind"],
        suffixes=("_source", "_target"),
        how="inner",
    )
    if edges.empty:
        return edges

    edges["date_gap_days"] = (
        edges["date_key_source"] - edges["date_key_target"]
    ).abs().dt.days.astype(int)
    edges = edges.loc[edges["date_gap_days"] <= int(date_tolerance_days)].copy()
    if edges.empty:
        return edges

    source_text = edges["text_key_source"].astype("string").fillna("").str.strip()
    target_text = edges["text_key_target"].astype("string").fillna("").str.strip()
    edges["text_match"] = (source_text != "") & (source_text == target_text)
    return edges


def _match_unique_edges(edges: pd.DataFrame) -> pd.DataFrame:
    if edges.empty:
        return pd.DataFrame()
    source_counts = (
        edges.groupby("source_row_id", dropna=False)
        .size()
        .reset_index(name="source_candidate_count")
    )
    target_counts = (
        edges.groupby("target_row_id", dropna=False)
        .size()
        .reset_index(name="target_candidate_count")
    )
    merged = edges.merge(source_counts, on="source_row_id", how="left")
    merged = merged.merge(target_counts, on="target_row_id", how="left")
    return merged.loc[
        (merged["source_candidate_count"] == 1)
        & (merged["target_candidate_count"] == 1)
    ].copy()


def _drop_matched_edges(edges: pd.DataFrame, matched_edges: pd.DataFrame) -> pd.DataFrame:
    if edges.empty or matched_edges.empty:
        return edges
    matched_source_ids = set(matched_edges["source_row_id"].tolist())
    matched_target_ids = set(matched_edges["target_row_id"].tolist())
    return edges.loc[
        ~edges["source_row_id"].isin(matched_source_ids)
        & ~edges["target_row_id"].isin(matched_target_ids)
    ].copy()


def _window_ambiguous_rows(edges: pd.DataFrame, *, reason: str) -> pd.DataFrame:
    if edges.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for source_row_id, group in edges.groupby("source_row_id", dropna=False):
        source_dates = sorted(group["date_key_source"].dt.strftime("%Y-%m-%d").unique().tolist())
        target_dates = sorted(group["date_key_target"].dt.strftime("%Y-%m-%d").unique().tolist())
        rows.append(
            {
                "status": "ambiguous",
                "reason": reason,
                "source_row_id": source_row_id,
                "target_row_ids": "; ".join(sorted(group["target_row_id"].astype("string").unique().tolist())),
                "signed_amount": group["signed_amount"].iloc[0],
                "row_kind": group["row_kind"].iloc[0],
                "source_dates": "; ".join(source_dates),
                "target_dates": "; ".join(target_dates),
                "source_payee": str(group["payee_raw_source"].iloc[0] or ""),
                "target_payees": "; ".join(sorted(group["payee_raw_target"].astype("string").unique().tolist())),
                "candidate_count": len(group),
            }
        )
    return pd.DataFrame(rows)


def match_cross_budget_rows(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    *,
    target_account: str,
    source_category: str | None = None,
    date_tolerance_days: int = 0,
) -> CrossBudgetMatchResult:
    prepared_source = prepare_cross_budget_source(
        source_df,
        source_category=source_category,
    )
    prepared_target = prepare_cross_budget_target(
        target_df,
        target_account=target_account,
    )

    if prepared_source.empty or prepared_target.empty:
        return CrossBudgetMatchResult(
            matched_pairs_df=pd.DataFrame(),
            unmatched_source_df=_finalize_unmatched(prepared_source, status="unmatched_source"),
            unmatched_target_df=_finalize_unmatched(prepared_target, status="unmatched_target"),
            ambiguous_matches_df=pd.DataFrame(),
        )

    source_counts = _exact_bucket_counts(prepared_source, "source_row_id")
    target_counts = _exact_bucket_counts(prepared_target, "target_row_id")
    bucket_counts = source_counts.merge(
        target_counts,
        on=["date_key", "signed_amount", "row_kind"],
        how="inner",
    )

    exact_unique_keys = bucket_counts.loc[
        (bucket_counts["source_row_id_count"] == 1)
        & (bucket_counts["target_row_id_count"] == 1),
        ["date_key", "signed_amount", "row_kind"],
    ].copy()
    exact_ambiguous_keys = bucket_counts.loc[
        ~(
            (bucket_counts["source_row_id_count"] == 1)
            & (bucket_counts["target_row_id_count"] == 1)
        ),
        ["date_key", "signed_amount", "row_kind"],
    ].copy()

    matched_pairs: list[pd.DataFrame] = []
    ambiguous_frames: list[pd.DataFrame] = []

    matched_source_ids: set[str] = set()
    matched_target_ids: set[str] = set()
    ambiguous_source_ids: set[str] = set()
    ambiguous_target_ids: set[str] = set()

    if not exact_unique_keys.empty:
        source_exact = prepared_source.merge(
            exact_unique_keys,
            on=["date_key", "signed_amount", "row_kind"],
            how="inner",
        )
        target_exact = prepared_target.merge(
            exact_unique_keys,
            on=["date_key", "signed_amount", "row_kind"],
            how="inner",
        )
        exact_edges = source_exact.merge(
            target_exact,
            on=["date_key", "signed_amount", "row_kind"],
            suffixes=("_source", "_target"),
            how="inner",
        )
        exact_edges["date_gap_days"] = 0
        matched_pairs.append(
            _pairs_from_edges(exact_edges, match_type="exact_date_amount")
        )
        matched_source_ids.update(source_exact["source_row_id"].astype("string").tolist())
        matched_target_ids.update(target_exact["target_row_id"].astype("string").tolist())

    if not exact_ambiguous_keys.empty:
        source_ambiguous = prepared_source.merge(
            exact_ambiguous_keys,
            on=["date_key", "signed_amount", "row_kind"],
            how="inner",
        )
        target_ambiguous = prepared_target.merge(
            exact_ambiguous_keys,
            on=["date_key", "signed_amount", "row_kind"],
            how="inner",
        )
        ambiguous_frames.append(
            _build_ambiguous_bucket_rows(
                source_ambiguous,
                target_ambiguous,
                reason="same_date_amount_bucket_not_unique",
            )
        )
        ambiguous_source_ids.update(source_ambiguous["source_row_id"].astype("string").tolist())
        ambiguous_target_ids.update(target_ambiguous["target_row_id"].astype("string").tolist())

    remaining_source = prepared_source.loc[
        ~prepared_source["source_row_id"].astype("string").isin(matched_source_ids | ambiguous_source_ids)
    ].copy()
    remaining_target = prepared_target.loc[
        ~prepared_target["target_row_id"].astype("string").isin(matched_target_ids | ambiguous_target_ids)
    ].copy()

    if int(date_tolerance_days) > 0 and not remaining_source.empty and not remaining_target.empty:
        edges = _candidate_edges(
            remaining_source,
            remaining_target,
            date_tolerance_days=int(date_tolerance_days),
        )
        if not edges.empty:
            unique_window_edges = _match_unique_edges(edges)
            if not unique_window_edges.empty:
                matched_pairs.append(
                    _pairs_from_edges(
                        unique_window_edges,
                        match_type="date_window_unique",
                    )
                )
                matched_source_ids.update(unique_window_edges["source_row_id"].astype("string").tolist())
                matched_target_ids.update(unique_window_edges["target_row_id"].astype("string").tolist())
                edges = _drop_matched_edges(edges, unique_window_edges)

            if not edges.empty:
                text_edges = edges.loc[edges["text_match"]].copy()
                unique_text_edges = _match_unique_edges(text_edges)
                if not unique_text_edges.empty:
                    matched_pairs.append(
                        _pairs_from_edges(
                            unique_text_edges,
                            match_type="date_window_text_tiebreak",
                        )
                    )
                    matched_source_ids.update(unique_text_edges["source_row_id"].astype("string").tolist())
                    matched_target_ids.update(unique_text_edges["target_row_id"].astype("string").tolist())
                    edges = _drop_matched_edges(edges, unique_text_edges)

            if not edges.empty:
                ambiguous_frames.append(
                    _window_ambiguous_rows(
                        edges,
                        reason="date_window_candidates_not_unique",
                    )
                )
                ambiguous_source_ids.update(edges["source_row_id"].astype("string").tolist())
                ambiguous_target_ids.update(edges["target_row_id"].astype("string").tolist())

    unmatched_source = prepared_source.loc[
        ~prepared_source["source_row_id"].astype("string").isin(
            matched_source_ids | ambiguous_source_ids
        )
    ].copy()
    unmatched_target = prepared_target.loc[
        ~prepared_target["target_row_id"].astype("string").isin(
            matched_target_ids | ambiguous_target_ids
        )
    ].copy()

    matched_pairs_df = (
        pd.concat([frame for frame in matched_pairs if not frame.empty], ignore_index=True)
        if matched_pairs
        else pd.DataFrame()
    )
    ambiguous_df = (
        pd.concat([frame for frame in ambiguous_frames if not frame.empty], ignore_index=True)
        if ambiguous_frames
        else pd.DataFrame()
    )

    return CrossBudgetMatchResult(
        matched_pairs_df=matched_pairs_df,
        unmatched_source_df=_finalize_unmatched(unmatched_source, status="unmatched_source").reset_index(drop=True),
        unmatched_target_df=_finalize_unmatched(unmatched_target, status="unmatched_target").reset_index(drop=True),
        ambiguous_matches_df=ambiguous_df.reset_index(drop=True),
    )
