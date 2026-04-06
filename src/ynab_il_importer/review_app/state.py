from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa

import ynab_il_importer.review_app.model as model
from ynab_il_importer.safe_types import normalize_flag_series
from ynab_il_importer.artifacts.transaction_schema import SPLIT_LINE_STRUCT


_SPLIT_LIST_DTYPE = pl.from_arrow(
    pa.table({"splits": pa.array([], type=pa.list_(SPLIT_LINE_STRUCT))})
).schema["splits"]
_REVIEW_DATA_TEXT_COLUMNS = [
    "fingerprint",
    "memo",
    "memo_append",
    "payee_options",
    "category_options",
    "source",
    "match_status",
    "decision_action",
    "update_maps",
    "account_name",
    "source_account",
    "target_account",
    "source_date",
    "target_date",
    "source_payee_current",
    "target_payee_current",
    "source_category_current",
    "target_category_current",
    "source_payee_selected",
    "target_payee_selected",
    "source_category_selected",
    "target_category_selected",
    "source_memo",
    "target_memo",
    "source_description_raw",
    "source_description_clean",
    "source_merchant_raw",
    "target_description_raw",
    "target_description_clean",
    "target_merchant_raw",
]
_REVIEW_DATA_BOOL_COLUMNS = ["reviewed", "source_present", "target_present"]


def _split_count(name: str) -> pl.Expr:
    return pl.col(name).list.len().fill_null(0).cast(pl.Int64)


def _split_text(name: str) -> pl.Expr:
    return (
        pl.col(name)
        .list.eval(
            pl.concat_str(
                [
                    pl.element().struct.field("split_id").fill_null(""),
                    pl.element().struct.field("payee_raw").fill_null(""),
                    pl.element().struct.field("category_raw").fill_null(""),
                    pl.element().struct.field("memo").fill_null(""),
                ],
                separator=" ",
                ignore_nulls=True,
            )
        )
        .list.join(" ")
        .fill_null("")
    ).alias(f"{name}_text")


def _with_split_schema(df: pl.DataFrame) -> pl.DataFrame:
    split_columns = [name for name in ("source_splits", "target_splits") if name in df.columns]
    if not split_columns:
        return df
    return df.with_columns(
        [pl.col(name).cast(_SPLIT_LIST_DTYPE, strict=False).alias(name) for name in split_columns]
    )


def _normalized_review_data_frame(df: pl.DataFrame) -> pl.DataFrame:
    df = _with_split_schema(df)
    text_columns = [name for name in _REVIEW_DATA_TEXT_COLUMNS if name in df.columns]
    bool_columns = [name for name in _REVIEW_DATA_BOOL_COLUMNS if name in df.columns]
    expressions: list[pl.Expr] = []
    expressions.extend(
        pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias(name)
        for name in text_columns
    )
    expressions.extend(
        pl.col(name).cast(pl.Boolean, strict=False).fill_null(False).alias(name)
        for name in bool_columns
    )
    if not expressions:
        return df
    return df.with_columns(expressions)


def canonical_review_helpers(df: pl.DataFrame) -> pl.DataFrame:
    df = _normalized_review_data_frame(df)
    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(False).alias("source_is_split"),
                pl.lit(False).alias("target_is_split"),
                pl.lit(0).alias("source_split_count"),
                pl.lit(0).alias("target_split_count"),
                pl.lit("").alias("source_display_payee"),
                pl.lit("").alias("target_display_payee"),
                pl.lit("").alias("source_display_category"),
                pl.lit("").alias("target_display_category"),
                pl.lit("").alias("source_display_account"),
                pl.lit("").alias("target_display_account"),
                pl.lit("").alias("source_display_date"),
                pl.lit("").alias("target_display_date"),
            ]
        )

    return df.with_columns(
        [
            _split_count("source_splits").gt(0).alias("source_is_split"),
            _split_count("target_splits").gt(0).alias("target_is_split"),
            _split_count("source_splits").alias("source_split_count"),
            _split_count("target_splits").alias("target_split_count"),
            pl.col("source_payee_current").alias("source_display_payee"),
            pl.col("target_payee_current").alias("target_display_payee"),
            pl.col("source_category_current").alias("source_display_category"),
            pl.col("target_category_current").alias("target_display_category"),
            pl.col("source_account").alias("source_display_account"),
            pl.col("target_account").alias("target_display_account"),
            pl.col("source_date").alias("source_display_date"),
            pl.col("target_date").alias("target_display_date"),
        ]
    )


def review_data_view(df: pd.DataFrame) -> pl.DataFrame:
    if df.empty:
        return pl.DataFrame(
            {
                "_row_pos": pl.Series([], dtype=pl.UInt32),
                "row_kind": pl.Series([], dtype=pl.Utf8),
                "action_label": pl.Series([], dtype=pl.Utf8),
                "reviewed_bool": pl.Series([], dtype=pl.Boolean),
                "has_suggestions": pl.Series([], dtype=pl.Boolean),
                "has_update_maps": pl.Series([], dtype=pl.Boolean),
                "missing_payee": pl.Series([], dtype=pl.Boolean),
                "missing_category": pl.Series([], dtype=pl.Boolean),
                "uncategorized_selected": pl.Series([], dtype=pl.Boolean),
                "search_text": pl.Series([], dtype=pl.Utf8),
            }
        )

    frame = pl.from_pandas(
        df,
        include_index=False,
        schema_overrides={
            "source_splits": _SPLIT_LIST_DTYPE,
            "target_splits": _SPLIT_LIST_DTYPE,
        },
    ).with_row_index("_row_pos")
    frame = _normalized_review_data_frame(frame)
    helpers = canonical_review_helpers(frame)

    target_payee_selected = pl.col("target_payee_selected")
    target_category_selected = pl.col("target_category_selected")
    source_payee_selected = pl.col("source_payee_selected")
    source_category_selected = pl.col("source_category_selected")
    source_present = pl.col("source_present")
    target_present = pl.col("target_present")
    action_expr = pl.when(pl.col("decision_action").eq("")).then(pl.lit("No decision")).otherwise(
        pl.col("decision_action")
    )
    update_maps_expr = pl.col("update_maps")
    has_update_maps = update_maps_expr.ne("")
    is_transfer = target_payee_selected.str.starts_with("Transfer :")
    no_category_required = target_category_selected.str.to_lowercase().eq(
        model.NO_CATEGORY_REQUIRED.casefold()
    )
    missing_payee = action_expr.eq("create_target") & target_payee_selected.eq("")
    missing_category = (
        action_expr.eq("create_target")
        & (target_category_selected.eq("") | no_category_required)
        & ~is_transfer
    )
    uncategorized_selected = (
        target_category_selected.str.to_lowercase().str.contains("uncategorized", literal=True)
        & ~is_transfer
    )
    has_suggestions = (
        (~source_present) & (source_payee_selected.ne("") | source_category_selected.ne(""))
    ) | (
        (~target_present)
        & (
            target_payee_selected.ne("")
            | target_category_selected.ne("")
            | pl.col("payee_options").ne("")
            | pl.col("category_options").ne("")
        )
    )

    row_kind = (
        pl.when(pl.col("match_status").str.to_lowercase().eq("matched_cleared"))
        .then(pl.lit("Matched cleared"))
        .when(pl.col("match_status").str.to_lowercase().eq("matched_auto"))
        .then(pl.lit("Matched"))
        .when(pl.col("match_status").str.to_lowercase().eq("source_only"))
        .then(pl.lit("Source only"))
        .when(pl.col("match_status").str.to_lowercase().eq("target_only"))
        .then(pl.lit("Target only"))
        .when(pl.col("match_status").str.to_lowercase().eq("ambiguous"))
        .then(pl.lit("Ambiguous"))
        .when(pl.col("match_status").str.to_lowercase().eq("unrecognized"))
        .then(pl.lit("Unrecognized"))
        .otherwise(pl.lit("Other"))
    )

    return helpers.with_columns(
        [
            action_expr.alias("action_label"),
            row_kind.alias("row_kind"),
            pl.col("reviewed").alias("reviewed_bool"),
            has_suggestions.alias("has_suggestions"),
            has_update_maps.alias("has_update_maps"),
            missing_payee.alias("missing_payee"),
            missing_category.alias("missing_category"),
            uncategorized_selected.alias("uncategorized_selected"),
            pl.col("source_memo").alias("source_transaction_memo"),
            pl.col("target_memo").alias("target_transaction_memo"),
            _split_text("source_splits"),
            _split_text("target_splits"),
        ]
    ).with_columns(
        [
            pl.concat_str(
                [
                    pl.col("fingerprint"),
                    pl.col("memo"),
                    pl.col("memo_append"),
                    pl.col("source_context_kind"),
                    pl.col("source_context_category_name"),
                    pl.col("source_context_matching_split_ids"),
                    pl.col("target_context_kind"),
                    pl.col("target_context_matching_split_ids"),
                    pl.col("source_description_raw"),
                    pl.col("source_description_clean"),
                    pl.col("source_merchant_raw"),
                    pl.col("target_description_raw"),
                    pl.col("target_description_clean"),
                    pl.col("target_merchant_raw"),
                    pl.col("payee_options"),
                    pl.col("category_options"),
                    pl.col("source_payee_current"),
                    source_payee_selected,
                    source_category_selected,
                    pl.col("target_payee_current"),
                    target_payee_selected,
                    target_category_selected,
                    pl.col("source_memo"),
                    pl.col("target_memo"),
                    pl.col("source_account"),
                    pl.col("target_account"),
                    pl.col("account_name"),
                    pl.col("source"),
                    action_expr,
                    update_maps_expr,
                    pl.col("source_splits_text"),
                    pl.col("target_splits_text"),
                ],
                separator=" ",
                ignore_nulls=True,
            )
            .str.to_lowercase()
            .alias("search_text"),
        ]
    )


def review_filter_state_view(
    data_view: pl.DataFrame,
    *,
    blocker_series: pd.Series,
    save_state: pd.Series,
    changed_mask: pd.Series | None = None,
    uncategorized_mask: pd.Series | None = None,
) -> pl.DataFrame:
    if data_view.is_empty():
        return pl.DataFrame(
            {
                "_row_pos": pl.Series([], dtype=pl.UInt32),
                "save_state": pl.Series([], dtype=pl.Utf8),
                "blocker_label": pl.Series([], dtype=pl.Utf8),
                "suggestion_label": pl.Series([], dtype=pl.Utf8),
                "map_update_label": pl.Series([], dtype=pl.Utf8),
                "primary_state": pl.Series([], dtype=pl.Utf8),
                "changed_bool": pl.Series([], dtype=pl.Boolean),
                "uncategorized_bool": pl.Series([], dtype=pl.Boolean),
            }
        )

    blocker_values = blocker_series.astype("string").fillna("").tolist()
    save_values = save_state.astype("string").fillna("Unsaved").tolist()
    changed_values = (
        changed_mask.astype(bool).tolist()
        if isinstance(changed_mask, pd.Series)
        else [False] * data_view.height
    )
    uncategorized_values = (
        uncategorized_mask.astype(bool).tolist()
        if isinstance(uncategorized_mask, pd.Series)
        else [False] * data_view.height
    )
    return data_view.select("_row_pos", "reviewed_bool", "action_label", "has_suggestions", "has_update_maps").with_columns(
        [
            pl.Series("blocker_label", blocker_values),
            pl.Series("save_state", save_values),
            pl.Series("changed_bool", changed_values),
            pl.Series("uncategorized_bool", uncategorized_values),
            pl.when(pl.col("has_suggestions"))
            .then(pl.lit("Has suggestions"))
            .otherwise(pl.lit("No suggestions"))
            .alias("suggestion_label"),
            pl.when(pl.col("has_update_maps"))
            .then(pl.lit("Has update_maps"))
            .otherwise(pl.lit("No update_maps"))
            .alias("map_update_label"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("reviewed_bool") & pl.col("blocker_label").is_in(["", "None"]))
            .then(pl.lit("Settled"))
            .when(~pl.col("blocker_label").is_in(["", "None"]))
            .then(pl.lit("Fix"))
            .when(pl.col("action_label").ne("No decision"))
            .then(pl.lit("Decide"))
            .otherwise(pl.lit("Fix"))
            .alias("primary_state"),
        ]
    ).select(
        "_row_pos",
        "save_state",
        "blocker_label",
        "suggestion_label",
        "map_update_label",
        "primary_state",
        "changed_bool",
        "uncategorized_bool",
    )


def filtered_row_indices_from_views(
    data_view: pl.DataFrame,
    state_view: pl.DataFrame,
    index: pd.Index | list[Any],
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    search_query: str,
) -> list[Any]:
    index_values = list(index)
    if data_view.is_empty() or state_view.is_empty() or not index_values:
        return []

    working_view = data_view.join(state_view, on="_row_pos", how="inner")
    filtered = working_view.filter(
        pl.col("primary_state").is_in(primary_state)
        & pl.col("row_kind").is_in(row_kind)
        & pl.col("action_label").is_in(action_filter)
        & pl.col("save_state").is_in(save_status)
        & pl.col("blocker_label").is_in(blocker_filter)
        & pl.col("suggestion_label").is_in(suggestion_filter)
        & pl.col("map_update_label").is_in(map_update_filter)
    )
    query = str(search_query or "").strip()
    if query:
        filtered = filtered.filter(pl.col("search_text").str.contains(query, literal=True))

    positions = filtered.select(pl.col("_row_pos").cast(pl.Int64)).to_series().to_list()
    return [
        index_values[pos]
        for pos in positions
        if isinstance(pos, int) and 0 <= pos < len(index_values)
    ]


def view_row_lookup(
    view: pl.DataFrame,
    index: pd.Index | list[Any],
) -> dict[Any, dict[str, Any]]:
    index_values = list(index)
    if view.is_empty() or not index_values or "_row_pos" not in view.columns:
        return {}
    lookup: dict[Any, dict[str, Any]] = {}
    for row in view.to_dicts():
        pos = row.get("_row_pos")
        if not isinstance(pos, int) or pos < 0 or pos >= len(index_values):
            continue
        lookup[index_values[pos]] = row
    return lookup


def series_or_default(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].astype("string").fillna("")
    if col == "payee_selected" and "target_payee_selected" in df.columns:
        return df["target_payee_selected"].astype("string").fillna("")
    if col == "category_selected" and "target_category_selected" in df.columns:
        return df["target_category_selected"].astype("string").fillna("")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _decision_action_series(df: pd.DataFrame) -> pd.Series:
    return (
        series_or_default(df, "decision_action")
        .str.strip()
        .replace("", "No decision")
        .str.casefold()
    )


def _update_maps_series(df: pd.DataFrame) -> pd.Series:
    if "update_maps" in df.columns:
        return df["update_maps"].astype("string").fillna("").str.strip()
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return normalize_flag_series(df[col])


def _id_series(df: pd.DataFrame, col: str) -> pd.Series:
    return series_or_default(df, col).str.strip()


def _id_list(df: pd.DataFrame | pl.DataFrame, col: str) -> list[str]:
    if isinstance(df, pd.DataFrame):
        return _id_series(df, col).tolist()
    if col not in df.columns:
        return [""] * df.height
    return [
        str(value or "").strip()
        for value in df.select(pl.col(col).cast(pl.Utf8, strict=False).fill_null("")).to_series().to_list()
    ]


def _component_mask_from_map(
    df: pd.DataFrame,
    idx: Any,
    component_map: dict[Any, int],
) -> pd.Series:
    component_label = component_map.get(idx)
    if component_label is None:
        return pd.Series([False] * len(df), index=df.index)
    return pd.Series(
        [component_map.get(current_idx) == component_label for current_idx in df.index],
        index=df.index,
    )


def _missing_value_masks(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    decision_action = _decision_action_series(df)
    create_target = decision_action.eq("create_target")
    payee_blank = series_or_default(df, "payee_selected").str.strip() == ""
    payee = series_or_default(df, "payee_selected").str.strip()
    category = series_or_default(df, "category_selected").map(model.normalize_category_value)
    category_blank = category.eq("")
    no_category_required = category.map(model.is_no_category_required)
    transfer_payee = payee.map(model.is_transfer_payee)
    missing_payee = create_target & payee_blank
    missing_category = create_target & (category_blank | no_category_required) & ~transfer_payee
    return missing_payee, missing_category, create_target


def unresolved_mask(df: pd.DataFrame) -> pd.Series:
    missing_payee, missing_category, _ = _missing_value_masks(df)
    decision_action = _decision_action_series(df)
    match_status = series_or_default(df, "match_status").str.strip().str.casefold()
    pending_decision = match_status.isin(["ambiguous", "source_only", "target_only"]) & decision_action.eq("no decision")
    return missing_payee | missing_category | pending_decision


def summary_counts(df: pd.DataFrame) -> dict[str, int]:
    missing_payee, missing_category, _ = _missing_value_masks(df)
    unresolved = unresolved_mask(df)
    update_maps = _update_maps_series(df).ne("")
    return {
        "total": len(df),
        "missing_payee": int(missing_payee.sum()),
        "missing_category": int(missing_category.sum()),
        "unresolved": int(unresolved.sum()),
        "update_maps": int(update_maps.sum()),
    }


def accept_defaults_mask(df: pd.DataFrame) -> pd.Series:
    return pd.Series([False] * len(df), index=df.index)


def modified_mask(df: pd.DataFrame, original: pd.DataFrame | None) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = [
        col
        for col in [
            "source_payee_selected",
            "source_category_selected",
            "target_payee_selected",
            "target_category_selected",
            "update_maps",
            "decision_action",
            "reviewed",
        ]
        if col in df.columns and col in original.columns
    ]
    if not cols:
        return pd.Series([False] * len(df), index=df.index)
    return (df[cols] != original[cols]).any(axis=1)


def modified_count(df: pd.DataFrame, original: pd.DataFrame | None) -> int:
    return int(modified_mask(df, original).sum())


def changed_mask(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    if "changed" in df.columns:
        return _bool_series(df, "changed")
    if base is None or base.empty:
        return pd.Series([False] * len(df), index=df.index)
    cols = [
        col
        for col in [
            "source_payee_selected",
            "source_category_selected",
            "target_payee_selected",
            "target_category_selected",
            "update_maps",
            "decision_action",
            "reviewed",
        ]
        if col in df.columns and col in base.columns
    ]
    if not cols:
        return pd.Series([False] * len(df), index=df.index)
    if "transaction_id" in df.columns and "transaction_id" in base.columns:
        df_ids = df["transaction_id"].astype("string").fillna("")
        base_ids = base["transaction_id"].astype("string").fillna("")
        df_keys = df_ids + "|" + df_ids.groupby(df_ids).cumcount().astype("string")
        base_keys = base_ids + "|" + base_ids.groupby(base_ids).cumcount().astype("string")
        current = df.assign(_key=df_keys).set_index("_key")[cols].copy()
        baseline = base.assign(_key=base_keys).set_index("_key")[cols].copy()
        aligned = baseline.reindex(current.index)
        missing_in_base = aligned.isna().all(axis=1)
        changed = (current != aligned).any(axis=1) | missing_in_base
        return pd.Series(changed.to_numpy(), index=df.index)

    current = df[cols].copy()
    baseline = base[cols].reindex(df.index)
    missing_in_base = baseline.isna().all(axis=1)
    return (current != baseline).any(axis=1) | missing_in_base


def saved_mask(original: pd.DataFrame | None, base: pd.DataFrame | None, current_index: pd.Index) -> pd.Series:
    if original is None or original.empty:
        return pd.Series([False] * len(current_index), index=current_index)

    changed = changed_mask(original, base).reindex(original.index, fill_value=False)
    reviewed = _bool_series(original, "reviewed")

    saved = (changed | reviewed).reindex(current_index, fill_value=False)
    return saved.astype(bool)


def apply_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()

    match_status = filters.get("match_status")
    if match_status:
        filtered = filtered[filtered["match_status"].isin(match_status)]

    reviewed = _bool_series(df, "reviewed")
    reviewed_mode = str(filters.get("reviewed_mode", "") or "").strip().lower()
    if reviewed_mode == "unreviewed":
        filtered = filtered[~reviewed.reindex(filtered.index, fill_value=False)]
    elif reviewed_mode == "reviewed":
        filtered = filtered[reviewed.reindex(filtered.index, fill_value=False)]

    missing_payee, missing_category, _ = _missing_value_masks(filtered)
    unresolved = unresolved_mask(filtered)
    if filters.get("unresolved_only"):
        filtered = filtered[unresolved]
    if filters.get("missing_payee_only"):
        filtered = filtered[missing_payee]
    if filters.get("missing_category_only"):
        filtered = filtered[missing_category]

    fingerprint_query = str(filters.get("fingerprint_query", "") or "").strip().casefold()
    if fingerprint_query:
        filtered = filtered[
            series_or_default(filtered, "fingerprint").str.casefold().str.contains(fingerprint_query, regex=False)
        ]

    payee_query = str(filters.get("payee_query", "") or "").strip().casefold()
    if payee_query:
        payee_text = series_or_default(filtered, "payee_selected") + " " + series_or_default(filtered, "payee_options")
        filtered = filtered[payee_text.str.casefold().str.contains(payee_query, regex=False)]

    memo_query = str(filters.get("memo_query", "") or "").strip().casefold()
    if memo_query:
        memo_text = series_or_default(filtered, "memo") + " " + series_or_default(filtered, "description_raw") + " " + series_or_default(filtered, "description_clean")
        filtered = filtered[memo_text.str.casefold().str.contains(memo_query, regex=False)]

    source_query = str(filters.get("source_query", "") or "").strip().casefold()
    if source_query:
        filtered = filtered[
            series_or_default(filtered, "source").str.casefold().str.contains(source_query, regex=False)
        ]

    account_query = str(filters.get("account_query", "") or "").strip().casefold()
    if account_query:
        filtered = filtered[
            series_or_default(filtered, "account_name").str.casefold().str.contains(account_query, regex=False)
        ]

    return filtered


def _clean_text_list(values: Any) -> list[str]:
    if isinstance(values, pd.Series):
        raw_values = values.astype("string").fillna("").tolist()
    elif isinstance(values, pl.Series):
        raw_values = values.cast(pl.Utf8, strict=False).fill_null("").to_list()
    elif isinstance(values, list):
        raw_values = values
    else:
        raw_values = list(values) if values is not None else []
    return [str(value or "").strip() for value in raw_values if str(value or "").strip()]


def _most_common_from_values(values: list[str]) -> str:
    if not values:
        return ""
    counts = Counter(values)
    best_count = max(counts.values())
    for value in values:
        if counts[value] == best_count:
            return value
    return ""


def most_common_value(series: pl.Series | list[str]) -> str:
    return _most_common_from_values(_clean_text_list(series))


def most_common_by_fingerprint(df: pl.DataFrame, column: str) -> dict[str, str]:
    if "fingerprint" not in df.columns or column not in df.columns:
        return {}
    fingerprints = [
        str(value or "").strip()
        for value in df.select(
            pl.col("fingerprint").cast(pl.Utf8, strict=False).fill_null("")
        ).to_series().to_list()
    ]
    values = [
        str(value or "").strip()
        for value in df.select(pl.col(column).cast(pl.Utf8, strict=False).fill_null("")).to_series().to_list()
    ]
    grouped: dict[str, list[str]] = {}
    for fp, value in zip(fingerprints, values, strict=False):
        fp_text = str(fp or "").strip()
        value_text = str(value or "").strip()
        if not fp_text or not value_text:
            continue
        grouped.setdefault(fp_text, []).append(value_text)
    result: dict[str, str] = {}
    for fp, grouped_values in grouped.items():
        best = _most_common_from_values(grouped_values)
        if best:
            result[fp] = best
    return result


def grouped_row_indices(filtered: pl.DataFrame) -> tuple[list[str], dict[str, list[Any]]]:
    indices = list(range(filtered.height))
    if "fingerprint" in filtered.columns:
        fingerprints = [
            str(value or "").strip()
            for value in filtered.select(
                pl.col("fingerprint").cast(pl.Utf8, strict=False).fill_null("")
            ).to_series().to_list()
        ]
    else:
        fingerprints = [""] * filtered.height
    group_indices: dict[str, list[Any]] = {}
    counts: Counter[str] = Counter()
    first_seen: list[str] = []
    for idx, fingerprint in zip(indices, fingerprints, strict=False):
        fp = str(fingerprint or "").strip()
        if not fp:
            continue
        if fp not in group_indices:
            group_indices[fp] = []
            first_seen.append(fp)
        group_indices[fp].append(idx)
        counts[fp] += 1
    ordered = sorted(first_seen, key=lambda fp: (-counts[fp], first_seen.index(fp)))
    return ordered, group_indices


def required_category_missing_mask(df: pd.DataFrame) -> pd.Series:
    payee = series_or_default(df, "payee_selected").str.strip()
    category = series_or_default(df, "category_selected").map(model.normalize_category_value)
    transfer = payee.map(model.is_transfer_payee)
    return (category.eq("") | category.map(model.is_no_category_required)) & ~transfer


def uncategorized_mask(df: pd.DataFrame) -> pd.Series:
    payee = series_or_default(df, "payee_selected").str.strip()
    transfer = payee.map(model.is_transfer_payee)
    category = (
        series_or_default(df, "category_selected")
        .map(model.normalize_category_value)
        .str.casefold()
    )
    return category.str.contains("uncategorized", regex=False) & ~transfer


def truthy_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return normalize_flag_series(df[column])


def primary_state_series(df: pd.DataFrame, blocker_series: pd.Series) -> pd.Series:
    reviewed = truthy_series(df, "reviewed")
    blocker = blocker_series.astype("string").fillna("").str.strip()
    action = action_series(df)
    states: list[str] = []
    for idx in df.index:
        if bool(reviewed.loc[idx]) and blocker.loc[idx] in {"", "None"}:
            states.append("Settled")
        elif blocker.loc[idx] not in {"", "None"}:
            states.append("Fix")
        elif action.loc[idx] != "No decision":
            states.append("Decide")
        else:
            states.append("Fix")
    return pd.Series(states, index=df.index, dtype="string")


def row_kind_series(df: pd.DataFrame) -> pd.Series:
    match_status = series_or_default(df, "match_status").str.strip().str.casefold()
    labels = pd.Series(["Other"] * len(df), index=df.index, dtype="string")
    labels = labels.where(~match_status.eq("matched_cleared"), "Matched cleared")
    labels = labels.where(~match_status.eq("matched_auto"), "Matched")
    labels = labels.where(~match_status.eq("source_only"), "Source only")
    labels = labels.where(~match_status.eq("target_only"), "Target only")
    labels = labels.where(~match_status.eq("ambiguous"), "Ambiguous")
    labels = labels.where(~match_status.eq("unrecognized"), "Unrecognized")
    return labels


def action_series(df: pd.DataFrame) -> pd.Series:
    import ynab_il_importer.review_app.validation as review_validation

    return review_validation.normalize_decision_actions(
        series_or_default(df, "decision_action")
    ).astype("string")


def suggestion_series(df: pd.DataFrame) -> pd.Series:
    source_present = truthy_series(df, "source_present")
    target_present = truthy_series(df, "target_present")
    source_payee_selected = series_or_default(df, "source_payee_selected").str.strip()
    source_category_selected = series_or_default(df, "source_category_selected").str.strip()
    target_payee_selected = series_or_default(df, "target_payee_selected").str.strip()
    target_category_selected = series_or_default(df, "target_category_selected").str.strip()
    payee_options = series_or_default(df, "payee_options").str.strip()
    category_options = series_or_default(df, "category_options").str.strip()
    has_missing_side_suggestions = (
        ~source_present
        & (
            source_payee_selected.ne("")
            | source_category_selected.ne("")
        )
    ) | (
        ~target_present
        & (
            target_payee_selected.ne("")
            | target_category_selected.ne("")
            | payee_options.ne("")
            | category_options.ne("")
        )
    )
    return pd.Series(
        ["Has suggestions" if bool(value) else "No suggestions" for value in has_missing_side_suggestions],
        index=df.index,
        dtype="string",
    )


def map_update_filter_series(df: pd.DataFrame) -> pd.Series:
    has_updates = series_or_default(df, "update_maps").str.strip().ne("")
    return pd.Series(
        ["Has update_maps" if bool(value) else "No update_maps" for value in has_updates],
        index=df.index,
        dtype="string",
    )


def state_matrix_counts(
    primary_state_series: pd.Series | pl.Series | list[str],
    save_state_series: pd.Series | pl.Series | list[str],
) -> dict[str, int]:
    primary_values = _clean_text_list(primary_state_series)
    save_values = _clean_text_list(save_state_series)
    counts: Counter[str] = Counter()
    for primary, save_state in zip(primary_values, save_values, strict=False):
        counts[f"{primary} / {save_state}"] += 1
    return dict(counts)


def search_text_series(df: pd.DataFrame) -> pd.Series:
    columns = [
        "fingerprint",
        "memo",
        "memo_append",
        "description_raw",
        "description_clean",
        "payee_options",
        "category_options",
        "source_payee_current",
        "source_payee_selected",
        "source_category_selected",
        "target_payee_current",
        "target_payee_selected",
        "target_category_selected",
        "source_memo",
        "target_memo",
        "source_account",
        "target_account",
        "account_name",
        "source",
        "decision_action",
        "update_maps",
    ]
    parts = [series_or_default(df, column) for column in columns]
    text = pd.Series([""] * len(df), index=df.index, dtype="string")
    for part in parts:
        text = text + " " + part.astype("string").fillna("")
    return text.str.casefold()


def _row_key_series(df: pd.DataFrame) -> pd.Series:
    if "transaction_id" not in df.columns:
        return pd.Series(df.index.astype("string"), index=df.index, dtype="string")
    txn_id = df["transaction_id"].astype("string").fillna("")
    occurrence = txn_id.groupby(txn_id).cumcount().astype("string")
    return txn_id + "|" + occurrence


def derive_inference_tags(df: pd.DataFrame) -> pd.Series:
    match_status = series_or_default(df, "match_status").str.strip().str.lower()
    payee = series_or_default(df, "payee_selected").str.strip()
    missing_required = payee.eq("") | required_category_missing_mask(df)

    inferred = pd.Series(["unique"] * len(df), index=df.index, dtype="string")
    inferred = inferred.where(~match_status.eq("none"), "unrecognized")
    inferred = inferred.where(~match_status.eq("ambiguous"), "ambiguous")
    inferred = inferred.where(
        ~(~match_status.isin(["none", "ambiguous"]) & missing_required), "missing"
    )
    unknown = (
        ~match_status.isin(["", "none", "ambiguous", "unique"])
        & ~missing_required
    )
    inferred = inferred.where(~unknown, match_status)
    return inferred


def initial_inference_tags(df: pd.DataFrame, base: pd.DataFrame | None) -> pd.Series:
    fallback = derive_inference_tags(df)
    if base is None or base.empty:
        return fallback

    base_keys = _row_key_series(base)
    base_inference = derive_inference_tags(base)
    base_map = pd.Series(base_inference.to_numpy(), index=base_keys)

    current_keys = _row_key_series(df)
    aligned = current_keys.map(base_map)
    return aligned.fillna(fallback).astype("string")


def apply_row_filters(
    df: pd.DataFrame,
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    primary_state_series: pd.Series,
    row_kind_series: pd.Series,
    action_series: pd.Series,
    save_state: pd.Series,
    blocker_series: pd.Series,
    suggestion_series: pd.Series,
    map_update_series: pd.Series,
    search_query: str,
    search_text: pd.Series,
) -> pd.DataFrame:
    mask = pd.Series([True] * len(df), index=df.index)
    mask &= primary_state_series.isin(primary_state)
    mask &= row_kind_series.isin(row_kind)
    mask &= action_series.isin(action_filter)
    mask &= save_state.isin(save_status)
    mask &= blocker_series.isin(blocker_filter)
    mask &= suggestion_series.isin(suggestion_filter)
    mask &= map_update_series.isin(map_update_filter)

    if search_query:
        mask &= search_text.str.contains(search_query, regex=False)

    return df[mask]


def filtered_row_indices(
    index: pd.Index | list[Any],
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    primary_state_series: pd.Series,
    row_kind_series: pd.Series,
    action_series: pd.Series,
    save_state: pd.Series,
    blocker_series: pd.Series,
    suggestion_series: pd.Series,
    map_update_series: pd.Series,
    search_query: str,
    search_text: pd.Series,
) -> list[Any]:
    indices = list(index)
    if not indices:
        return []

    allowed_primary = set(primary_state)
    allowed_row_kind = set(row_kind)
    allowed_action = set(action_filter)
    allowed_save_status = set(save_status)
    allowed_blocker = set(blocker_filter)
    allowed_suggestion = set(suggestion_filter)
    allowed_map_update = set(map_update_filter)

    primary_values = (
        primary_state_series.reindex(indices).astype("string").fillna("").tolist()
    )
    row_kind_values = row_kind_series.reindex(indices).astype("string").fillna("").tolist()
    action_values = action_series.reindex(indices).astype("string").fillna("").tolist()
    save_values = save_state.reindex(indices).astype("string").fillna("").tolist()
    blocker_values = blocker_series.reindex(indices).astype("string").fillna("").tolist()
    suggestion_values = (
        suggestion_series.reindex(indices).astype("string").fillna("").tolist()
    )
    map_update_values = (
        map_update_series.reindex(indices).astype("string").fillna("").tolist()
    )
    search_values = search_text.reindex(indices).astype("string").fillna("").tolist()

    selected: list[Any] = []
    query = str(search_query or "")
    for idx, primary, kind, action, save_value, blocker, suggestion, map_update, text in zip(
        indices,
        primary_values,
        row_kind_values,
        action_values,
        save_values,
        blocker_values,
        suggestion_values,
        map_update_values,
        search_values,
        strict=False,
    ):
        if primary not in allowed_primary:
            continue
        if kind not in allowed_row_kind:
            continue
        if action not in allowed_action:
            continue
        if save_value not in allowed_save_status:
            continue
        if blocker not in allowed_blocker:
            continue
        if suggestion not in allowed_suggestion:
            continue
        if map_update not in allowed_map_update:
            continue
        if query and query not in str(text or ""):
            continue
        selected.append(idx)
    return selected


def related_row_indices(
    df: pd.DataFrame | pl.DataFrame,
    idx: Any,
    *,
    include_source: bool = False,
    include_target: bool = False,
) -> list[Any]:
    if isinstance(df, pd.DataFrame):
        if idx not in df.index:
            return []
        index_values = list(df.index)
        source_ids = _id_series(df, "source_row_id").tolist()
        target_ids = _id_series(df, "target_row_id").tolist()
        try:
            pos = index_values.index(idx)
        except ValueError:
            return []
    else:
        if not isinstance(idx, int) or idx < 0 or idx >= df.height:
            return []
        index_values = list(range(df.height))
        source_ids = _id_list(df, "source_row_id")
        target_ids = _id_list(df, "target_row_id")
        pos = idx

    matched: list[Any] = [idx]
    matched_set = {idx}
    source_row_id = source_ids[pos] if pos < len(source_ids) else ""
    target_row_id = target_ids[pos] if pos < len(target_ids) else ""

    for current_idx, current_source_id, current_target_id in zip(
        index_values,
        source_ids,
        target_ids,
        strict=False,
    ):
        include = False
        if include_source and source_row_id and current_source_id == source_row_id:
            include = True
        if include_target and target_row_id and current_target_id == target_row_id:
            include = True
        if include and current_idx not in matched_set:
            matched.append(current_idx)
            matched_set.add(current_idx)
    return matched


def related_rows_mask(
    df: pd.DataFrame,
    idx: Any,
    *,
    include_source: bool = False,
    include_target: bool = False,
) -> pd.Series:
    indices = related_row_indices(
        df,
        idx,
        include_source=include_source,
        include_target=include_target,
    )
    mask = pd.Series([False] * len(df), index=df.index)
    for related_idx in indices:
        if related_idx in mask.index:
            mask.loc[related_idx] = True
    return mask


def _original_side_present(df: pd.DataFrame, idx: Any, side: str) -> bool:
    original_column = f"{side}_original_transaction"
    if original_column in df.columns:
        value = df.at[idx, original_column]
        return isinstance(value, dict) and bool(value)
    present_column = f"{side}_present"
    if present_column in df.columns:
        return bool(df.at[idx, present_column])
    return False


def _presence_after_action(
    *,
    source_present_original: bool,
    target_present_original: bool,
    action: str,
) -> tuple[bool, bool]:
    import ynab_il_importer.review_app.validation as review_validation

    normalized = review_validation.normalize_decision_action(action)
    source_present = source_present_original
    target_present = target_present_original

    if normalized == "create_source":
        source_present = True
    elif normalized == "create_target":
        target_present = True
    elif normalized == "delete_source":
        source_present = False
    elif normalized == "delete_target":
        target_present = False
    elif normalized == "delete_both":
        source_present = False
        target_present = False

    return source_present, target_present


def _recompute_presence(df: pd.DataFrame, indices: list[Any]) -> pd.DataFrame:
    if "source_present" not in df.columns or "target_present" not in df.columns:
        return df
    updated = df.copy()
    import ynab_il_importer.review_app.validation as review_validation

    for idx in indices:
        if idx not in updated.index:
            continue
        action = (
            updated.at[idx, "decision_action"]
            if "decision_action" in updated.columns
            else review_validation.NO_DECISION
        )
        source_present, target_present = _presence_after_action(
            source_present_original=_original_side_present(updated, idx, "source"),
            target_present_original=_original_side_present(updated, idx, "target"),
            action=str(action).strip(),
        )
        updated.at[idx, "source_present"] = source_present
        updated.at[idx, "target_present"] = target_present
    return updated


def _transaction_reference_column(side: str, *, kind: str) -> str:
    return f"{side}_{kind}_transaction"


def _review_record_row(row: pd.Series) -> dict[str, Any]:
    import ynab_il_importer.review_app.io as review_io

    table = review_io.coerce_review_artifact_table(pd.DataFrame([row.to_dict()]))
    rows = table.to_pylist()
    return rows[0] if rows else {}


def _transaction_reference_from_row(
    row: pd.Series,
    *,
    side: str,
    kind: str,
) -> dict[str, Any] | None:
    import ynab_il_importer.review_app.io as review_io

    column = _transaction_reference_column(side, kind=kind)
    value = row.get(column)
    if isinstance(value, dict):
        normalized = review_io._normalize_transaction_record(value)
        return dict(normalized) if isinstance(normalized, dict) else None
    try:
        record = _review_record_row(row)
    except ValueError:
        return None
    resolved = record.get(f"{side}_{kind}")
    normalized = review_io._normalize_transaction_record(resolved)
    return dict(normalized) if isinstance(normalized, dict) else None


def _category_id_for_transaction_value(
    row: pd.Series,
    *,
    side: str,
    category_value: str,
    current_txn: dict[str, Any] | None,
) -> str:
    normalized_category = model.normalize_category_value(category_value)
    for candidate in [
        current_txn,
        _transaction_reference_from_row(row, side=side, kind="original"),
    ]:
        if not isinstance(candidate, dict):
            continue
        if model.normalize_category_value(candidate.get("category_raw")) == normalized_category:
            return str(candidate.get("category_id", "") or "").strip()
    return ""


def _update_current_transaction_values(
    df: pd.DataFrame,
    indices: list[Any],
    *,
    side: str,
    payee: str | None = None,
    category: str | None = None,
) -> pd.DataFrame:
    updated = df.copy()
    column = _transaction_reference_column(side, kind="current")
    touched = [current_idx for current_idx in indices if current_idx in updated.index]
    for current_idx in touched:
        row = updated.loc[current_idx]
        txn = _transaction_reference_from_row(row, side=side, kind="current")
        if txn is None:
            continue
        prior_txn = dict(txn)
        if payee is not None:
            txn["payee_raw"] = str(payee).strip()
        if category is not None:
            normalized_category = model.normalize_category_value(category)
            txn["category_raw"] = normalized_category
            txn["category_id"] = _category_id_for_transaction_value(
                row,
                side=side,
                category_value=normalized_category,
                current_txn=prior_txn,
            )
        updated.at[current_idx, column] = txn
    return updated


def rebuild_working_rows(df: pd.DataFrame, indices: list[Any]) -> pd.DataFrame:
    import ynab_il_importer.review_app.io as review_io
    import ynab_il_importer.review_app.working_schema as working_schema

    touched = [current_idx for current_idx in dict.fromkeys(indices) if current_idx in df.index]
    if not touched:
        return df.copy()

    updated = df.copy()
    subset = df.loc[touched].copy()
    missing_input = working_schema.missing_working_columns(
        subset.columns,
        working_schema.WORKING_INPUT_REQUIRED_COLUMNS,
    )
    if missing_input:
        return updated
    rebuilt = review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(review_io.coerce_review_artifact_table(subset))
    ).to_pandas()
    if len(rebuilt) != len(subset):
        return updated
    rebuilt.index = subset.index

    for column in rebuilt.columns:
        if column not in updated.columns:
            updated[column] = pd.Series([None] * len(updated), index=updated.index, dtype="object")
        updated.loc[touched, column] = rebuilt.loc[touched, column]
    return updated


def recompute_changed_for_rows(df: pd.DataFrame, indices: list[Any]) -> pd.DataFrame:
    if "changed" not in df.columns:
        return df.copy()

    updated = df.copy()
    touched = [current_idx for current_idx in dict.fromkeys(indices) if current_idx in updated.index]
    for current_idx in touched:
        row = updated.loc[current_idx]
        source_current = _transaction_reference_from_row(row, side="source", kind="current")
        source_original = _transaction_reference_from_row(row, side="source", kind="original")
        target_current = _transaction_reference_from_row(row, side="target", kind="current")
        target_original = _transaction_reference_from_row(row, side="target", kind="original")
        updated.at[current_idx, "changed"] = bool(
            source_current != source_original or target_current != target_original
        )
    return updated


def _signed_amount_from_row_values(*, inflow: Any, outflow: Any) -> float:
    inflow_value = float(pd.to_numeric(pd.Series([inflow]), errors="coerce").fillna(0.0).iloc[0])
    outflow_value = float(pd.to_numeric(pd.Series([outflow]), errors="coerce").fillna(0.0).iloc[0])
    return inflow_value - outflow_value


def _target_transaction_for_split_edit(row: pd.Series) -> dict[str, Any]:
    import ynab_il_importer.review_app.io as review_io

    current = _transaction_reference_from_row(row, side="target", kind="current")
    if bool(row.get("target_present", False)) and current is not None:
        return current
    original = _transaction_reference_from_row(row, side="target", kind="original")
    return review_io._transaction_from_flat_row(
        row,
        side="target",
        use_selected_values=True,
        base_transaction=original,
    )


def _normalize_split_editor_lines(
    lines: list[dict[str, Any]],
    *,
    parent_transaction: dict[str, Any],
) -> list[dict[str, Any]]:
    normalized_lines: list[dict[str, Any]] = []
    existing_splits = parent_transaction.get("splits")
    existing_by_id: dict[str, dict[str, Any]] = {}
    if isinstance(existing_splits, list):
        for split in existing_splits:
            if not isinstance(split, dict):
                continue
            existing_split_id = str(split.get("split_id", "") or "").strip()
            if existing_split_id:
                existing_by_id[existing_split_id] = split
    parent_transaction_id = str(
        parent_transaction.get("transaction_id")
        or parent_transaction.get("parent_transaction_id")
        or ""
    ).strip()

    for index, raw in enumerate(lines, start=1):
        if not isinstance(raw, dict):
            continue
        amount_value = float(
            pd.to_numeric(
                pd.Series([raw.get("amount_ils", raw.get("amount", 0.0))]),
                errors="coerce",
            ).fillna(0.0).iloc[0]
        )
        payee_value = str(raw.get("payee_raw", raw.get("payee", "")) or "").strip()
        category_value = model.normalize_category_value(
            raw.get("category_raw", raw.get("category", ""))
        )
        memo_value = str(raw.get("memo", "") or "").strip()
        split_id = str(raw.get("split_id", "") or "").strip()
        existing_line = existing_by_id.get(split_id, {})
        if not split_id and not payee_value and not category_value and not memo_value and abs(amount_value) <= 1e-9:
            continue
        existing_category = model.normalize_category_value(existing_line.get("category_raw", ""))
        preserve_category_id = (
            str(existing_line.get("category_id", "") or "").strip()
            if split_id and category_value == existing_category
            else ""
        )
        normalized_lines.append(
            {
                "split_id": split_id or f"{parent_transaction_id or 'split'}-{index}",
                "parent_transaction_id": str(
                    raw.get("parent_transaction_id")
                    or existing_line.get("parent_transaction_id")
                    or parent_transaction_id
                ).strip(),
                "ynab_subtransaction_id": str(
                    raw.get("ynab_subtransaction_id")
                    or existing_line.get("ynab_subtransaction_id")
                    or ""
                ).strip(),
                "payee_raw": payee_value,
                "category_id": str(
                    raw.get("category_id")
                    or preserve_category_id
                    or ""
                ).strip(),
                "category_raw": category_value,
                "memo": memo_value,
                "inflow_ils": amount_value if amount_value > 0 else 0.0,
                "outflow_ils": -amount_value if amount_value < 0 else 0.0,
                "import_id": str(
                    raw.get("import_id")
                    or existing_line.get("import_id")
                    or ""
                ).strip(),
                "matched_transaction_id": str(
                    raw.get("matched_transaction_id")
                    or existing_line.get("matched_transaction_id")
                    or ""
                ).strip(),
            }
        )
    return normalized_lines


def _apply_target_split_lines_to_transaction(
    row: pd.Series,
    *,
    target_transaction: dict[str, Any],
    split_lines: list[dict[str, Any]],
) -> dict[str, Any]:
    updated_target = dict(target_transaction)
    if len(split_lines) == 1:
        only_line = split_lines[0]
        category_value = model.normalize_category_value(only_line.get("category_raw", ""))
        updated_target["payee_raw"] = str(only_line.get("payee_raw", "") or "").strip()
        updated_target["category_raw"] = category_value
        updated_target["category_id"] = _category_id_for_transaction_value(
            row,
            side="target",
            category_value=category_value,
            current_txn=target_transaction,
        )
        updated_target["splits"] = None
        return updated_target

    updated_target["category_raw"] = "Split"
    updated_target["category_id"] = ""
    updated_target["splits"] = split_lines
    return updated_target


def apply_target_split_edit(
    df: pl.DataFrame,
    idx: Any,
    *,
    lines: list[dict[str, Any]],
) -> pl.DataFrame:
    updated = _apply_target_split_edit_pandas(df.to_pandas(), idx, lines=lines)
    return pl.from_pandas(updated)


def _apply_target_split_edit_pandas(
    df: pd.DataFrame,
    idx: Any,
    *,
    lines: list[dict[str, Any]],
) -> pd.DataFrame:
    if idx not in df.index:
        return df.copy()

    import ynab_il_importer.review_app.validation as review_validation

    updated = df.copy()
    row = updated.loc[idx]
    target_transaction = _target_transaction_for_split_edit(row)
    normalized_lines = _normalize_split_editor_lines(lines, parent_transaction=target_transaction)
    if not normalized_lines:
        raise ValueError("Split save requires at least one non-empty line.")

    updated_target = _apply_target_split_lines_to_transaction(
        row,
        target_transaction=target_transaction,
        split_lines=normalized_lines,
    )
    split_errors = review_validation.validate_target_split_transaction(
        updated.loc[idx],
        updated_target,
    )
    if split_errors:
        raise ValueError("; ".join(split_errors))

    updated.at[idx, "target_current_transaction"] = updated_target
    updated.at[idx, "target_payee_selected"] = str(updated_target.get("payee_raw", "") or "").strip()
    updated.at[idx, "payee_selected"] = str(updated_target.get("payee_raw", "") or "").strip()
    updated.at[idx, "target_category_selected"] = model.normalize_category_value(
        updated_target.get("category_raw", "")
    )
    updated.at[idx, "category_selected"] = model.normalize_category_value(
        updated_target.get("category_raw", "")
    )

    updated = recompute_changed_for_rows(updated, [idx])
    updated = rebuild_working_rows(updated, [idx])
    updated = recompute_changed_for_rows(updated, [idx])
    return updated


def apply_row_edit(
    df: pl.DataFrame,
    idx: Any,
    *,
    payee: str | None = None,
    category: str | None = None,
    source_payee: str | None = None,
    source_category: str | None = None,
    target_payee: str | None = None,
    target_category: str | None = None,
    memo_append: str | None = None,
    update_maps: str | None = None,
    reviewed: bool | None = None,
    decision_action: str | None = None,
    component_map: dict[Any, int] | None = None,
) -> pl.DataFrame:
    updated = _apply_row_edit_pandas(
        df.to_pandas(),
        idx,
        payee=payee,
        category=category,
        source_payee=source_payee,
        source_category=source_category,
        target_payee=target_payee,
        target_category=target_category,
        memo_append=memo_append,
        update_maps=update_maps,
        reviewed=reviewed,
        decision_action=decision_action,
        component_map=component_map,
    )
    return pl.from_pandas(updated)


def _apply_row_edit_pandas(
    df: pd.DataFrame,
    idx: Any,
    *,
    payee: str | None = None,
    category: str | None = None,
    source_payee: str | None = None,
    source_category: str | None = None,
    target_payee: str | None = None,
    target_category: str | None = None,
    memo_append: str | None = None,
    update_maps: str | None = None,
    reviewed: bool | None = None,
    decision_action: str | None = None,
    component_map: dict[Any, int] | None = None,
) -> pd.DataFrame:
    updated = df.copy()
    target_payee = payee if target_payee is None else target_payee
    target_category = category if target_category is None else target_category

    source_indices = related_row_indices(updated, idx, include_source=True, include_target=False) or [idx]
    target_indices = related_row_indices(updated, idx, include_source=False, include_target=True) or [idx]

    if source_payee is not None or source_category is not None:
        if source_payee is not None and "source_payee_selected" in updated.columns:
            updated.loc[source_indices, "source_payee_selected"] = str(source_payee).strip()
        if source_category is not None and "source_category_selected" in updated.columns:
            updated.loc[source_indices, "source_category_selected"] = model.normalize_category_value(
                source_category
            )
        updated = _update_current_transaction_values(
            updated,
            source_indices,
            side="source",
            payee=source_payee,
            category=source_category,
        )

    if target_payee is not None or target_category is not None:
        if target_payee is not None:
            if "payee_selected" in updated.columns:
                updated.loc[target_indices, "payee_selected"] = str(target_payee).strip()
            if "target_payee_selected" in updated.columns:
                updated.loc[target_indices, "target_payee_selected"] = str(target_payee).strip()
        if target_category is not None:
            if "category_selected" in updated.columns:
                updated.loc[target_indices, "category_selected"] = model.normalize_category_value(
                    target_category
                )
            if "target_category_selected" in updated.columns:
                updated.loc[target_indices, "target_category_selected"] = model.normalize_category_value(
                    target_category
                )
        updated = _update_current_transaction_values(
            updated,
            target_indices,
            side="target",
            payee=target_payee,
            category=target_category,
        )

    if update_maps is not None and "update_maps" in updated.columns:
        updated.at[idx, "update_maps"] = str(update_maps).strip()
    if memo_append is not None and "memo_append" in updated.columns:
        updated.at[idx, "memo_append"] = str(memo_append).strip()

    if decision_action is not None and "decision_action" in updated.columns:
        updated.at[idx, "decision_action"] = str(decision_action).strip()
    updated = _recompute_presence(updated, [idx])
    if reviewed is not None and "reviewed" in updated.columns:
        if component_map is None:
            from ynab_il_importer.review_app.validation import connected_component_mask

            reviewed_mask = connected_component_mask(updated, idx)
            reviewed_indices = updated.index[reviewed_mask].tolist()
        else:
            reviewed_indices = [
                current_idx
                for current_idx, label in component_map.items()
                if label == component_map.get(idx)
            ]
        updated.loc[reviewed_indices, "reviewed"] = bool(reviewed)
    changed_indices = list(dict.fromkeys([*source_indices, *target_indices, idx]))
    updated = recompute_changed_for_rows(updated, changed_indices)
    updated = rebuild_working_rows(updated, changed_indices)
    return updated
