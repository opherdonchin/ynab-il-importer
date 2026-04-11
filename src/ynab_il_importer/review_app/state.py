from __future__ import annotations

import math
from collections import Counter
from typing import Any

import polars as pl
import pyarrow as pa

import ynab_il_importer.review_app.model as model
from ynab_il_importer.artifacts.transaction_schema import SPLIT_LINE_STRUCT
from ynab_il_importer.safe_types import TRUE_VALUES


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


def _empty_text_series(length: int) -> pl.Series:
    return pl.Series([""] * length, dtype=pl.Utf8)


def _empty_bool_series(length: int) -> pl.Series:
    return pl.Series([False] * length, dtype=pl.Boolean)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _normalize_flag_series(series: pl.Series) -> pl.Series:
    return pl.Series(
        [_normalize_text(value).casefold() in TRUE_VALUES for value in series.to_list()],
        dtype=pl.Boolean,
    )


def _parse_float_value(value: Any) -> float:
    text = _normalize_text(value)
    if not text:
        return 0.0
    try:
        parsed = float(text)
    except ValueError:
        return 0.0
    return 0.0 if math.isnan(parsed) else parsed


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


def review_data_view(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
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

    frame = _normalized_review_data_frame(
        df.with_row_index("_row_pos").with_columns(
            [
                pl.col(name).cast(_SPLIT_LIST_DTYPE, strict=False).alias(name)
                for name in ("source_splits", "target_splits")
                if name in df.columns
            ]
        )
    )
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
    blocker_series: pl.Series,
    save_state: pl.Series,
    changed_mask: pl.Series | None = None,
    uncategorized_mask: pl.Series | None = None,
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

    blocker_values = blocker_series.cast(pl.Utf8, strict=False).fill_null("").to_list()
    save_values = save_state.cast(pl.Utf8, strict=False).fill_null("Unsaved").to_list()
    changed_values = (
        changed_mask.cast(pl.Boolean, strict=False).fill_null(False).to_list()
        if isinstance(changed_mask, pl.Series)
        else [False] * data_view.height
    )
    uncategorized_values = (
        uncategorized_mask.cast(pl.Boolean, strict=False).fill_null(False).to_list()
        if isinstance(uncategorized_mask, pl.Series)
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
            .when(pl.col("blocker_label").eq("Decision required"))
            .then(pl.lit("Needs decision"))
            .when(~pl.col("blocker_label").is_in(["", "None"]))
            .then(pl.lit("Needs fix"))
            .when(pl.col("action_label").ne("No decision"))
            .then(pl.lit("Needs review"))
            .otherwise(pl.lit("Needs decision"))
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
    index: list[Any],
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
    index: list[Any],
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


def series_or_default(df: pl.DataFrame, col: str) -> pl.Series:
    if col in df.columns:
        return df.get_column(col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    if col == "payee_selected" and "target_payee_selected" in df.columns:
        return df.get_column("target_payee_selected").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    if col == "category_selected" and "target_category_selected" in df.columns:
        return df.get_column("target_category_selected").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    return _empty_text_series(len(df))


def _decision_action_series(df: pl.DataFrame) -> pl.Series:
    return pl.Series(
        [(_normalize_text(value) or "No decision").casefold() for value in series_or_default(df, "decision_action").to_list()],
        dtype=pl.Utf8,
    )


def _update_maps_series(df: pl.DataFrame) -> pl.Series:
    if "update_maps" in df.columns:
        return df.get_column("update_maps").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    return _empty_text_series(len(df))


def _bool_series(df: pl.DataFrame, col: str) -> pl.Series:
    if col not in df.columns:
        return _empty_bool_series(len(df))
    return _normalize_flag_series(df.get_column(col))


def _id_series(df: pl.DataFrame, col: str) -> pl.Series:
    return series_or_default(df, col).str.strip_chars()


def _id_list(df: pl.DataFrame, col: str) -> list[str]:
    if col not in df.columns:
        return [""] * df.height
    return [str(value or "").strip() for value in _id_series(df, col).to_list()]


def _component_mask_from_map(
    df: pl.DataFrame,
    idx: Any,
    component_map: dict[Any, int],
) -> pl.Series:
    component_label = component_map.get(idx)
    if component_label is None:
        return _empty_bool_series(len(df))
    return pl.Series(
        [component_map.get(current_idx) == component_label for current_idx in range(len(df))],
        dtype=pl.Boolean,
    )


def _missing_value_masks(df: pl.DataFrame) -> tuple[pl.Series, pl.Series, pl.Series]:
    decision_action = _decision_action_series(df)
    create_target = decision_action.eq("create_target")
    payee = series_or_default(df, "payee_selected").str.strip_chars()
    payee_blank = payee.eq("")
    category = pl.Series(
        [model.normalize_category_value(value) for value in series_or_default(df, "category_selected").to_list()],
        dtype=pl.Utf8,
    )
    category_blank = category.eq("")
    no_category_required = pl.Series(
        [model.is_no_category_required(value) for value in category.to_list()],
        dtype=pl.Boolean,
    )
    transfer_payee = pl.Series(
        [model.is_transfer_payee(value) for value in payee.to_list()],
        dtype=pl.Boolean,
    )
    missing_payee = create_target & payee_blank
    missing_category = create_target & (category_blank | no_category_required) & ~transfer_payee
    return missing_payee, missing_category, create_target


def unresolved_mask(df: pl.DataFrame) -> pl.Series:
    missing_payee, missing_category, _ = _missing_value_masks(df)
    decision_action = _decision_action_series(df)
    match_status = series_or_default(df, "match_status").str.strip_chars().str.to_lowercase()
    pending_decision = match_status.is_in(["ambiguous", "source_only", "target_only"]) & decision_action.eq("no decision")
    return missing_payee | missing_category | pending_decision


def summary_counts(df: pl.DataFrame) -> dict[str, int]:
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


def accept_defaults_mask(df: pl.DataFrame) -> pl.Series:
    return _empty_bool_series(len(df))


def modified_mask(df: pl.DataFrame, original: pl.DataFrame | None) -> pl.Series:
    if original is None or original.is_empty():
        return _empty_bool_series(len(df))
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
        return _empty_bool_series(len(df))
    return pl.Series(
        [
            any(left.get(col) != right.get(col) for col in cols)
            for left, right in zip(df.to_dicts(), original.to_dicts(), strict=False)
        ],
        dtype=pl.Boolean,
    )


def modified_count(df: pl.DataFrame, original: pl.DataFrame | None) -> int:
    return int(modified_mask(df, original).sum())


def changed_mask(df: pl.DataFrame, base: pl.DataFrame | None) -> pl.Series:
    if "changed" in df.columns:
        return _bool_series(df, "changed")
    if base is None or base.is_empty():
        return _empty_bool_series(len(df))
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
        return _empty_bool_series(len(df))
    if "transaction_id" in df.columns and "transaction_id" in base.columns:
        current_rows = df.select(["transaction_id", *cols]).to_dicts()
        base_rows = base.select(["transaction_id", *cols]).to_dicts()
        base_counts: dict[str, int] = {}
        base_lookup: dict[tuple[str, int], dict[str, Any]] = {}
        for row in base_rows:
            txn_id = _normalize_text(row.get("transaction_id"))
            occurrence = base_counts.get(txn_id, 0)
            base_counts[txn_id] = occurrence + 1
            base_lookup[(txn_id, occurrence)] = row
        current_counts: dict[str, int] = {}
        changed = []
        for row in current_rows:
            txn_id = _normalize_text(row.get("transaction_id"))
            occurrence = current_counts.get(txn_id, 0)
            current_counts[txn_id] = occurrence + 1
            baseline = base_lookup.get((txn_id, occurrence))
            if baseline is None:
                changed.append(True)
                continue
            changed.append(any(row.get(col) != baseline.get(col) for col in cols))
        return pl.Series(changed, dtype=pl.Boolean)

    current_rows = df.select(cols).to_dicts()
    baseline_rows = base.select(cols).to_dicts()
    changed = []
    for current_row, baseline_row in zip(current_rows, baseline_rows, strict=False):
        if baseline_row is None:
            changed.append(True)
            continue
        changed.append(any(current_row.get(col) != baseline_row.get(col) for col in cols))
    if len(current_rows) > len(baseline_rows):
        changed.extend([True] * (len(current_rows) - len(baseline_rows)))
    return pl.Series(changed[: len(df)], dtype=pl.Boolean)


def saved_mask(original: pl.DataFrame | None, base: pl.DataFrame | None, current_index: list[Any]) -> pl.Series:
    if original is None or original.is_empty():
        return _empty_bool_series(len(current_index))

    changed = changed_mask(original, base)
    reviewed = _bool_series(original, "reviewed")

    saved = (changed | reviewed).to_list()
    return pl.Series(saved[: len(current_index)], dtype=pl.Boolean)


def apply_filters(df: pl.DataFrame, filters: dict[str, Any]) -> pl.DataFrame:
    filtered = df

    match_status = filters.get("match_status")
    if match_status:
        filtered = filtered.filter(pl.col("match_status").is_in(match_status))

    reviewed = _bool_series(df, "reviewed")
    reviewed_mode = str(filters.get("reviewed_mode", "") or "").strip().lower()
    if reviewed_mode == "unreviewed":
        filtered = filtered.filter(~reviewed)
    elif reviewed_mode == "reviewed":
        filtered = filtered.filter(reviewed)

    missing_payee, missing_category, _ = _missing_value_masks(filtered)
    unresolved = unresolved_mask(filtered)
    if filters.get("unresolved_only"):
        filtered = filtered.filter(unresolved)
    if filters.get("missing_payee_only"):
        filtered = filtered.filter(missing_payee)
    if filters.get("missing_category_only"):
        filtered = filtered.filter(missing_category)

    fingerprint_query = str(filters.get("fingerprint_query", "") or "").strip().casefold()
    if fingerprint_query:
        filtered = filtered.filter(
            series_or_default(filtered, "fingerprint")
            .str.to_lowercase()
            .str.contains(fingerprint_query, literal=True)
        )

    payee_query = str(filters.get("payee_query", "") or "").strip().casefold()
    if payee_query:
        payee_text = series_or_default(filtered, "payee_selected") + " " + series_or_default(filtered, "payee_options")
        filtered = filtered.filter(payee_text.str.to_lowercase().str.contains(payee_query, literal=True))

    memo_query = str(filters.get("memo_query", "") or "").strip().casefold()
    if memo_query:
        memo_text = series_or_default(filtered, "memo") + " " + series_or_default(filtered, "description_raw") + " " + series_or_default(filtered, "description_clean")
        filtered = filtered.filter(memo_text.str.to_lowercase().str.contains(memo_query, literal=True))

    source_query = str(filters.get("source_query", "") or "").strip().casefold()
    if source_query:
        filtered = filtered.filter(
            series_or_default(filtered, "source").str.to_lowercase().str.contains(source_query, literal=True)
        )

    account_query = str(filters.get("account_query", "") or "").strip().casefold()
    if account_query:
        filtered = filtered.filter(
            series_or_default(filtered, "account_name").str.to_lowercase().str.contains(account_query, literal=True)
        )

    return filtered


def _clean_text_list(values: Any) -> list[str]:
    if isinstance(values, pl.Series):
        raw_values = values.cast(pl.Utf8, strict=False).fill_null("").to_list()
    elif isinstance(values, list):
        raw_values = values
    else:
        raw_values = list(values) if values is not None else []
    return [str(value or "").strip() for value in raw_values]


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
    fingerprints = _clean_text_list(df.get_column("fingerprint"))
    values = _clean_text_list(df.get_column(column))
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
    if "_row_pos" in filtered.columns:
        indices = filtered.get_column("_row_pos").to_list()
    else:
        indices = list(range(filtered.height))
    fingerprints = _clean_text_list(filtered.get_column("fingerprint")) if "fingerprint" in filtered.columns else [""] * filtered.height
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


def _optional_row_bool(row: dict[str, Any], key: str) -> bool | None:
    if key not in row:
        return None
    value = row.get(key)
    if value is None or value == "":
        return None
    return _normalize_text(value).casefold() in TRUE_VALUES or bool(value) is True


def required_category_missing_mask(df: pl.DataFrame) -> pl.Series:
    rows = df.to_dicts()
    return pl.Series(
        [
            (
                model.normalize_category_value(row.get("category_selected", "")) == ""
                or model.is_no_category_required(row.get("category_selected", ""))
            )
            and model.category_required_for_payee(
                row.get("payee_selected", ""),
                current_account_on_budget=_optional_row_bool(
                    row, "target_account_on_budget"
                ),
                transfer_target_on_budget=_optional_row_bool(
                    row, "target_transfer_account_on_budget"
                ),
            )
            for row in rows
        ],
        dtype=pl.Boolean,
    )


def uncategorized_mask(df: pl.DataFrame) -> pl.Series:
    rows = df.to_dicts()
    return pl.Series(
        [
            (
                "uncategorized"
                in model.normalize_category_value(
                    row.get("category_selected", "")
                ).casefold()
            )
            and model.category_required_for_payee(
                row.get("payee_selected", ""),
                current_account_on_budget=_optional_row_bool(
                    row, "target_account_on_budget"
                ),
                transfer_target_on_budget=_optional_row_bool(
                    row, "target_transfer_account_on_budget"
                ),
            )
            for row in rows
        ],
        dtype=pl.Boolean,
    )


def truthy_series(df: pl.DataFrame, column: str) -> pl.Series:
    if column not in df.columns:
        return _empty_bool_series(len(df))
    return _normalize_flag_series(df.get_column(column))


def primary_state_series(df: pl.DataFrame, blocker_series: pl.Series) -> pl.Series:
    reviewed = truthy_series(df, "reviewed")
    blocker = [str(value or "").strip() for value in blocker_series.to_list()]
    action = [str(value or "").strip() for value in action_series(df).to_list()]
    reviewed_values = reviewed.to_list()
    states: list[str] = []
    for is_reviewed, blocker_value, action_value in zip(reviewed_values, blocker, action, strict=False):
        if is_reviewed and blocker_value in {"", "None"}:
            states.append("Settled")
        elif blocker_value == "Decision required":
            states.append("Needs decision")
        elif blocker_value not in {"", "None"}:
            states.append("Needs fix")
        elif action_value != "No decision":
            states.append("Needs review")
        else:
            states.append("Needs decision")
    return pl.Series(states, dtype=pl.Utf8)


def row_kind_series(df: pl.DataFrame) -> pl.Series:
    match_status = [
        str(value or "").strip().casefold()
        for value in series_or_default(df, "match_status").to_list()
    ]
    return pl.Series(
        [
            "Matched cleared"
            if value == "matched_cleared"
            else "Matched"
            if value == "matched_auto"
            else "Source only"
            if value == "source_only"
            else "Target only"
            if value == "target_only"
            else "Ambiguous"
            if value == "ambiguous"
            else "Unrecognized"
            if value == "unrecognized"
            else "Other"
            for value in match_status
        ],
        dtype=pl.Utf8,
    )


def action_series(df: pl.DataFrame) -> pl.Series:
    import ynab_il_importer.review_app.validation as review_validation

    return review_validation.normalize_decision_actions(
        series_or_default(df, "decision_action")
    )


def suggestion_series(df: pl.DataFrame) -> pl.Series:
    source_present = truthy_series(df, "source_present")
    target_present = truthy_series(df, "target_present")
    source_payee_selected = _clean_text_list(series_or_default(df, "source_payee_selected"))
    source_category_selected = _clean_text_list(series_or_default(df, "source_category_selected"))
    target_payee_selected = _clean_text_list(series_or_default(df, "target_payee_selected"))
    target_category_selected = _clean_text_list(series_or_default(df, "target_category_selected"))
    payee_options = _clean_text_list(series_or_default(df, "payee_options"))
    category_options = _clean_text_list(series_or_default(df, "category_options"))
    source_present_values = source_present.to_list()
    target_present_values = target_present.to_list()
    return pl.Series(
        [
            "Has suggestions"
            if (
                (not src_present and (src_payee != "" or src_category != ""))
                or (
                    not tgt_present
                    and (
                        tgt_payee != ""
                        or tgt_category != ""
                        or payee_option != ""
                        or category_option != ""
                    )
                )
            )
            else "No suggestions"
            for src_present, tgt_present, src_payee, src_category, tgt_payee, tgt_category, payee_option, category_option in zip(
                source_present_values,
                target_present_values,
                source_payee_selected,
                source_category_selected,
                target_payee_selected,
                target_category_selected,
                payee_options,
                category_options,
                strict=False,
            )
        ],
        dtype=pl.Utf8,
    )


def map_update_filter_series(df: pl.DataFrame) -> pl.Series:
    has_updates = _clean_text_list(series_or_default(df, "update_maps"))
    return pl.Series(
        ["Has update_maps" if value else "No update_maps" for value in has_updates],
        dtype=pl.Utf8,
    )


def state_matrix_counts(
    primary_state_series: pl.Series | list[str],
    save_state_series: pl.Series | list[str],
) -> dict[str, int]:
    primary_values = _clean_text_list(primary_state_series)
    save_values = _clean_text_list(save_state_series)
    counts: Counter[str] = Counter()
    for primary, save_state in zip(primary_values, save_values, strict=False):
        counts[f"{primary} / {save_state}"] += 1
    return dict(counts)


def search_text_series(df: pl.DataFrame) -> pl.Series:
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
    parts = [series_or_default(df, column).to_list() for column in columns]
    rows: list[str] = []
    for values in zip(*parts, strict=False):
        text = " ".join(_normalize_text(value) for value in values if _normalize_text(value))
        rows.append(text.casefold())
    return pl.Series(rows, dtype=pl.Utf8)


def _row_key_series(df: pl.DataFrame) -> pl.Series:
    if "transaction_id" not in df.columns:
        return pl.Series([str(index) for index in range(len(df))], dtype=pl.Utf8)
    txn_ids = _clean_text_list(df.get_column("transaction_id"))
    counts: dict[str, int] = {}
    keys: list[str] = []
    for txn_id in txn_ids:
        occurrence = counts.get(txn_id, 0)
        counts[txn_id] = occurrence + 1
        keys.append(f"{txn_id}|{occurrence}")
    return pl.Series(keys, dtype=pl.Utf8)


def derive_inference_tags(df: pl.DataFrame) -> pl.Series:
    match_status = [
        str(value or "").strip().lower()
        for value in series_or_default(df, "match_status").to_list()
    ]
    payee = _clean_text_list(series_or_default(df, "payee_selected"))
    missing_required = required_category_missing_mask(df).to_list()
    inferred: list[str] = []
    for status, payee_value, missing in zip(match_status, payee, missing_required, strict=False):
        if status == "none":
            value = "unrecognized"
        elif status == "ambiguous":
            value = "ambiguous"
        elif status not in {"", "none", "ambiguous", "unique"} and not missing:
            value = status
        elif (status not in {"none", "ambiguous"} and missing) or payee_value == "":
            value = "missing" if missing else "unique"
        else:
            value = "unique"
        inferred.append(value)
    return pl.Series(inferred, dtype=pl.Utf8)


def initial_inference_tags(df: pl.DataFrame, base: pl.DataFrame | None) -> pl.Series:
    fallback = derive_inference_tags(df)
    if base is None or base.is_empty():
        return fallback

    base_keys = _row_key_series(base)
    base_inference = derive_inference_tags(base)
    base_map = {key: value for key, value in zip(base_keys.to_list(), base_inference.to_list(), strict=False)}
    current_keys = _row_key_series(df)
    return pl.Series(
        [base_map.get(key, fallback_value) for key, fallback_value in zip(current_keys.to_list(), fallback.to_list(), strict=False)],
        dtype=pl.Utf8,
    )


def apply_row_filters(
    df: pl.DataFrame,
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    primary_state_series: pl.Series,
    row_kind_series: pl.Series,
    action_series: pl.Series,
    save_state: pl.Series,
    blocker_series: pl.Series,
    suggestion_series: pl.Series,
    map_update_series: pl.Series,
    search_query: str,
    search_text: pl.Series,
) -> pl.DataFrame:
    mask = (
        primary_state_series.is_in(primary_state)
        & row_kind_series.is_in(row_kind)
        & action_series.is_in(action_filter)
        & save_state.is_in(save_status)
        & blocker_series.is_in(blocker_filter)
        & suggestion_series.is_in(suggestion_filter)
        & map_update_series.is_in(map_update_filter)
    )

    if search_query:
        mask &= search_text.str.contains(search_query, literal=True)

    return df.filter(mask)


def filtered_row_indices(
    index: list[Any],
    *,
    primary_state: list[str],
    row_kind: list[str],
    action_filter: list[str],
    save_status: list[str],
    blocker_filter: list[str],
    suggestion_filter: list[str],
    map_update_filter: list[str],
    primary_state_series: pl.Series,
    row_kind_series: pl.Series,
    action_series: pl.Series,
    save_state: pl.Series,
    blocker_series: pl.Series,
    suggestion_series: pl.Series,
    map_update_series: pl.Series,
    search_query: str,
    search_text: pl.Series,
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

    primary_values = _clean_text_list(primary_state_series)
    row_kind_values = _clean_text_list(row_kind_series)
    action_values = _clean_text_list(action_series)
    save_values = _clean_text_list(save_state)
    blocker_values = _clean_text_list(blocker_series)
    suggestion_values = _clean_text_list(suggestion_series)
    map_update_values = _clean_text_list(map_update_series)
    search_values = _clean_text_list(search_text)

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
    df: pl.DataFrame,
    idx: Any,
    *,
    include_source: bool = False,
    include_target: bool = False,
) -> list[Any]:
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
    df: pl.DataFrame,
    idx: Any,
    *,
    include_source: bool = False,
    include_target: bool = False,
) -> pl.Series:
    indices = related_row_indices(
        df,
        idx,
        include_source=include_source,
        include_target=include_target,
    )
    values = [False] * len(df)
    for related_idx in indices:
        if isinstance(related_idx, int) and 0 <= related_idx < len(df):
            values[related_idx] = True
    return pl.Series(values, dtype=pl.Boolean)


def _original_side_present(df: pl.DataFrame, idx: Any, side: str) -> bool:
    if not isinstance(idx, int) or idx < 0 or idx >= df.height:
        return False
    original_column = f"{side}_original_transaction"
    if original_column in df.columns:
        value = df.row(idx, named=True).get(original_column)
        return isinstance(value, dict) and bool(value)
    present_column = f"{side}_present"
    if present_column in df.columns:
        return bool(df.row(idx, named=True).get(present_column))
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


def _recompute_presence(df: pl.DataFrame, indices: list[Any]) -> pl.DataFrame:
    if "source_present" not in df.columns or "target_present" not in df.columns:
        return df
    import ynab_il_importer.review_app.validation as review_validation

    rows = df.to_dicts()
    for idx in dict.fromkeys(indices):
        if not isinstance(idx, int) or idx < 0 or idx >= len(rows):
            continue
        row = rows[idx]
        action = row.get("decision_action", review_validation.NO_DECISION)
        source_present, target_present = _presence_after_action(
            source_present_original=_original_side_present(df, idx, "source"),
            target_present_original=_original_side_present(df, idx, "target"),
            action=str(action).strip(),
        )
        row["source_present"] = source_present
        row["target_present"] = target_present
    return pl.from_dicts(rows, infer_schema_length=None)


def _transaction_reference_column(side: str, *, kind: str) -> str:
    return f"{side}_{kind}_transaction"


def _review_record_row(row: dict[str, Any]) -> dict[str, Any]:
    import ynab_il_importer.review_app.io as review_io

    table = review_io.coerce_review_artifact_table(pl.DataFrame([row]))
    rows = table.to_pylist()
    return rows[0] if rows else {}


def _transaction_reference_from_row(
    row: dict[str, Any],
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
    row: dict[str, Any],
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
    df: pl.DataFrame,
    indices: list[Any],
    *,
    side: str,
    payee: str | None = None,
    category: str | None = None,
) -> pl.DataFrame:
    column = _transaction_reference_column(side, kind="current")
    rows = df.to_dicts()
    for current_idx in dict.fromkeys(indices):
        if not isinstance(current_idx, int) or current_idx < 0 or current_idx >= len(rows):
            continue
        row = rows[current_idx]
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
        row[column] = txn
    return pl.from_dicts(rows, infer_schema_length=None)


def rebuild_working_rows(df: pl.DataFrame, indices: list[Any]) -> pl.DataFrame:
    import ynab_il_importer.review_app.io as review_io
    import ynab_il_importer.review_app.working_schema as working_schema

    touched = [current_idx for current_idx in dict.fromkeys(indices) if isinstance(current_idx, int) and 0 <= current_idx < len(df)]
    if not touched:
        return df

    rows = df.to_dicts()
    subset = pl.from_dicts([rows[current_idx] for current_idx in touched], infer_schema_length=None)
    missing_input = working_schema.missing_working_columns(
        subset.columns,
        working_schema.WORKING_INPUT_REQUIRED_COLUMNS,
    )
    if missing_input:
        return df
    rebuilt = review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(review_io.coerce_review_artifact_table(subset))
    )
    if len(rebuilt) != len(subset):
        return df

    rebuilt_rows = rebuilt.to_dicts()
    for position, current_idx in enumerate(touched):
        rows[current_idx] = rebuilt_rows[position]
    return pl.from_dicts(rows, infer_schema_length=None)


def recompute_changed_for_rows(df: pl.DataFrame, indices: list[Any]) -> pl.DataFrame:
    if "changed" not in df.columns:
        return df

    rows = df.to_dicts()
    touched = [current_idx for current_idx in dict.fromkeys(indices) if isinstance(current_idx, int) and 0 <= current_idx < len(rows)]
    for current_idx in touched:
        row = rows[current_idx]
        source_current = _transaction_reference_from_row(row, side="source", kind="current")
        source_original = _transaction_reference_from_row(row, side="source", kind="original")
        target_current = _transaction_reference_from_row(row, side="target", kind="current")
        target_original = _transaction_reference_from_row(row, side="target", kind="original")
        row["changed"] = bool(
            source_current != source_original or target_current != target_original
        )
    return pl.from_dicts(rows, infer_schema_length=None)


def apply_review_flag(
    df: pl.DataFrame,
    indices: list[Any],
    *,
    reviewed: bool,
    component_map: dict[Any, int] | None = None,
) -> tuple[pl.DataFrame, list[int]]:
    touched = [
        current_idx
        for current_idx in dict.fromkeys(indices)
        if isinstance(current_idx, int) and 0 <= current_idx < len(df)
    ]
    if not touched or "reviewed" not in df.columns:
        return df, []

    if component_map is None:
        import ynab_il_importer.review_app.validation as review_validation

        component_map = review_validation.compute_components(df)

    affected: list[int] = []
    seen_components: set[int] = set()
    for current_idx in touched:
        component_label = component_map.get(current_idx)
        if component_label is None:
            affected.append(current_idx)
            continue
        if component_label in seen_components:
            continue
        seen_components.add(component_label)
        affected.extend(
            idx
            for idx, label in component_map.items()
            if label == component_label and isinstance(idx, int) and 0 <= idx < len(df)
        )

    affected = list(dict.fromkeys(affected))
    if not affected:
        return df, []

    rows = df.to_dicts()
    for current_idx in affected:
        rows[current_idx]["reviewed"] = bool(reviewed)
    return pl.from_dicts(rows, infer_schema_length=None), affected


def _signed_amount_from_row_values(*, inflow: Any, outflow: Any) -> float:
    inflow_value = _parse_float_value(inflow)
    outflow_value = _parse_float_value(outflow)
    return inflow_value - outflow_value


def _target_transaction_for_split_edit(row: dict[str, Any]) -> dict[str, Any]:
    import ynab_il_importer.review_app.io as review_io

    class _RowAdapter:
        def __init__(self, data: dict[str, Any]) -> None:
            self._data = data
            self.index = list(data.keys())

        def get(self, key: str, default: Any = None) -> Any:
            return self._data.get(key, default)

    current = _transaction_reference_from_row(row, side="target", kind="current")
    if bool(row.get("target_present", False)) and current is not None:
        return current
    original = _transaction_reference_from_row(row, side="target", kind="original")
    return review_io._transaction_from_flat_row(
        _RowAdapter(row),
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
        amount_value = _parse_float_value(raw.get("amount_ils", raw.get("amount", 0.0)))
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
    row: dict[str, Any],
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
    if not isinstance(idx, int) or idx < 0 or idx >= len(df):
        return df

    import ynab_il_importer.review_app.validation as review_validation

    rows = df.to_dicts()
    row = rows[idx]
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
        row,
        updated_target,
    )
    if split_errors:
        raise ValueError("; ".join(split_errors))

    row["target_current_transaction"] = updated_target
    row["target_payee_selected"] = str(updated_target.get("payee_raw", "") or "").strip()
    row["payee_selected"] = str(updated_target.get("payee_raw", "") or "").strip()
    row["target_category_selected"] = model.normalize_category_value(
        updated_target.get("category_raw", "")
    )
    row["category_selected"] = model.normalize_category_value(
        updated_target.get("category_raw", "")
    )

    updated = pl.from_dicts(rows, infer_schema_length=None)
    if "reviewed" in updated.columns:
        updated, _ = apply_review_flag(updated, [idx], reviewed=False)
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
    if not isinstance(idx, int) or idx < 0 or idx >= len(df):
        return df

    import ynab_il_importer.review_app.validation as review_validation

    rows = df.to_dicts()
    original_rows = [dict(current_row) for current_row in rows]
    row = rows[idx]
    target_payee = payee if target_payee is None else target_payee
    target_category = category if target_category is None else target_category

    source_indices = related_row_indices(df, idx, include_source=True, include_target=False) or [idx]
    target_indices = related_row_indices(df, idx, include_source=False, include_target=True) or [idx]

    if source_payee is not None or source_category is not None:
        for current_idx in source_indices:
            if current_idx < 0 or current_idx >= len(rows):
                continue
            if source_payee is not None and "source_payee_selected" in rows[current_idx]:
                rows[current_idx]["source_payee_selected"] = str(source_payee).strip()
            if source_category is not None and "source_category_selected" in rows[current_idx]:
                rows[current_idx]["source_category_selected"] = model.normalize_category_value(
                    source_category
                )
        updated = pl.from_dicts(rows, infer_schema_length=None)
        updated = _update_current_transaction_values(
            updated,
            source_indices,
            side="source",
            payee=source_payee,
            category=source_category,
        )
        rows = updated.to_dicts()

    if target_payee is not None or target_category is not None:
        for current_idx in target_indices:
            if current_idx < 0 or current_idx >= len(rows):
                continue
            if target_payee is not None:
                if "payee_selected" in rows[current_idx]:
                    rows[current_idx]["payee_selected"] = str(target_payee).strip()
                if "target_payee_selected" in rows[current_idx]:
                    rows[current_idx]["target_payee_selected"] = str(target_payee).strip()
            if target_category is not None:
                normalized_category = model.normalize_category_value(target_category)
                if "category_selected" in rows[current_idx]:
                    rows[current_idx]["category_selected"] = normalized_category
                if "target_category_selected" in rows[current_idx]:
                    rows[current_idx]["target_category_selected"] = normalized_category
        updated = pl.from_dicts(rows, infer_schema_length=None)
        updated = _update_current_transaction_values(
            updated,
            target_indices,
            side="target",
            payee=target_payee,
            category=target_category,
        )
        rows = updated.to_dicts()

    if update_maps is not None and "update_maps" in rows[idx]:
        rows[idx]["update_maps"] = str(update_maps).strip()
    if memo_append is not None and "memo_append" in rows[idx]:
        rows[idx]["memo_append"] = str(memo_append).strip()

    if decision_action is not None and "decision_action" in rows[idx]:
        rows[idx]["decision_action"] = str(decision_action).strip()
    updated = pl.from_dicts(rows, infer_schema_length=None)
    updated = _recompute_presence(updated, [idx])
    rows = updated.to_dicts()
    changed_indices = list(dict.fromkeys([*source_indices, *target_indices, idx]))
    if reviewed is None and "reviewed" in updated.columns:
        edited_fields = [
            "source_payee_selected",
            "source_category_selected",
            "target_payee_selected",
            "target_category_selected",
            "memo_append",
            "update_maps",
            "decision_action",
        ]
        implicit_reopen_indices = [
            current_idx
            for current_idx in changed_indices
            if 0 <= current_idx < len(rows)
            and any(
                rows[current_idx].get(field) != original_rows[current_idx].get(field)
                for field in edited_fields
            )
        ]
        if implicit_reopen_indices:
            updated, _ = apply_review_flag(
                updated,
                implicit_reopen_indices,
                reviewed=False,
                component_map=component_map,
            )
            rows = updated.to_dicts()
    if reviewed is not None and "reviewed" in rows[idx]:
        if component_map is None:
            reviewed_mask = review_validation.connected_component_mask(updated, idx)
            reviewed_indices = [
                current_idx
                for current_idx, flag in enumerate(reviewed_mask.to_list())
                if flag
            ]
        else:
            reviewed_indices = [
                current_idx
                for current_idx, label in component_map.items()
                if label == component_map.get(idx)
            ]
        for current_idx in reviewed_indices:
            if 0 <= current_idx < len(rows):
                rows[current_idx]["reviewed"] = bool(reviewed)
        updated = pl.from_dicts(rows, infer_schema_length=None)
    updated = recompute_changed_for_rows(updated, changed_indices)
    updated = rebuild_working_rows(updated, changed_indices)
    return updated
