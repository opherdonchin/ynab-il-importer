from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import polars as pl

import ynab_il_importer.export as export
import ynab_il_importer.rules as rules


_SLUG_RE = re.compile(r"[^a-z0-9]+")
_TRANSFER_PREFIX = "Transfer :"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _string_series(df: pl.DataFrame, column: str) -> pl.Series:
    if column in df.columns:
        return df.get_column(column).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    return pl.Series([""] * len(df), dtype=pl.Utf8)


def _bool_series(df: pl.DataFrame, column: str) -> pl.Series:
    if column not in df.columns:
        return pl.Series([False] * len(df), dtype=pl.Boolean)
    values = df.get_column(column).to_list()
    return pl.Series([str(value or "").strip().casefold() in {"1", "true", "t", "yes", "y"} for value in values], dtype=pl.Boolean)


def _selected_series(df: pl.DataFrame, *, side: str, field: str) -> pl.Series:
    side_column = f"{side}_{field}_selected"
    if side_column in df.columns:
        return _string_series(df, side_column)
    if side == "target":
        return _string_series(df, f"{field}_selected")
    return pl.Series([""] * len(df), dtype=pl.Utf8)


def _slug(value: str) -> str:
    lowered = value.casefold()
    ascii_text = lowered.encode("ascii", "ignore").decode("ascii")
    slug = _SLUG_RE.sub("_", ascii_text).strip("_")
    return slug[:32] or "rule"


def _is_transfer_payee(value: Any) -> bool:
    return _text(value).startswith(_TRANSFER_PREFIX)


def _candidate_rule_id(row: Any) -> str:
    basis = "|".join(
        [
            _text(row.get("txn_kind", "")),
            _text(row.get("fingerprint", "")),
            _text(row.get("account_name", "")),
            _text(row.get("source", "")),
            _text(row.get("direction", "")),
            _text(row.get("currency", "")),
            _text(row.get("card_suffix", "")),
            _text(row.get("payee_canonical", "")),
            _text(row.get("category_target", "")),
        ]
    )
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:8]
    base = _slug(_text(row.get("fingerprint", ""))) or _slug(_text(row.get("payee_canonical", "")))
    return f"candidate_{base}_{digest}"


def _candidate_notes(row: Any) -> str:
    notes = f"review-log count={int(row.get('count', 0) or 0)}"
    update_maps = _text(row.get("update_maps_any", ""))
    if update_maps:
        notes += f" update_maps={update_maps}"
    example_memo = _text(row.get("example_memo", ""))
    if example_memo:
        notes += f" example={example_memo[:80]}"
    return notes.strip()


def _normalize_for_compare(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        [
            pl.col("transaction_id").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias("transaction_id")
            if "transaction_id" in df.columns
            else pl.lit("").alias("transaction_id"),
            _selected_series(df, side="target", field="payee").alias("target_payee_selected"),
            _selected_series(df, side="target", field="category").alias("target_category_selected"),
            _bool_series(df, "reviewed").alias("reviewed"),
            _string_series(df, "update_maps").alias("update_maps"),
        ]
    )
    if "source_present" in df.columns:
        out = out.with_columns(_bool_series(df, "source_present").alias("source_present"))
    return out


def _changed_mask(current: pl.DataFrame, base: pl.DataFrame) -> pl.Series:
    if "transaction_id" not in current.columns or "transaction_id" not in base.columns:
        return pl.Series([False] * len(current), dtype=pl.Boolean)

    base_lookup = (
        base.select(["transaction_id", "target_payee_selected", "target_category_selected"])
        .unique(subset=["transaction_id"], keep="last")
        .rename(
            {
                "target_payee_selected": "base_target_payee_selected",
                "target_category_selected": "base_target_category_selected",
            }
        )
    )
    joined = current.select(
        [
            "transaction_id",
            "target_payee_selected",
            "target_category_selected",
        ]
    ).join(base_lookup, on="transaction_id", how="left")
    return (
        joined.get_column("target_payee_selected")
        != joined.get_column("base_target_payee_selected").fill_null("")
    ) | (
        joined.get_column("target_category_selected")
        != joined.get_column("base_target_category_selected").fill_null("")
    )


def build_map_update_candidates(current_df: pl.DataFrame, base_df: pl.DataFrame | None) -> pl.DataFrame:
    current = _normalize_for_compare(current_df)
    if current.is_empty():
        columns = list(rules.PAYEE_MAP_COLUMNS) + ["count"]
        return pl.DataFrame({column: [] for column in columns})

    if base_df is None or base_df.is_empty():
        changed = pl.Series([False] * len(current), dtype=pl.Boolean)
    else:
        changed = _changed_mask(current, _normalize_for_compare(base_df))

    reviewed = _bool_series(current, "reviewed")
    update_maps = _string_series(current, "update_maps")
    payee = _selected_series(current, side="target", field="payee")
    category = _selected_series(current, side="target", field="category")
    transfer = pl.Series([_is_transfer_payee(value) for value in payee.to_list()], dtype=pl.Boolean)
    usable = payee.ne("") & (transfer | category.ne(""))
    if "source_present" in current.columns:
        usable = usable & _bool_series(current, "source_present")
    candidate_mask = reviewed & usable & (changed | update_maps.ne(""))
    if not bool(candidate_mask.any()):
        columns = list(rules.PAYEE_MAP_COLUMNS) + ["count"]
        return pl.DataFrame({column: [] for column in columns})

    candidates = current.filter(candidate_mask)
    prepared = pl.from_pandas(rules.prepare_transactions_for_rules(candidates.to_pandas()))
    payee_canonical = _selected_series(candidates, side="target", field="payee")
    category_target = _selected_series(candidates, side="target", field="category").to_list()
    category_target = [
        "" if _is_transfer_payee(payee_value) else category_value
        for payee_value, category_value in zip(payee_canonical.to_list(), category_target, strict=False)
    ]
    out = pl.DataFrame(
        {
            "txn_kind": _string_series(prepared, "txn_kind"),
            "fingerprint": _string_series(prepared, "fingerprint"),
            "description_clean_norm": _string_series(prepared, "description_clean_norm"),
            "account_name": _string_series(prepared, "account_name"),
            "source": _string_series(prepared, "source"),
            "direction": _string_series(prepared, "direction"),
            "currency": _string_series(prepared, "currency"),
            "amount_bucket": _string_series(prepared, "amount_bucket"),
            "payee_canonical": payee_canonical,
            "category_target": pl.Series(category_target, dtype=pl.Utf8),
            "card_suffix": _string_series(prepared, "card_suffix"),
            "review_memo": _string_series(candidates, "memo"),
            "update_maps": _string_series(candidates, "update_maps"),
        }
    )

    group_cols = [
        "txn_kind",
        "fingerprint",
        "description_clean_norm",
        "account_name",
        "source",
        "direction",
        "currency",
        "amount_bucket",
        "payee_canonical",
        "category_target",
        "card_suffix",
    ]
    grouped = (
        out.group_by(group_cols)
        .agg(
            pl.len().alias("count"),
            pl.col("update_maps").filter(pl.col("update_maps").ne("")).unique().sort().alias("update_maps_values"),
            pl.col("review_memo").filter(pl.col("review_memo").ne("")).first().fill_null("").alias("example_memo"),
        )
        .with_columns(
            pl.col("update_maps_values").list.join(";").fill_null("").alias("update_maps_any"),
        )
    )
    grouped = grouped.with_columns(
        pl.struct(group_cols + ["count", "update_maps_any", "example_memo"])
        .map_elements(_candidate_rule_id, return_dtype=pl.Utf8)
        .alias("rule_id"),
        pl.lit(True).alias("is_active"),
        pl.lit(0).alias("priority"),
        pl.struct(["count", "update_maps_any", "example_memo"])
        .map_elements(_candidate_notes, return_dtype=pl.Utf8)
        .alias("notes"),
    )
    final = grouped.select(rules.PAYEE_MAP_COLUMNS + ["count"])
    sort_cols = ["fingerprint", "payee_canonical", "category_target", "account_name", "source", "card_suffix"]
    return final.sort(sort_cols)


def default_map_updates_path(reviewed_path: str | Path) -> Path:
    reviewed = Path(reviewed_path)
    suffix = reviewed.suffix or ".csv"
    stem = reviewed.with_suffix("") if reviewed.suffix else reviewed
    return Path(f"{stem}_map_updates{suffix}")


def save_map_update_candidates(
    current_df: pl.DataFrame,
    base_df: pl.DataFrame | None,
    path: str | Path,
) -> pl.DataFrame:
    out = build_map_update_candidates(current_df, base_df)
    export.write_dataframe(out.to_pandas(), path)
    return out
