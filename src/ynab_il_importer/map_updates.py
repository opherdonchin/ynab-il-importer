from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import pandas as pd

import ynab_il_importer.export as export
import ynab_il_importer.rules as rules


_SLUG_RE = re.compile(r"[^a-z0-9]+")
_TRANSFER_PREFIX = "Transfer :"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _string_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column].astype("string").fillna("").str.strip()
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[column].astype(bool).fillna(False)


def _selected_series(df: pd.DataFrame, *, side: str, field: str) -> pd.Series:
    side_column = f"{side}_{field}_selected"
    if side_column in df.columns:
        return _string_series(df, side_column)
    if side == "target":
        return _string_series(df, f"{field}_selected")
    return pd.Series([""] * len(df), index=df.index, dtype="string")


def _slug(value: str) -> str:
    lowered = value.casefold()
    ascii_text = lowered.encode("ascii", "ignore").decode("ascii")
    slug = _SLUG_RE.sub("_", ascii_text).strip("_")
    return slug[:32] or "rule"


def _is_transfer_payee(value: Any) -> bool:
    return _text(value).startswith(_TRANSFER_PREFIX)


def _candidate_rule_id(row: pd.Series) -> str:
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


def _normalize_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["transaction_id"] = _string_series(out, "transaction_id")
    out["target_payee_selected"] = _selected_series(out, side="target", field="payee")
    out["target_category_selected"] = _selected_series(out, side="target", field="category")
    out["reviewed"] = _bool_series(out, "reviewed")
    out["update_maps"] = _string_series(out, "update_maps")
    if "source_present" in out.columns:
        out["source_present"] = _bool_series(out, "source_present")
    return out


def _changed_mask(current: pd.DataFrame, base: pd.DataFrame) -> pd.Series:
    base_lookup = base[
        ["transaction_id", "target_payee_selected", "target_category_selected"]
    ].copy()
    base_lookup = base_lookup.drop_duplicates(subset=["transaction_id"], keep="last").set_index("transaction_id")
    current_ids = current["transaction_id"]
    base_payee = current_ids.map(base_lookup["target_payee_selected"]).fillna("")
    base_category = current_ids.map(base_lookup["target_category_selected"]).fillna("")
    return (current["target_payee_selected"] != base_payee) | (
        current["target_category_selected"] != base_category
    )


def build_map_update_candidates(current_df: pd.DataFrame, base_df: pd.DataFrame | None) -> pd.DataFrame:
    current = _normalize_for_compare(current_df)
    if current.empty:
        columns = list(rules.PAYEE_MAP_COLUMNS) + ["count"]
        return pd.DataFrame(columns=columns)

    if base_df is None or base_df.empty:
        changed = pd.Series([False] * len(current), index=current.index)
    else:
        changed = _changed_mask(current, _normalize_for_compare(base_df))

    reviewed = current["reviewed"]
    update_maps = current["update_maps"]
    payee = current["target_payee_selected"]
    category = current["target_category_selected"]
    transfer = payee.map(_is_transfer_payee)
    usable = payee.ne("") & (transfer | category.ne(""))
    source_present = _bool_series(current, "source_present")
    if "source_present" in current.columns:
        usable = usable & source_present
    candidate_mask = reviewed & usable & (changed | update_maps.ne(""))
    if not candidate_mask.any():
        columns = list(rules.PAYEE_MAP_COLUMNS) + ["count"]
        return pd.DataFrame(columns=columns)

    candidates = current_df.loc[candidate_mask].copy()
    prepared = rules.prepare_transactions_for_rules(candidates)
    out = pd.DataFrame(index=prepared.index)
    out["txn_kind"] = prepared["txn_kind"].astype("string").fillna("")
    out["fingerprint"] = prepared["fingerprint"].astype("string").fillna("")
    out["description_clean_norm"] = ""
    out["account_name"] = prepared["account_name"].astype("string").fillna("")
    out["source"] = prepared["source"].astype("string").fillna("")
    out["direction"] = prepared["direction"].astype("string").fillna("")
    out["currency"] = prepared["currency"].astype("string").fillna("")
    out["amount_bucket"] = ""
    out["payee_canonical"] = _selected_series(candidates, side="target", field="payee")
    out["category_target"] = _selected_series(candidates, side="target", field="category").where(
        ~_selected_series(candidates, side="target", field="payee").map(_is_transfer_payee),
        "",
    )
    out["card_suffix"] = prepared["card_suffix"].astype("string").fillna("")
    out["review_memo"] = _string_series(candidates, "memo")
    out["update_maps"] = _string_series(candidates, "update_maps")

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
        out.groupby(group_cols, dropna=False)
        .agg(
            count=("fingerprint", "size"),
            update_maps_any=("update_maps", lambda s: ";".join(sorted({v for v in s if _text(v)}))),
            example_memo=("review_memo", lambda s: next((v for v in s if _text(v)), "")),
        )
        .reset_index()
    )
    grouped["rule_id"] = grouped.apply(_candidate_rule_id, axis=1)
    grouped["is_active"] = True
    grouped["priority"] = 0
    grouped["notes"] = grouped.apply(
        lambda row: (
            f"review-log count={int(row['count'])}"
            + (
                f" update_maps={row['update_maps_any']}"
                if _text(row["update_maps_any"])
                else ""
            )
            + (f" example={row['example_memo'][:80]}" if _text(row["example_memo"]) else "")
        ).strip(),
        axis=1,
    )
    final = grouped[rules.PAYEE_MAP_COLUMNS + ["count"]].copy()
    sort_cols = ["fingerprint", "payee_canonical", "category_target", "account_name", "source", "card_suffix"]
    return final.sort_values(sort_cols, na_position="last").reset_index(drop=True)


def default_map_updates_path(reviewed_path: str | Path) -> Path:
    reviewed = Path(reviewed_path)
    suffix = reviewed.suffix or ".csv"
    stem = reviewed.with_suffix("") if reviewed.suffix else reviewed
    return Path(f"{stem}_map_updates{suffix}")


def save_map_update_candidates(
    current_df: pd.DataFrame,
    base_df: pd.DataFrame | None,
    path: str | Path,
) -> pd.DataFrame:
    out = build_map_update_candidates(current_df, base_df)
    export.write_dataframe(out, path)
    return out
