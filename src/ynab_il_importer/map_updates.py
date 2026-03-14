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
    for column in ["transaction_id", "payee_selected", "category_selected"]:
        out[column] = _string_series(out, column)
    out["reviewed"] = _bool_series(out, "reviewed")
    out["update_map"] = _bool_series(out, "update_map")
    return out


def _changed_mask(current: pd.DataFrame, base: pd.DataFrame) -> pd.Series:
    base_lookup = base[["transaction_id", "payee_selected", "category_selected"]].copy()
    base_lookup = base_lookup.drop_duplicates(subset=["transaction_id"], keep="last").set_index("transaction_id")
    current_ids = current["transaction_id"]
    base_payee = current_ids.map(base_lookup["payee_selected"]).fillna("")
    base_category = current_ids.map(base_lookup["category_selected"]).fillna("")
    return (current["payee_selected"] != base_payee) | (current["category_selected"] != base_category)


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
    update_map = current["update_map"]
    payee = current["payee_selected"]
    category = current["category_selected"]
    transfer = payee.map(_is_transfer_payee)
    usable = payee.ne("") & (transfer | category.ne(""))
    candidate_mask = reviewed & usable & (changed | update_map)
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
    out["payee_canonical"] = _string_series(candidates, "payee_selected")
    out["category_target"] = _string_series(candidates, "category_selected").where(
        ~_string_series(candidates, "payee_selected").map(_is_transfer_payee),
        "",
    )
    out["card_suffix"] = prepared["card_suffix"].astype("string").fillna("")
    out["review_memo"] = _string_series(candidates, "memo")
    out["update_map"] = _bool_series(candidates, "update_map")

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
            update_map_any=("update_map", "max"),
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
            + (" update_map=TRUE" if bool(row["update_map_any"]) else "")
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
