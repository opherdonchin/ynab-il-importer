from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ynab_il_importer.fingerprint import fingerprint_hash_v1
from ynab_il_importer.fingerprint import fingerprint_v0
from ynab_il_importer.normalize import normalize_text

# Decision:
# - Ambiguity approach: a rule can return payee_canonical and optional category_target
#   in the same row (Option 2). category_target may stay blank.
# - Wildcard semantics: blank key cells in rules are wildcards and do not constrain match.
# - Precedence: active rules are ordered by priority DESC, then specificity DESC, then
#   rule_id ASC. If top rules tie on (priority, specificity), match is ambiguous.

PAYEE_MAP_COLUMNS = [
    "rule_id",
    "is_active",
    "priority",
    "txn_kind",
    "fingerprint_hash",
    "fingerprint",
    "description_clean_norm",
    "account_name",
    "source",
    "direction",
    "currency",
    "amount_bucket",
    "payee_canonical",
    "category_target",
    "notes",
]

RULE_KEY_COLUMNS = [
    "txn_kind",
    "fingerprint_hash",
    "fingerprint",
    "description_clean_norm",
    "account_name",
    "source",
    "direction",
    "currency",
    "amount_bucket",
]

_TRUE_VALUES = {"1", "true", "t", "yes", "y"}
_FALSE_VALUES = {"0", "false", "f", "no", "n"}


def _blank_to_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_key_value(column: str, value: Any) -> str | None:
    text = _blank_to_none(value)
    if text is None:
        return None
    if column == "txn_kind":
        return text.lower()
    if column in {"source", "direction"}:
        return text.lower()
    if column == "fingerprint_hash":
        return text.lower()
    if column == "currency":
        return text.upper()
    if column == "description_clean_norm":
        return normalize_text(text)
    return text


def _normalize_is_active(value: Any) -> bool:
    text = _blank_to_none(value)
    if text is None:
        return True
    lowered = text.lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise ValueError(f"Invalid is_active value: {value!r}")


def _normalize_priority(value: Any) -> int:
    text = _blank_to_none(value)
    if text is None:
        return 0
    return int(text)


def normalize_payee_map_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in PAYEE_MAP_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[PAYEE_MAP_COLUMNS].copy()

    out["rule_id"] = out["rule_id"].astype("string").fillna("").str.strip()
    if (out["rule_id"] == "").any():
        raise ValueError("payee_map.csv contains empty rule_id values")
    duplicate_ids = out["rule_id"][out["rule_id"].duplicated()].unique().tolist()
    if duplicate_ids:
        raise ValueError(f"payee_map.csv contains duplicate rule_id values: {duplicate_ids}")

    out["is_active"] = out["is_active"].map(_normalize_is_active)
    out["priority"] = out["priority"].map(_normalize_priority)

    for col in RULE_KEY_COLUMNS:
        out[col] = out[col].map(lambda v, c=col: _normalize_key_value(c, v))

    out["payee_canonical"] = out["payee_canonical"].map(_blank_to_none)
    out["category_target"] = out["category_target"].map(_blank_to_none)
    out["notes"] = out["notes"].map(_blank_to_none)

    out["_specificity"] = out.apply(_compute_specificity, axis=1)
    return out


def load_payee_map(path: str | Path) -> pd.DataFrame:
    map_path = Path(path)
    if not map_path.exists():
        raise FileNotFoundError(f"Missing payee map file: {map_path}")
    raw = pd.read_csv(map_path, dtype="string").fillna("")
    return normalize_payee_map_rules(raw)


def _compute_direction(amount_ils: Any) -> str:
    amount = pd.to_numeric(pd.Series([amount_ils]), errors="coerce").fillna(0.0).iloc[0]
    if amount > 0:
        return "inflow"
    if amount < 0:
        return "outflow"
    return "zero"


def _pick_series(df: pd.DataFrame, columns: list[str], default: str = "") -> pd.Series:
    for col in columns:
        if col in df.columns:
            series = df[col].astype("string").fillna("").str.strip()
            if (series != "").any():
                return series
    return pd.Series([default] * len(df), index=df.index, dtype="string")


def prepare_transactions_for_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["txn_kind"] = _pick_series(out, ["txn_kind"]).str.lower()
    out["source"] = _pick_series(out, ["source"]).str.lower()
    out["account_name"] = _pick_series(out, ["account_name"])
    out["currency"] = _pick_series(out, ["currency"], default="ILS")
    out["currency"] = out["currency"].replace("", "ILS").str.upper()
    out["amount_bucket"] = _pick_series(out, ["amount_bucket"])

    if "direction" in out.columns:
        out["direction"] = out["direction"].astype("string").fillna("").str.strip().str.lower()
    else:
        out["direction"] = ""

    if "amount_ils" in out.columns:
        out["direction"] = out["direction"].where(out["direction"] != "", out["amount_ils"].map(_compute_direction))
    out["direction"] = out["direction"].replace("", "zero")

    raw_for_norm = _pick_series(
        out,
        [
            "description_clean_norm",
            "description_clean",
            "merchant_raw",
            "description_raw",
            "raw_norm",
            "raw_text",
        ],
    )
    out["description_clean_norm"] = raw_for_norm.map(normalize_text)
    out["fingerprint_hash"] = [
        fingerprint_hash_v1(txn_kind, description_clean_norm)
        for txn_kind, description_clean_norm in zip(
            out["txn_kind"].tolist(),
            out["description_clean_norm"].tolist(),
        )
    ]
    out["fingerprint_hash"] = out["fingerprint_hash"].astype("string").fillna("").str.strip()

    out["fingerprint"] = _pick_series(out, ["fingerprint", "fingerprint_v0"])
    no_fp = out["fingerprint"] == ""
    out.loc[no_fp, "fingerprint"] = out.loc[no_fp, "description_clean_norm"].map(fingerprint_v0)
    out["fingerprint"] = out["fingerprint"].astype("string").fillna("").str.strip()

    out["example_text"] = _pick_series(
        out,
        ["description_raw", "raw_text", "description_clean", "merchant_raw", "description_clean_norm"],
    )
    return out


def _rule_matches(rule: pd.Series, txn: pd.Series) -> bool:
    has_fingerprint_hash = _blank_to_none(rule["fingerprint_hash"]) is not None
    has_fingerprint = _blank_to_none(rule["fingerprint"]) is not None
    for col in RULE_KEY_COLUMNS:
        if col in {"fingerprint", "description_clean_norm"} and has_fingerprint_hash:
            continue
        if col == "description_clean_norm" and has_fingerprint:
            continue
        rule_value = _blank_to_none(rule[col])
        if rule_value is None:
            continue
        txn_value = _normalize_key_value(col, txn.get(col))
        if txn_value != rule_value:
            return False
    return True


def _compute_specificity(rule: pd.Series) -> int:
    score = 0
    has_fingerprint_hash = _blank_to_none(rule["fingerprint_hash"]) is not None
    has_fingerprint = _blank_to_none(rule["fingerprint"]) is not None
    for col in RULE_KEY_COLUMNS:
        if col in {"fingerprint", "description_clean_norm"} and has_fingerprint_hash:
            continue
        if col == "description_clean_norm" and has_fingerprint:
            continue
        if _blank_to_none(rule[col]) is not None:
            score += 1
    return score


def apply_payee_map_rules(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    tx = prepare_transactions_for_rules(transactions)
    active_rules = rules[rules["is_active"]].copy()

    results: list[dict[str, Any]] = []
    for _, txn in tx.iterrows():
        matched_rules = [rule for _, rule in active_rules.iterrows() if _rule_matches(rule, txn)]
        if not matched_rules:
            results.append(
                {
                    "payee_canonical_suggested": "",
                    "category_target_suggested": "",
                    "match_rule_id": "",
                    "match_specificity_score": 0,
                    "match_status": "none",
                    "match_candidate_rule_ids": "",
                    "match_rule_count": 0,
                }
            )
            continue

        ranked = sorted(
            matched_rules,
            key=lambda r: (-int(r["priority"]), -int(r["_specificity"]), str(r["rule_id"])),
        )
        top = ranked[0]
        tie_pool = [
            rule
            for rule in ranked
            if int(rule["priority"]) == int(top["priority"])
            and int(rule["_specificity"]) == int(top["_specificity"])
        ]

        if len(tie_pool) > 1:
            tie_ids = ";".join(str(rule["rule_id"]) for rule in tie_pool)
            all_ids = ";".join(str(rule["rule_id"]) for rule in ranked)
            results.append(
                {
                    "payee_canonical_suggested": "",
                    "category_target_suggested": "",
                    "match_rule_id": tie_ids,
                    "match_specificity_score": int(top["_specificity"]),
                    "match_status": "ambiguous",
                    "match_candidate_rule_ids": all_ids,
                    "match_rule_count": len(matched_rules),
                }
            )
            continue

        results.append(
            {
                "payee_canonical_suggested": _blank_to_none(top["payee_canonical"]) or "",
                "category_target_suggested": _blank_to_none(top["category_target"]) or "",
                "match_rule_id": str(top["rule_id"]),
                "match_specificity_score": int(top["_specificity"]),
                "match_status": "unique",
                "match_candidate_rule_ids": ";".join(str(rule["rule_id"]) for rule in ranked),
                "match_rule_count": len(matched_rules),
            }
        )

    return pd.DataFrame(results, index=tx.index)
