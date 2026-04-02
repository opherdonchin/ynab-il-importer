from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import re

import pandas as pd

import ynab_il_importer.normalize as normalize

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
    "card_suffix",
]

RULE_KEY_COLUMNS = [
    "txn_kind",
    "fingerprint",
    "description_clean_norm",
    "account_name",
    "source",
    "direction",
    "currency",
    "amount_bucket",
    "card_suffix",
]

_TRUE_VALUES = {"1", "true", "t", "yes", "y"}
_FALSE_VALUES = {"0", "false", "f", "no", "n"}
_AMOUNT_BUCKET_RE = re.compile(
    r"^(?P<op><=|>=|=|<|>)(?P<value>\d+(?:\.\d+)?)$"
)
_AMOUNT_RANGE_RE = re.compile(
    r"^(?P<low>\d+(?:\.\d+)?)[\\s]*-[\\s]*(?P<high>\d+(?:\.\d+)?)$"
)
_CARD_SUFFIX_DIGITS_RE = re.compile(r"\D+")
_DECIMAL_ZERO_RE = re.compile(r"^\d+\.0+$")


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
    if column == "currency":
        return text.upper()
    if column == "description_clean_norm":
        return normalize.normalize_text(text)
    if column == "card_suffix":
        if _DECIMAL_ZERO_RE.match(text):
            digits = text.split(".", 1)[0]
        else:
            digits = _CARD_SUFFIX_DIGITS_RE.sub("", text)
        if not digits:
            return None
        return digits[-4:]
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


def _compute_direction_from_flows(inflow_ils: Any, outflow_ils: Any) -> str:
    inflow = pd.to_numeric(pd.Series([inflow_ils]), errors="coerce").fillna(0.0).iloc[0]
    outflow = pd.to_numeric(pd.Series([outflow_ils]), errors="coerce").fillna(0.0).iloc[0]
    if inflow > 0:
        return "inflow"
    if outflow > 0:
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
    out["card_suffix"] = _pick_series(out, ["card_suffix"])

    if "direction" in out.columns:
        out["direction"] = out["direction"].astype("string").fillna("").str.strip().str.lower()
    else:
        out["direction"] = ""

    if "inflow_ils" in out.columns or "outflow_ils" in out.columns:
        inflow = pd.to_numeric(
            out["inflow_ils"] if "inflow_ils" in out.columns else 0.0, errors="coerce"
        ).fillna(0.0)
        outflow = pd.to_numeric(
            out["outflow_ils"] if "outflow_ils" in out.columns else 0.0, errors="coerce"
        ).fillna(0.0)
        flow_direction = pd.Series(
            [_compute_direction_from_flows(i, o) for i, o in zip(inflow, outflow)],
            index=out.index,
        )
        out["direction"] = out["direction"].where(out["direction"] != "", flow_direction)
    elif "amount_ils" in out.columns:
        out["direction"] = out["direction"].where(
            out["direction"] != "", out["amount_ils"].map(_compute_direction)
        )
        inflow = pd.Series([0.0] * len(out), index=out.index)
        outflow = pd.Series([0.0] * len(out), index=out.index)
        amount_vals = pd.to_numeric(out["amount_ils"], errors="coerce").fillna(0.0)
        inflow = inflow.where(amount_vals <= 0, amount_vals)
        outflow = outflow.where(amount_vals >= 0, amount_vals.abs())
    out["direction"] = out["direction"].replace("", "zero")
    out["amount_value"] = 0.0
    if "inflow_ils" in out.columns or "outflow_ils" in out.columns:
        out["amount_value"] = inflow.where(out["direction"] == "inflow", outflow)
    elif "amount_ils" in out.columns:
        out["amount_value"] = inflow.where(out["direction"] == "inflow", outflow)

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
    out["description_clean_norm"] = raw_for_norm.map(normalize.normalize_text)

    if "fingerprint" not in out.columns:
        raise ValueError("Transactions are missing required fingerprint column")
    out["fingerprint"] = out["fingerprint"].astype("string").fillna("").str.strip()
    if (out["fingerprint"] == "").any():
        raise ValueError("Transactions contain empty fingerprint values")

    out["example_text"] = _pick_series(
        out,
        ["description_raw", "raw_text", "description_clean", "merchant_raw", "description_clean_norm"],
    )
    return out


def _rule_matches(rule: pd.Series, txn: pd.Series) -> bool:
    has_fingerprint = _blank_to_none(rule["fingerprint"]) is not None
    for col in RULE_KEY_COLUMNS:
        if col == "description_clean_norm" and has_fingerprint:
            continue
        rule_value = _blank_to_none(rule[col])
        if rule_value is None:
            continue
        if col == "amount_bucket":
            bucket_match = _match_amount_bucket(rule_value, txn)
            if bucket_match is None:
                txn_value = _normalize_key_value(col, txn.get(col))
                if txn_value != rule_value:
                    return False
            elif not bucket_match:
                return False
            continue
        txn_value = _normalize_key_value(col, txn.get(col))
        if txn_value != rule_value:
            return False
    return True


def _compile_active_rules(
    rules: pd.DataFrame,
) -> tuple[dict[str, list[pd.Series]], list[pd.Series]]:
    active_rules = rules[rules["is_active"]].copy()
    by_fingerprint: dict[str, list[pd.Series]] = {}
    wildcard_rules: list[pd.Series] = []
    for _, rule in active_rules.iterrows():
        fingerprint = _blank_to_none(rule.get("fingerprint"))
        if fingerprint is None:
            wildcard_rules.append(rule)
            continue
        by_fingerprint.setdefault(fingerprint, []).append(rule)
    return by_fingerprint, wildcard_rules


def _candidate_rules_for_txn(
    compiled_rules: tuple[dict[str, list[pd.Series]], list[pd.Series]],
    txn: pd.Series,
) -> list[pd.Series]:
    by_fingerprint, wildcard_rules = compiled_rules
    fingerprint = _blank_to_none(txn.get("fingerprint"))
    candidates = list(wildcard_rules)
    if fingerprint is not None:
        candidates.extend(by_fingerprint.get(fingerprint, []))
    return candidates


def _parse_amount_bucket(rule_value: str) -> tuple[Callable[[float], bool], str] | None:
    text = rule_value.strip().replace(" ", "")
    if not text:
        return None
    match = _AMOUNT_BUCKET_RE.match(text)
    if match:
        op = match.group("op")
        value = float(match.group("value"))
        if op == "<":
            return (lambda amt: amt < value), text
        if op == "<=":
            return (lambda amt: amt <= value), text
        if op == "=":
            return (lambda amt: amt == value), text
        if op == ">":
            return (lambda amt: amt > value), text
        if op == ">=":
            return (lambda amt: amt >= value), text
    match = _AMOUNT_RANGE_RE.match(text)
    if match:
        low = float(match.group("low"))
        high = float(match.group("high"))
        if low > high:
            low, high = high, low
        return (lambda amt: low <= amt <= high), text
    return None


def _match_amount_bucket(rule_value: str, txn: pd.Series) -> bool | None:
    parsed = _parse_amount_bucket(rule_value)
    if parsed is None:
        return None
    predicate, _ = parsed
    amount = pd.to_numeric(pd.Series([txn.get("amount_value")]), errors="coerce").fillna(0.0).iloc[0]
    return bool(predicate(float(amount)))


def _compute_specificity(rule: pd.Series) -> int:
    score = 0
    has_fingerprint = _blank_to_none(rule["fingerprint"]) is not None
    for col in RULE_KEY_COLUMNS:
        if col == "description_clean_norm" and has_fingerprint:
            continue
        if _blank_to_none(rule[col]) is not None:
            score += 1
    return score


def apply_payee_map_rules(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    tx = prepare_transactions_for_rules(transactions)
    compiled_rules = _compile_active_rules(rules)

    results: list[dict[str, Any]] = []
    for _, txn in tx.iterrows():
        matched_rules = [
            rule
            for rule in _candidate_rules_for_txn(compiled_rules, txn)
            if _rule_matches(rule, txn)
        ]
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
