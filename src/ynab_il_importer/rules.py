from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Callable

import re

import polars as pl

import ynab_il_importer.fingerprint as fingerprint
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
APPLY_RESULT_SCHEMA = {
    "payee_canonical_suggested": pl.Utf8,
    "category_target_suggested": pl.Utf8,
    "match_rule_id": pl.Utf8,
    "match_specificity_score": pl.Int64,
    "match_status": pl.Utf8,
    "match_candidate_rule_ids": pl.Utf8,
    "match_rule_count": pl.Int64,
}


def _blank_to_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
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


def normalize_payee_map_rules(df: pl.DataFrame) -> pl.DataFrame:
    missing_columns = [col for col in PAYEE_MAP_COLUMNS if col not in df.columns]
    if missing_columns:
        df = df.with_columns([pl.lit("").alias(col) for col in missing_columns])
    df = df.select(PAYEE_MAP_COLUMNS)

    df = df.with_columns(
        pl.col("rule_id").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars(),
        pl.col("is_active").map_elements(_normalize_is_active, return_dtype=pl.Boolean),
        pl.col("priority").map_elements(_normalize_priority, return_dtype=pl.Int64),
        *[
            pl.col(col).map_elements(
                lambda value, *, _col=col: _normalize_key_value(_col, value),
                return_dtype=pl.Utf8,
            )
            for col in RULE_KEY_COLUMNS
        ],
        pl.col("payee_canonical").map_elements(_blank_to_none, return_dtype=pl.Utf8),
        pl.col("category_target").map_elements(_blank_to_none, return_dtype=pl.Utf8),
        pl.col("notes").map_elements(_blank_to_none, return_dtype=pl.Utf8),
    )
    if bool(df["rule_id"].eq("").any()):
        raise ValueError("payee_map.csv contains empty rule_id values")
    duplicate_ids = (
        df.filter(pl.col("rule_id").is_duplicated())["rule_id"].unique().sort().to_list()
    )
    if duplicate_ids:
        raise ValueError(f"payee_map.csv contains duplicate rule_id values: {duplicate_ids}")

    return df.with_columns(
        pl.struct(RULE_KEY_COLUMNS)
        .map_elements(_compute_specificity, return_dtype=pl.Int64)
        .alias("_specificity")
    )


def _canonicalize_payee_rule_fingerprints(
    rules: pl.DataFrame,
    *,
    fingerprint_map_path: str | Path,
) -> pl.DataFrame:
    map_rules = fingerprint.load_fingerprint_map(fingerprint_map_path)
    return rules.with_columns(
        pl.col("fingerprint").map_elements(
            lambda value: (
                fingerprint.canonicalize_fingerprint_value(
                    value,
                    map_rules=map_rules,
                )
                if _blank_to_none(value) is not None
                else None
            ),
            return_dtype=pl.Utf8,
        )
    )


def load_payee_map(
    path: str | Path,
    *,
    fingerprint_map_path: str | Path | None = None,
) -> pl.DataFrame:
    map_path = Path(path)
    if not map_path.exists():
        raise FileNotFoundError(f"Missing payee map file: {map_path}")
    raw = pl.read_csv(map_path, infer_schema_length=0).fill_null("")
    normalized = normalize_payee_map_rules(raw)
    if fingerprint_map_path is None:
        return normalized
    return _canonicalize_payee_rule_fingerprints(
        normalized,
        fingerprint_map_path=fingerprint_map_path,
    )


def _compute_direction(amount_ils: Any) -> str:
    try:
        amount = float(amount_ils or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    if amount > 0:
        return "inflow"
    if amount < 0:
        return "outflow"
    return "zero"


def _compute_direction_from_flows(inflow_ils: Any, outflow_ils: Any) -> str:
    try:
        inflow = float(inflow_ils or 0.0)
    except (TypeError, ValueError):
        inflow = 0.0
    try:
        outflow = float(outflow_ils or 0.0)
    except (TypeError, ValueError):
        outflow = 0.0
    if inflow > 0:
        return "inflow"
    if outflow > 0:
        return "outflow"
    return "zero"


def _pick_col(df: pl.DataFrame, columns: list[str], default: str = "") -> pl.Series:
    for col in columns:
        if col in df.columns:
            series = df[col].cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            if bool(series.ne("").any()):
                return series
    return pl.Series("picked", [default] * len(df), dtype=pl.Utf8)


def prepare_transactions_for_rules(df: pl.DataFrame) -> pl.DataFrame:
    txn_kind = _pick_col(df, ["txn_kind"]).str.to_lowercase()
    source = _pick_col(df, ["source"]).str.to_lowercase()
    account_name = _pick_col(df, ["account_name"])
    amount_bucket = _pick_col(df, ["amount_bucket"])
    card_suffix = _pick_col(df, ["card_suffix"])

    currency_series = _pick_col(df, ["currency"], default="ILS").to_list()
    currency = pl.Series(
        "currency",
        [("ILS" if value == "" else value).upper() for value in currency_series],
        dtype=pl.Utf8,
    )

    prepared = df.with_columns(
        pl.Series("txn_kind", txn_kind.to_list(), dtype=pl.Utf8),
        pl.Series("source", source.to_list(), dtype=pl.Utf8),
        pl.Series("account_name", account_name.to_list(), dtype=pl.Utf8),
        currency,
        pl.Series("amount_bucket", amount_bucket.to_list(), dtype=pl.Utf8),
        pl.Series("card_suffix", card_suffix.to_list(), dtype=pl.Utf8),
    )

    if "direction" in prepared.columns:
        direction_values = (
            prepared["direction"]
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_lowercase()
            .to_list()
        )
    else:
        direction_values = [""] * len(prepared)

    inflow = pl.Series("inflow", [0.0] * len(prepared), dtype=pl.Float64)
    outflow = pl.Series("outflow", [0.0] * len(prepared), dtype=pl.Float64)
    if "inflow_ils" in prepared.columns or "outflow_ils" in prepared.columns:
        inflow = (
            prepared["inflow_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
            if "inflow_ils" in prepared.columns
            else inflow
        )
        outflow = (
            prepared["outflow_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
            if "outflow_ils" in prepared.columns
            else outflow
        )
        flow_direction = [
            _compute_direction_from_flows(i, o)
            for i, o in zip(inflow.to_list(), outflow.to_list(), strict=False)
        ]
        direction_values = [
            current if current else fallback
            for current, fallback in zip(direction_values, flow_direction, strict=False)
        ]
    elif "amount_ils" in prepared.columns:
        amount_vals = prepared["amount_ils"].cast(pl.Float64, strict=False).fill_null(0.0)
        direction_values = [
            current if current else fallback
            for current, fallback in zip(
                direction_values,
                [_compute_direction(value) for value in amount_vals.to_list()],
                strict=False,
            )
        ]
        inflow = pl.Series(
            "inflow",
            [value if value > 0 else 0.0 for value in amount_vals.to_list()],
            dtype=pl.Float64,
        )
        outflow = pl.Series(
            "outflow",
            [abs(value) if value < 0 else 0.0 for value in amount_vals.to_list()],
            dtype=pl.Float64,
        )

    direction_values = [value if value else "zero" for value in direction_values]
    prepared = prepared.with_columns(
        pl.Series("direction", direction_values, dtype=pl.Utf8)
    )

    if "inflow_ils" in prepared.columns or "outflow_ils" in prepared.columns or "amount_ils" in prepared.columns:
        amount_value = pl.Series(
            "amount_value",
            [
                i if direction == "inflow" else o
                for direction, i, o in zip(
                    direction_values,
                    inflow.to_list(),
                    outflow.to_list(),
                    strict=False,
                )
            ],
            dtype=pl.Float64,
        )
    else:
        amount_value = pl.Series("amount_value", [0.0] * len(prepared), dtype=pl.Float64)
    prepared = prepared.with_columns(amount_value)

    raw_for_norm = _pick_col(
        prepared,
        [
            "description_clean_norm",
            "description_clean",
            "merchant_raw",
            "description_raw",
            "raw_norm",
            "raw_text",
        ],
    )
    prepared = prepared.with_columns(
        pl.Series(
            "description_clean_norm",
            [normalize.normalize_text(value) for value in raw_for_norm.to_list()],
            dtype=pl.Utf8,
        )
    )

    if "fingerprint" not in prepared.columns:
        raise ValueError("Transactions are missing required fingerprint column")
    fingerprint = (
        prepared["fingerprint"].cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    )
    if bool(fingerprint.eq("").any()):
        raise ValueError("Transactions contain empty fingerprint values")
    prepared = prepared.with_columns(
        pl.Series("fingerprint", fingerprint.to_list(), dtype=pl.Utf8)
    )

    prepared = prepared.with_columns(
        pl.Series(
            "example_text",
            _pick_col(
                prepared,
                [
                    "description_raw",
                    "raw_text",
                    "description_clean",
                    "merchant_raw",
                    "description_clean_norm",
                ],
            ).to_list(),
            dtype=pl.Utf8,
        )
    )
    return prepared


def _rule_matches(rule: dict[str, Any], txn: dict[str, Any]) -> bool:
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
    rules: pl.DataFrame,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    active_rules = rules.filter(pl.col("is_active"))
    by_fingerprint: dict[str, list[dict[str, Any]]] = {}
    wildcard_rules: list[dict[str, Any]] = []
    for rule in active_rules.iter_rows(named=True):
        fingerprint = _blank_to_none(rule.get("fingerprint"))
        if fingerprint is None:
            wildcard_rules.append(rule)
            continue
        by_fingerprint.setdefault(fingerprint, []).append(rule)
    return by_fingerprint, wildcard_rules


def _candidate_rules_for_txn(
    compiled_rules: tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]],
    txn: dict[str, Any],
) -> list[dict[str, Any]]:
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


def _match_amount_bucket(rule_value: str, txn: dict[str, Any]) -> bool | None:
    parsed = _parse_amount_bucket(rule_value)
    if parsed is None:
        return None
    predicate, _ = parsed
    try:
        amount = float(txn.get("amount_value") or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    return bool(predicate(float(amount)))


def _compute_specificity(rule: dict[str, Any]) -> int:
    score = 0
    has_fingerprint = _blank_to_none(rule["fingerprint"]) is not None
    for col in RULE_KEY_COLUMNS:
        if col == "description_clean_norm" and has_fingerprint:
            continue
        if _blank_to_none(rule[col]) is not None:
            score += 1
    return score


def apply_payee_map_rules(transactions: pl.DataFrame, rules: pl.DataFrame) -> pl.DataFrame:
    tx = prepare_transactions_for_rules(transactions)
    compiled_rules = _compile_active_rules(rules)

    results: list[dict[str, Any]] = []
    for txn in tx.iter_rows(named=True):
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
            key=lambda rule: (
                -int(rule["priority"]),
                -int(rule["_specificity"]),
                str(rule["rule_id"]),
            ),
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

    if not results:
        return pl.DataFrame(schema=APPLY_RESULT_SCHEMA)
    return pl.from_dicts(results, schema=APPLY_RESULT_SCHEMA)
