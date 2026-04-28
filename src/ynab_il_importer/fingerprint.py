from __future__ import annotations

import csv
from datetime import datetime
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

import ynab_il_importer.normalize as normalize


DEFAULT_TOKEN_LIMIT = 10
FINGERPRINT_MAP_COLUMNS = ["rule_id", "is_active", "priority", "pattern", "canonical_text", "notes"]
DEFAULT_FINGERPRINT_MAP_PATH = Path("mappings/fingerprint_map.csv")
DEFAULT_FINGERPRINT_LOG_PATH = Path("outputs/fingerprint_log.csv")
LOG_COLUMNS = [
    "run_id",
    "row_index",
    "source",
    "source_file",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "text_source_column",
    "text_raw",
    "text_normalized",
    "matched_rule_id",
    "matched_pattern",
    "canonical_text",
    "fingerprint",
]

_STANDALONE_NUMBER_RE = re.compile(r"\b\d+\b")
_SPACE_RE = re.compile(r"\s+")

_DROP_TOKENS = {
    # Hebrew transactional noise
    "הו",
    "הוראת",
    "הוראה",
    "קבע",
    "קבוע",
    "קבועה",
    "חיוב",
    "תשלום",
    "תשלומים",
    "עסקה",
    "זיכוי",
    "עמלה",
    "הפקדה",
    "העברה",
    "העברת",
    "הורדת",
    "משיכה",
    "משיכת",
    "מטבע",
    "מטח",
    "אשראי",
    "כרטיס",
    "כרטיסי",
    "כרטיסים",
    "מכשיר",
    "סניף",
    "קוד",
    "מספר",
    "למי",
    "חדש",
    # Hebrew corporate suffixes
    "בע",
    "בעמ",
    # English transactional noise
    "payment",
    "payments",
    "debit",
    "credit",
    "charge",
    "fee",
    "fees",
    "transaction",
    "transfer",
    "standing",
    "order",
    "recurring",
    "installment",
    "installments",
    "atm",
    "pos",
    "purchase",
    "refund",
    "reversal",
    "pmt",
    "pmts",
    "online",
    "web",
    "app",
    # English corporate/web suffixes
    "www",
    "http",
    "https",
    "com",
    "co",
    "company",
    "ltd",
    "limited",
    "inc",
    "llc",
    "corp",
    "gmbh",
    "sarl",
    "sa",
    "spa",
    "plc",
    "ag",
    "bv",
    "oy",
    "pty",
    "srl",
    "sro",
    # Common country codes
    "il",
    "us",
    "uk",
    "de",
    "fr",
    "es",
    "it",
    "bg",
    "gr",
    "sg",
    "au",
    "ca",
    "ch",
    "nl",
    "be",
    "at",
    "cz",
    "ro",
    "hu",
    "pl",
    "pt",
    "se",
    "no",
    "dk",
    "fi",
    "ie",
}

_LOCATION_TOKENS = {
    # Hebrew common locations / mall terms
    "באר",
    "שבע",
    "בש",
    "תל",
    "אביב",
    "ביג",
    "קניון",
    "סנטר",
    "פלזה",
    "צפון",
    "דרום",
    "מזרח",
    "מערב",
    "נמל",
    "תחנה",
    "תחנת",
    "תחנות",
    # English common locations / mall terms
    "tel",
    "aviv",
    "center",
    "central",
    "north",
    "south",
    "east",
    "west",
    "mall",
    "plaza",
    "station",
    "port",
}


def _looks_like_high_entropy(token: str) -> bool:
    if len(token) >= 24:
        return True
    digit_count = sum(ch.isdigit() for ch in token)
    if digit_count == 0:
        return False
    alpha_count = sum(ch.isalpha() for ch in token)
    if digit_count >= 3:
        return True
    if digit_count >= 2 and alpha_count >= 2:
        return True
    return False


def _strip_noise_tokens(text: str) -> str:
    tokens = []
    location_tokens = []
    for token in text.split():
        if not token:
            continue
        if len(token) == 1:
            continue
        if token in _DROP_TOKENS:
            continue
        if _looks_like_high_entropy(token):
            continue
        if token in _LOCATION_TOKENS:
            location_tokens.append(token)
            continue
        tokens.append(token)
    if tokens:
        return " ".join(tokens)
    return " ".join(location_tokens)


def fingerprint_v0(value: Any, token_limit: int = DEFAULT_TOKEN_LIMIT) -> str:
    text = normalize.normalize_text(value)
    text = _STANDALONE_NUMBER_RE.sub(" ", text)
    stripped = _strip_noise_tokens(text)
    if stripped.strip() == "":
        tokens_all = text.split()
        if "תשלום" in tokens_all:
            return "subject"
        stripped = text
    stripped = _SPACE_RE.sub(" ", stripped).strip()
    tokens = stripped.split()
    return " ".join(tokens[:token_limit])


def canonicalize_text(
    value: Any,
    *,
    map_rules: list[dict[str, Any]] | None = None,
) -> tuple[str, str, str, str]:
    text_normalized = normalize.normalize_text(value)
    if not map_rules:
        return text_normalized, text_normalized, "", ""

    for rule in map_rules:
        pattern = str(rule["pattern"]).strip()
        if not pattern:
            continue
        if pattern in text_normalized:
            return (
                text_normalized,
                str(rule["canonical_text"]),
                str(rule["rule_id"]),
                pattern,
            )
    return text_normalized, text_normalized, "", ""


def canonicalize_fingerprint_value(
    value: Any,
    *,
    map_rules: list[dict[str, Any]] | None = None,
) -> str:
    _text_normalized, canonical_text, _rule_id, _pattern = canonicalize_text(
        value,
        map_rules=map_rules,
    )
    return fingerprint_v0(canonical_text)


def _blank_to_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_is_active(value: Any) -> bool:
    text = _blank_to_none(value)
    if text is None:
        return True
    lowered = text.lower()
    if lowered in {"1", "true", "t", "yes", "y"}:
        return True
    if lowered in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Invalid is_active value: {value!r}")


def _normalize_priority(value: Any) -> int:
    text = _blank_to_none(value)
    if text is None:
        return 0
    return int(text)


def load_fingerprint_map(path: str | Path) -> list[dict[str, Any]]:
    map_path = Path(path)
    if not map_path.exists():
        raise FileNotFoundError(f"Missing fingerprint map file: {map_path}")

    with open(map_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        raw_rows = [row for row in reader]

    for col in FINGERPRINT_MAP_COLUMNS:
        for row in raw_rows:
            if col not in row:
                row[col] = ""

    rule_ids = [row["rule_id"].strip() for row in raw_rows]
    if any(r == "" for r in rule_ids):
        raise ValueError("fingerprint_map.csv contains empty rule_id values")
    seen: set[str] = set()
    duplicates: list[str] = []
    for r in rule_ids:
        if r in seen:
            duplicates.append(r)
        seen.add(r)
    if duplicates:
        raise ValueError(f"fingerprint_map.csv contains duplicate rule_id values: {list(dict.fromkeys(duplicates))}")

    expanded: list[dict[str, Any]] = []
    for row in raw_rows:
        rule_id = row["rule_id"].strip()
        is_active = _normalize_is_active(row.get("is_active", ""))
        if not is_active:
            continue
        pattern_raw = row.get("pattern", "").strip()
        if not pattern_raw:
            continue
        canonical_raw = row.get("canonical_text", "").strip()
        if not canonical_raw:
            raise ValueError(f"fingerprint_map.csv has empty canonical_text for rule_id={rule_id}")
        priority = _normalize_priority(row.get("priority", ""))
        notes = row.get("notes", "").strip()
        patterns = [p.strip() for p in pattern_raw.split("|") if p.strip()]
        for pattern in patterns:
            expanded.append(
                {
                    "rule_id": rule_id,
                    "priority": priority,
                    "pattern": normalize.normalize_text(pattern),
                    "canonical_text": normalize.normalize_text(canonical_raw),
                    "notes": notes,
                }
            )

    return sorted(
        expanded,
        key=lambda r: (-r["priority"], -len(r["pattern"]), r["rule_id"]),
    )


def _pick_text_source(df: pd.DataFrame, candidates: list[str]) -> tuple[pd.Series, pd.Series]:
    text = pd.Series([""] * len(df), index=df.index, dtype="string")
    source_col = pd.Series([""] * len(df), index=df.index, dtype="string")
    for col in candidates:
        if col not in df.columns:
            continue
        series = df[col].astype("string").fillna("").str.strip()
        missing = text == ""
        text = text.where(~missing, series)
        source_col = source_col.where(~missing, col)
    return text, source_col


def apply_fingerprints(
    df: pd.DataFrame,
    map_rules: list[dict[str, Any]] | None = None,
    log_path: str | Path = DEFAULT_FINGERPRINT_LOG_PATH,
    use_fingerprint_map: bool = True,
    fingerprint_map_path: str | Path = DEFAULT_FINGERPRINT_MAP_PATH,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy()

    out = df.copy()
    text_raw, text_source = _pick_text_source(
        out, ["description_clean", "merchant_raw", "description_raw", "raw_text"]
    )
    text_normalized = text_raw.map(normalize.normalize_text)

    rules: list[dict[str, Any]]
    if not use_fingerprint_map:
        rules = []
    elif map_rules is not None:
        rules = map_rules
    else:
        rules = load_fingerprint_map(fingerprint_map_path)

    matched_rule_id = pd.Series([""] * len(out), index=out.index, dtype="string")
    matched_pattern = pd.Series([""] * len(out), index=out.index, dtype="string")
    canonical_text = text_normalized.copy()

    if rules:
        canonicalized = [
            canonicalize_text(value, map_rules=rules) for value in text_normalized.tolist()
        ]
        canonical_text = pd.Series(
            [canonical for _norm, canonical, _rule_id, _pattern in canonicalized],
            index=out.index,
            dtype="string",
        )
        matched_rule_id = pd.Series(
            [rule_id for _norm, _canonical, rule_id, _pattern in canonicalized],
            index=out.index,
            dtype="string",
        )
        matched_pattern = pd.Series(
            [pattern for _norm, _canonical, _rule_id, pattern in canonicalized],
            index=out.index,
            dtype="string",
        )

    out["description_clean_norm"] = text_normalized
    out["fingerprint"] = canonical_text.map(fingerprint_v0)

    run_id = datetime.now().astimezone().isoformat()
    log_df = pd.DataFrame(
        {
            "run_id": run_id,
            "row_index": out.index.astype("string"),
            "source": out.get("source", ""),
            "source_file": out.get("source_file", ""),
            "account_name": out.get("account_name", ""),
            "date": out.get("date", ""),
            "outflow_ils": out.get("outflow_ils", ""),
            "inflow_ils": out.get("inflow_ils", ""),
            "text_source_column": text_source,
            "text_raw": text_raw,
            "text_normalized": text_normalized,
            "matched_rule_id": matched_rule_id,
            "matched_pattern": matched_pattern,
            "canonical_text": canonical_text,
            "fingerprint": out["fingerprint"],
        }
    )

    log_df = log_df[LOG_COLUMNS].copy()
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists()
    encoding = "utf-8-sig" if write_header else "utf-8"
    log_df.to_csv(log_path, index=False, mode="a", header=write_header, encoding=encoding)

    return out
