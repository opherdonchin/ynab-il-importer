from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any

import pandas as pd

from ynab_il_importer.normalize import normalize_text


DEFAULT_TOKEN_LIMIT = 10
FINGERPRINT_MAP_COLUMNS = ["rule_id", "is_active", "priority", "pattern", "canonical_text", "notes"]
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
    "משיכה",
    "משיכת",
    "מטבע",
    "מטח",
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
    # English corporate/web suffixes
    "www",
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


def _strip_noise_tokens(text: str) -> str:
    tokens = []
    for token in text.split():
        if not token:
            continue
        if len(token) == 1:
            continue
        if token in _DROP_TOKENS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def fingerprint_v0(value: Any, token_limit: int = DEFAULT_TOKEN_LIMIT) -> str:
    text = normalize_text(value)
    text = _STANDALONE_NUMBER_RE.sub(" ", text)
    text = _strip_noise_tokens(text)
    text = _SPACE_RE.sub(" ", text).strip()
    tokens = text.split()
    return " ".join(tokens[:token_limit])


def _blank_to_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
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


def load_fingerprint_map(path: str | Path) -> pd.DataFrame:
    map_path = Path(path)
    if not map_path.exists():
        raise FileNotFoundError(f"Missing fingerprint map file: {map_path}")
    raw = pd.read_csv(map_path, dtype="string").fillna("")
    for col in FINGERPRINT_MAP_COLUMNS:
        if col not in raw.columns:
            raw[col] = ""
    raw = raw[FINGERPRINT_MAP_COLUMNS].copy()

    raw["rule_id"] = raw["rule_id"].astype("string").fillna("").str.strip()
    if (raw["rule_id"] == "").any():
        raise ValueError("fingerprint_map.csv contains empty rule_id values")
    duplicate_ids = raw["rule_id"][raw["rule_id"].duplicated()].unique().tolist()
    if duplicate_ids:
        raise ValueError(f"fingerprint_map.csv contains duplicate rule_id values: {duplicate_ids}")

    raw["is_active"] = raw["is_active"].map(_normalize_is_active)
    raw["priority"] = raw["priority"].map(_normalize_priority)
    raw["pattern"] = raw["pattern"].astype("string").fillna("").str.strip()
    raw["canonical_text"] = raw["canonical_text"].astype("string").fillna("").str.strip()
    raw["notes"] = raw["notes"].astype("string").fillna("").str.strip()

    expanded: list[dict[str, Any]] = []
    for _, row in raw.iterrows():
        if not row["is_active"]:
            continue
        if row["pattern"] == "":
            continue
        if row["canonical_text"] == "":
            raise ValueError(f"fingerprint_map.csv has empty canonical_text for rule_id={row['rule_id']}")
        patterns = [p.strip() for p in str(row["pattern"]).split("|") if p.strip()]
        for pattern in patterns:
            expanded.append(
                {
                    "rule_id": row["rule_id"],
                    "priority": int(row["priority"]),
                    "pattern": normalize_text(pattern),
                    "canonical_text": normalize_text(row["canonical_text"]),
                    "notes": row["notes"],
                }
            )

    if not expanded:
        return pd.DataFrame(columns=["rule_id", "priority", "pattern", "canonical_text", "notes"])

    rules = pd.DataFrame(expanded)
    rules["pattern_length"] = rules["pattern"].astype("string").fillna("").str.len()
    rules = rules.sort_values(
        ["priority", "pattern_length", "rule_id"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return rules


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
    map_rules: pd.DataFrame | None = None,
    log_path: str | Path = Path("outputs/fingerprint_log.csv"),
) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy()

    out = df.copy()
    text_raw, text_source = _pick_text_source(
        out, ["description_clean", "merchant_raw", "description_raw", "raw_text"]
    )
    text_normalized = text_raw.map(normalize_text)

    rules = map_rules
    if rules is None:
        rules = load_fingerprint_map(Path("mappings/fingerprint_map.csv"))

    matched_rule_id = pd.Series([""] * len(out), index=out.index, dtype="string")
    matched_pattern = pd.Series([""] * len(out), index=out.index, dtype="string")
    canonical_text = text_normalized.copy()

    if not rules.empty:
        unmatched = matched_rule_id == ""
        for _, rule in rules.iterrows():
            pattern = str(rule["pattern"]).strip()
            if not pattern:
                continue
            mask = unmatched & text_normalized.str.contains(pattern, na=False, regex=False)
            if not mask.any():
                continue
            canonical_text.loc[mask] = rule["canonical_text"]
            matched_rule_id.loc[mask] = rule["rule_id"]
            matched_pattern.loc[mask] = pattern
            unmatched = matched_rule_id == ""
            if not unmatched.any():
                break

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
