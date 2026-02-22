import hashlib
import re
from typing import Any

from ynab_il_importer.normalize import normalize_text


_STANDALONE_NUMBER_RE = re.compile(r"\b\d+\b")
_SPACE_RE = re.compile(r"\s+")


def fingerprint_v0(value: Any) -> str:
    text = normalize_text(value)
    text = _STANDALONE_NUMBER_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    tokens = text.split()
    return " ".join(tokens[:6])


def fingerprint_hash_v1(txn_kind: Any, description_clean_norm: Any, length: int = 12) -> str:
    kind = str(txn_kind or "").strip().lower()
    description = normalize_text(description_clean_norm)
    payload = f"{kind}\n{description}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return digest[:length]
