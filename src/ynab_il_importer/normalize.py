import re
import unicodedata
from typing import Any


_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_LONG_DIGIT_RUN_RE = re.compile(r"\d{4,}")
_SPACE_RE = re.compile(r"\s+")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""

    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = _PUNCT_RE.sub(" ", text)
    text = _LONG_DIGIT_RUN_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text
