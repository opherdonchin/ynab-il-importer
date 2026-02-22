import csv
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


_PURCHASE_DATE_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{2})\b")
_HEBREW_VISUAL_RUN_RE = re.compile(r"[\u0590-\u05FF][\u0590-\u05FF\s'\-\"״׳]*")


def _parse_ddmmyy_compact(value: str) -> datetime.date:
    return datetime.strptime(value.strip(), "%d%m%y").date()


def _parse_ddmmyy_slash(value: str) -> datetime.date:
    return datetime.strptime(value.strip(), "%d/%m/%y").date()


def _parse_amount(value: str) -> float:
    text = str(value).strip().replace(",", "")
    if not text:
        return 0.0
    return float(text)


def fix_hebrew_visual_order(value: str) -> str:
    text = str(value)
    return _HEBREW_VISUAL_RUN_RE.sub(lambda m: m.group(0)[::-1], text)


def read_bankin_dat(path: str | Path, account_name: str) -> pd.DataFrame:
    source_path = Path(path)
    decoded_lines = [line.decode("cp862", errors="replace").strip() for line in source_path.read_bytes().splitlines()]
    decoded_lines = [line for line in decoded_lines if line]
    reader = csv.reader(decoded_lines, delimiter=",", quotechar='"')

    rows: list[dict[str, object]] = []
    account = str(account_name).strip()

    for fields in reader:
        if len(fields) < 5:
            continue

        ref = str(fields[0]).strip()
        posting_date_code = str(fields[1]).strip()
        description_decoded = str(fields[2]).strip()
        description_fixed = fix_hebrew_visual_order(description_decoded).strip()

        posting_date = _parse_ddmmyy_compact(posting_date_code)
        purchase_date_match = _PURCHASE_DATE_RE.search(description_decoded)
        if purchase_date_match:
            purchase_date = _parse_ddmmyy_slash("/".join(purchase_date_match.groups()))
        else:
            purchase_date = posting_date

        outflow_value = _parse_amount(fields[3])
        inflow_value = _parse_amount(fields[4])
        outflow_ils = abs(outflow_value) if outflow_value < 0 else 0.0
        inflow_ils = inflow_value if inflow_value > 0 else 0.0
        amount_ils = round(inflow_ils - outflow_ils, 2)

        rows.append(
            {
                "source": "bank",
                "account_name": account,
                "date": purchase_date,
                "posting_date": posting_date,
                "description_raw": description_fixed,
                "ref": ref,
                "outflow_ils": round(outflow_ils, 2),
                "inflow_ils": round(inflow_ils, 2),
                "amount_ils": amount_ils,
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "source",
            "account_name",
            "date",
            "posting_date",
            "description_raw",
            "ref",
            "outflow_ils",
            "inflow_ils",
            "amount_ils",
        ],
    )
