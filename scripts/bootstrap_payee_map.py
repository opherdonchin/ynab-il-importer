import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.rules import PAYEE_MAP_COLUMNS


def _slug(text: str, max_len: int = 40) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text.strip())
    cleaned = "_".join([part for part in cleaned.split("_") if part])
    if not cleaned:
        return ""
    return cleaned[:max_len]


def _make_rule_id(fingerprint: str, payee: str, category: str, index: int) -> str:
    base = _slug(fingerprint) or _slug(payee) or _slug(category)
    if not base:
        digest = hashlib.sha1(f"{fingerprint}|{payee}|{category}".encode("utf-8")).hexdigest()[:8]
        return f"fp_{index}_{digest}"
    return f"{base}_{index}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap payee_map.csv from matched pairs")
    parser.add_argument("--pairs", action="append", required=True, help="matched_pairs.csv path")
    parser.add_argument("--out", type=Path, default=Path("mappings/payee_map.csv"))
    args = parser.parse_args()

    frames: list[pd.DataFrame] = []
    for path in args.pairs:
        df = pd.read_csv(path)
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        raise ValueError("No rows found in matched pairs inputs.")

    pairs = pd.concat(frames, ignore_index=True)
    required = {"fingerprint", "ynab_payee_raw", "ynab_category_raw"}
    missing = required - set(pairs.columns)
    if missing:
        raise ValueError(f"matched_pairs missing columns: {sorted(missing)}")

    grouped = (
        pairs.groupby(["fingerprint", "ynab_payee_raw", "ynab_category_raw"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    grouped["ynab_payee_raw"] = (
        grouped["ynab_payee_raw"].astype("string").fillna("").str.strip()
    )
    grouped["ynab_category_raw"] = (
        grouped["ynab_category_raw"].astype("string").fillna("").str.strip()
    )
    grouped = grouped[grouped["fingerprint"].astype("string").fillna("").str.strip() != ""]

    if grouped["ynab_payee_raw"].str.contains(";", regex=False).any():
        raise ValueError("YNAB payee contains ';' which is not allowed in payee_map")
    if grouped["ynab_category_raw"].str.contains(";", regex=False).any():
        raise ValueError("YNAB category contains ';' which is not allowed in payee_map")

    grouped = grouped.sort_values(["fingerprint", "count"], ascending=[True, False]).reset_index(
        drop=True
    )

    rows: list[dict[str, object]] = []
    for idx, row in grouped.iterrows():
        fingerprint = str(row["fingerprint"]).strip()
        payee = str(row["ynab_payee_raw"]).strip()
        category = str(row["ynab_category_raw"]).strip()
        rule_id = _make_rule_id(fingerprint, payee, category, idx + 1)
        rows.append(
            {
                "rule_id": rule_id,
                "is_active": True,
                "priority": 0,
                "txn_kind": "",
                "fingerprint": fingerprint,
                "description_clean_norm": "",
                "account_name": "",
                "source": "",
                "direction": "",
                "currency": "",
                "amount_bucket": "",
                "payee_canonical": payee,
                "category_target": category,
                "notes": f"bootstrap count={int(row['count'])}",
            }
        )

    out = pd.DataFrame(rows)
    for col in PAYEE_MAP_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[PAYEE_MAP_COLUMNS]
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Wrote {args.out} ({len(out)} rows)")


if __name__ == "__main__":
    main()
