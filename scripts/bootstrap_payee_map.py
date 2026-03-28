import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.export as export
import ynab_il_importer.rules as rules


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


def _amount_value(pairs: pd.DataFrame) -> pd.Series:
    if "signed_amount" in pairs.columns:
        return pd.to_numeric(pairs["signed_amount"], errors="coerce").fillna(0.0).abs().round(2)
    inflow = pd.to_numeric(pairs.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
    outflow = pd.to_numeric(pairs.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
    return inflow.where(inflow > 0, outflow).round(2)


def _format_amount_bucket(amount_value: float) -> str:
    text = f"{float(amount_value):.2f}".rstrip("0").rstrip(".")
    return f"={text}"


def _load_pairs(paths: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
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

    pairs = pairs.copy()
    pairs["fingerprint"] = pairs["fingerprint"].astype("string").fillna("").str.strip()
    pairs["ynab_payee_raw"] = pairs["ynab_payee_raw"].astype("string").fillna("").str.strip()
    pairs["ynab_category_raw"] = pairs["ynab_category_raw"].astype("string").fillna("").str.strip()
    pairs = pairs.loc[pairs["fingerprint"] != ""].copy()
    pairs["amount_value"] = _amount_value(pairs)

    if pairs["ynab_payee_raw"].str.contains(";", regex=False).any():
        raise ValueError("YNAB payee contains ';' which is not allowed in payee_map")
    if pairs["ynab_category_raw"].str.contains(";", regex=False).any():
        raise ValueError("YNAB category contains ';' which is not allowed in payee_map")

    return pairs


def _build_bootstrap_rules(pairs: pd.DataFrame) -> pd.DataFrame:
    pairs = pairs.copy()
    if "amount_value" not in pairs.columns:
        pairs["amount_value"] = _amount_value(pairs)
    pairs["fingerprint"] = pairs["fingerprint"].astype("string").fillna("").str.strip()
    pairs["ynab_payee_raw"] = pairs["ynab_payee_raw"].astype("string").fillna("").str.strip()
    pairs["ynab_category_raw"] = pairs["ynab_category_raw"].astype("string").fillna("").str.strip()
    pairs = pairs.loc[pairs["fingerprint"] != ""].copy()

    grouped = (
        pairs.groupby(
            ["fingerprint", "ynab_payee_raw", "ynab_category_raw", "amount_value"],
            dropna=False,
        )
        .size()
        .reset_index(name="count")
    )
    grouped["target_key"] = list(
        zip(
            grouped["ynab_payee_raw"].astype("string"),
            grouped["ynab_category_raw"].astype("string"),
        )
    )
    outcome_counts = (
        grouped.groupby("fingerprint", dropna=False)["target_key"]
        .nunique()
        .rename("target_outcome_count")
        .reset_index()
    )
    grouped = grouped.merge(outcome_counts, on="fingerprint", how="left")

    single_outcome = (
        grouped.loc[grouped["target_outcome_count"] <= 1]
        .groupby(["fingerprint", "ynab_payee_raw", "ynab_category_raw"], dropna=False)
        .agg(count=("count", "sum"), target_outcome_count=("target_outcome_count", "max"))
        .reset_index()
    )
    single_outcome["amount_value"] = 0.0
    single_outcome["amount_bucket"] = ""

    multi_outcome = grouped.loc[grouped["target_outcome_count"] > 1].copy()
    multi_outcome["amount_bucket"] = multi_outcome["amount_value"].map(_format_amount_bucket)

    frames = [frame for frame in [single_outcome, multi_outcome] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=rules.PAYEE_MAP_COLUMNS)
    grouped = frames[0].copy() if len(frames) == 1 else pd.concat(frames, ignore_index=True, sort=False)

    grouped = grouped.sort_values(
        [
            "fingerprint",
            "target_outcome_count",
            "amount_value",
            "count",
            "ynab_payee_raw",
            "ynab_category_raw",
        ],
        ascending=[True, False, True, False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    rows: list[dict[str, object]] = []
    for idx, row in grouped.iterrows():
        fingerprint = str(row["fingerprint"]).strip()
        payee = str(row["ynab_payee_raw"]).strip()
        category = str(row["ynab_category_raw"]).strip()
        amount_bucket = str(row["amount_bucket"]).strip()
        rule_id = _make_rule_id(fingerprint, payee, category, idx + 1)
        note_bits = [f"bootstrap count={int(row['count'])}"]
        if amount_bucket:
            note_bits.append(f"amount_bucket={amount_bucket}")
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
                "amount_bucket": amount_bucket,
                "payee_canonical": payee,
                "category_target": category,
                "notes": "; ".join(note_bits),
            }
        )

    out = pd.DataFrame(rows)
    for col in rules.PAYEE_MAP_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out[rules.PAYEE_MAP_COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap payee_map.csv from matched pairs")
    parser.add_argument("--pairs", action="append", required=True, help="matched_pairs.csv path")
    parser.add_argument("--out", type=Path, default=Path("mappings/payee_map.csv"))
    args = parser.parse_args()

    pairs = _load_pairs(args.pairs)
    out = _build_bootstrap_rules(pairs)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(export.wrote_message(args.out, len(out)))


if __name__ == "__main__":
    main()
