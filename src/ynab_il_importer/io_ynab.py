from pathlib import Path
import io
import zipfile

import pandas as pd

import ynab_il_importer.fingerprint as fingerprint
from ynab_il_importer.artifacts.transaction_io import flat_projection_to_canonical_table


def _read_ynab_csv(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            register_members = [
                name for name in archive.namelist() if name.lower().endswith("register.csv")
            ]
            if not register_members:
                raise ValueError(f"Could not find Register.csv inside {path}")
            if len(register_members) > 1:
                raise ValueError(
                    f"Multiple register CSV files found inside {path}: {register_members}"
                )
            raw = archive.read(register_members[0]).decode("utf-8-sig")
        return pd.read_csv(io.StringIO(raw))
    return pd.read_csv(path)


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return str(normalized[key])
    return None


def _series_or_default(df: pd.DataFrame, col: str | None, default: str = "") -> pd.Series:
    if col and col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def _parse_amount(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("")
    text = text.str.replace(",", "", regex=False)
    text = text.str.replace("₪", "", regex=False)
    text = text.str.replace(r"[^\d.\-()]", "", regex=True)
    text = text.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(text, errors="coerce").fillna(0.0)


def _infer_txn_kind(
    inflow_ils: pd.Series, outflow_ils: pd.Series, payee_raw: pd.Series, category_raw: pd.Series
) -> pd.Series:
    inflow = pd.to_numeric(inflow_ils, errors="coerce").fillna(0.0)
    payee = payee_raw.astype("string").fillna("").str.strip().str.lower()
    category = category_raw.astype("string").fillna("").str.strip().str.lower()

    kind = pd.Series(["expense"] * len(inflow), index=inflow.index, dtype="string")
    is_transfer = payee.str.startswith("transfer :") | payee.str.startswith("transfer:")
    kind.loc[is_transfer] = "transfer"

    is_inflow = inflow > 0
    is_income = is_inflow & category.str.contains("ready to assign", regex=False)
    kind.loc[is_income] = "income"
    kind.loc[is_inflow & ~is_income & ~is_transfer] = "credit"
    return kind


def is_proper_format(path: str | Path) -> bool:
    source_path = Path(path)
    suffix = source_path.suffix.lower()
    if suffix and suffix not in {".csv", ".zip"}:
        return False
    try:
        sample = _read_ynab_csv(source_path).head(1)
    except Exception:
        return False
    if sample.empty:
        return False
    date_col = _find_column(sample, ["Date", "תאריך"])
    outflow_col = _find_column(sample, ["Outflow", "הוצאה", "חיוב"])
    inflow_col = _find_column(sample, ["Inflow", "הכנסה", "זיכוי"])
    payee_col = _find_column(sample, ["Payee", "מוטב", "שם מוטב"])
    category_col = _find_column(sample, ["Category", "קטגוריה", "Category Name"])
    if date_col is None:
        return False
    if outflow_col is None and inflow_col is None:
        return False
    if payee_col is None and category_col is None:
        return False
    return True


def read_raw(
    path: str | Path,
    *,
    use_fingerprint_map: bool = True,
    account_map_path: str | Path | None = None,
    fingerprint_map_path: str | Path | None = None,
    fingerprint_log_path: str | Path | None = None,
) -> pd.DataFrame:
    _ = use_fingerprint_map
    _ = account_map_path
    _ = fingerprint_map_path
    _ = fingerprint_log_path
    raw = _read_ynab_csv(Path(path))
    raw.columns = [str(col).strip() for col in raw.columns]

    date_col = _find_column(raw, ["Date", "תאריך"])
    if date_col is None:
        raise ValueError("Could not find date column in YNAB register")

    payee_col = _find_column(raw, ["Payee", "מוטב", "שם מוטב"])
    category_col = _find_column(raw, ["Category", "קטגוריה", "Category Name"])
    master_col = _find_column(raw, ["Master Category", "קטגוריה ראשית"])
    sub_col = _find_column(raw, ["Sub Category", "Subcategory", "קטגוריית משנה"])
    outflow_col = _find_column(raw, ["Outflow", "הוצאה", "חיוב"])
    inflow_col = _find_column(raw, ["Inflow", "הכנסה", "זיכוי"])
    memo_col = _find_column(raw, ["Memo", "הערה", "הערות"])
    cleared_col = _find_column(raw, ["Cleared", "פיוס"])
    account_col = _find_column(raw, ["Account", "account"])

    if category_col:
        category = _series_or_default(raw, category_col).astype("string").fillna("")
    else:
        master = _series_or_default(raw, master_col).astype("string").fillna("").str.strip()
        sub = _series_or_default(raw, sub_col).astype("string").fillna("").str.strip()
        category = master.where(sub == "", master + ":" + sub).str.strip(":")

    outflow_ils = _parse_amount(_series_or_default(raw, outflow_col, "0")).round(2)
    inflow_ils = _parse_amount(_series_or_default(raw, inflow_col, "0")).round(2)

    memo = _series_or_default(raw, memo_col).astype("string").fillna("")
    payee_raw = _series_or_default(raw, payee_col).astype("string").fillna("")
    account_name = _series_or_default(raw, account_col).astype("string").fillna("").str.strip()

    result = pd.DataFrame(
        {
            "source": "ynab",
            "account_name": account_name,
            "source_account": account_name,
            "date": pd.to_datetime(raw[date_col], errors="coerce", dayfirst=True).dt.date,
            "payee_raw": payee_raw,
            "category_raw": category,
            "merchant_raw": payee_raw,
            "description_clean": payee_raw,
            "description_raw": memo.where(memo.str.strip() != "", payee_raw),
            "outflow_ils": outflow_ils,
            "inflow_ils": inflow_ils,
            "memo": memo,
            "cleared": _series_or_default(raw, cleared_col).astype("string").fillna(""),
            "currency": "ILS",
            "amount_bucket": "",
        }
    )
    result["txn_kind"] = _infer_txn_kind(
        result["inflow_ils"],
        result["outflow_ils"],
        result["payee_raw"],
        result["category_raw"],
    )
    result = fingerprint.apply_fingerprints(
        result,
        use_fingerprint_map=use_fingerprint_map,
        fingerprint_map_path=fingerprint_map_path or fingerprint.DEFAULT_FINGERPRINT_MAP_PATH,
        log_path=fingerprint_log_path or fingerprint.DEFAULT_FINGERPRINT_LOG_PATH,
    )

    return result[
        [
            "source",
            "account_name",
            "source_account",
            "date",
            "payee_raw",
            "category_raw",
            "merchant_raw",
            "description_clean",
            "description_raw",
            "description_clean_norm",
            "fingerprint",
            "outflow_ils",
            "inflow_ils",
            "txn_kind",
            "cleared",
            "currency",
            "amount_bucket",
            "memo",
        ]
    ]


def read_canonical(
    path: str | Path,
    **kwargs,
):
    df = read_raw(path, **kwargs)
    return flat_projection_to_canonical_table(
        df,
        artifact_kind="normalized_source_transaction",
        source_system="ynab",
    )
