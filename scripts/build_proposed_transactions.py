# ruff: noqa: E402

import argparse
import hashlib
import math
import re
import sys
import warnings
from collections.abc import Iterable, Mapping
from pathlib import Path

import pandas as pd
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.artifacts.transaction_io import (
    read_transactions_arrow,
)
import ynab_il_importer.export as export
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.rules as rules_mod
import ynab_il_importer.workflow_profiles as workflow_profiles
from ynab_il_importer import bank_identity, card_identity
from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_SCHEMA

_CARD_SUFFIX_DIGITS_RE = re.compile(r"\D+")
_CARD_SUFFIX_MEMO_TAG_RE = re.compile(r"\[card x\d{4}\]", flags=re.IGNORECASE)
_BANK_CARD_SUFFIX_RE = re.compile(r"(?<!\d)(\d{4})-\s*בכרטיס המסתיים\b", re.IGNORECASE)
TARGET_SUGGESTION_COLUMNS = [
    "transaction_id",
    "source",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "fingerprint",
    "payee_options",
    "category_options",
    "payee_selected",
    "category_selected",
]
REVIEW_ROW_COLUMNS = [
    "transaction_id",
    "source",
    "account_name",
    "date",
    "outflow_ils",
    "inflow_ils",
    "memo",
    "fingerprint",
    "payee_options",
    "category_options",
    "match_status",
    "update_maps",
    "decision_action",
    "reviewed",
    "workflow_type",
    "relation_kind",
    "match_method",
    "source_present",
    "target_present",
    "source_row_id",
    "target_row_id",
    "source_account",
    "target_account",
    "source_date",
    "target_date",
    "source_payee_current",
    "target_payee_current",
    "source_category_current",
    "target_category_current",
    "source_memo",
    "target_memo",
    "source_fingerprint",
    "target_fingerprint",
    "source_bank_txn_id",
    "source_card_txn_id",
    "source_card_suffix",
    "source_secondary_date",
    "source_ref",
    "source_context_kind",
    "source_context_category_id",
    "source_context_category_name",
    "source_context_matching_split_ids",
    "source_payee_selected",
    "source_category_selected",
    "target_context_kind",
    "target_context_matching_split_ids",
    "target_payee_selected",
    "target_category_selected",
    "source_transaction",
    "target_transaction",
]


def _load_source_inputs(paths: list[Path]) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    skipped: list[str] = []
    for path in paths:
        if path.suffix.lower() != ".parquet":
            raise ValueError(
                f"Canonical transaction input must be parquet: {path}. "
                "Provide the normalized parquet artifact directly."
            )
        table = read_transactions_arrow(path)
        if table.num_rows == 0:
            warnings.warn(f"Skipping {path} (no rows).", UserWarning)
            continue
        df = pl.from_arrow(table)
        string_cols = [name for name, dtype in df.schema.items() if dtype == pl.String]
        if string_cols:
            df = df.with_columns([pl.col(name).fill_null("").alias(name) for name in string_cols])
        fp = (
            df.get_column("fingerprint")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
        )
        if (fp == "").all():
            warnings.warn(f"Skipping {path} (all fingerprint values empty).", UserWarning)
            skipped.append(str(path))
            continue
        frames.append(df)

    if not frames:
        detail = f" Skipped: {', '.join(skipped)}" if skipped else ""
        raise ValueError(
            "No usable source rows found. Ensure normalized source files with fingerprint." + detail
        )

    return pl.concat(frames, how="diagonal_relaxed", rechunk=True)

def _expand_source_paths(files: list[Path], dirs: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for file_path in files:
        if not file_path.exists():
            raise FileNotFoundError(f"Source file does not exist: {file_path}")
        paths.append(file_path)

    for dir_path in dirs:
        if not dir_path.exists():
            raise FileNotFoundError(f"Source directory does not exist: {dir_path}")
        if not dir_path.is_dir():
            raise ValueError(f"Source path is not a directory: {dir_path}")
        parquet_paths = sorted(dir_path.glob("*.parquet"))
        if not parquet_paths:
            raise ValueError(f"No parquet files found in source directory: {dir_path}")
        paths.extend(parquet_paths)

    return paths


def _dedupe_source_overlaps(source_df: pl.DataFrame) -> pl.DataFrame:
    if source_df.is_empty():
        return source_df.clone()

    text = lambda name: pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    source_norm = text("source_system").str.to_lowercase().replace("", "source")
    bank_card_suffix = text("description_raw").map_elements(
        _extract_bank_card_suffix,
        return_dtype=pl.String,
    )
    card_card_suffix = pl.coalesce([text("source_account"), text("account_name"), pl.lit("")]).map_elements(
        _normalize_card_suffix,
        return_dtype=pl.String,
    )
    work = source_df.with_row_index("_row_index").with_columns(
        source_norm.alias("_source_norm"),
        text("date").replace("", None).str.strptime(pl.Date, strict=False).alias("_date_key"),
        text("secondary_date")
        .replace("", None)
        .str.strptime(pl.Date, strict=False)
        .alias("_secondary_date_key"),
        text("account_name").alias("_account_key"),
        pl.when(source_norm == "bank")
        .then(bank_card_suffix)
        .when(source_norm == "card")
        .then(card_card_suffix)
        .otherwise(pl.lit(""))
        .alias("_linked_card_suffix"),
        pl.col("outflow_ils").round(2).alias("_outflow_key"),
        pl.col("inflow_ils").round(2).alias("_inflow_key"),
        text("fingerprint").alias("_fingerprint_key"),
    )
    has_bank, has_card = work.select(
        (pl.col("_source_norm") == "bank").any().alias("has_bank"),
        (pl.col("_source_norm") == "card").any().alias("has_card"),
    ).row(0)
    if not has_bank or not has_card:
        return source_df.clone()

    key_cols = [
        "_account_key",
        "_date_key",
        "_outflow_key",
        "_inflow_key",
        "_fingerprint_key",
        "_linked_card_suffix",
    ]
    valid = (
        pl.col("_date_key").is_not_null()
        & (pl.col("_fingerprint_key") != "")
        & (pl.col("_linked_card_suffix") != "")
    )

    bank = (
        work.filter((pl.col("_source_norm") == "bank") & valid)
        .select(key_cols)
        .with_columns(pl.int_range(0, pl.len()).over(key_cols).alias("_dup_rank"))
    )
    card = work.filter((pl.col("_source_norm") == "card") & valid).select(
        "_row_index", *key_cols
    ).with_columns(pl.int_range(0, pl.len()).over(key_cols).alias("_dup_rank"))

    direct_drop = (
        card.join(bank.with_columns(pl.lit(True).alias("_matched")), on=key_cols + ["_dup_rank"], how="left")
        .filter(pl.col("_matched") == True)
        .select("_row_index")
    )

    second_key_cols = [
        "_account_key",
        "_secondary_date_key",
        "_outflow_key",
        "_inflow_key",
        "_fingerprint_key",
        "_linked_card_suffix",
    ]
    secondary_card = work.filter(
        (pl.col("_source_norm") == "card")
        & valid
        & pl.col("_secondary_date_key").is_not_null()
    ).select("_row_index", *second_key_cols).with_columns(
        pl.int_range(0, pl.len()).over(second_key_cols).alias("_dup_rank")
    )
    secondary_bank = work.filter(
        (pl.col("_source_norm") == "bank")
        & valid
        & pl.col("_secondary_date_key").is_not_null()
    ).select(second_key_cols).with_columns(
        pl.int_range(0, pl.len()).over(second_key_cols).alias("_dup_rank")
    )
    secondary_drop = (
        secondary_card.join(
            secondary_bank.with_columns(pl.lit(True).alias("_matched")),
            on=second_key_cols + ["_dup_rank"],
            how="left",
        )
        .filter(pl.col("_matched") == True)
        .select("_row_index")
    )

    drop_rows = pl.concat([direct_drop, secondary_drop], how="vertical_relaxed").unique().sort("_row_index")
    if drop_rows.is_empty():
        return source_df.clone()

    drop_count = drop_rows.height
    warnings.warn(
        f"Dropping {drop_count} bank/card overlap rows matched on aligned account/date/amount keys.",
        UserWarning,
    )
    return (
        work.join(drop_rows.with_columns(pl.lit(True).alias("_drop")), on="_row_index", how="left")
        .filter(pl.col("_drop").is_null())
        .sort("_row_index")
        .select(source_df.columns)
    )



def _build_options_from_applied(applied: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    if applied.empty:
        return pd.DataFrame(columns=["payee_options", "category_options"], index=applied.index)

    active_rules = rules.loc[rules["is_active"]].copy()
    rule_lookup = active_rules.set_index("rule_id", drop=False).to_dict(orient="index")
    payee_options: list[str] = []
    category_options: list[str] = []

    for rule_ids in (
        applied.get(
            "match_candidate_rule_ids",
            pd.Series([""] * len(applied), index=applied.index, dtype="string"),
        )
        .astype("string")
        .fillna("")
        .tolist()
    ):
        payees: list[str] = []
        categories: list[str] = []
        for rule_id in [part.strip() for part in str(rule_ids).split(";") if part.strip()]:
            rule = rule_lookup.get(rule_id)
            if not rule:
                continue
            payee = str(rule.get("payee_canonical") or "").strip()
            category = str(rule.get("category_target") or "").strip()
            if payee and payee not in payees:
                payees.append(payee)
            if category and category not in categories:
                categories.append(category)
        payee_options.append("; ".join(payees))
        category_options.append("; ".join(categories))

    return pd.DataFrame(
        {"payee_options": payee_options, "category_options": category_options},
        index=applied.index,
    )


def _rules_are_simple(rules: pd.DataFrame) -> bool:
    non_fingerprint_cols = [col for col in rules_mod.RULE_KEY_COLUMNS if col != "fingerprint"]
    has_other_keys = rules[non_fingerprint_cols].notna() & (rules[non_fingerprint_cols] != "")
    return not has_other_keys.any().any()


def _fast_apply_rules(transactions: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    rules = rules.copy()
    rules["payee_canonical"] = rules["payee_canonical"].astype("string").fillna("")
    rules["category_target"] = rules["category_target"].astype("string").fillna("")

    grouped = (
        rules.groupby("fingerprint", dropna=False)
        .agg(
            payee_options=("payee_canonical", lambda s: "; ".join([p for p in s if p])),
            category_options=("category_target", lambda s: "; ".join([c for c in s if c])),
            rule_count=("rule_id", "size"),
            payee_single=("payee_canonical", lambda s: next((p for p in s if p), "")),
            category_single=("category_target", lambda s: next((c for c in s if c), "")),
        )
        .reset_index()
    )

    merged = transactions.merge(grouped, on="fingerprint", how="left")
    merged["rule_count"] = merged["rule_count"].fillna(0).astype(int)
    merged["match_status"] = merged["rule_count"].map(
        lambda n: "none" if n == 0 else ("unique" if n == 1 else "ambiguous")
    )
    merged["payee_selected"] = merged["payee_single"].where(merged["match_status"] == "unique", "")
    merged["category_selected"] = merged["category_single"].where(
        merged["match_status"] == "unique", ""
    )
    return merged


def _make_transaction_id(row: Mapping[str, object]) -> str:
    parts = [
        str(row.get("account_name", "")),
        str(row.get("date", "")),
        str(row.get("outflow_ils", "")),
        str(row.get("inflow_ils", "")),
        str(row.get("fingerprint", "")),
        str(row.get("raw_text", row.get("description_raw", ""))),
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"txn_{digest}"


def _extract_bank_card_suffix(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = _BANK_CARD_SUFFIX_RE.search(text)
    if not match:
        return ""
    return match.group(1).zfill(4)


def _normalize_card_suffix(value: object) -> str:
    text = _optional_text(value)
    if not text:
        return ""
    digits = _CARD_SUFFIX_DIGITS_RE.sub("", text)
    if not digits:
        return ""
    return digits[-4:]


def _annotate_bank_debit_card_memo(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    memo = (
        out.get("memo", pd.Series([""] * len(out), index=out.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    source = (
        out.get("source", pd.Series([""] * len(out), index=out.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .str.lower()
    )
    suffix = (
        out.get("card_suffix", pd.Series([""] * len(out), index=out.index))
        .astype("string")
        .fillna("")
        .map(_normalize_card_suffix)
    )
    tag = suffix.map(lambda value: f"[card x{value}]" if value else "")
    has_tag = memo.str.contains(_CARD_SUFFIX_MEMO_TAG_RE)
    needs_tag = source.eq("bank") & tag.ne("") & ~has_tag
    out["memo"] = memo.where(~needs_tag, (memo + " " + tag).str.strip())
    return out



def _source_lineage_id(row: Mapping[str, object]) -> str:
    bank_txn_id = _optional_text(row.get("bank_txn_id", ""))
    if bank_txn_id:
        return bank_txn_id
    card_txn_id = _optional_text(row.get("card_txn_id", ""))
    if card_txn_id:
        return card_txn_id
    import_id = _optional_text(row.get("import_id", ""))
    if import_id:
        return import_id
    return _optional_text(row.get("transaction_id", ""))


def _target_lineage_ids(row: Mapping[str, object]) -> tuple[str, ...]:
    values: list[str] = []
    for candidate in [
        _optional_text(row.get("ynab_import_id") or row.get("import_id")),
        *bank_identity.extract_bank_txn_ids_from_memo(row.get("target_memo", row.get("memo", ""))),
        *card_identity.extract_card_txn_ids_from_memo(row.get("target_memo", row.get("memo", ""))),
    ]:
        text = _optional_text(candidate)
        if text and text not in values:
            values.append(text)
    return tuple(values)


def _target_memo_lineage_ids(row: Mapping[str, object]) -> tuple[str, ...]:
    values: list[str] = []
    for candidate in [
        *bank_identity.extract_bank_txn_ids_from_memo(row.get("target_memo", row.get("memo", ""))),
        *card_identity.extract_card_txn_ids_from_memo(row.get("target_memo", row.get("memo", ""))),
    ]:
        text = _optional_text(candidate)
        if text and text not in values:
            values.append(text)
    return tuple(values)


def _to_string_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (str, bytes)):
        text = _optional_text(value)
        return {text} if text else set()
    if isinstance(value, Iterable):
        out = {_optional_text(item) for item in value}
        out.discard("")
        return out
    if isinstance(value, float) and math.isnan(value):
        return set()
    text = _optional_text(value)
    return {text} if text else set()



def _optional_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()



def _build_target_suggestions_pandas(
    transactions: pd.DataFrame, *, map_path: Path
) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame(columns=TARGET_SUGGESTION_COLUMNS)

    rules = rules_mod.load_payee_map(map_path)
    out = transactions.copy()
    out["transaction_id"] = out.apply(_make_transaction_id, axis=1)
    out["memo"] = out.get("raw_text", out.get("description_raw", ""))
    out = _annotate_bank_debit_card_memo(out)
    if _rules_are_simple(rules):
        out = _fast_apply_rules(out, rules)
    else:
        applied = rules_mod.apply_payee_map_rules(out, rules)
        options = _build_options_from_applied(applied, rules)
        out = out.join(options)
        out = out.join(applied)
        out["payee_selected"] = out["payee_canonical_suggested"].where(
            out["match_status"] == "unique", ""
        )
        out["category_selected"] = out["category_target_suggested"].where(
            out["match_status"] == "unique", ""
        )
    optional_columns = [
        "source_account",
        "source_row_id",
        "card_suffix",
        "secondary_date",
        "ref",
        "balance_ils",
        "ynab_account_id",
        "bank_txn_id",
        "card_txn_id",
        "max_sheet",
        "max_txn_type",
        "max_original_amount",
        "max_original_currency",
        "max_report_period",
        "max_report_scope",
    ]
    columns = TARGET_SUGGESTION_COLUMNS + [col for col in optional_columns if col in out.columns]
    return out[columns].copy()


def build_target_suggestions(transactions: pl.DataFrame, *, map_path: Path) -> pl.DataFrame:
    if transactions.is_empty():
        return pl.DataFrame(schema={column: pl.String for column in TARGET_SUGGESTION_COLUMNS})
    suggested = _build_target_suggestions_pandas(transactions.to_pandas(), map_path=map_path)
    if suggested.empty:
        return pl.DataFrame(schema={column: pl.String for column in TARGET_SUGGESTION_COLUMNS})
    return pl.from_pandas(suggested)


def _review_split_options(value: object) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for part in _optional_text(value).split(";"):
        item = part.strip()
        if not item or item in seen:
            continue
        ordered.append(item)
        seen.add(item)
    return ordered


def _review_join_options(*values: object) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in _review_split_options(value):
            if item in seen:
                continue
            ordered.append(item)
            seen.add(item)
    return "; ".join(ordered)


def _is_cleared_match(row: Mapping[str, object]) -> bool:
    cleared = _optional_text(row.get("ynab_cleared")).casefold()
    return cleared in {"cleared", "reconciled"}


def _normalize_selected_category(payee: str, category: str) -> str:
    normalized = review_model.normalize_category_value(category)
    if review_model.is_transfer_payee(payee) and normalized.casefold() == "uncategorized":
        return review_model.NO_CATEGORY_REQUIRED
    return normalized


def _is_target_only_transfer_counterpart(row: Mapping[str, object]) -> bool:
    target_payee = _optional_text(row.get("ynab_payee_raw"))
    return review_model.is_transfer_payee(target_payee)


def _is_target_only_manual_entry(row: Mapping[str, object]) -> bool:
    approved = _optional_text(row.get("ynab_approved")).casefold() in {"true", "1", "yes", "y"}
    if not approved:
        return False

    target_payee = _optional_text(row.get("ynab_payee_raw"))
    if not target_payee:
        return False

    target_category = _normalize_selected_category(
        target_payee,
        _optional_text(row.get("ynab_category_raw")),
    )
    if not target_category and not review_model.is_transfer_payee(target_payee):
        return False

    has_lineage = any(
        _optional_text(row.get(column))
        for column in ["ynab_import_id", "ynab_matched_transaction_id"]
    )
    if has_lineage:
        return False

    target_memo = _optional_text(row.get("target_memo") or row.get("memo"))
    if bank_identity.extract_bank_txn_ids_from_memo(target_memo):
        return False
    if card_identity.extract_card_txn_ids_from_memo(target_memo):
        return False

    return True


def _is_target_only_settled(row: Mapping[str, object]) -> bool:
    return _is_cleared_match(row) or _is_target_only_manual_entry(row)


def _stable_row_ids(
    df: pl.DataFrame,
    *,
    prefix: str,
    id_columns: list[str],
) -> pl.Series:
    if df.is_empty():
        return pl.Series(name="row_id", values=[], dtype=pl.String)
    joined = pl.concat_str(
        [
            pl.col(column).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            for column in id_columns
        ],
        separator="|",
    )
    return (
        df.with_columns(joined.alias("_joined"))
        .with_columns(
            pl.col("_joined")
            .map_elements(
                lambda value: hashlib.sha1(value.encode("utf-8")).hexdigest()[:16],
                return_dtype=pl.String,
            )
            .alias("_digest")
        )
        .with_columns(
            (pl.col("_digest").cum_count().over("_digest") - 1)
            .cast(pl.String)
            .alias("_suffix")
        )
        .select(
            pl.concat_str(
                [pl.lit(prefix), pl.col("_digest"), pl.col("_suffix")],
                separator="_",
            ).alias("row_id")
        )
        .to_series()
    )


def _source_transaction_record(row: dict[str, object]) -> dict[str, object]:
    transaction = {name: row.get(name) for name in TRANSACTION_SCHEMA.names}

    transaction["source_system"] = _optional_text(transaction.get("source_system")) or "source"
    transaction["transaction_id"] = (
        _optional_text(transaction.get("transaction_id"))
        or _optional_text(transaction.get("ynab_id"))
    )
    transaction["parent_transaction_id"] = (
        _optional_text(transaction.get("parent_transaction_id"))
        or _optional_text(transaction.get("transaction_id"))
    )
    transaction["account_name"] = _optional_text(transaction.get("account_name")) or _optional_text(
        transaction.get("source_account")
    )
    transaction["source_account"] = _optional_text(
        transaction.get("source_account")
    ) or _optional_text(transaction.get("account_name"))
    transaction["payee_raw"] = (
        _optional_text(transaction.get("payee_raw"))
        or _optional_text(transaction.get("merchant_raw"))
        or _optional_text(transaction.get("description_clean"))
        or _optional_text(transaction.get("description_raw"))
        or _optional_text(transaction.get("memo"))
        or _optional_text(transaction.get("fingerprint"))
    )
    transaction["category_raw"] = _optional_text(transaction.get("category_raw"))
    transaction["memo"] = _optional_text(transaction.get("memo"))
    transaction["fingerprint"] = _optional_text(transaction.get("fingerprint"))
    return transaction


def _prepare_review_source_rows(source_df: pl.DataFrame) -> pl.DataFrame:
    text = lambda name: pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    nonempty = lambda name: text(name).replace("", None)
    canonical_lineage = pl.coalesce([nonempty("import_id"), nonempty("transaction_id"), pl.lit("")])
    normalized_suffix = pl.col("card_suffix").map_elements(
        _normalize_card_suffix,
        return_dtype=pl.String,
    )
    source_type = text("source_system").str.to_lowercase().replace("", "source")
    bank_raw_text = pl.coalesce(
        [
            nonempty("description_clean"),
            nonempty("merchant_raw"),
            nonempty("description_raw"),
            pl.lit(""),
        ]
    )
    card_raw_text = pl.coalesce(
        [
            nonempty("description_clean"),
            nonempty("description_raw"),
            nonempty("merchant_raw"),
            pl.lit(""),
        ]
    )
    default_raw_text = pl.coalesce(
        [
            nonempty("description_clean"),
            nonempty("merchant_raw"),
            nonempty("description_raw"),
            nonempty("raw_text"),
            pl.lit(""),
        ]
    )
    source_work = source_df.with_row_index("_row_index").with_columns(
        pl.lit("").alias("source_file"),
        pl.lit("").alias("raw_text"),
        canonical_lineage.alias("_canonical_lineage_id"),
        pl.lit("").alias("card_suffix"),
    ).with_columns(
        pl.when(source_type == "bank")
        .then(bank_raw_text)
        .when(source_type == "card")
        .then(card_raw_text)
        .otherwise(default_raw_text)
        .alias("raw_text"),
    ).with_columns(
        source_type.alias("source_type"),
        text("source_file").alias("source_file"),
        text("source_account").alias("source_account"),
        text("account_name").alias("account_name"),
        text("fingerprint").alias("fingerprint"),
        pl.col("outflow_ils").round(2).alias("outflow_ils"),
        pl.col("inflow_ils").round(2).alias("inflow_ils"),
        text("date").replace("", None).str.strptime(pl.Date, strict=False).alias("date_key"),
        pl.coalesce([nonempty("raw_text"), nonempty("description_raw"), pl.lit("")]).alias("memo"),
        pl.when(source_type == "bank")
        .then(pl.col("_canonical_lineage_id"))
        .otherwise(pl.lit(""))
        .alias("bank_txn_id"),
        pl.when(source_type == "card")
        .then(pl.col("_canonical_lineage_id"))
        .otherwise(pl.lit(""))
        .alias("card_txn_id"),
    ).with_columns(
        pl.when(
            (pl.col("source_type") == "bank")
            & normalized_suffix.replace("", None).is_not_null()
            & ~pl.col("memo").str.contains(_CARD_SUFFIX_MEMO_TAG_RE.pattern)
        )
        .then(
            pl.concat_str(
                [pl.col("memo"), pl.format("[card x{}]", normalized_suffix)],
                separator=" ",
            ).str.strip_chars()
        )
        .otherwise(pl.col("memo"))
        .alias("memo"),
        pl.coalesce(
            [
                nonempty("payee_raw"),
                nonempty("merchant_raw"),
                nonempty("description_clean"),
                nonempty("description_raw"),
                nonempty("raw_text"),
                nonempty("memo"),
                nonempty("fingerprint"),
                pl.lit(""),
            ]
        ).alias("source_payee_current"),
        text("category_raw").alias("source_category_current"),
        text("bank_txn_id").alias("source_bank_txn_id"),
        text("card_txn_id").alias("source_card_txn_id"),
        text("card_suffix").alias("source_card_suffix"),
        text("ref").alias("source_ref"),
        pl.coalesce([nonempty("bank_txn_id"), nonempty("card_txn_id"), pl.lit("")]).alias(
            "source_lineage_id"
        ),
        text("memo").alias("source_memo"),
        text("date")
        .replace("", None)
        .str.strptime(pl.Date, strict=False)
        .dt.strftime("%Y-%m-%d")
        .fill_null("")
        .alias("source_date"),
        text("secondary_date")
        .replace("", None)
        .str.strptime(pl.Date, strict=False)
        .dt.strftime("%Y-%m-%d")
        .fill_null("")
        .alias("source_secondary_date"),
        pl.coalesce([nonempty("bank_txn_id"), nonempty("card_txn_id"), nonempty("import_id"), pl.lit("")]).alias(
            "import_id"
        ),
    )
    if source_work.select(pl.col("fingerprint").eq("").any()).item():
        raise ValueError("Source data missing fingerprint values; run fingerprinting first.")

    prepared_pl = source_work.with_columns(
        pl.col("account_name").alias("account_key"),
        (pl.col("inflow_ils") - pl.col("outflow_ils")).round(2).alias("amount_key"),
        pl.col("date_key").alias("date"),
    ).filter(
        pl.col("account_key").is_not_null()
        & pl.col("date_key").is_not_null()
        & pl.col("amount_key").is_not_null()
    ).select(
        "_row_index",
        "source_type",
        "source_file",
        "source_account",
        "account_name",
        "date",
        "outflow_ils",
        "inflow_ils",
        "raw_text",
        "fingerprint",
        "account_key",
        "date_key",
        "amount_key",
    )
    if prepared_pl.is_empty():
        return prepared_pl

    aligned_pl = prepared_pl.join(
        source_work.select(
            "_row_index",
            "source_date",
            "source_payee_current",
            "source_category_current",
            "source_memo",
            "source_bank_txn_id",
            "source_card_txn_id",
            "source_card_suffix",
            "source_secondary_date",
            "source_ref",
            "source_lineage_id",
            *TRANSACTION_SCHEMA.names,
        ),
        on="_row_index",
        how="left",
    ).with_columns(
        _stable_row_ids(
            prepared_pl.join(
                source_work.select(
                    "_row_index",
                    "bank_txn_id",
                    "card_txn_id",
                    "ref",
                    "source_system",
                    "account_name",
                    "date",
                    "outflow_ils",
                    "inflow_ils",
                    "fingerprint",
                    "memo",
                ),
                on="_row_index",
                how="left",
            ),
            prefix="src",
            id_columns=[
                "bank_txn_id",
                "card_txn_id",
                "ref",
                "source_system",
                "account_name",
                "date",
                "outflow_ils",
                "inflow_ils",
                "fingerprint",
                "memo",
            ],
        ).alias("source_row_id"),
        pl.struct(TRANSACTION_SCHEMA.names)
        .map_elements(
            _source_transaction_record,
            return_dtype=pl.Object,
        )
        .alias("source_transaction"),
    ).drop("_row_index")
    return aligned_pl


def _prepare_review_target_rows(ynab_df: pl.DataFrame) -> pl.DataFrame:
    text = lambda name: pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    target_work = ynab_df.with_row_index("_row_index").with_columns(
        pl.lit("").alias("ynab_file"),
        text("account_id").alias("ynab_account_id"),
        text("account_name").alias("ynab_account"),
        pl.col("outflow_ils").round(2).alias("ynab_outflow_ils"),
        pl.col("inflow_ils").round(2).alias("ynab_inflow_ils"),
        text("payee_raw").alias("ynab_payee_raw"),
        text("category_raw").alias("ynab_category_raw"),
        text("fingerprint").alias("ynab_fingerprint"),
        text("ynab_id").alias("ynab_id"),
        text("import_id").alias("ynab_import_id"),
        text("matched_transaction_id").alias("ynab_matched_transaction_id"),
        text("cleared").alias("ynab_cleared"),
        text("approved").alias("ynab_approved"),
        text("account_name").alias("account_key"),
        text("date").replace("", None).str.strptime(pl.Date, strict=False).alias("date_key"),
        (pl.col("inflow_ils") - pl.col("outflow_ils")).round(2).alias("amount_key"),
        text("date")
        .replace("", None)
        .str.strptime(pl.Date, strict=False)
        .dt.strftime("%Y-%m-%d")
        .fill_null("")
        .alias("target_date"),
        text("memo").alias("target_memo"),
    )
    prepared_pl = target_work.filter(
        pl.col("account_key").is_not_null()
        & pl.col("date_key").is_not_null()
        & pl.col("amount_key").is_not_null()
    ).select(
        "_row_index",
        "account_key",
        "date_key",
        "amount_key",
        "ynab_file",
        "ynab_account_id",
        "ynab_account",
        "ynab_outflow_ils",
        "ynab_inflow_ils",
        "ynab_payee_raw",
        "ynab_category_raw",
        "ynab_fingerprint",
        "ynab_id",
        "ynab_import_id",
        "ynab_matched_transaction_id",
        "ynab_cleared",
        "ynab_approved",
    )
    if prepared_pl.is_empty():
        return prepared_pl

    aligned_pl = prepared_pl.join(
        target_work.select(
            "_row_index",
            "target_date",
            "target_memo",
            *TRANSACTION_SCHEMA.names,
        ),
        on="_row_index",
        how="left",
    ).with_columns(
        _stable_row_ids(
            prepared_pl.join(
                target_work.select(
                    "_row_index",
                    "ynab_id",
                    "account_id",
                    "account_name",
                    "date",
                    "outflow_ils",
                    "inflow_ils",
                    "payee_raw",
                    "category_raw",
                    "fingerprint",
                    "memo",
                ),
                on="_row_index",
                how="left",
            ),
            prefix="tgt",
            id_columns=[
                "ynab_id",
                "account_id",
                "account_name",
                "date",
                "outflow_ils",
                "inflow_ils",
                "payee_raw",
                "category_raw",
                "fingerprint",
                "memo",
            ],
        ).alias("target_row_id"),
        pl.struct(TRANSACTION_SCHEMA.names)
        .map_elements(
            lambda row: {name: row.get(name) for name in TRANSACTION_SCHEMA.names},
            return_dtype=pl.Object,
        )
        .alias("target_transaction"),
    ).drop("_row_index")
    return aligned_pl


def _default_source_context(
    *,
    source_category: str,
) -> dict[str, str]:
    return {
        "source_context_kind": "direct_source",
        "source_context_category_id": "",
        "source_context_category_name": review_model.normalize_category_value(source_category),
        "source_context_matching_split_ids": "",
    }


def _institutional_candidate_pairs(
    prepared_source: pl.DataFrame,
    prepared_target: pl.DataFrame,
) -> pl.DataFrame:
    if prepared_source.is_empty() or prepared_target.is_empty():
        return pl.DataFrame()
    pairs = prepared_source.join(
        prepared_target,
        on=["account_key", "date_key", "amount_key"],
        how="inner",
        suffix="_target",
    )
    if pairs.is_empty():
        return pairs

    pairs = pairs.with_columns(
        pl.col("source_lineage_id")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .alias("source_lineage_id"),
        pl.col("ynab_import_id")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .alias("ynab_import_id"),
    ).with_columns(
        (
            (pl.col("source_lineage_id") != "")
            & (pl.col("source_lineage_id") == pl.col("ynab_import_id"))
        ).alias("_exact_import_match")
    ).with_columns(
        pl.col("_exact_import_match").any().over("source_row_id").alias("_import_match_by_source")
    ).filter(
        ~pl.col("_import_match_by_source") | pl.col("_exact_import_match")
    ).drop("_import_match_by_source", "_exact_import_match")

    pairs = pairs.with_columns(
        pl.struct(["source_lineage_id", "target_memo"])
        .map_elements(
            lambda row: bool(row["source_lineage_id"])
            and row["source_lineage_id"] in _target_memo_lineage_ids(row),
            return_dtype=pl.Boolean,
        )
        .alias("_exact_memo_lineage_match")
    ).with_columns(
        pl.col("_exact_memo_lineage_match").any().over("source_row_id").alias("_memo_lineage_by_source")
    ).filter(
        ~pl.col("_memo_lineage_by_source") | pl.col("_exact_memo_lineage_match")
    ).drop("_memo_lineage_by_source", "_exact_memo_lineage_match")

    pairs = pairs.with_columns(
        pl.len().over("source_row_id").alias("_source_candidate_count"),
        pl.len().over("target_row_id").alias("_target_candidate_count"),
    ).with_columns(
        (
            (pl.col("_source_candidate_count") > 1)
            | (pl.col("_target_candidate_count") > 1)
        ).alias("ambiguous_key")
    )
    return pairs


def _apply_review_target_suggestions(
    relations: pl.DataFrame,
    *,
    map_path: Path,
) -> pl.DataFrame:
    source_rows = relations.filter(pl.col("source_present"))
    if source_rows.is_empty():
        return relations

    text = lambda name: pl.col(name).cast(pl.String, strict=False).fill_null("").str.strip_chars()
    candidates = source_rows.select(
        text("source").alias("source"),
        text("source_account").alias("account_name"),
        text("source_account").alias("source_account"),
        text("source_row_id").alias("source_row_id"),
        text("source_date").alias("date"),
        pl.col("outflow_ils").cast(pl.Float64, strict=False).fill_null(0.0).alias("outflow_ils"),
        pl.col("inflow_ils").cast(pl.Float64, strict=False).fill_null(0.0).alias("inflow_ils"),
        text("source_memo").alias("memo"),
        text("source_memo").alias("raw_text"),
        text("source_fingerprint").alias("fingerprint"),
    ).filter(pl.col("fingerprint") != "")

    if candidates.is_empty():
        suggested = pl.DataFrame(
            schema=[
                ("source_row_id", pl.String),
                ("suggested_payee_options", pl.String),
                ("suggested_category_options", pl.String),
                ("suggested_payee_selected", pl.String),
                ("suggested_category_selected", pl.String),
            ]
        )
    else:
        suggested = build_target_suggestions(candidates, map_path=map_path).rename(
            {
                "payee_options": "suggested_payee_options",
                "category_options": "suggested_category_options",
                "payee_selected": "suggested_payee_selected",
                "category_selected": "suggested_category_selected",
            }
        )
        if not suggested.is_empty():
            suggested = suggested.group_by("source_row_id", maintain_order=True).agg(
                pl.col("suggested_payee_options"),
                pl.col("suggested_category_options"),
                pl.col("suggested_payee_selected"),
                pl.col("suggested_category_selected"),
            ).with_columns(
                pl.col("suggested_payee_options")
                .map_elements(lambda values: _review_join_options(*values), return_dtype=pl.String)
                .alias("suggested_payee_options"),
                pl.col("suggested_category_options")
                .map_elements(lambda values: _review_join_options(*values), return_dtype=pl.String)
                .alias("suggested_category_options"),
                pl.col("suggested_payee_selected")
                .map_elements(
                    lambda values: next(
                        (item.strip() for item in values if item and item.strip()),
                        "",
                    ),
                    return_dtype=pl.String,
                )
                .alias("suggested_payee_selected"),
                pl.col("suggested_category_selected")
                .map_elements(
                    lambda values: next(
                        (item.strip() for item in values if item and item.strip()),
                        "",
                    ),
                    return_dtype=pl.String,
                )
                .alias("suggested_category_selected"),
            )

    merged = relations.join(suggested, on="source_row_id", how="left").with_columns(
        text("target_payee_current").alias("_current_target_payee"),
        pl.struct(["target_payee_current", "target_category_current"])
        .map_elements(
            lambda row: _normalize_selected_category(
                _optional_text(row["target_payee_current"]),
                _optional_text(row["target_category_current"]),
            ),
            return_dtype=pl.String,
        )
        .alias("_current_target_category"),
        text("suggested_payee_selected").alias("_suggested_payee"),
        pl.col("suggested_category_selected")
        .cast(pl.String, strict=False)
        .fill_null("")
        .map_elements(review_model.normalize_category_value, return_dtype=pl.String)
        .alias("_suggested_category"),
    ).with_columns(
        pl.struct(["_current_target_payee", "suggested_payee_options"])
        .map_elements(
            lambda row: _review_join_options(
                row["_current_target_payee"],
                _optional_text(row["suggested_payee_options"]),
            ),
            return_dtype=pl.String,
        )
        .alias("payee_options"),
        pl.struct(["_current_target_category", "suggested_category_options"])
        .map_elements(
            lambda row: _review_join_options(
                row["_current_target_category"],
                _optional_text(row["suggested_category_options"]),
            ),
            return_dtype=pl.String,
        )
        .alias("category_options"),
        pl.when(pl.col("target_present") & (pl.col("_current_target_payee") != ""))
        .then(pl.col("_current_target_payee"))
        .otherwise(pl.col("_suggested_payee"))
        .alias("target_payee_selected"),
        pl.when(pl.col("target_present") & (pl.col("_current_target_category") != ""))
        .then(pl.col("_current_target_category"))
        .otherwise(pl.col("_suggested_category"))
        .alias("target_category_selected"),
    )

    return merged.drop(
        "suggested_payee_options",
        "suggested_category_options",
        "suggested_payee_selected",
        "suggested_category_selected",
        "_current_target_payee",
        "_current_target_category",
        "_suggested_payee",
        "_suggested_category",
    )


def build_review_rows(
    source_df: pl.DataFrame,
    ynab_df: pl.DataFrame,
    *,
    map_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    prepared_source_pl = _prepare_review_source_rows(source_df)
    prepared_target_pl = _prepare_review_target_rows(ynab_df)
    pairs_pl = _institutional_candidate_pairs(prepared_source_pl, prepared_target_pl)
    text = lambda name: pl.col(name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    bool_text = lambda name: text(name).str.to_lowercase()
    transaction_id_expr = lambda account, date, outflow, inflow, fingerprint, raw_text: pl.struct(
        account_name=account,
        date=date,
        outflow_ils=outflow,
        inflow_ils=inflow,
        fingerprint=fingerprint,
        raw_text=raw_text,
    ).map_elements(lambda row: _make_transaction_id(row), return_dtype=pl.String)
    normalized_category_expr = lambda col: pl.col(col).map_elements(
        review_model.normalize_category_value,
        return_dtype=pl.String,
    )
    selected_category_expr = lambda payee_col, category_col: pl.struct(
        payee=pl.col(payee_col), category=pl.col(category_col)
    ).map_elements(
        lambda row: _normalize_selected_category(
            _optional_text(row["payee"]),
            _optional_text(row["category"]),
        ),
        return_dtype=pl.String,
    )
    settled_target_expr = pl.struct(
        ["ynab_approved", "ynab_payee_raw", "ynab_category_raw", "ynab_import_id", "ynab_matched_transaction_id", "target_memo", "ynab_cleared"]
    ).map_elements(
        lambda row: _is_target_only_settled(row),
        return_dtype=pl.Boolean,
    )
    transfer_counterpart_expr = pl.struct(["ynab_payee_raw"]).map_elements(
        lambda row: _is_target_only_transfer_counterpart(row),
        return_dtype=pl.Boolean,
    )

    matched_pl = pairs_pl.with_columns(
        normalized_category_expr("source_category_current").alias("_source_category_norm"),
        selected_category_expr("ynab_payee_raw", "ynab_category_raw").alias("_target_category_selected"),
        (((~pl.col("ambiguous_key")) & bool_text("ynab_cleared").is_in(["cleared", "reconciled"]))).alias("_matched_cleared"),
        transaction_id_expr(
            text("account_name"),
            text("source_date"),
            pl.col("outflow_ils"),
            pl.col("inflow_ils"),
            text("fingerprint"),
            text("source_memo"),
        ).alias("transaction_id"),
    ).with_columns(
        text("source_type").alias("source"),
        pl.coalesce([text("ynab_account"), text("account_name")]).alias("account_name"),
        pl.coalesce([text("source_date"), text("target_date")]).alias("date"),
        pl.col("outflow_ils").alias("outflow_ils"),
        pl.col("inflow_ils").alias("inflow_ils"),
        pl.coalesce([text("source_memo"), text("target_memo")]).alias("memo"),
        pl.coalesce([text("fingerprint"), text("ynab_fingerprint")]).alias("fingerprint"),
        text("ynab_payee_raw").alias("payee_options"),
        pl.col("_target_category_selected").alias("category_options"),
        pl.when(pl.col("ambiguous_key"))
        .then(pl.lit("ambiguous"))
        .when(pl.col("_matched_cleared"))
        .then(pl.lit("matched_cleared"))
        .otherwise(pl.lit("matched_auto"))
        .alias("match_status"),
        pl.lit("").alias("update_maps"),
        pl.when(pl.col("ambiguous_key")).then(pl.lit("No decision")).otherwise(pl.lit("keep_match")).alias("decision_action"),
        pl.col("_matched_cleared").alias("reviewed"),
        pl.lit("institutional").alias("workflow_type"),
        pl.when(pl.col("ambiguous_key"))
        .then(pl.lit("ambiguous_candidate"))
        .when(pl.col("_matched_cleared"))
        .then(pl.lit("matched_cleared_pair"))
        .otherwise(pl.lit("matched_pair"))
        .alias("relation_kind"),
        pl.when(pl.col("ambiguous_key"))
        .then(pl.lit("exact_date_amount_not_unique"))
        .otherwise(pl.lit("exact_date_amount"))
        .alias("match_method"),
        pl.lit(True).alias("source_present"),
        pl.lit(True).alias("target_present"),
        text("source_row_id").alias("source_row_id"),
        text("target_row_id").alias("target_row_id"),
        pl.coalesce([text("source_account"), text("account_name")]).alias("source_account"),
        pl.coalesce([text("ynab_account"), text("account_name")]).alias("target_account"),
        text("source_date").alias("source_date"),
        text("target_date").alias("target_date"),
        text("source_payee_current").alias("source_payee_current"),
        text("ynab_payee_raw").alias("target_payee_current"),
        pl.col("_source_category_norm").alias("source_category_current"),
        text("ynab_category_raw").alias("target_category_current"),
        text("source_memo").alias("source_memo"),
        text("target_memo").alias("target_memo"),
        text("fingerprint").alias("source_fingerprint"),
        text("ynab_fingerprint").alias("target_fingerprint"),
        text("source_bank_txn_id").alias("source_bank_txn_id"),
        text("source_card_txn_id").alias("source_card_txn_id"),
        text("source_card_suffix").alias("source_card_suffix"),
        text("source_secondary_date").alias("source_secondary_date"),
        text("source_ref").alias("source_ref"),
        pl.lit("direct_source").alias("source_context_kind"),
        pl.lit("").alias("source_context_category_id"),
        pl.col("_source_category_norm").alias("source_context_category_name"),
        pl.lit("").alias("source_context_matching_split_ids"),
        text("source_payee_current").alias("source_payee_selected"),
        pl.col("_source_category_norm").alias("source_category_selected"),
        pl.lit("").alias("target_context_kind"),
        pl.lit("").alias("target_context_matching_split_ids"),
        text("ynab_payee_raw").alias("target_payee_selected"),
        pl.col("_target_category_selected").alias("target_category_selected"),
    ).select(REVIEW_ROW_COLUMNS)

    matched_source_ids = pairs_pl.select("source_row_id").unique()
    unmatched_source_pl = prepared_source_pl.join(
        matched_source_ids,
        on="source_row_id",
        how="anti",
    ).with_columns(
        normalized_category_expr("source_category_current").alias("_source_category_norm"),
        transaction_id_expr(
            text("account_name"),
            text("source_date"),
            pl.col("outflow_ils"),
            pl.col("inflow_ils"),
            text("fingerprint"),
            text("source_memo"),
        ).alias("transaction_id"),
    ).with_columns(
        text("source_type").alias("source"),
        text("account_name").alias("account_name"),
        text("source_date").alias("date"),
        pl.col("outflow_ils").alias("outflow_ils"),
        pl.col("inflow_ils").alias("inflow_ils"),
        text("source_memo").alias("memo"),
        text("fingerprint").alias("fingerprint"),
        pl.lit("").alias("payee_options"),
        pl.lit("").alias("category_options"),
        pl.lit("source_only").alias("match_status"),
        pl.lit("").alias("update_maps"),
        pl.lit("create_target").alias("decision_action"),
        pl.lit(False).alias("reviewed"),
        pl.lit("institutional").alias("workflow_type"),
        pl.lit("source_only").alias("relation_kind"),
        pl.lit("").alias("match_method"),
        pl.lit(True).alias("source_present"),
        pl.lit(False).alias("target_present"),
        text("source_row_id").alias("source_row_id"),
        pl.lit("").alias("target_row_id"),
        pl.coalesce([text("source_account"), text("account_name")]).alias("source_account"),
        text("account_name").alias("target_account"),
        text("source_date").alias("source_date"),
        pl.lit("").alias("target_date"),
        text("source_payee_current").alias("source_payee_current"),
        pl.lit("").alias("target_payee_current"),
        pl.col("_source_category_norm").alias("source_category_current"),
        pl.lit("").alias("target_category_current"),
        text("source_memo").alias("source_memo"),
        pl.lit("").alias("target_memo"),
        text("fingerprint").alias("source_fingerprint"),
        pl.lit("").alias("target_fingerprint"),
        text("source_bank_txn_id").alias("source_bank_txn_id"),
        text("source_card_txn_id").alias("source_card_txn_id"),
        text("source_card_suffix").alias("source_card_suffix"),
        text("source_secondary_date").alias("source_secondary_date"),
        text("source_ref").alias("source_ref"),
        pl.lit("direct_source").alias("source_context_kind"),
        pl.lit("").alias("source_context_category_id"),
        pl.col("_source_category_norm").alias("source_context_category_name"),
        pl.lit("").alias("source_context_matching_split_ids"),
        text("source_payee_current").alias("source_payee_selected"),
        pl.col("_source_category_norm").alias("source_category_selected"),
        pl.lit("").alias("target_context_kind"),
        pl.lit("").alias("target_context_matching_split_ids"),
        pl.lit("").alias("target_payee_selected"),
        pl.lit("").alias("target_category_selected"),
        pl.lit(None, dtype=pl.Object).alias("target_transaction"),
    ).select(REVIEW_ROW_COLUMNS)

    matched_target_ids = pairs_pl.select("target_row_id").unique()
    unmatched_target_pl = prepared_target_pl.join(
        matched_target_ids,
        on="target_row_id",
        how="anti",
    ).with_columns(
        selected_category_expr("ynab_payee_raw", "ynab_category_raw").alias("_target_category_selected"),
        transfer_counterpart_expr.alias("_settled_transfer_counterpart"),
        settled_target_expr.alias("_settled_target_only"),
        transaction_id_expr(
            text("ynab_account"),
            text("target_date"),
            pl.col("ynab_outflow_ils"),
            pl.col("ynab_inflow_ils"),
            text("ynab_fingerprint"),
            text("target_memo"),
        ).alias("transaction_id"),
    ).with_columns(
        pl.lit("ynab").alias("source"),
        text("ynab_account").alias("account_name"),
        text("target_date").alias("date"),
        pl.col("ynab_outflow_ils").alias("outflow_ils"),
        pl.col("ynab_inflow_ils").alias("inflow_ils"),
        text("target_memo").alias("memo"),
        text("ynab_fingerprint").alias("fingerprint"),
        text("ynab_payee_raw").alias("payee_options"),
        pl.col("_target_category_selected").alias("category_options"),
        pl.lit("target_only").alias("match_status"),
        pl.lit("").alias("update_maps"),
        pl.when(pl.col("_settled_target_only")).then(pl.lit("ignore_row")).otherwise(pl.lit("No decision")).alias("decision_action"),
        pl.col("_settled_target_only").alias("reviewed"),
        pl.lit("institutional").alias("workflow_type"),
        pl.when(pl.col("_settled_transfer_counterpart"))
        .then(pl.lit("target_only_transfer_counterpart"))
        .when(bool_text("ynab_cleared").is_in(["cleared", "reconciled"]))
        .then(pl.lit("target_only_cleared"))
        .when(pl.col("_settled_target_only"))
        .then(pl.lit("target_only_manual"))
        .otherwise(pl.lit("target_only"))
        .alias("relation_kind"),
        pl.lit("").alias("match_method"),
        pl.lit(False).alias("source_present"),
        pl.lit(True).alias("target_present"),
        pl.lit("").alias("source_row_id"),
        text("target_row_id").alias("target_row_id"),
        pl.lit("").alias("source_account"),
        text("ynab_account").alias("target_account"),
        pl.lit("").alias("source_date"),
        text("target_date").alias("target_date"),
        pl.lit("").alias("source_payee_current"),
        text("ynab_payee_raw").alias("target_payee_current"),
        pl.lit("").alias("source_category_current"),
        text("ynab_category_raw").alias("target_category_current"),
        pl.lit("").alias("source_memo"),
        text("target_memo").alias("target_memo"),
        pl.lit("").alias("source_fingerprint"),
        text("ynab_fingerprint").alias("target_fingerprint"),
        pl.lit("").alias("source_bank_txn_id"),
        pl.lit("").alias("source_card_txn_id"),
        pl.lit("").alias("source_card_suffix"),
        pl.lit("").alias("source_secondary_date"),
        pl.lit("").alias("source_ref"),
        pl.lit("").alias("source_context_kind"),
        pl.lit("").alias("source_context_category_id"),
        pl.lit("").alias("source_context_category_name"),
        pl.lit("").alias("source_context_matching_split_ids"),
        pl.lit("").alias("source_payee_selected"),
        pl.lit("").alias("source_category_selected"),
        pl.lit("").alias("target_context_kind"),
        pl.lit("").alias("target_context_matching_split_ids"),
        text("ynab_payee_raw").alias("target_payee_selected"),
        pl.col("_target_category_selected").alias("target_category_selected"),
        pl.lit(None, dtype=pl.Object).alias("source_transaction"),
    ).select(REVIEW_ROW_COLUMNS)

    relations_pl = pl.concat([matched_pl, unmatched_source_pl, unmatched_target_pl], how="diagonal_relaxed")
    relations = _apply_review_target_suggestions(relations_pl, map_path=map_path)
    relations = review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(review_io.coerce_review_artifact_table(relations))
    )
    return relations, pairs_pl


def run_build(
    *,
    source_paths: list[Path],
    ynab_path: Path,
    map_path: Path,
    out_path: Path,
    pairs_out: str = "",
) -> None:
    if not source_paths:
        raise ValueError("Provide at least one --source or --source-dir input.")

    source_df = _dedupe_source_overlaps(_load_source_inputs(source_paths))
    ynab_df = pl.from_arrow(read_transactions_arrow(ynab_path))
    if ynab_df.is_empty():
        raise ValueError("No rows found in YNAB input.")

    out, pairs = build_review_rows(source_df, ynab_df, map_path=map_path)
    if pairs_out:
        export.write_dataframe(pairs.to_pandas(), pairs_out)
        print(export.wrote_message(pairs_out, len(pairs)))

    if out_path.suffix.lower() == ".parquet":
        review_io.save_review_artifact(out, out_path)
    else:
        review_io.save_reviewed_transactions(out, out_path)
    print(export.wrote_message(out_path, len(out)))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build institutional source-vs-target review rows."
    )
    parser.add_argument("--profile", default="", help="Workflow profile (for default map paths).")
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--source-dir", action="append", default=[])
    parser.add_argument("--ynab", required=True)
    parser.add_argument("--map", dest="map_path", type=Path, default=None)
    parser.add_argument("--out", dest="out_path", default="outputs/proposed_transactions.parquet")
    parser.add_argument("--pairs-out", dest="pairs_out", default="")
    args = parser.parse_args()

    profile = workflow_profiles.resolve_profile(args.profile or None)
    map_path = args.map_path or profile.payee_map_path

    source_paths = _expand_source_paths(
        [Path(p) for p in args.source],
        [Path(p) for p in args.source_dir],
    )
    run_build(
        source_paths=source_paths,
        ynab_path=Path(args.ynab),
        map_path=map_path,
        out_path=Path(args.out_path),
        pairs_out=args.pairs_out,
    )


if __name__ == "__main__":
    main()
