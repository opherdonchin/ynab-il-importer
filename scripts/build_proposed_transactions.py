# ruff: noqa: E402

import argparse
import hashlib
import re
import sys
import warnings
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ynab_il_importer.artifacts.transaction_io import (
    read_transactions_pandas,
)
import ynab_il_importer.export as export
import ynab_il_importer.pairing as pairing
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.review_app.model as review_model
import ynab_il_importer.rules as rules_mod
import ynab_il_importer.workflow_profiles as workflow_profiles
from ynab_il_importer import bank_identity, card_identity
from ynab_il_importer.artifacts.transaction_schema import TRANSACTION_ARTIFACT_VERSION, TRANSACTION_SCHEMA

_CARD_SUFFIX_DIGITS_RE = re.compile(r"\D+")
_CARD_SUFFIX_MEMO_TAG_RE = re.compile(r"\[card x\d{4}\]", flags=re.IGNORECASE)
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


def _load_csvs(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    skipped: list[str] = []
    for path in paths:
        df = _load_canonical_transaction_input(path)
        for col in [
            "outflow_ils",
            "inflow_ils",
            "balance_ils",
            "max_original_amount",
            "max_exchange_rate",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if df.empty:
            warnings.warn(f"Skipping {path} (no rows).", UserWarning)
            continue
        if "fingerprint" not in df.columns:
            warnings.warn(f"Skipping {path} (missing fingerprint column).", UserWarning)
            skipped.append(str(path))
            continue
        fp = df["fingerprint"].astype("string").fillna("").str.strip()
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

    return pd.concat(frames, ignore_index=True)


def _load_canonical_transaction_input(path: Path) -> pd.DataFrame:
    if path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Canonical transaction input must be parquet: {path}. "
            "Provide the normalized parquet artifact directly."
        )
    return read_transactions_pandas(path).fillna("")


def _canonical_transaction_dict(
    row: pd.Series,
    *,
    artifact_kind: str,
    source_system_fallback: str,
) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for field in TRANSACTION_SCHEMA:
        raw = row.get(field.name)
        if raw is None or raw is pd.NA:
            normalized[field.name] = None
            continue
        if pa.types.is_boolean(field.type):
            normalized[field.name] = bool(raw)
        elif pa.types.is_floating(field.type):
            number = pd.to_numeric(pd.Series([raw]), errors="coerce").fillna(0.0).iloc[0]
            normalized[field.name] = float(number)
        elif pa.types.is_list(field.type):
            normalized[field.name] = raw if isinstance(raw, list) else None
        else:
            normalized[field.name] = str(raw).strip()

    normalized["artifact_kind"] = normalized.get("artifact_kind") or artifact_kind
    normalized["artifact_version"] = (
        normalized.get("artifact_version") or TRANSACTION_ARTIFACT_VERSION
    )
    normalized["source_system"] = (
        normalized.get("source_system") or source_system_fallback
    )
    normalized["transaction_id"] = normalized.get("transaction_id") or normalized.get("ynab_id") or ""
    normalized["parent_transaction_id"] = (
        normalized.get("parent_transaction_id") or normalized.get("transaction_id") or ""
    )
    normalized["account_name"] = normalized.get("account_name") or normalized.get("source_account") or ""
    normalized["source_account"] = normalized.get("source_account") or normalized.get("account_name") or ""
    normalized["date"] = normalized.get("date") or ""
    normalized["payee_raw"] = (
        normalized.get("payee_raw")
        or normalized.get("merchant_raw")
        or normalized.get("description_clean")
        or normalized.get("description_raw")
        or normalized.get("memo")
        or ""
    )
    normalized["category_raw"] = normalized.get("category_raw") or ""
    normalized["memo"] = normalized.get("memo") or ""
    normalized["fingerprint"] = normalized.get("fingerprint") or ""
    return normalized


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
        csv_paths = sorted(dir_path.glob("*.csv"))
        if not csv_paths:
            raise ValueError(f"No CSV files found in source directory: {dir_path}")
        paths.extend(csv_paths)

    return paths


def _dedupe_source_overlaps(source_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty or "source" not in source_df.columns:
        return source_df.reset_index(drop=True)

    source_norm = source_df["source"].astype("string").fillna("").str.strip().str.lower()
    if not (source_norm == "bank").any() or not (source_norm == "card").any():
        return source_df.reset_index(drop=True)

    work = source_df.copy()
    work["_source_norm"] = source_norm
    work["_date_key"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["_secondary_date_key"] = pd.to_datetime(
        work.get("secondary_date", pd.Series([pd.NA] * len(work), index=work.index)),
        errors="coerce",
    ).dt.date
    work["_account_key"] = (
        work.get("account_name", pd.Series([""] * len(work), index=work.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    work["_card_suffix_key"] = (
        work.get("card_suffix", pd.Series([""] * len(work), index=work.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    work["_outflow_key"] = pd.to_numeric(work["outflow_ils"], errors="coerce").fillna(0.0).round(2)
    work["_inflow_key"] = pd.to_numeric(work["inflow_ils"], errors="coerce").fillna(0.0).round(2)
    work["_fingerprint_key"] = work["fingerprint"].astype("string").fillna("").str.strip()

    key_cols = [
        "_account_key",
        "_date_key",
        "_outflow_key",
        "_inflow_key",
        "_fingerprint_key",
        "_card_suffix_key",
    ]
    valid = work["_date_key"].notna() & (work["_fingerprint_key"] != "")

    bank = work.loc[(work["_source_norm"] == "bank") & valid, key_cols].copy()
    bank["_dup_rank"] = bank.groupby(key_cols, dropna=False).cumcount()

    card = work.loc[(work["_source_norm"] == "card") & valid, key_cols].copy()
    card["_dup_rank"] = card.groupby(key_cols, dropna=False).cumcount()
    card["_row_index"] = card.index

    matched_cards = card.merge(
        bank.assign(_matched=True),
        on=key_cols + ["_dup_rank"],
        how="left",
    )
    drop_index = matched_cards.loc[matched_cards["_matched"].eq(True), "_row_index"].tolist()

    secondary_aligned_card = work.loc[
        (work["_source_norm"] == "card")
        & valid
        & work["_secondary_date_key"].notna()
        & (work["_account_key"] != ""),
        [
            "_account_key",
            "_secondary_date_key",
            "_outflow_key",
            "_inflow_key",
            "_card_suffix_key",
        ],
    ].copy()
    if not secondary_aligned_card.empty:
        second_key_cols = [
            "_account_key",
            "_secondary_date_key",
            "_outflow_key",
            "_inflow_key",
            "_card_suffix_key",
        ]
        secondary_aligned_card["_dup_rank"] = secondary_aligned_card.groupby(
            second_key_cols, dropna=False
        ).cumcount()
        secondary_aligned_card["_row_index"] = secondary_aligned_card.index

        bank_secondary = work.loc[
            (work["_source_norm"] == "bank") & valid & work["_secondary_date_key"].notna(),
            second_key_cols,
        ].copy()
        bank_secondary["_dup_rank"] = bank_secondary.groupby(
            second_key_cols, dropna=False
        ).cumcount()

        matched_secondary = secondary_aligned_card.merge(
            bank_secondary.assign(_matched=True),
            on=second_key_cols + ["_dup_rank"],
            how="left",
        )
        secondary_drop = matched_secondary.loc[matched_secondary["_matched"].eq(True), "_row_index"]
        if not secondary_drop.empty:
            drop_index.extend(secondary_drop.tolist())

    drop_index = sorted(set(drop_index))
    if not drop_index:
        return source_df.reset_index(drop=True)

    warnings.warn(
        f"Dropping {len(drop_index)} bank/card overlap rows matched on aligned account/date/amount keys.",
        UserWarning,
    )
    return source_df.drop(index=drop_index).reset_index(drop=True).copy()


def _dedupe_sources(
    source_df: pd.DataFrame, ynab_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    source_clean = source_df.reset_index(drop=True).copy()
    source_clean["candidate_import_id"] = _candidate_import_ids(source_clean)

    ynab_with_import = ynab_df.copy()
    if "import_id" not in ynab_with_import.columns:
        ynab_with_import["import_id"] = ""
    ynab_with_import["import_id"] = (
        ynab_with_import["import_id"].astype("string").fillna("").str.strip()
    )
    ynab_import_keys = {
        (key, row["import_id"])
        for _, row in ynab_with_import.iterrows()
        for key in _account_key_candidates(row, id_col="account_id", name_col="account_name")
        if row["import_id"] and key
    }

    exact_import_mask = source_clean.apply(
        lambda row: any(
            (key, str(row.get("candidate_import_id", "")).strip()) in ynab_import_keys
            for key in _account_key_candidates(
                row, id_col="ynab_account_id", name_col="account_name"
            )
        ),
        axis=1,
    )
    if exact_import_mask.any():
        warnings.warn(
            f"Dropping {int(exact_import_mask.sum())} source rows matched to YNAB by exact import_id.",
            UserWarning,
        )

    source_remaining = (
        source_clean.loc[~exact_import_mask].drop(columns=["candidate_import_id"]).copy()
    )
    pairs = pairing.match_pairs(source_remaining, ynab_df)
    if pairs.empty:
        return source_remaining.copy(), pairs

    key_cols = ["account_name", "date", "outflow_ils", "inflow_ils"]
    ambiguous_mask = (
        pairs["ambiguous_key"].fillna(False).astype(bool)
        if "ambiguous_key" in pairs.columns
        else pd.Series([False] * len(pairs), index=pairs.index)
    )
    ambiguous_keys = pairs.loc[ambiguous_mask, key_cols].drop_duplicates().copy()
    if not ambiguous_keys.empty:
        warnings.warn(
            f"Retaining {len(ambiguous_keys)} source rows with ambiguous YNAB date+amount matches.",
            UserWarning,
        )

    non_ambiguous_pairs = pairs.loc[~ambiguous_mask].copy()
    if non_ambiguous_pairs.empty:
        return source_remaining.copy(), pairs

    non_ambiguous_pairs["date"] = pd.to_datetime(
        non_ambiguous_pairs["date"], errors="coerce"
    ).dt.date
    non_ambiguous_pairs["outflow_ils"] = pd.to_numeric(
        non_ambiguous_pairs["outflow_ils"], errors="coerce"
    ).fillna(0.0)
    non_ambiguous_pairs["inflow_ils"] = pd.to_numeric(
        non_ambiguous_pairs["inflow_ils"], errors="coerce"
    ).fillna(0.0)
    non_ambiguous_pairs["ynab_import_id"] = (
        non_ambiguous_pairs.get(
            "ynab_import_id",
            pd.Series([""] * len(non_ambiguous_pairs), index=non_ambiguous_pairs.index),
        )
        .astype("string")
        .fillna("")
        .str.strip()
    )
    non_ambiguous_pairs["ynab_fingerprint"] = (
        non_ambiguous_pairs.get(
            "ynab_fingerprint",
            pd.Series([""] * len(non_ambiguous_pairs), index=non_ambiguous_pairs.index),
        )
        .astype("string")
        .fillna("")
        .str.strip()
    )
    pair_summary = (
        non_ambiguous_pairs.groupby(key_cols, dropna=False)
        .agg(
            ynab_import_ids=(
                "ynab_import_id",
                lambda values: sorted({str(v).strip() for v in values if str(v).strip()}),
            ),
            ynab_fingerprints=(
                "ynab_fingerprint",
                lambda values: sorted({str(v).strip() for v in values if str(v).strip()}),
            ),
        )
        .reset_index()
    )
    pair_summary["_pair_key_hit"] = True

    source_compare = source_remaining.copy()
    source_compare["date"] = pd.to_datetime(source_compare["date"], errors="coerce").dt.date
    source_compare["outflow_ils"] = pd.to_numeric(
        source_compare["outflow_ils"], errors="coerce"
    ).fillna(0.0)
    source_compare["inflow_ils"] = pd.to_numeric(
        source_compare["inflow_ils"], errors="coerce"
    ).fillna(0.0)
    source_compare["source_lineage_id"] = source_compare.apply(_source_lineage_id, axis=1)
    merged = source_compare.merge(pair_summary, on=key_cols, how="left")
    has_key_hit = merged["_pair_key_hit"].eq(True)
    protected_mask = merged.apply(_protect_from_weak_dedupe, axis=1)
    is_dup = has_key_hit & ~protected_mask
    protected_count = int((has_key_hit & protected_mask).sum())
    if protected_count:
        warnings.warn(
            "Retaining "
            f"{protected_count} source rows with lineage conflict "
            "against weak YNAB date+amount matches.",
            UserWarning,
        )
    deduped = source_remaining.reset_index(drop=True).loc[~is_dup.to_numpy()].copy()
    return deduped, pairs


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


def _make_transaction_id(row: pd.Series) -> str:
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


def _candidate_import_ids(source_df: pd.DataFrame) -> pd.Series:
    if source_df.empty:
        return pd.Series(dtype="string")

    work = source_df.reset_index().copy()
    if "transaction_id" not in work.columns:
        work["transaction_id"] = work.apply(_make_transaction_id, axis=1)
    else:
        blank_transaction_id = work["transaction_id"].astype("string").fillna("").str.strip() == ""
        if blank_transaction_id.any():
            work.loc[blank_transaction_id, "transaction_id"] = work.loc[blank_transaction_id].apply(
                _make_transaction_id, axis=1
            )

    work["account_key"] = work["account_name"].astype("string").fillna("").str.strip()
    work["date_key"] = (
        pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    )
    work["amount_milliunits"] = (
        (
            (
                pd.to_numeric(work.get("inflow_ils", 0.0), errors="coerce").fillna(0.0)
                - pd.to_numeric(work.get("outflow_ils", 0.0), errors="coerce").fillna(0.0)
            )
            * 1000
        )
        .round()
        .astype(int)
    )
    work["bank_txn_id"] = (
        work.get("bank_txn_id", pd.Series([""] * len(work), index=work.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    work["card_txn_id"] = (
        work.get("card_txn_id", pd.Series([""] * len(work), index=work.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )

    ordered = work.sort_values(
        ["account_key", "date_key", "amount_milliunits", "transaction_id", "index"]
    ).copy()
    ordered["import_occurrence"] = (
        ordered.groupby(["account_key", "date_key", "amount_milliunits"], dropna=False)
        .cumcount()
        .add(1)
    )
    ordered["candidate_import_id"] = ordered.apply(
        lambda row: (
            row["bank_txn_id"]
            or row["card_txn_id"]
            or f"YNAB:{int(row['amount_milliunits'])}:{row['date_key']}:{int(row['import_occurrence'])}"
        ),
        axis=1,
    )
    return (
        ordered.set_index("index")["candidate_import_id"].reindex(source_df.index).astype("string")
    )


def _source_lineage_id(row: pd.Series) -> str:
    bank_txn_id = _optional_text(row.get("bank_txn_id", ""))
    if bank_txn_id:
        return bank_txn_id
    return _optional_text(row.get("card_txn_id", ""))


def _target_lineage_ids(row: pd.Series) -> tuple[str, ...]:
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


def _target_memo_lineage_ids(row: pd.Series) -> tuple[str, ...]:
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
    if pd.isna(value):
        return set()
    text = _optional_text(value)
    return {text} if text else set()


def _protect_from_weak_dedupe(row: pd.Series) -> bool:
    if not bool(row.get("_pair_key_hit", False)):
        return False

    source_lineage = _optional_text(row.get("source_lineage_id", ""))
    ynab_import_ids = _to_string_set(row.get("ynab_import_ids", []))

    lineage_conflict = (
        bool(source_lineage) and bool(ynab_import_ids) and source_lineage not in ynab_import_ids
    )
    return lineage_conflict


def _optional_text(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, (str, bytes)) and pd.isna(value):
        return ""
    return str(value).strip()


def _account_key_candidates(row: pd.Series, *, id_col: str, name_col: str) -> list[str]:
    candidates: list[str] = []
    account_id = str(row.get(id_col, "")).strip()
    account_name = str(row.get(name_col, "")).strip()
    if account_id:
        candidates.append(account_id)
    if account_name and account_name not in candidates:
        candidates.append(account_name)
    return candidates


def build_target_suggestions(transactions: pd.DataFrame, *, map_path: Path) -> pd.DataFrame:
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


def _review_first_nonempty(values: pd.Series) -> str:
    for value in values.astype("string").fillna("").tolist():
        text = str(value).strip()
        if text:
            return text
    return ""


def _is_cleared_match(row: pd.Series) -> bool:
    cleared = _optional_text(row.get("ynab_cleared")).casefold()
    return cleared in {"cleared", "reconciled"}


def _normalize_selected_category(payee: str, category: str) -> str:
    normalized = review_model.normalize_category_value(category)
    if review_model.is_transfer_payee(payee) and normalized.casefold() == "uncategorized":
        return review_model.NO_CATEGORY_REQUIRED
    return normalized


def _is_target_only_transfer_counterpart(row: pd.Series) -> bool:
    target_payee = _optional_text(row.get("ynab_payee_raw"))
    return review_model.is_transfer_payee(target_payee)


def _is_target_only_manual_entry(row: pd.Series) -> bool:
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


def _is_target_only_settled(row: pd.Series) -> bool:
    return _is_cleared_match(row) or _is_target_only_manual_entry(row)


def _stable_row_ids(
    df: pd.DataFrame,
    *,
    prefix: str,
    part_getter,
) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")
    digests = df.apply(
        lambda row: hashlib.sha1("|".join(part_getter(row)).encode("utf-8")).hexdigest()[:16],
        axis=1,
    ).astype("string")
    suffix = digests.groupby(digests, dropna=False).cumcount().astype("string")
    return prefix + "_" + digests + "_" + suffix


def _source_row_id_parts(row: pd.Series) -> list[str]:
    return [
        _optional_text(row.get("bank_txn_id")),
        _optional_text(row.get("card_txn_id")),
        _optional_text(row.get("ref")),
        _optional_text(row.get("source")),
        _optional_text(row.get("account_name")),
        _optional_text(row.get("date")),
        _optional_text(row.get("outflow_ils")),
        _optional_text(row.get("inflow_ils")),
        _optional_text(row.get("fingerprint")),
        _optional_text(row.get("memo") or row.get("raw_text") or row.get("description_raw")),
    ]


def _target_row_id_parts(row: pd.Series) -> list[str]:
    return [
        _optional_text(row.get("ynab_id") or row.get("id")),
        _optional_text(row.get("account_id")),
        _optional_text(row.get("account_name")),
        _optional_text(row.get("date")),
        _optional_text(row.get("outflow_ils")),
        _optional_text(row.get("inflow_ils")),
        _optional_text(row.get("payee_raw")),
        _optional_text(row.get("category_raw")),
        _optional_text(row.get("fingerprint")),
        _optional_text(row.get("memo")),
    ]


def _source_current_payee(row: pd.Series) -> str:
    for column in [
        "payee_raw",
        "merchant_raw",
        "description_clean",
        "description_raw",
        "raw_text",
        "memo",
        "fingerprint",
    ]:
        value = _optional_text(row.get(column))
        if value:
            return value
    return ""


def _prepare_review_source_rows(source_df: pd.DataFrame) -> pd.DataFrame:
    source_work = source_df.copy()
    source_work["memo"] = source_work.get("raw_text", source_work.get("description_raw", ""))
    source_work = _annotate_bank_debit_card_memo(source_work)
    prepared = pairing._prepare_source(source_work)
    if prepared.empty:
        return prepared
    aligned = source_work.loc[prepared.index].copy()
    prepared = prepared.copy()
    prepared["source_row_id"] = _stable_row_ids(
        aligned, prefix="src", part_getter=_source_row_id_parts
    ).to_numpy()
    prepared["source_date"] = pd.to_datetime(aligned.get("date"), errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    prepared["source_date"] = prepared["source_date"].fillna("")
    prepared["source_payee_current"] = aligned.apply(_source_current_payee, axis=1).to_numpy()
    prepared["source_category_current"] = (
        aligned.get("category_raw", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_memo"] = (
        aligned.get("memo", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_bank_txn_id"] = (
        aligned.get("bank_txn_id", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_card_txn_id"] = (
        aligned.get("card_txn_id", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_card_suffix"] = (
        aligned.get("card_suffix", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_secondary_date"] = (
        pd.to_datetime(
            aligned.get("secondary_date", pd.Series([""] * len(aligned), index=aligned.index)),
            errors="coerce",
        )
        .dt.strftime("%Y-%m-%d")
        .fillna("")
        .to_numpy()
    )
    prepared["source_ref"] = (
        aligned.get("ref", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["source_transaction"] = aligned.apply(
        lambda row: _canonical_transaction_dict(
            row,
            artifact_kind="normalized_source",
            source_system_fallback=_optional_text(row.get("source")) or "source",
        ),
        axis=1,
    ).to_numpy()
    prepared["source_lineage_id"] = (
        aligned.apply(_source_lineage_id, axis=1).astype("string").to_numpy()
    )
    return prepared


def _prepare_review_target_rows(ynab_df: pd.DataFrame) -> pd.DataFrame:
    prepared = pairing._prepare_ynab(ynab_df)
    if prepared.empty:
        return prepared
    aligned = ynab_df.loc[prepared.index].copy()
    prepared = prepared.copy()
    prepared["target_row_id"] = _stable_row_ids(
        aligned, prefix="tgt", part_getter=_target_row_id_parts
    ).to_numpy()
    prepared["target_date"] = pd.to_datetime(aligned.get("date"), errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    prepared["target_date"] = prepared["target_date"].fillna("")
    prepared["target_memo"] = (
        aligned.get("memo", pd.Series([""] * len(aligned), index=aligned.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .to_numpy()
    )
    prepared["target_transaction"] = aligned.apply(
        lambda row: _canonical_transaction_dict(
            row,
            artifact_kind="ynab_transaction",
            source_system_fallback="ynab",
        ),
        axis=1,
    ).to_numpy()
    return prepared


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
    prepared_source: pd.DataFrame,
    prepared_target: pd.DataFrame,
) -> pd.DataFrame:
    if prepared_source.empty or prepared_target.empty:
        return pd.DataFrame()
    pairs = prepared_source.merge(
        prepared_target,
        on=["account_key", "date_key", "amount_key"],
        how="inner",
        suffixes=("_source", "_target"),
    )
    if pairs.empty:
        return pairs

    # If a source row has an explicit lineage id and one or more targets share that import id,
    # keep only those exact-lineage candidates for that source row.
    source_lineage = (
        pairs.get(
            "source_lineage_id",
            pd.Series([""] * len(pairs), index=pairs.index, dtype="string"),
        )
        .astype("string")
        .fillna("")
        .str.strip()
    )
    target_import = (
        pairs.get(
            "ynab_import_id",
            pd.Series([""] * len(pairs), index=pairs.index, dtype="string"),
        )
        .astype("string")
        .fillna("")
        .str.strip()
    )
    exact_import_match = source_lineage.ne("") & source_lineage.eq(target_import)
    import_match_by_source = exact_import_match.groupby(
        pairs["source_row_id"], dropna=False
    ).transform("any")
    pairs = pairs.loc[~import_match_by_source | exact_import_match].copy()

    source_lineage = (
        pairs.get(
            "source_lineage_id",
            pd.Series([""] * len(pairs), index=pairs.index, dtype="string"),
        )
        .astype("string")
        .fillna("")
        .str.strip()
    )
    target_memo_lineages = pairs.apply(_target_memo_lineage_ids, axis=1)
    exact_memo_lineage_match = source_lineage.ne("") & pd.Series(
        [
            lineage in lineage_ids
            for lineage, lineage_ids in zip(source_lineage.tolist(), target_memo_lineages.tolist())
        ],
        index=pairs.index,
    )
    memo_lineage_by_source = exact_memo_lineage_match.groupby(
        pairs["source_row_id"], dropna=False
    ).transform("any")
    pairs = pairs.loc[~memo_lineage_by_source | exact_memo_lineage_match].copy()

    source_candidate_counts = (
        pairs.groupby("source_row_id", dropna=False)
        .size()
        .reset_index(name="_source_candidate_count")
    )
    target_candidate_counts = (
        pairs.groupby("target_row_id", dropna=False)
        .size()
        .reset_index(name="_target_candidate_count")
    )
    pairs = pairs.merge(source_candidate_counts, on="source_row_id", how="left")
    pairs = pairs.merge(target_candidate_counts, on="target_row_id", how="left")
    pairs["ambiguous_key"] = pairs["_source_candidate_count"].fillna(0).astype(int).gt(1) | pairs[
        "_target_candidate_count"
    ].fillna(0).astype(int).gt(1)
    return pairs


def _apply_review_target_suggestions(relations: pd.DataFrame, *, map_path: Path) -> pd.DataFrame:
    source_rows = relations.loc[relations["source_present"].astype(bool)].copy()
    if source_rows.empty:
        return relations

    candidates = pd.DataFrame(
        {
            "source": source_rows["source"].astype("string").fillna("").str.strip(),
            "account_name": source_rows["source_account"].astype("string").fillna("").str.strip(),
            "source_account": source_rows["source_account"].astype("string").fillna("").str.strip(),
            "source_row_id": source_rows["source_row_id"].astype("string").fillna("").str.strip(),
            "date": source_rows["source_date"].astype("string").fillna("").str.strip(),
            "outflow_ils": pd.to_numeric(source_rows["outflow_ils"], errors="coerce").fillna(0.0),
            "inflow_ils": pd.to_numeric(source_rows["inflow_ils"], errors="coerce").fillna(0.0),
            "memo": source_rows["source_memo"].astype("string").fillna("").str.strip(),
            "raw_text": source_rows["source_memo"].astype("string").fillna("").str.strip(),
            "fingerprint": source_rows["source_fingerprint"]
            .astype("string")
            .fillna("")
            .str.strip(),
        }
    )
    candidates = candidates.loc[candidates["fingerprint"].ne("")].copy()
    if candidates.empty:
        suggested = pd.DataFrame(
            columns=[
                "source_row_id",
                "suggested_payee_options",
                "suggested_category_options",
                "suggested_payee_selected",
                "suggested_category_selected",
            ]
        )
    else:
        suggested = build_target_suggestions(candidates, map_path=map_path)
    suggested = suggested.rename(
        columns={
            "payee_options": "suggested_payee_options",
            "category_options": "suggested_category_options",
            "payee_selected": "suggested_payee_selected",
            "category_selected": "suggested_category_selected",
        }
    )
    if not suggested.empty:
        suggested = (
            suggested.groupby("source_row_id", dropna=False, sort=False)
            .agg(
                suggested_payee_options=(
                    "suggested_payee_options",
                    lambda values: _review_join_options(*values.tolist()),
                ),
                suggested_category_options=(
                    "suggested_category_options",
                    lambda values: _review_join_options(*values.tolist()),
                ),
                suggested_payee_selected=("suggested_payee_selected", _review_first_nonempty),
                suggested_category_selected=("suggested_category_selected", _review_first_nonempty),
            )
            .reset_index()
        )
    merged = relations.merge(
        suggested[
            [
                "source_row_id",
                "suggested_payee_options",
                "suggested_category_options",
                "suggested_payee_selected",
                "suggested_category_selected",
            ]
        ],
        on="source_row_id",
        how="left",
    )

    current_target_payee = merged["target_payee_current"].astype("string").fillna("").str.strip()
    current_target_category = pd.Series(
        [
            _normalize_selected_category(payee, category)
            for payee, category in zip(
                merged["target_payee_current"].astype("string").fillna("").str.strip(),
                merged["target_category_current"].astype("string").fillna("").str.strip(),
            )
        ],
        index=merged.index,
        dtype="string",
    )
    suggested_payee = (
        merged.get("suggested_payee_selected", pd.Series([""] * len(merged)))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    suggested_category = (
        merged.get("suggested_category_selected", pd.Series([""] * len(merged)))
        .astype("string")
        .fillna("")
        .map(review_model.normalize_category_value)
    )
    has_target = merged["target_present"].astype(bool)

    merged["payee_options"] = [
        _review_join_options(current, suggested)
        for current, suggested in zip(
            current_target_payee,
            merged.get("suggested_payee_options", pd.Series([""] * len(merged))),
        )
    ]
    merged["category_options"] = [
        _review_join_options(current, suggested)
        for current, suggested in zip(
            current_target_category,
            merged.get("suggested_category_options", pd.Series([""] * len(merged))),
        )
    ]
    merged["target_payee_selected"] = current_target_payee.where(
        has_target & current_target_payee.ne(""),
        suggested_payee,
    )
    merged["target_category_selected"] = current_target_category.where(
        has_target & current_target_category.ne(""),
        suggested_category,
    )

    return merged.drop(
        columns=[
            col
            for col in [
                "suggested_payee_options",
                "suggested_category_options",
                "suggested_payee_selected",
                "suggested_category_selected",
            ]
            if col in merged.columns
        ]
    )


def build_review_rows(
    source_df: pd.DataFrame,
    ynab_df: pd.DataFrame,
    *,
    map_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    prepared_source = _prepare_review_source_rows(source_df)
    prepared_target = _prepare_review_target_rows(ynab_df)
    pairs = _institutional_candidate_pairs(prepared_source, prepared_target)

    rows: list[dict[str, object]] = []
    pair_source_ids = set(
        pairs.get("source_row_id", pd.Series(dtype="string")).astype("string").tolist()
    )
    pair_target_ids = set(
        pairs.get("target_row_id", pd.Series(dtype="string")).astype("string").tolist()
    )

    for _, row in pairs.iterrows():
        is_ambiguous = bool(row.get("ambiguous_key", False))
        target_payee = _optional_text(row.get("ynab_payee_raw"))
        target_category_current = _optional_text(row.get("ynab_category_raw"))
        target_category = _normalize_selected_category(
            target_payee,
            target_category_current,
        )
        source_payee = _optional_text(row.get("source_payee_current"))
        source_category = review_model.normalize_category_value(
            _optional_text(row.get("source_category_current"))
        )
        matched_cleared = (not is_ambiguous) and _is_cleared_match(row)
        rows.append(
            {
                "transaction_id": _make_transaction_id(
                    pd.Series(
                        {
                            "account_name": row.get("account_name"),
                            "date": row.get("source_date"),
                            "outflow_ils": row.get("outflow_ils"),
                            "inflow_ils": row.get("inflow_ils"),
                            "fingerprint": row.get("fingerprint"),
                            "raw_text": row.get("source_memo"),
                            "target_row_id": row.get("target_row_id"),
                        }
                    )
                ),
                "source": _optional_text(row.get("source_type")),
                "account_name": _optional_text(row.get("ynab_account") or row.get("account_name")),
                "date": _optional_text(row.get("source_date") or row.get("target_date")),
                "outflow_ils": row.get("outflow_ils", 0.0),
                "inflow_ils": row.get("inflow_ils", 0.0),
                "memo": _optional_text(row.get("source_memo") or row.get("target_memo")),
                "fingerprint": _optional_text(
                    row.get("fingerprint") or row.get("ynab_fingerprint")
                ),
                "payee_options": target_payee,
                "category_options": target_category,
                "match_status": (
                    "ambiguous"
                    if is_ambiguous
                    else ("matched_cleared" if matched_cleared else "matched_auto")
                ),
                "update_maps": "",
                "decision_action": "No decision" if is_ambiguous else "keep_match",
                "reviewed": matched_cleared,
                "workflow_type": "institutional",
                "relation_kind": (
                    "ambiguous_candidate"
                    if is_ambiguous
                    else ("matched_cleared_pair" if matched_cleared else "matched_pair")
                ),
                "match_method": "exact_date_amount_not_unique"
                if is_ambiguous
                else "exact_date_amount",
                "source_present": True,
                "target_present": True,
                "source_row_id": _optional_text(row.get("source_row_id")),
                "target_row_id": _optional_text(row.get("target_row_id")),
                "source_account": _optional_text(
                    row.get("source_account") or row.get("account_name")
                ),
                "target_account": _optional_text(
                    row.get("ynab_account") or row.get("account_name")
                ),
                "source_date": _optional_text(row.get("source_date")),
                "target_date": _optional_text(row.get("target_date")),
                "source_payee_current": source_payee,
                "target_payee_current": target_payee,
                "source_category_current": source_category,
                "target_category_current": target_category_current,
                "source_memo": _optional_text(row.get("source_memo")),
                "target_memo": _optional_text(row.get("target_memo")),
                "source_fingerprint": _optional_text(row.get("fingerprint")),
                "target_fingerprint": _optional_text(row.get("ynab_fingerprint")),
                "source_bank_txn_id": _optional_text(row.get("source_bank_txn_id")),
                "source_card_txn_id": _optional_text(row.get("source_card_txn_id")),
                "source_card_suffix": _optional_text(row.get("source_card_suffix")),
                "source_secondary_date": _optional_text(row.get("source_secondary_date")),
                "source_ref": _optional_text(row.get("source_ref")),
                **_default_source_context(source_category=source_category),
                "source_payee_selected": source_payee,
                "source_category_selected": source_category,
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": target_payee,
                "target_category_selected": target_category,
                "source_transaction": row.get("source_transaction"),
                "target_transaction": row.get("target_transaction"),
            }
        )

    unmatched_source = prepared_source.loc[
        ~prepared_source["source_row_id"].astype("string").isin(pair_source_ids)
    ].copy()
    for _, row in unmatched_source.iterrows():
        source_payee = _optional_text(row.get("source_payee_current"))
        source_category = review_model.normalize_category_value(
            _optional_text(row.get("source_category_current"))
        )
        rows.append(
            {
                "transaction_id": _make_transaction_id(
                    pd.Series(
                        {
                            "account_name": row.get("account_name"),
                            "date": row.get("source_date"),
                            "outflow_ils": row.get("outflow_ils"),
                            "inflow_ils": row.get("inflow_ils"),
                            "fingerprint": row.get("fingerprint"),
                            "raw_text": row.get("source_memo"),
                        }
                    )
                ),
                "source": _optional_text(row.get("source_type")),
                "account_name": _optional_text(row.get("account_name")),
                "date": _optional_text(row.get("source_date")),
                "outflow_ils": row.get("outflow_ils", 0.0),
                "inflow_ils": row.get("inflow_ils", 0.0),
                "memo": _optional_text(row.get("source_memo")),
                "fingerprint": _optional_text(row.get("fingerprint")),
                "payee_options": "",
                "category_options": "",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "institutional",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": _optional_text(row.get("source_row_id")),
                "target_row_id": "",
                "source_account": _optional_text(
                    row.get("source_account") or row.get("account_name")
                ),
                "target_account": _optional_text(row.get("account_name")),
                "source_date": _optional_text(row.get("source_date")),
                "target_date": "",
                "source_payee_current": source_payee,
                "target_payee_current": "",
                "source_category_current": source_category,
                "target_category_current": "",
                "source_memo": _optional_text(row.get("source_memo")),
                "target_memo": "",
                "source_fingerprint": _optional_text(row.get("fingerprint")),
                "target_fingerprint": "",
                "source_bank_txn_id": _optional_text(row.get("source_bank_txn_id")),
                "source_card_txn_id": _optional_text(row.get("source_card_txn_id")),
                "source_card_suffix": _optional_text(row.get("source_card_suffix")),
                "source_secondary_date": _optional_text(row.get("source_secondary_date")),
                "source_ref": _optional_text(row.get("source_ref")),
                **_default_source_context(source_category=source_category),
                "source_payee_selected": source_payee,
                "source_category_selected": source_category,
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": row.get("source_transaction"),
                "target_transaction": None,
            }
        )

    unmatched_target = prepared_target.loc[
        ~prepared_target["target_row_id"].astype("string").isin(pair_target_ids)
    ].copy()
    for _, row in unmatched_target.iterrows():
        target_payee = _optional_text(row.get("ynab_payee_raw"))
        target_category_current = _optional_text(row.get("ynab_category_raw"))
        target_category = _normalize_selected_category(
            target_payee,
            target_category_current,
        )
        settled_transfer_counterpart = _is_target_only_transfer_counterpart(row)
        settled_target_only = settled_transfer_counterpart or _is_target_only_settled(row)
        rows.append(
            {
                "transaction_id": _make_transaction_id(
                    pd.Series(
                        {
                            "account_name": row.get("ynab_account"),
                            "date": row.get("target_date"),
                            "outflow_ils": row.get("ynab_outflow_ils"),
                            "inflow_ils": row.get("ynab_inflow_ils"),
                            "fingerprint": row.get("ynab_fingerprint"),
                            "raw_text": row.get("target_memo"),
                        }
                    )
                ),
                "source": "ynab",
                "account_name": _optional_text(row.get("ynab_account")),
                "date": _optional_text(row.get("target_date")),
                "outflow_ils": row.get("ynab_outflow_ils", 0.0),
                "inflow_ils": row.get("ynab_inflow_ils", 0.0),
                "memo": _optional_text(row.get("target_memo")),
                "fingerprint": _optional_text(row.get("ynab_fingerprint")),
                "payee_options": target_payee,
                "category_options": target_category,
                "match_status": "target_only",
                "update_maps": "",
                "decision_action": "ignore_row" if settled_target_only else "No decision",
                "reviewed": settled_target_only,
                "workflow_type": "institutional",
                "relation_kind": (
                    "target_only_transfer_counterpart"
                    if settled_transfer_counterpart
                    else (
                        "target_only_cleared"
                        if _is_cleared_match(row)
                        else ("target_only_manual" if settled_target_only else "target_only")
                    )
                ),
                "match_method": "",
                "source_present": False,
                "target_present": True,
                "source_row_id": "",
                "target_row_id": _optional_text(row.get("target_row_id")),
                "source_account": "",
                "target_account": _optional_text(row.get("ynab_account")),
                "source_date": "",
                "target_date": _optional_text(row.get("target_date")),
                "source_payee_current": "",
                "target_payee_current": target_payee,
                "source_category_current": "",
                "target_category_current": target_category_current,
                "source_memo": "",
                "target_memo": _optional_text(row.get("target_memo")),
                "source_fingerprint": "",
                "target_fingerprint": _optional_text(row.get("ynab_fingerprint")),
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": "",
                "source_ref": "",
                "source_context_kind": "",
                "source_context_category_id": "",
                "source_context_category_name": "",
                "source_context_matching_split_ids": "",
                "source_payee_selected": "",
                "source_category_selected": "",
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": target_payee,
                "target_category_selected": target_category,
                "source_transaction": None,
                "target_transaction": row.get("target_transaction"),
            }
        )

    relations = pd.DataFrame(rows, columns=REVIEW_ROW_COLUMNS)
    relations = _apply_review_target_suggestions(relations, map_path=map_path)
    relations = review_io.project_review_artifact_to_flat_dataframe(relations)
    return relations, pairs


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
    if not source_paths:
        raise ValueError("Provide at least one --source or --source-dir input.")

    source_df = _load_csvs(source_paths)
    source_df = _dedupe_source_overlaps(source_df)
    ynab_df = _load_canonical_transaction_input(Path(args.ynab))
    if ynab_df.empty:
        raise ValueError("No rows found in YNAB input.")

    out, pairs = build_review_rows(source_df, ynab_df, map_path=map_path)
    if args.pairs_out:
        export.write_dataframe(pairs, args.pairs_out)
        print(export.wrote_message(args.pairs_out, len(pairs)))

    out_path = Path(args.out_path)
    if out_path.suffix.lower() == ".parquet":
        review_io.save_review_artifact(out, out_path)
    else:
        review_io.save_reviewed_transactions(out, out_path)
    print(export.wrote_message(out_path, len(out)))


if __name__ == "__main__":
    main()
