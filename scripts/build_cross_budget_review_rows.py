# ruff: noqa: E402

import argparse
import hashlib
import importlib.util
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.cross_budget_pairing as cross_budget_pairing
import ynab_il_importer.export as export
import ynab_il_importer.review_app.io as review_io
import ynab_il_importer.workflow_profiles as workflow_profiles

REVIEW_BUILDER_SPEC = importlib.util.spec_from_file_location(
    "build_proposed_transactions_script",
    ROOT / "scripts" / "build_proposed_transactions.py",
)
assert REVIEW_BUILDER_SPEC is not None and REVIEW_BUILDER_SPEC.loader is not None
review_builder = importlib.util.module_from_spec(REVIEW_BUILDER_SPEC)
sys.modules["build_proposed_transactions_script"] = review_builder
REVIEW_BUILDER_SPEC.loader.exec_module(review_builder)

BASE_COLUMNS = review_builder.REVIEW_ROW_COLUMNS


def _read_csv_or_empty(path: Path, *, prefer_sidecar_parquet: bool = False) -> pd.DataFrame:
    try:
        return review_builder._load_transaction_input(
            path,
            prefer_sidecar_parquet=prefer_sidecar_parquet,
        ).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _filter_by_date(df: pd.DataFrame, since: str | None, until: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    date_series = pd.to_datetime(out["date"], errors="coerce")
    if since:
        out = out.loc[date_series >= pd.to_datetime(since, errors="coerce")].copy()
        date_series = pd.to_datetime(out["date"], errors="coerce")
    if until:
        out = out.loc[date_series <= pd.to_datetime(until, errors="coerce")].copy()
    return out


def _default_artifact_root(target_profile: str, phase: str) -> Path:
    profile_name = str(target_profile or "").strip().lower()
    if profile_name:
        return Path("data/paired") / f"{profile_name}_cross_budget_{phase}"
    return Path("data/paired") / f"cross_budget_{phase}"


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _float(value: object) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0])


def _make_id(*parts: object, prefix: str) -> str:
    digest = hashlib.sha1("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[
        :16
    ]
    return f"{prefix}_{digest}"


def _split_options(value: object) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for part in _text(value).split(";"):
        item = part.strip()
        if not item or item in seen:
            continue
        ordered.append(item)
        seen.add(item)
    return ordered


def _join_options(*values: object) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in _split_options(value):
            if item in seen:
                continue
            ordered.append(item)
            seen.add(item)
    return "; ".join(ordered)


def _first_nonempty(values: pd.Series) -> str:
    for value in values.astype("string").fillna("").tolist():
        text = str(value).strip()
        if text:
            return text
    return ""


def _snapshot_lookup(df: pd.DataFrame, *, row_id_column: str) -> dict[str, dict[str, object]]:
    if df.empty or row_id_column not in df.columns:
        return {}
    lookup: dict[str, dict[str, object]] = {}
    for _, row in df.iterrows():
        row_id = _text(row.get(row_id_column))
        if not row_id or row_id in lookup:
            continue
        lookup[row_id] = row.to_dict()
    return lookup


def _canonical_snapshot(
    snapshot: dict[str, object],
    *,
    artifact_kind: str,
    source_system_fallback: str,
) -> dict[str, object] | None:
    if not snapshot:
        return None
    return review_builder._canonical_transaction_dict(
        pd.Series(snapshot),
        artifact_kind=artifact_kind,
        source_system_fallback=source_system_fallback,
    )


def _source_context_from_snapshot(
    snapshot: dict[str, object] | None,
) -> dict[str, str]:
    if not snapshot:
        return {
            "source_context_kind": "",
            "source_context_category_id": "",
            "source_context_category_name": "",
            "source_context_matching_split_ids": "",
        }

    category_id = _text(snapshot.get("category_id"))
    category_name = _text(snapshot.get("category_raw"))
    is_subtransaction = bool(snapshot.get("is_subtransaction", False))
    if is_subtransaction:
        split_id = _text(snapshot.get("ynab_id")) or _text(snapshot.get("source_row_id"))
        return {
            "source_context_kind": "ynab_split_category_match",
            "source_context_category_id": category_id,
            "source_context_category_name": category_name,
            "source_context_matching_split_ids": split_id,
        }

    return {
        "source_context_kind": "ynab_parent_category_match",
        "source_context_category_id": category_id,
        "source_context_category_name": category_name,
        "source_context_matching_split_ids": "",
    }


def _expand_ambiguous_relation_rows(
    ambiguous_df: pd.DataFrame,
    *,
    target_account: str,
    source_lookup: dict[str, dict[str, object]],
    target_lookup: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    if ambiguous_df.empty:
        return []

    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for _, row in ambiguous_df.iterrows():
        source_ids = _split_options(row.get("source_row_id")) or _split_options(
            row.get("source_row_ids")
        )
        target_ids = _split_options(row.get("target_row_id")) or _split_options(
            row.get("target_row_ids")
        )
        if not source_ids:
            source_ids = [""]
        if not target_ids:
            target_ids = [""]

        match_method = _text(row.get("reason"))
        fallback_date = _text(row.get("date"))
        fallback_signed_amount = _float(row.get("signed_amount"))

        for source_row_id in source_ids:
            source_snapshot = source_lookup.get(source_row_id, {})
            for target_row_id in target_ids:
                target_snapshot = target_lookup.get(target_row_id, {})
                relation_key = (
                    source_row_id,
                    target_row_id,
                    match_method,
                    fallback_date,
                )
                if relation_key in seen:
                    continue
                seen.add(relation_key)

                source_date = (
                    _text(source_snapshot.get("date"))
                    or _text(row.get("source_dates"))
                    or fallback_date
                )
                target_date = (
                    _text(target_snapshot.get("date"))
                    or _text(row.get("target_dates"))
                    or fallback_date
                )
                source_payee = _text(source_snapshot.get("payee_raw")) or _text(
                    row.get("source_payee")
                )
                target_payee = _text(target_snapshot.get("payee_raw"))
                source_category = _text(source_snapshot.get("category_raw"))
                target_category = _text(target_snapshot.get("category_raw"))
                source_memo = _text(source_snapshot.get("memo") or source_snapshot.get("raw_text"))
                target_memo = _text(target_snapshot.get("memo") or target_snapshot.get("raw_text"))
                source_fingerprint = _text(source_snapshot.get("fingerprint"))
                target_fingerprint = _text(target_snapshot.get("fingerprint"))
                target_account_name = _text(target_snapshot.get("account_name")) or _text(
                    target_account
                )
                source_account_name = _text(
                    source_snapshot.get("source_account") or source_snapshot.get("account_name")
                )
                signed_amount = (
                    _float(source_snapshot.get("inflow_ils"))
                    - _float(source_snapshot.get("outflow_ils"))
                    if source_snapshot
                    else (
                        _float(target_snapshot.get("inflow_ils"))
                        - _float(target_snapshot.get("outflow_ils"))
                        if target_snapshot
                        else fallback_signed_amount
                    )
                )
                summary_memo = " / ".join(
                    [
                        part
                        for part in [source_memo or source_payee, target_memo or target_payee]
                        if part
                    ]
                )

                rows.append(
                    {
                        "transaction_id": _make_id(
                            source_row_id, target_row_id, fallback_date, prefix="txn"
                        ),
                        "source": "cross_budget",
                        "account_name": target_account_name,
                        "date": source_date or target_date or fallback_date,
                        "outflow_ils": max(-signed_amount, 0.0),
                        "inflow_ils": max(signed_amount, 0.0),
                        "memo": summary_memo,
                        "fingerprint": source_fingerprint
                        or target_fingerprint
                        or source_payee
                        or target_payee,
                        "payee_options": target_payee,
                        "category_options": target_category,
                        "match_status": "ambiguous",
                        "update_maps": "",
                        "decision_action": "No decision",
                        "reviewed": False,
                        "workflow_type": "cross_budget",
                        "relation_kind": "ambiguous_candidate",
                        "match_method": match_method,
                        "source_present": bool(source_row_id),
                        "target_present": bool(target_row_id),
                        "source_row_id": source_row_id,
                        "target_row_id": target_row_id,
                        "source_account": source_account_name,
                        "target_account": target_account_name,
                        "source_date": source_date,
                        "target_date": target_date,
                        "source_payee_current": source_payee,
                        "target_payee_current": target_payee,
                        "source_category_current": source_category,
                        "target_category_current": target_category,
                        "source_memo": source_memo,
                        "target_memo": target_memo,
                        "source_fingerprint": source_fingerprint,
                        "target_fingerprint": target_fingerprint,
                        "source_bank_txn_id": "",
                        "source_card_txn_id": "",
                        "source_card_suffix": "",
                        "source_secondary_date": _text(source_snapshot.get("secondary_date")),
                        "source_ref": _text(source_snapshot.get("ref")),
                        **_source_context_from_snapshot(source_snapshot),
                        "source_payee_selected": source_payee,
                        "source_category_selected": source_category,
                        "target_context_kind": "",
                        "target_context_matching_split_ids": "",
                        "target_payee_selected": target_payee,
                        "target_category_selected": target_category,
                        "source_transaction": _canonical_snapshot(
                            source_snapshot,
                            artifact_kind="ynab_transaction",
                            source_system_fallback="ynab",
                        ),
                        "target_transaction": _canonical_snapshot(
                            target_snapshot,
                            artifact_kind="ynab_transaction",
                            source_system_fallback="ynab",
                        ),
                    }
                )

    return rows


def _relation_rows(
    result: cross_budget_pairing.CrossBudgetMatchResult,
    *,
    target_account: str,
    source_lookup: dict[str, dict[str, object]] | None = None,
    target_lookup: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    source_lookup = source_lookup or {}
    target_lookup = target_lookup or {}

    for _, row in result.matched_pairs_df.iterrows():
        source_row_id = _text(row.get("source_row_id"))
        target_row_id = _text(row.get("target_row_id"))
        source_snapshot = source_lookup.get(source_row_id, row.to_dict())
        target_snapshot = target_lookup.get(target_row_id, row.to_dict())
        source_date = _text(row.get("date"))
        target_date = _text(row.get("date"))
        source_payee = _text(row.get("source_payee_raw"))
        target_payee = _text(row.get("ynab_payee_raw"))
        source_category = _text(row.get("source_category_raw"))
        target_category = _text(row.get("ynab_category_raw"))
        source_memo = _text(row.get("source_memo") or row.get("raw_text"))
        target_memo = _text(row.get("ynab_memo"))
        source_fingerprint = _text(row.get("fingerprint"))
        target_fingerprint = _text(row.get("ynab_fingerprint"))
        rows.append(
            {
                "transaction_id": _make_id(source_row_id, target_row_id, prefix="txn"),
                "source": "cross_budget",
                "account_name": _text(row.get("ynab_account")),
                "date": source_date or target_date,
                "outflow_ils": _float(row.get("outflow_ils")),
                "inflow_ils": _float(row.get("inflow_ils")),
                "memo": source_memo,
                "fingerprint": source_fingerprint or target_fingerprint,
                "payee_options": target_payee,
                "category_options": target_category,
                "match_status": "matched_auto",
                "update_maps": "",
                "decision_action": "keep_match",
                "reviewed": False,
                "workflow_type": "cross_budget",
                "relation_kind": "matched_pair",
                "match_method": _text(row.get("match_type")),
                "source_present": True,
                "target_present": True,
                "source_row_id": source_row_id,
                "target_row_id": target_row_id,
                "source_account": _text(row.get("source_account") or row.get("account_name")),
                "target_account": _text(row.get("ynab_account")),
                "source_date": source_date,
                "target_date": target_date,
                "source_payee_current": source_payee,
                "target_payee_current": target_payee,
                "source_category_current": source_category,
                "target_category_current": target_category,
                "source_memo": source_memo,
                "target_memo": target_memo,
                "source_fingerprint": source_fingerprint,
                "target_fingerprint": target_fingerprint,
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": _text(source_snapshot.get("secondary_date")),
                "source_ref": _text(source_snapshot.get("ref")),
                **_source_context_from_snapshot(source_snapshot),
                "source_payee_selected": source_payee,
                "source_category_selected": source_category,
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": target_payee,
                "target_category_selected": target_category,
                "source_transaction": _canonical_snapshot(
                    source_snapshot,
                    artifact_kind="ynab_transaction",
                    source_system_fallback="ynab",
                ),
                "target_transaction": _canonical_snapshot(
                    target_snapshot,
                    artifact_kind="ynab_transaction",
                    source_system_fallback="ynab",
                ),
            }
        )

    for _, row in result.unmatched_source_df.iterrows():
        source_row_id = _text(row.get("source_row_id"))
        source_date = _text(row.get("date"))
        source_payee = _text(row.get("payee_raw"))
        source_category = _text(row.get("category_raw"))
        source_memo = _text(row.get("memo") or row.get("raw_text"))
        source_fingerprint = _text(row.get("fingerprint"))
        rows.append(
            {
                "transaction_id": _make_id(source_row_id, source_date, prefix="txn"),
                "source": "cross_budget",
                "account_name": _text(target_account),
                "date": source_date,
                "outflow_ils": _float(row.get("outflow_ils")),
                "inflow_ils": _float(row.get("inflow_ils")),
                "memo": source_memo,
                "fingerprint": source_fingerprint,
                "payee_options": "",
                "category_options": "",
                "match_status": "source_only",
                "update_maps": "",
                "decision_action": "create_target",
                "reviewed": False,
                "workflow_type": "cross_budget",
                "relation_kind": "source_only",
                "match_method": "",
                "source_present": True,
                "target_present": False,
                "source_row_id": source_row_id,
                "target_row_id": "",
                "source_account": _text(row.get("source_account") or row.get("account_name")),
                "target_account": _text(target_account),
                "source_date": source_date,
                "target_date": "",
                "source_payee_current": source_payee,
                "target_payee_current": "",
                "source_category_current": source_category,
                "target_category_current": "",
                "source_memo": source_memo,
                "target_memo": "",
                "source_fingerprint": source_fingerprint,
                "target_fingerprint": "",
                "source_bank_txn_id": "",
                "source_card_txn_id": "",
                "source_card_suffix": "",
                "source_secondary_date": _text(row.get("secondary_date")),
                "source_ref": _text(row.get("ref")),
                **_source_context_from_snapshot(row.to_dict()),
                "source_payee_selected": source_payee,
                "source_category_selected": source_category,
                "target_context_kind": "",
                "target_context_matching_split_ids": "",
                "target_payee_selected": "",
                "target_category_selected": "",
                "source_transaction": _canonical_snapshot(
                    row.to_dict(),
                    artifact_kind="ynab_transaction",
                    source_system_fallback="ynab",
                ),
                "target_transaction": None,
            }
        )

    for _, row in result.unmatched_target_df.iterrows():
        target_row_id = _text(row.get("target_row_id"))
        target_date = _text(row.get("date"))
        target_payee = _text(row.get("payee_raw"))
        target_category = _text(row.get("category_raw"))
        target_memo = _text(row.get("memo") or row.get("raw_text"))
        target_fingerprint = _text(row.get("fingerprint"))
        rows.append(
            {
                "transaction_id": _make_id(target_row_id, target_date, prefix="txn"),
                "source": "cross_budget",
                "account_name": _text(row.get("account_name")),
                "date": target_date,
                "outflow_ils": _float(row.get("outflow_ils")),
                "inflow_ils": _float(row.get("inflow_ils")),
                "memo": target_memo,
                "fingerprint": target_fingerprint,
                "payee_options": target_payee,
                "category_options": target_category,
                "match_status": "target_only",
                "update_maps": "",
                "decision_action": "create_source",
                "reviewed": False,
                "workflow_type": "cross_budget",
                "relation_kind": "target_only",
                "match_method": "",
                "source_present": False,
                "target_present": True,
                "source_row_id": "",
                "target_row_id": target_row_id,
                "source_account": "",
                "target_account": _text(row.get("account_name")),
                "source_date": "",
                "target_date": target_date,
                "source_payee_current": "",
                "target_payee_current": target_payee,
                "source_category_current": "",
                "target_category_current": target_category,
                "source_memo": "",
                "target_memo": target_memo,
                "source_fingerprint": "",
                "target_fingerprint": target_fingerprint,
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
                "target_transaction": _canonical_snapshot(
                    row.to_dict(),
                    artifact_kind="ynab_transaction",
                    source_system_fallback="ynab",
                ),
            }
        )

    rows.extend(
        _expand_ambiguous_relation_rows(
            result.ambiguous_matches_df,
            target_account=target_account,
            source_lookup=source_lookup,
            target_lookup=target_lookup,
        )
    )

    return pd.DataFrame(rows, columns=BASE_COLUMNS)


def _apply_target_suggestions(relations: pd.DataFrame, *, map_path: Path) -> pd.DataFrame:
    source_rows = relations.loc[relations["source_present"].astype(bool)].copy()
    if source_rows.empty:
        return relations

    candidates = pd.DataFrame(
        {
            "source": "ynab",
            "account_name": source_rows["target_account"].astype("string").fillna("").str.strip(),
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
        suggested = review_builder.build_target_suggestions(candidates, map_path=map_path)
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
                    lambda values: _join_options(*values.tolist()),
                ),
                suggested_category_options=(
                    "suggested_category_options",
                    lambda values: _join_options(*values.tolist()),
                ),
                suggested_payee_selected=("suggested_payee_selected", _first_nonempty),
                suggested_category_selected=("suggested_category_selected", _first_nonempty),
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
    current_target_category = (
        merged["target_category_current"].astype("string").fillna("").str.strip()
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
        .str.strip()
    )
    has_target = merged["target_present"].astype(bool)

    merged["payee_options"] = [
        _join_options(current, suggested)
        for current, suggested in zip(
            current_target_payee,
            merged.get("suggested_payee_options", pd.Series([""] * len(merged))),
        )
    ]
    merged["category_options"] = [
        _join_options(current, suggested)
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
    target_df: pd.DataFrame,
    *,
    source_category: str,
    target_account: str,
    map_path: Path,
    date_tolerance_days: int,
) -> tuple[pd.DataFrame, cross_budget_pairing.CrossBudgetMatchResult]:
    prepared_source = cross_budget_pairing.prepare_cross_budget_source(
        source_df,
        source_category=source_category or None,
    )
    prepared_target = cross_budget_pairing.prepare_cross_budget_target(
        target_df,
        target_account=target_account,
    )
    result = cross_budget_pairing.match_cross_budget_rows(
        source_df,
        target_df,
        target_account=target_account,
        source_category=source_category or None,
        date_tolerance_days=int(date_tolerance_days),
    )
    relations = _relation_rows(
        result,
        target_account=target_account,
        source_lookup=_snapshot_lookup(prepared_source, row_id_column="source_row_id"),
        target_lookup=_snapshot_lookup(prepared_target, row_id_column="target_row_id"),
    )
    relations = _apply_target_suggestions(relations, map_path=map_path)
    relations = review_io.project_review_artifact_to_working_dataframe(
        pl.from_arrow(review_io.coerce_review_artifact_table(relations))
    ).to_pandas()
    return relations, result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build cross-budget source-vs-target review rows.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--ynab", required=True)
    parser.add_argument("--source-category", default="")
    parser.add_argument("--target-profile", default="")
    parser.add_argument("--target-account", required=True)
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--date-tolerance-days", type=int, default=0)
    parser.add_argument("--map", dest="map_path", type=Path, default=None)
    parser.add_argument("--out", dest="out_path", default="")
    parser.add_argument("--pairs-out", default="")
    parser.add_argument("--unmatched-source-out", default="")
    parser.add_argument("--unmatched-target-out", default="")
    parser.add_argument("--ambiguous-out", default="")
    args = parser.parse_args()

    target_profile = workflow_profiles.resolve_profile(args.target_profile or None)
    map_path = args.map_path or target_profile.payee_map_path

    source_path = Path(args.source)
    target_path = Path(args.ynab)
    source_df = _read_csv_or_empty(source_path, prefer_sidecar_parquet=False)
    target_df = _read_csv_or_empty(target_path, prefer_sidecar_parquet=True)
    source_df["source_file"] = source_path.name
    target_df["target_file"] = target_path.name
    source_df = _filter_by_date(source_df, args.since or None, args.until or None)
    target_df = _filter_by_date(target_df, args.since or None, args.until or None)

    review_rows, result = build_review_rows(
        source_df,
        target_df,
        source_category=str(args.source_category or "").strip(),
        target_account=str(args.target_account or "").strip(),
        map_path=map_path,
        date_tolerance_days=int(args.date_tolerance_days),
    )

    artifact_root = _default_artifact_root(target_profile.name, "live")
    out_path = (
        Path(args.out_path) if args.out_path else artifact_root / "proposed_transactions.parquet"
    )
    pairs_out = Path(args.pairs_out) if args.pairs_out else artifact_root / "matched_pairs.csv"
    unmatched_source_out = (
        Path(args.unmatched_source_out)
        if args.unmatched_source_out
        else artifact_root / "unmatched_source.csv"
    )
    unmatched_target_out = (
        Path(args.unmatched_target_out)
        if args.unmatched_target_out
        else artifact_root / "unmatched_target.csv"
    )
    ambiguous_out = (
        Path(args.ambiguous_out) if args.ambiguous_out else artifact_root / "ambiguous_matches.csv"
    )

    if out_path.suffix.lower() == ".parquet":
        review_io.save_review_artifact(review_rows, out_path)
    else:
        review_io.save_reviewed_transactions(review_rows, out_path)
    export.write_dataframe(result.matched_pairs_df, pairs_out)
    export.write_dataframe(result.unmatched_source_df, unmatched_source_out)
    export.write_dataframe(result.unmatched_target_df, unmatched_target_out)
    export.write_dataframe(result.ambiguous_matches_df, ambiguous_out)

    print(export.wrote_message(out_path, len(review_rows)))
    print(export.wrote_message(pairs_out, len(result.matched_pairs_df)))
    print(export.wrote_message(unmatched_source_out, len(result.unmatched_source_df)))
    print(export.wrote_message(unmatched_target_out, len(result.unmatched_target_df)))
    print(export.wrote_message(ambiguous_out, len(result.ambiguous_matches_df)))


if __name__ == "__main__":
    main()
