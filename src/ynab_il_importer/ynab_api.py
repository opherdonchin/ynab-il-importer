from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import requests

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Py <3.11 fallback
    import tomli as tomllib  # type: ignore

from ynab_il_importer.artifacts.transaction_schema import (
    TRANSACTION_ARTIFACT_VERSION,
    TRANSACTION_SCHEMA,
)
from ynab_il_importer.artifacts.transaction_io import normalize_transaction_table
import ynab_il_importer.io_ynab as ynab
import ynab_il_importer.normalize as normalize


BASE_URL = "https://api.ynab.com/v1"


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip("'").strip('"')
    return data


def _get_token() -> str:
    token = os.getenv("YNAB_ACCESS_TOKEN", "").strip()
    if not token:
        token = _load_env_file(Path(".env")).get("YNAB_ACCESS_TOKEN", "").strip()
    if not token:
        raise ValueError("Missing YNAB_ACCESS_TOKEN (env var or .env)")
    return token


def _get_budget_id() -> str:
    budget_id = os.getenv("YNAB_BUDGET_ID", "").strip()
    if not budget_id:
        budget_id = os.getenv("YNAB_PLAN_ID", "").strip()
    if not budget_id:
        config_path = Path("config/ynab.local.toml")
        if config_path.exists():
            data = tomllib.loads(config_path.read_text(encoding="utf-8"))
            budget_id = str(data.get("budget_id", "")).strip()
    if not budget_id:
        raise ValueError("Missing budget_id (config/ynab.local.toml or YNAB_BUDGET_ID)")
    return budget_id


def _ynab_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {_get_token()}"}
    response = requests.get(url, headers=headers, params=params, timeout=30)
    if response.status_code >= 400:
        raise ValueError(f"YNAB API error {response.status_code}: {response.text}")
    return response.json()


def _ynab_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code >= 400:
        raise ValueError(f"YNAB API error {response.status_code}: {response.text}")
    return response.json()


def _ynab_patch(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }
    response = requests.patch(url, headers=headers, json=payload, timeout=30)
    if response.status_code >= 400:
        raise ValueError(f"YNAB API error {response.status_code}: {response.text}")
    return response.json()


def _ynab_delete(path: str) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {_get_token()}"}
    response = requests.delete(url, headers=headers, timeout=30)
    if response.status_code >= 400:
        raise ValueError(f"YNAB API error {response.status_code}: {response.text}")
    if not response.text.strip():
        return {}
    return response.json()


def fetch_months(plan_id: str | None = None) -> list[dict[str, Any]]:
    plan = plan_id or _get_budget_id()
    payload = _ynab_get(f"/plans/{plan}/months")
    return payload.get("data", {}).get("months", [])


def fetch_month_detail(
    month: str,
    *,
    plan_id: str | None = None,
) -> dict[str, Any]:
    plan = plan_id or _get_budget_id()
    payload = _ynab_get(f"/plans/{plan}/months/{month}")
    return payload.get("data", {}).get("month", {})


def fetch_accounts(plan_id: str | None = None) -> list[dict[str, Any]]:
    plan = plan_id or _get_budget_id()
    payload = _ynab_get(f"/plans/{plan}/accounts")
    return payload.get("data", {}).get("accounts", [])


def fetch_transactions(
    plan_id: str | None = None, since_date: str | None = None
) -> list[dict[str, Any]]:
    plan = plan_id or _get_budget_id()
    params: dict[str, Any] = {}
    if since_date:
        params["since_date"] = since_date
    payload = _ynab_get(f"/plans/{plan}/transactions", params=params)
    return payload.get("data", {}).get("transactions", [])


def fetch_categories(plan_id: str | None = None) -> list[dict[str, Any]]:
    plan = plan_id or _get_budget_id()
    payload = _ynab_get(f"/plans/{plan}/categories")
    return payload.get("data", {}).get("category_groups", [])


def create_transactions(
    transactions: list[dict[str, Any]],
    plan_id: str | None = None,
) -> dict[str, Any]:
    if not transactions:
        return {"transaction_ids": [], "duplicate_import_ids": []}
    plan = plan_id or _get_budget_id()
    payload = _ynab_post(f"/plans/{plan}/transactions", {"transactions": transactions})
    data = payload.get("data", {})
    if "bulk" in data:
        return data.get("bulk", {})
    return {
        "transaction_ids": data.get("transaction_ids", []),
        "duplicate_import_ids": data.get("duplicate_import_ids", []),
        "transactions": data.get("transactions", []),
        "transaction": data.get("transaction", {}),
        "server_knowledge": data.get("server_knowledge"),
    }


def update_transactions(
    transactions: list[dict[str, Any]],
    plan_id: str | None = None,
) -> dict[str, Any]:
    if not transactions:
        return {"transactions": [], "server_knowledge": None}
    plan = plan_id or _get_budget_id()
    payload = _ynab_patch(f"/plans/{plan}/transactions", {"transactions": transactions})
    data = payload.get("data", {})
    if "bulk" in data:
        return data.get("bulk", {})
    return {
        "transactions": data.get("transactions", []),
        "transaction": data.get("transaction", {}),
        "server_knowledge": data.get("server_knowledge"),
    }


def delete_transaction(
    transaction_id: str,
    *,
    plan_id: str | None = None,
) -> dict[str, Any]:
    txn_id = str(transaction_id or "").strip()
    if not txn_id:
        raise ValueError("transaction_id is required for delete_transaction().")
    plan = plan_id or _get_budget_id()
    payload = _ynab_delete(f"/plans/{plan}/transactions/{txn_id}")
    return payload.get("data", {})


def _amount_components(amount_milliunits: Any) -> tuple[float, float]:
    amount = float(amount_milliunits or 0) / 1000.0
    outflow = abs(amount) if amount < 0 else 0.0
    inflow = amount if amount > 0 else 0.0
    return round(outflow, 2), round(inflow, 2)


def _base_transaction_row(
    txn: dict[str, Any],
    *,
    account_name_map: dict[str, str],
    amount_milliunits: Any,
    payee_name: str,
    category_name: str,
    category_id: str,
    memo: str,
    ynab_id: str,
    parent_ynab_id: str,
    is_subtransaction: bool,
) -> dict[str, Any]:
    outflow, inflow = _amount_components(amount_milliunits)
    account_id = txn.get("account_id", "") or ""
    return {
        "source": "ynab",
        "ynab_id": ynab_id,
        "parent_ynab_id": parent_ynab_id,
        "is_subtransaction": bool(is_subtransaction),
        "account_id": account_id,
        "account_name": account_name_map.get(account_id, account_id),
        "date": txn.get("date", ""),
        "payee_raw": payee_name or "",
        "category_raw": category_name or "",
        "category_id": category_id or "",
        "outflow_ils": outflow,
        "inflow_ils": inflow,
        "memo": memo or "",
        "import_id": txn.get("import_id", "") or "",
        "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
        "cleared": txn.get("cleared", "") or "",
        "approved": bool(txn.get("approved", False)),
        "currency": "ILS",
        "amount_bucket": "",
    }


def _canonical_split_rows(
    txn: dict[str, Any],
    *,
    parent_ynab_id: str,
    payee_name: str,
    memo: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, subtxn in enumerate(txn.get("subtransactions") or []):
        if bool(subtxn.get("deleted", False)):
            continue
        subtxn_id = subtxn.get("id") or f"{parent_ynab_id}::sub::{index}"
        outflow, inflow = _amount_components(subtxn.get("amount", 0))
        rows.append(
            {
                "split_id": subtxn_id,
                "parent_transaction_id": parent_ynab_id,
                "ynab_subtransaction_id": subtxn_id,
                "payee_raw": subtxn.get("payee_name", "") or payee_name,
                "category_id": subtxn.get("category_id", "") or "",
                "category_raw": subtxn.get("category_name", "") or "",
                "memo": subtxn.get("memo", "") or memo,
                "inflow_ils": inflow,
                "outflow_ils": outflow,
                "import_id": txn.get("import_id", "") or "",
                "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
            }
        )
    return rows


def _canonical_transaction_row(
    txn: dict[str, Any],
    *,
    account_name_map: dict[str, str],
) -> dict[str, Any]:
    txn_id = txn.get("id", "") or ""
    outflow, inflow = _amount_components(txn.get("amount", 0))
    amount = float(txn.get("amount", 0) or 0) / 1000.0
    account_id = txn.get("account_id", "") or ""
    payee_name = txn.get("payee_name", "") or ""
    memo = txn.get("memo", "") or ""
    return {
        "artifact_kind": "ynab_transaction",
        "artifact_version": TRANSACTION_ARTIFACT_VERSION,
        "source_system": "ynab",
        "transaction_id": txn_id,
        "ynab_id": txn_id,
        "import_id": txn.get("import_id", "") or "",
        "parent_transaction_id": txn_id,
        "account_id": account_id,
        "account_name": account_name_map.get(account_id, account_id),
        "source_account": account_name_map.get(account_id, account_id),
        "date": txn.get("date", "") or "",
        "secondary_date": "",
        "inflow_ils": inflow,
        "outflow_ils": outflow,
        "signed_amount_ils": round(amount, 2),
        "payee_raw": payee_name,
        "category_id": txn.get("category_id", "") or "",
        "category_raw": txn.get("category_name", "") or "",
        "memo": memo,
        "txn_kind": "",
        "fingerprint": normalize.normalize_text(payee_name),
        "description_raw": memo or payee_name,
        "description_clean": payee_name,
        "description_clean_norm": normalize.normalize_text(payee_name),
        "merchant_raw": payee_name,
        "ref": "",
        "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
        "cleared": txn.get("cleared", "") or "",
        "approved": bool(txn.get("approved", False)),
        "is_subtransaction": False,
        "splits": _canonical_split_rows(
            txn,
            parent_ynab_id=txn_id,
            payee_name=payee_name,
            memo=memo,
        ),
    }


def transactions_to_canonical_table(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]] | None = None,
) -> pa.Table:
    account_name_map = {acc.get("id"): acc.get("name") for acc in accounts or []}
    rows = [
        _canonical_transaction_row(txn, account_name_map=account_name_map)
        for txn in transactions
        if not bool(txn.get("deleted", False))
    ]
    return pa.Table.from_pylist(rows, schema=TRANSACTION_SCHEMA)


def transactions_to_dataframe(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    canonical = transactions_to_canonical_table(transactions, accounts)
    return canonical.to_pandas()


def _canonical_rows(data: Any) -> list[dict[str, Any]]:
    return normalize_transaction_table(data).to_pylist()


def project_transactions_to_flat_dataframe(data: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for txn in _canonical_rows(data):
        rows.append(
            {
                "source": "ynab",
                "ynab_id": txn.get("ynab_id", "") or txn.get("transaction_id", "") or "",
                "account_id": txn.get("account_id", "") or "",
                "account_name": txn.get("account_name", "") or "",
                "date": txn.get("date", "") or "",
                "payee_raw": txn.get("payee_raw", "") or "",
                "category_raw": txn.get("category_raw", "") or "",
                "fingerprint": txn.get("fingerprint", "")
                or normalize.normalize_text(txn.get("payee_raw", "") or ""),
                "outflow_ils": txn.get("outflow_ils", 0.0) or 0.0,
                "inflow_ils": txn.get("inflow_ils", 0.0) or 0.0,
                "txn_kind": txn.get("txn_kind", "") or "",
                "currency": "ILS",
                "amount_bucket": "",
                "memo": txn.get("memo", "") or "",
                "import_id": txn.get("import_id", "") or "",
                "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
                "cleared": txn.get("cleared", "") or "",
                "approved": bool(txn.get("approved", False)),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "txn_kind" not in df.columns or df["txn_kind"].astype("string").fillna("").eq("").any():
        df["txn_kind"] = ynab._infer_txn_kind(
            df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
        )
    return df[
        [
            "source",
            "ynab_id",
            "account_id",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "fingerprint",
            "outflow_ils",
            "inflow_ils",
            "txn_kind",
            "currency",
            "amount_bucket",
            "memo",
            "import_id",
            "matched_transaction_id",
            "cleared",
            "approved",
        ]
    ]


def _matches_category(
    txn: dict[str, Any],
    *,
    category_id: str | None = None,
    category_name: str | None = None,
) -> bool:
    wanted_id = str(category_id or "").strip()
    wanted_name = str(category_name or "").strip()
    parent_match = False
    if wanted_id and str(txn.get("category_id", "") or "").strip() == wanted_id:
        parent_match = True
    if wanted_name and str(txn.get("category_raw", "") or "").strip() == wanted_name:
        parent_match = True

    split_match = False
    for split in txn.get("splits") or []:
        if wanted_id and str(split.get("category_id", "") or "").strip() == wanted_id:
            split_match = True
            break
        if wanted_name and str(split.get("category_raw", "") or "").strip() == wanted_name:
            split_match = True
            break

    return parent_match or split_match


def extract_category_transactions(
    data: Any,
    *,
    category_id: str | None = None,
    category_name: str | None = None,
) -> pd.DataFrame:
    if not str(category_id or "").strip() and not str(category_name or "").strip():
        raise ValueError("extract_category_transactions() requires category_id or category_name.")
    # Return canonical parent transactions whenever either the parent category
    # matches directly or one of the nested split rows matches.
    rows = [
        txn
        for txn in _canonical_rows(data)
        if _matches_category(
            txn,
            category_id=category_id,
            category_name=category_name,
        )
    ]
    return pd.DataFrame(rows)


def project_category_transactions_to_source_rows(
    data: Any,
    *,
    category_id: str | None = None,
    category_name: str | None = None,
) -> pd.DataFrame:
    if not str(category_id or "").strip() and not str(category_name or "").strip():
        raise ValueError(
            "project_category_transactions_to_source_rows() requires category_id or category_name."
        )

    wanted_id = str(category_id or "").strip()
    wanted_name = str(category_name or "").strip()
    rows: list[dict[str, Any]] = []
    for txn in _canonical_rows(data):
        txn_id = txn.get("transaction_id", "") or txn.get("ynab_id", "") or ""
        parent_match = False
        if not txn.get("splits"):
            parent_match = _matches_category(
                txn,
                category_id=wanted_id or None,
                category_name=wanted_name or None,
            )
        if parent_match:
            rows.append(
                {
                    "source": "ynab",
                    "ynab_id": txn_id,
                    "parent_ynab_id": txn.get("parent_transaction_id", "") or txn_id,
                    "is_subtransaction": False,
                    "account_id": txn.get("account_id", "") or "",
                    "account_name": txn.get("account_name", "") or "",
                    "date": txn.get("date", "") or "",
                    "payee_raw": txn.get("payee_raw", "") or "",
                    "category_raw": txn.get("category_raw", "") or "",
                    "category_id": txn.get("category_id", "") or "",
                    "fingerprint": txn.get("fingerprint", "")
                    or normalize.normalize_text(txn.get("payee_raw", "") or ""),
                    "outflow_ils": txn.get("outflow_ils", 0.0) or 0.0,
                    "inflow_ils": txn.get("inflow_ils", 0.0) or 0.0,
                    "txn_kind": txn.get("txn_kind", "") or "",
                    "currency": "ILS",
                    "amount_bucket": "",
                    "memo": txn.get("memo", "") or "",
                    "import_id": txn.get("import_id", "") or "",
                    "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
                    "cleared": txn.get("cleared", "") or "",
                    "approved": bool(txn.get("approved", False)),
                }
            )
            continue

        for index, split in enumerate(txn.get("splits") or []):
            if not _matches_category(
                {"category_id": split.get("category_id", ""), "category_raw": split.get("category_raw", "")},
                category_id=wanted_id or None,
                category_name=wanted_name or None,
            ):
                continue
            split_id = split.get("ynab_subtransaction_id") or split.get("split_id") or f"{txn_id}::sub::{index}"
            payee_raw = split.get("payee_raw", "") or txn.get("payee_raw", "") or ""
            rows.append(
                {
                    "source": "ynab",
                    "ynab_id": split_id,
                    "parent_ynab_id": txn.get("parent_transaction_id", "") or txn_id,
                    "is_subtransaction": True,
                    "account_id": txn.get("account_id", "") or "",
                    "account_name": txn.get("account_name", "") or "",
                    "date": txn.get("date", "") or "",
                    "payee_raw": payee_raw,
                    "category_raw": split.get("category_raw", "") or "",
                    "category_id": split.get("category_id", "") or "",
                    "fingerprint": normalize.normalize_text(payee_raw),
                    "outflow_ils": split.get("outflow_ils", 0.0) or 0.0,
                    "inflow_ils": split.get("inflow_ils", 0.0) or 0.0,
                    "txn_kind": txn.get("txn_kind", "") or "",
                    "currency": "ILS",
                    "amount_bucket": "",
                    "memo": split.get("memo", "") or txn.get("memo", "") or "",
                    "import_id": split.get("import_id", "") or txn.get("import_id", "") or "",
                    "matched_transaction_id": split.get("matched_transaction_id", "")
                    or txn.get("matched_transaction_id", "")
                    or "",
                    "cleared": txn.get("cleared", "") or "",
                    "approved": bool(txn.get("approved", False)),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "txn_kind" not in df.columns or df["txn_kind"].astype("string").fillna("").eq("").any():
        df["txn_kind"] = ynab._infer_txn_kind(
            df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
        )
    return df[
        [
            "source",
            "ynab_id",
            "parent_ynab_id",
            "is_subtransaction",
            "account_id",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "category_id",
            "fingerprint",
            "outflow_ils",
            "inflow_ils",
            "txn_kind",
            "currency",
            "amount_bucket",
            "memo",
            "import_id",
            "matched_transaction_id",
            "cleared",
            "approved",
        ]
    ]


def categories_to_dataframe(
    category_groups: list[dict[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group in category_groups or []:
        group_name = group.get("name", "") or ""
        group_id = group.get("id", "") or ""
        for category in group.get("categories", []) or []:
            if category.get("deleted"):
                continue
            rows.append(
                {
                    "category_group": group_name,
                    "category_group_id": group_id,
                    "category_name": category.get("name", "") or "",
                    "category_id": category.get("id", "") or "",
                    "hidden": bool(category.get("hidden", False)),
                }
            )
    df = pd.DataFrame(rows)
    return df


def categories_from_transactions_to_dataframe(
    transactions: list[dict[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _add(name: str, category_id: str) -> None:
        normalized_name = str(name or "").strip()
        normalized_id = str(category_id or "").strip()
        if not normalized_name:
            return
        key = (normalized_name, normalized_id)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "category_group": "",
                "category_group_id": "",
                "category_name": normalized_name,
                "category_id": normalized_id,
                "hidden": False,
            }
        )

    for txn in transactions or []:
        if bool(txn.get("deleted", False)):
            continue
        subtransactions = txn.get("subtransactions") or []
        emitted_sub = False
        for subtxn in subtransactions:
            if bool(subtxn.get("deleted", False)):
                continue
            _add(subtxn.get("category_name", ""), subtxn.get("category_id", ""))
            emitted_sub = True
        if emitted_sub:
            continue
        _add(txn.get("category_name", ""), txn.get("category_id", ""))

    return pd.DataFrame(rows)
