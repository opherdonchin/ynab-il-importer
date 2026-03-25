from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Py <3.11 fallback
    import tomli as tomllib  # type: ignore

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


def transactions_to_dataframe(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    account_name_map = {acc.get("id"): acc.get("name") for acc in accounts or []}
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        if bool(txn.get("deleted", False)):
            continue
        rows.append(
            _base_transaction_row(
                txn,
                account_name_map=account_name_map,
                amount_milliunits=txn.get("amount", 0),
                payee_name=txn.get("payee_name", "") or "",
                category_name=txn.get("category_name", "") or "",
                category_id=txn.get("category_id", "") or "",
                memo=txn.get("memo", "") or "",
                ynab_id=txn.get("id", "") or "",
                parent_ynab_id=txn.get("id", "") or "",
                is_subtransaction=False,
            )
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["txn_kind"] = ynab._infer_txn_kind(
        df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
    )
    df["fingerprint"] = df["payee_raw"].map(normalize.normalize_text)
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


def category_transactions_to_dataframe(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    account_name_map = {acc.get("id"): acc.get("name") for acc in accounts or []}
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        if bool(txn.get("deleted", False)):
            continue
        txn_id = txn.get("id", "") or ""
        payee_name = txn.get("payee_name", "") or ""
        memo = txn.get("memo", "") or ""
        subtransactions = txn.get("subtransactions") or []
        emitted = False
        for index, subtxn in enumerate(subtransactions):
            if bool(subtxn.get("deleted", False)):
                continue
            subtxn_id = subtxn.get("id") or f"{txn_id}::sub::{index}"
            rows.append(
                _base_transaction_row(
                    txn,
                    account_name_map=account_name_map,
                    amount_milliunits=subtxn.get("amount", 0),
                    payee_name=subtxn.get("payee_name", "") or payee_name,
                    category_name=subtxn.get("category_name", "") or "",
                    category_id=subtxn.get("category_id", "") or "",
                    memo=subtxn.get("memo", "") or memo,
                    ynab_id=subtxn_id,
                    parent_ynab_id=txn_id,
                    is_subtransaction=True,
                )
            )
            emitted = True
        if emitted:
            continue
        rows.append(
            _base_transaction_row(
                txn,
                account_name_map=account_name_map,
                amount_milliunits=txn.get("amount", 0),
                payee_name=payee_name,
                category_name=txn.get("category_name", "") or "",
                category_id=txn.get("category_id", "") or "",
                memo=memo,
                ynab_id=txn_id,
                parent_ynab_id=txn_id,
                is_subtransaction=False,
            )
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["txn_kind"] = ynab._infer_txn_kind(
        df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
    )
    df["fingerprint"] = df["payee_raw"].map(normalize.normalize_text)
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
