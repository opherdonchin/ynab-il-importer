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


def transactions_to_dataframe(
    transactions: list[dict[str, Any]],
    accounts: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    account_name_map = {acc.get("id"): acc.get("name") for acc in accounts or []}
    rows: list[dict[str, Any]] = []
    for txn in transactions:
        amount = float(txn.get("amount", 0)) / 1000.0
        outflow = abs(amount) if amount < 0 else 0.0
        inflow = amount if amount > 0 else 0.0
        account_id = txn.get("account_id", "")
        rows.append(
            {
                "source": "ynab",
                "ynab_id": txn.get("id", "") or "",
                "account_id": account_id,
                "account_name": account_name_map.get(account_id, account_id),
                "date": txn.get("date", ""),
                "payee_raw": txn.get("payee_name", "") or "",
                "category_raw": txn.get("category_name", "") or "",
                "outflow_ils": round(outflow, 2),
                "inflow_ils": round(inflow, 2),
                "memo": txn.get("memo", "") or "",
                "import_id": txn.get("import_id", "") or "",
                "matched_transaction_id": txn.get("matched_transaction_id", "") or "",
                "cleared": txn.get("cleared", "") or "",
                "approved": bool(txn.get("approved", False)),
                "currency": "ILS",
                "amount_bucket": "",
            }
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
