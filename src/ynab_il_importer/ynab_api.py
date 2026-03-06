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

from ynab_il_importer.io_ynab import _infer_txn_kind


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
                "account_name": account_name_map.get(account_id, account_id),
                "date": txn.get("date", ""),
                "payee_raw": txn.get("payee_name", "") or "",
                "category_raw": txn.get("category_name", "") or "",
                "outflow_ils": round(outflow, 2),
                "inflow_ils": round(inflow, 2),
                "memo": txn.get("memo", "") or "",
                "currency": "ILS",
                "amount_bucket": "",
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["txn_kind"] = _infer_txn_kind(
        df["inflow_ils"], df["outflow_ils"], df["payee_raw"], df["category_raw"]
    )
    return df[
        [
            "source",
            "account_name",
            "date",
            "payee_raw",
            "category_raw",
            "outflow_ils",
            "inflow_ils",
            "txn_kind",
            "currency",
            "amount_bucket",
            "memo",
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
