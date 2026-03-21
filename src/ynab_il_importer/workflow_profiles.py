from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Py <3.11 fallback
    import tomli as tomllib  # type: ignore


DEFAULT_PROFILE_NAME = "family"
CONFIG_PATH = Path("config/ynab.local.toml")


@dataclass(frozen=True, slots=True)
class WorkflowProfile:
    name: str
    account_map_path: Path
    fingerprint_map_path: Path
    payee_map_path: Path
    categories_path: Path
    budget_id: str = ""


def _default_profile_paths(name: str) -> dict[str, Path]:
    normalized = str(name or "").strip().lower() or DEFAULT_PROFILE_NAME
    if normalized == "family":
        return {
            "account_map_path": Path("mappings/account_name_map.csv"),
            "fingerprint_map_path": Path("mappings/fingerprint_map.csv"),
            "payee_map_path": Path("mappings/payee_map.csv"),
            "categories_path": Path("outputs/ynab_categories.csv"),
        }

    return {
        "account_map_path": Path("mappings") / normalized / "account_name_map.csv",
        "fingerprint_map_path": Path("mappings") / normalized / "fingerprint_map.csv",
        "payee_map_path": Path("mappings") / normalized / "payee_map.csv",
        "categories_path": Path("outputs") / normalized / "ynab_categories.csv",
    }


def _load_config(path: Path = CONFIG_PATH) -> dict:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _profile_section(config: dict, profile: str) -> dict:
    profiles = config.get("profiles", {})
    if not isinstance(profiles, dict):
        return {}
    section = profiles.get(profile, {})
    return section if isinstance(section, dict) else {}


def _config_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _env_budget_id(profile: str) -> str:
    profile_key = str(profile or "").strip().upper()
    candidates = [
        f"YNAB_{profile_key}_BUDGET_ID",
        f"YNAB_{profile_key}_PLAN_ID",
    ]
    if profile == DEFAULT_PROFILE_NAME:
        candidates.extend(["YNAB_BUDGET_ID", "YNAB_PLAN_ID"])
    for key in candidates:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def _config_budget_id(config: dict, profile: str) -> str:
    section = _profile_section(config, profile)
    for key in ("budget_id", "plan_id"):
        value = _config_text(section.get(key, ""))
        if value:
            return value

    if profile == DEFAULT_PROFILE_NAME:
        for key in ("budget_id", "plan_id"):
            value = _config_text(config.get(key, ""))
            if value:
                return value
    return ""


def default_profile_name(config_path: Path = CONFIG_PATH) -> str:
    config = _load_config(config_path)
    configured = _config_text(config.get("default_profile", ""))
    return configured or DEFAULT_PROFILE_NAME


def resolve_profile(
    profile: str | None = None,
    *,
    config_path: Path = CONFIG_PATH,
) -> WorkflowProfile:
    profile_name = (str(profile or "").strip().lower() or default_profile_name(config_path))
    defaults = _default_profile_paths(profile_name)
    config = _load_config(config_path)
    section = _profile_section(config, profile_name)

    def _path_value(key: str) -> Path:
        configured = _config_text(section.get(key, ""))
        return Path(configured) if configured else defaults[key]

    budget_id = _env_budget_id(profile_name) or _config_budget_id(config, profile_name)
    return WorkflowProfile(
        name=profile_name,
        account_map_path=_path_value("account_map_path"),
        fingerprint_map_path=_path_value("fingerprint_map_path"),
        payee_map_path=_path_value("payee_map_path"),
        categories_path=_path_value("categories_path"),
        budget_id=budget_id,
    )


def resolve_budget_id(
    *,
    profile: str | None = None,
    budget_id: str | None = None,
    config_path: Path = CONFIG_PATH,
) -> str:
    explicit = str(budget_id or "").strip()
    if explicit:
        return explicit
    return resolve_profile(profile, config_path=config_path).budget_id
