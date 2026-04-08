from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re

from pydantic import BaseModel, ConfigDict, Field, model_validator

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


DEFAULTS_PATH = Path("contexts/defaults.toml")
CONTEXTS_ROOT = Path("contexts")
LOCAL_BUDGET_CONFIG_PATH = Path("config/ynab.local.toml")


class DefaultsFilesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fingerprint_log: str = "fingerprint_log.csv"
    categories: str = "ynab_categories.csv"
    proposed_review: str = "{context}_proposed_transactions.parquet"
    reviewed_review: str = "{context}_proposed_transactions_reviewed.parquet"
    matched_pairs: str = "{context}_matched_pairs.parquet"
    upload_csv: str = "{context}_upload.csv"
    upload_json: str = "{context}_upload.json"
    bank_sync_report: str = "{context}_{source_id}_bank_sync_report.csv"
    bank_uncleared_report: str = "{context}_{source_id}_bank_uncleared_ynab_report.csv"
    bank_reconcile_report: str = "{context}_{source_id}_bank_reconcile_report.csv"
    card_sync_report: str = "{context}_{source_id}_{account_key}_card_sync_report.csv"
    card_reconcile_report: str = (
        "{context}_{source_id}_{account_key}_card_reconcile_report.csv"
    )


class DefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_root: Path = Path("data/raw")
    derived_root: Path = Path("data/derived")
    paired_root: Path = Path("data/paired")
    outputs_root: Path = Path("outputs")
    files: DefaultsFilesConfig = Field(default_factory=DefaultsFilesConfig)


class ContextMapsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_map: Path
    fingerprint_map: Path
    payee_map: Path


class ContextYnabConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    normalized_name: str


class ContextSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    kind: str
    raw_file: str | None = None
    raw_match: str | None = None
    normalized_name: str = ""

    @model_validator(mode="after")
    def _validate_source_selector(self) -> "ContextSourceConfig":
        selectors = [bool(self.raw_file), bool(self.raw_match)]
        if sum(selectors) != 1:
            raise ValueError(
                "Each source must define exactly one of raw_file or raw_match."
            )
        return self


class ContextConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    budget_id_env: str = ""
    maps: ContextMapsConfig
    ynab: ContextYnabConfig
    sources: list[ContextSourceConfig] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ContextRunPaths:
    raw_dir: Path
    derived_dir: Path
    paired_dir: Path
    outputs_dir: Path

    def proposal_review_path(self, defaults: DefaultsConfig, context_name: str) -> Path:
        return self.paired_dir / defaults.files.proposed_review.format(
            context=context_name
        )

    def reviewed_review_path(self, defaults: DefaultsConfig, context_name: str) -> Path:
        return self.paired_dir / defaults.files.reviewed_review.format(
            context=context_name
        )

    def matched_pairs_path(self, defaults: DefaultsConfig, context_name: str) -> Path:
        return self.paired_dir / defaults.files.matched_pairs.format(
            context=context_name
        )

    def upload_csv_path(self, defaults: DefaultsConfig, context_name: str) -> Path:
        return self.paired_dir / defaults.files.upload_csv.format(context=context_name)

    def upload_json_path(self, defaults: DefaultsConfig, context_name: str) -> Path:
        return self.paired_dir / defaults.files.upload_json.format(context=context_name)

    def bank_sync_report_path(
        self, defaults: DefaultsConfig, context_name: str, source_id: str
    ) -> Path:
        return self.paired_dir / defaults.files.bank_sync_report.format(
            context=context_name,
            source_id=source_id,
        )

    def bank_uncleared_report_path(
        self, defaults: DefaultsConfig, context_name: str, source_id: str
    ) -> Path:
        return self.paired_dir / defaults.files.bank_uncleared_report.format(
            context=context_name,
            source_id=source_id,
        )

    def bank_reconcile_report_path(
        self, defaults: DefaultsConfig, context_name: str, source_id: str
    ) -> Path:
        return self.paired_dir / defaults.files.bank_reconcile_report.format(
            context=context_name,
            source_id=source_id,
        )

    def card_sync_report_path(
        self,
        defaults: DefaultsConfig,
        context_name: str,
        source_id: str,
        account_key: str,
    ) -> Path:
        return self.paired_dir / defaults.files.card_sync_report.format(
            context=context_name,
            source_id=source_id,
            account_key=account_key,
        )

    def card_reconcile_report_path(
        self,
        defaults: DefaultsConfig,
        context_name: str,
        source_id: str,
        account_key: str,
    ) -> Path:
        return self.paired_dir / defaults.files.card_reconcile_report.format(
            context=context_name,
            source_id=source_id,
            account_key=account_key,
        )


@dataclass(frozen=True, slots=True)
class LoadedContext:
    config: ContextConfig
    context_dir: Path

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def budget_id_env(self) -> str:
        return self.config.budget_id_env

    @property
    def account_map_path(self) -> Path:
        return _resolve_relative_path(self.context_dir, self.config.maps.account_map)

    @property
    def fingerprint_map_path(self) -> Path:
        return _resolve_relative_path(
            self.context_dir, self.config.maps.fingerprint_map
        )

    @property
    def payee_map_path(self) -> Path:
        return _resolve_relative_path(self.context_dir, self.config.maps.payee_map)

    @property
    def ynab_normalized_name(self) -> str:
        return self.config.ynab.normalized_name


@dataclass(frozen=True, slots=True)
class ResolvedContextSource:
    id: str
    kind: str
    raw_path: Path
    normalized_name: str


def _read_toml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _resolve_relative_path(base_dir: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _read_optional_toml(path: Path) -> dict:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def load_defaults(path: Path = DEFAULTS_PATH) -> DefaultsConfig:
    return DefaultsConfig.model_validate(_read_toml(path))


def load_context(name: str, *, contexts_root: Path = CONTEXTS_ROOT) -> LoadedContext:
    context_name = str(name or "").strip().lower()
    if not context_name:
        raise ValueError("Context name cannot be empty.")
    context_dir = contexts_root / context_name
    context_path = context_dir / "context.toml"
    loaded = ContextConfig.model_validate(_read_toml(context_path))
    if loaded.name.strip().lower() != context_name:
        raise ValueError(
            f"Context file name mismatch: expected {context_name!r}, found {loaded.name!r}."
        )
    return LoadedContext(config=loaded, context_dir=context_dir.resolve())


def resolve_context_sources(
    context: LoadedContext, raw_dir: Path
) -> list[ResolvedContextSource]:
    if not raw_dir.exists():
        raise FileNotFoundError(f"Missing raw run directory: {raw_dir}")
    if not raw_dir.is_dir():
        raise ValueError(f"Raw run path is not a directory: {raw_dir}")

    files = [path for path in sorted(raw_dir.iterdir()) if path.is_file()]
    resolved: list[ResolvedContextSource] = []
    for source in context.config.sources:
        if source.raw_file:
            raw_path = raw_dir / source.raw_file
            if not raw_path.exists():
                raise FileNotFoundError(
                    f"Context source {source.id!r} expects raw file {source.raw_file!r} in {raw_dir}"
                )
            resolved.append(
                ResolvedContextSource(
                    id=source.id,
                    kind=source.kind,
                    raw_path=raw_path,
                    normalized_name=source.normalized_name,
                )
            )
            continue

        pattern = re.compile(str(source.raw_match or ""))
        matches = [path for path in files if pattern.fullmatch(path.name)]
        if len(matches) != 1:
            raise ValueError(
                f"Context source {source.id!r} regex {source.raw_match!r} matched "
                f"{len(matches)} files in {raw_dir}: {[path.name for path in matches]}"
            )
        resolved.append(
            ResolvedContextSource(
                id=source.id,
                kind=source.kind,
                raw_path=matches[0],
                normalized_name=source.normalized_name,
            )
        )

    return resolved


def resolve_run_paths(
    defaults: DefaultsConfig,
    *,
    run_tag: str,
) -> ContextRunPaths:
    normalized_run_tag = str(run_tag or "").strip()
    if not normalized_run_tag:
        raise ValueError("run_tag cannot be empty.")
    return ContextRunPaths(
        raw_dir=(defaults.raw_root / normalized_run_tag).resolve(),
        derived_dir=(defaults.derived_root / normalized_run_tag).resolve(),
        paired_dir=(defaults.paired_root / normalized_run_tag).resolve(),
        outputs_dir=defaults.outputs_root.resolve(),
    )


def resolve_context_normalized_source_paths(
    context: LoadedContext,
    run_paths: ContextRunPaths,
) -> list[Path]:
    paths = [
        run_paths.derived_dir / source.normalized_name
        for source in context.config.sources
    ]
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing normalized source artifacts for context {context.name!r}: "
            f"{[path.as_posix() for path in missing]}"
        )
    return paths


def select_context_sources(
    context: LoadedContext,
    *,
    source_id: str | None = None,
    allowed_kinds: set[str] | None = None,
) -> list[ContextSourceConfig]:
    selected = list(context.config.sources)
    if source_id:
        normalized_source_id = str(source_id).strip()
        selected = [source for source in selected if source.id == normalized_source_id]
        if not selected:
            raise ValueError(
                f"Context {context.name!r} has no declared source with id {normalized_source_id!r}."
            )
    if allowed_kinds is not None:
        selected = [source for source in selected if source.kind in allowed_kinds]
        if not selected:
            raise ValueError(
                f"Context {context.name!r} has no declared sources with kind in {sorted(allowed_kinds)}."
            )
    return selected


def resolve_context_normalized_source_path(
    context: LoadedContext,
    run_paths: ContextRunPaths,
    *,
    source_id: str | None = None,
    allowed_kinds: set[str] | None = None,
) -> Path:
    selected = select_context_sources(
        context,
        source_id=source_id,
        allowed_kinds=allowed_kinds,
    )
    if len(selected) != 1:
        raise ValueError(
            f"Context {context.name!r} must resolve to exactly one source, found {[source.id for source in selected]}."
        )
    path = run_paths.derived_dir / selected[0].normalized_name
    if not path.exists():
        raise FileNotFoundError(
            f"Missing normalized source artifact for context {context.name!r}: {path}"
        )
    return path


def resolve_context_ynab_path(
    context: LoadedContext, run_paths: ContextRunPaths
) -> Path:
    path = run_paths.derived_dir / context.ynab_normalized_name
    if not path.exists():
        raise FileNotFoundError(
            f"Missing normalized YNAB artifact for context {context.name!r}: {path}"
        )
    return path


def resolve_context_budget_id(context: LoadedContext, *, budget_id: str = "") -> str:
    override = str(budget_id or "").strip()
    if override:
        return override
    env_name = str(context.budget_id_env or "").strip()
    resolved = str(os.environ.get(env_name, "")).strip() if env_name else ""
    if not resolved:
        config = _read_optional_toml(LOCAL_BUDGET_CONFIG_PATH)
        profiles = config.get("profiles", {})
        if isinstance(profiles, dict):
            section = profiles.get(context.name, {})
            if isinstance(section, dict):
                resolved = str(
                    section.get("budget_id", "") or section.get("plan_id", "")
                ).strip()
        if not resolved and context.name == "family":
            resolved = str(
                config.get("budget_id", "") or config.get("plan_id", "")
            ).strip()
    if not resolved:
        if env_name:
            raise ValueError(
                f"Context {context.name!r} requires budget id env var {env_name!r} or a matching entry in {LOCAL_BUDGET_CONFIG_PATH}, but neither is set."
            )
        raise ValueError(
            f"Context {context.name!r} requires a budget id override or a matching entry in {LOCAL_BUDGET_CONFIG_PATH}."
        )
    return resolved
