from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import Any


def _slug_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^\w]+", "_", text, flags=re.UNICODE)
    return text.strip("_") or "unnamed"


def window_label(
    *,
    window_start: str = "",
    window_end: str = "",
    label: str = "",
) -> str:
    explicit = str(label or "").strip()
    if explicit:
        return _slug_text(explicit)
    start = str(window_start or "").strip()
    end = str(window_end or "").strip()
    if start and end:
        return f"{start}_to_{end}"
    if start:
        return f"since_{start}"
    if end:
        return f"until_{end}"
    return datetime.now(UTC).strftime("%Y-%m-%d_%H%M%S")


def default_packet_dir(
    *,
    kind: str,
    profile: str = "",
    account: str = "",
    source_profile: str = "",
    source_category: str = "",
    target_profile: str = "",
    target_account: str = "",
    window_start: str = "",
    window_end: str = "",
    label: str = "",
) -> Path:
    packet_kind = _slug_text(kind)
    root = Path("data/packets") / packet_kind
    packet_window = window_label(
        window_start=window_start,
        window_end=window_end,
        label=label,
    )
    if packet_kind in {"bank", "card"}:
        profile_slug = _slug_text(profile)
        account_slug = _slug_text(account)
        return root / profile_slug / account_slug / packet_window
    if packet_kind == "cross_budget":
        target_profile_slug = _slug_text(target_profile)
        bridge_slug = "__".join(
            [
                _slug_text(source_profile),
                _slug_text(source_category),
                _slug_text(target_account),
            ]
        )
        return root / target_profile_slug / bridge_slug / packet_window
    return root / packet_window


def parse_role_path(value: str) -> tuple[str, Path]:
    text = str(value or "").strip()
    if "=" not in text:
        raise ValueError(
            f"Expected ROLE=PATH entry, got {value!r}."
        )
    role, raw_path = text.split("=", 1)
    role = str(role or "").strip()
    path = Path(str(raw_path or "").strip())
    if not role or not str(path):
        raise ValueError(f"Expected ROLE=PATH entry, got {value!r}.")
    return role, path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def infer_acquisition_method(
    *,
    section: str,
    role: str,
    source_path: Path,
    acquisition_method: str = "",
) -> str:
    explicit = str(acquisition_method or "").strip()
    if explicit:
        return explicit

    normalized_section = str(section or "").strip().lower()
    normalized_role = str(role or "").strip().lower()
    resolved = str(source_path).replace("\\", "/").lower()

    if normalized_section == "inputs":
        if "/data/raw/" in resolved or normalized_role.endswith("_raw") or normalized_role.startswith("statement_"):
            return "human_download"
        if normalized_role in {"ynab_snapshot", "source_category_export", "source_month_report"}:
            return "agent_download"
        return "derived_input"
    if normalized_section == "artifacts":
        return "agent_generated"
    if normalized_section == "outputs":
        if "review" in normalized_role:
            return "human_reviewed"
        if "upload" in normalized_role:
            return "upload_payload"
        return "workflow_output"
    return "unspecified"


def copy_role_file(
    *,
    packet_dir: Path,
    section: str,
    role: str,
    source_path: Path,
) -> Path:
    section_dir = packet_dir / section
    section_dir.mkdir(parents=True, exist_ok=True)
    basename = source_path.name or "artifact"
    dest_name = f"{_slug_text(role)}__{basename}"
    dest_path = section_dir / dest_name
    shutil.copy2(source_path, dest_path)
    return dest_path


def file_record(
    *,
    section: str,
    role: str,
    source_path: Path,
    copied_path: Path | None = None,
    acquisition_method: str = "",
) -> dict[str, Any]:
    stat = source_path.stat()
    record: dict[str, Any] = {
        "role": str(role or "").strip(),
        "section": str(section or "").strip(),
        "acquisition_method": infer_acquisition_method(
            section=section,
            role=role,
            source_path=source_path,
            acquisition_method=acquisition_method,
        ),
        "original_path": str(source_path.resolve()),
        "size_bytes": int(stat.st_size),
        "sha256": sha256_file(source_path),
        "modified_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
    }
    if copied_path is not None:
        record["packet_path"] = str(copied_path)
    return record


def build_manifest(
    *,
    kind: str,
    packet_dir: Path,
    scope: dict[str, Any],
    inputs: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    exceptions: dict[str, Any] | None = None,
    commands: list[str] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "packet_version": 2,
        "kind": str(kind or "").strip(),
        "created_at_utc": datetime.now(UTC).isoformat(),
        "packet_dir": str(packet_dir),
        "scope": scope,
        "inputs": inputs,
        "artifacts": artifacts,
        "outputs": outputs,
        "exceptions": exceptions or {},
        "commands": [str(v or "").strip() for v in (commands or []) if str(v or "").strip()],
        "notes": [str(v or "").strip() for v in (notes or []) if str(v or "").strip()],
    }


def build_summary_markdown(manifest: dict[str, Any]) -> str:
    scope = manifest.get("scope") or {}
    inputs = manifest.get("inputs") or []
    artifacts = manifest.get("artifacts") or []
    outputs = manifest.get("outputs") or []
    exceptions = manifest.get("exceptions") or {}
    commands = manifest.get("commands") or []
    notes = manifest.get("notes") or []

    lines: list[str] = []
    lines.append("# Reconciliation Packet")
    lines.append("")
    lines.append(f"- Kind: `{manifest.get('kind', '')}`")
    lines.append(f"- Created: `{manifest.get('created_at_utc', '')}`")
    lines.append(f"- Packet dir: `{manifest.get('packet_dir', '')}`")
    if any(str(v or "").strip() for v in scope.values()):
        lines.append("- Scope:")
        for key, value in scope.items():
            text = str(value or "").strip()
            if text:
                lines.append(f"  - `{key}` = `{text}`")

    def _append_records(title: str, records: list[dict[str, Any]]) -> None:
        lines.append("")
        lines.append(f"## {title}")
        if not records:
            lines.append("")
            lines.append("_None_")
            return
        lines.append("")
        for record in records:
            role = str(record.get("role", "") or "").strip()
            acquisition = str(record.get("acquisition_method", "") or "").strip()
            original_path = str(record.get("original_path", "") or "").strip()
            packet_path = str(record.get("packet_path", "") or "").strip()
            lines.append(
                f"- `{role}` [{acquisition}]"
            )
            lines.append(f"  - original: `{original_path}`")
            if packet_path:
                lines.append(f"  - packet: `{packet_path}`")

    _append_records("Inputs", inputs)
    _append_records("Artifacts", artifacts)
    _append_records("Outputs", outputs)

    if exceptions:
        lines.append("")
        lines.append("## Exceptions")
        lines.append("")
        empty = True
        for key, values in exceptions.items():
            if values:
                empty = False
                lines.append(f"- `{key}`: {', '.join(f'`{value}`' for value in values)}")
        if empty:
            lines.append("_None_")

    if commands:
        lines.append("")
        lines.append("## Commands")
        lines.append("")
        for command in commands:
            lines.append("```powershell")
            lines.append(command)
            lines.append("```")

    if notes:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        for note in notes:
            lines.append(f"- {note}")

    lines.append("")
    return "\n".join(lines)


def write_manifest(packet_dir: Path, manifest: dict[str, Any]) -> Path:
    packet_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = packet_dir / "packet_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def write_summary(packet_dir: Path, manifest: dict[str, Any]) -> Path:
    packet_dir.mkdir(parents=True, exist_ok=True)
    summary_path = packet_dir / "packet_summary.md"
    summary_path.write_text(
        build_summary_markdown(manifest),
        encoding="utf-8",
    )
    return summary_path
