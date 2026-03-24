from __future__ import annotations

import json
from pathlib import Path

import ynab_il_importer.reconciliation_packets as packets


def test_default_packet_dir_for_cross_budget() -> None:
    path = packets.default_packet_dir(
        kind="cross_budget",
        source_profile="family",
        source_category="Pilates",
        target_profile="pilates",
        target_account="In Family",
        window_start="2025-11-01",
        window_end="2026-03-24",
    )

    assert path == Path(
        "data/packets/cross_budget/pilates/family__pilates__in_family/2025-11-01_to_2026-03-24"
    )


def test_parse_role_path_requires_role_and_path() -> None:
    role, path = packets.parse_role_path("source_csv=data/file.csv")

    assert role == "source_csv"
    assert path == Path("data/file.csv")


def test_copy_and_manifest_roundtrip(tmp_path: Path) -> None:
    source_file = tmp_path / "source.csv"
    source_file.write_text("a,b\n1,2\n", encoding="utf-8")
    packet_dir = tmp_path / "packet"

    copied = packets.copy_role_file(
        packet_dir=packet_dir,
        section="inputs",
        role="source_csv",
        source_path=source_file,
    )
    record = packets.file_record(
        section="inputs",
        role="source_csv",
        source_path=source_file,
        copied_path=copied,
    )
    manifest = packets.build_manifest(
        kind="cross_budget",
        packet_dir=packet_dir,
        scope={"target_profile": "pilates"},
        inputs=[record],
        artifacts=[],
        outputs=[],
        exceptions={"ignore_target_ids": ["row-1"]},
        commands=["pixi run python scripts/package_reconciliation_packet.py --kind cross_budget"],
        notes=["cached Family month report reused"],
    )
    manifest_path = packets.write_manifest(packet_dir, manifest)
    summary_path = packets.write_summary(packet_dir, manifest)

    assert copied.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["kind"] == "cross_budget"
    assert payload["packet_version"] == 2
    assert payload["inputs"][0]["role"] == "source_csv"
    assert payload["inputs"][0]["section"] == "inputs"
    assert payload["inputs"][0]["acquisition_method"] == "derived_input"
    assert payload["inputs"][0]["packet_path"].endswith("inputs\\source_csv__source.csv") or payload["inputs"][0]["packet_path"].endswith("inputs/source_csv__source.csv")
    assert payload["exceptions"]["ignore_target_ids"] == ["row-1"]
    assert payload["commands"] == ["pixi run python scripts/package_reconciliation_packet.py --kind cross_budget"]
    assert payload["notes"] == ["cached Family month report reused"]

    summary_text = summary_path.read_text(encoding="utf-8")
    assert "Reconciliation Packet" in summary_text
    assert "[derived_input]" in summary_text
    assert "cached Family month report reused" in summary_text


def test_infer_acquisition_method_prefers_raw_paths_for_human_download(tmp_path: Path) -> None:
    raw_file = tmp_path / "data" / "raw" / "statement.xlsx"
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_text("stub", encoding="utf-8")

    method = packets.infer_acquisition_method(
        section="inputs",
        role="statement_raw",
        source_path=raw_file,
    )

    assert method == "human_download"
