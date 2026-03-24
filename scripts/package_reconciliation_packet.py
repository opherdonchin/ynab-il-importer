import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.reconciliation_packets as packets


def _collect_records(
    *,
    entries: list[str],
    section: str,
    packet_dir: Path,
    copy_files: bool,
    acquisition_method: str = "",
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for item in entries:
        role, source_path = packets.parse_role_path(item)
        if not source_path.exists():
            raise FileNotFoundError(f"Missing file for {section} role {role!r}: {source_path}")
        copied_path = (
            packets.copy_role_file(
                packet_dir=packet_dir,
                section=section,
                role=role,
                source_path=source_path,
            )
            if copy_files
            else None
        )
        records.append(
            packets.file_record(
                section=section,
                role=role,
                source_path=source_path,
                copied_path=copied_path,
                acquisition_method=acquisition_method,
            )
        )
    return records


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Snapshot a bank/card/cross-budget reconciliation run into a dated packet "
            "folder with copied files and a manifest."
        )
    )
    parser.add_argument("--kind", required=True, choices=["bank", "card", "cross_budget"])
    parser.add_argument("--profile", default="", help="Profile for bank/card packets.")
    parser.add_argument("--account", default="", help="Account name for bank/card packets.")
    parser.add_argument("--source-profile", default="", help="Source profile for cross-budget packets.")
    parser.add_argument("--source-category", default="", help="Source category for cross-budget packets.")
    parser.add_argument("--target-profile", default="", help="Target profile for cross-budget packets.")
    parser.add_argument("--target-account", default="", help="Target account for cross-budget packets.")
    parser.add_argument("--window-start", default="", help="Window start date YYYY-MM-DD.")
    parser.add_argument("--window-end", default="", help="Window end date YYYY-MM-DD.")
    parser.add_argument("--label", default="", help="Optional packet label.")
    parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Input file to include in the packet.",
    )
    parser.add_argument(
        "--input-human",
        dest="input_human",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Human-downloaded input file to include in the packet.",
    )
    parser.add_argument(
        "--input-agent",
        dest="input_agent",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Agent-downloaded input file to include in the packet.",
    )
    parser.add_argument(
        "--input-derived",
        dest="input_derived",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Derived input file to include in the packet.",
    )
    parser.add_argument(
        "--artifact",
        dest="artifacts",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Generated report/artifact file to include in the packet.",
    )
    parser.add_argument(
        "--output",
        dest="outputs",
        action="append",
        default=[],
        metavar="ROLE=PATH",
        help="Reviewed/upload/output file to include in the packet.",
    )
    parser.add_argument(
        "--ignore-source-id",
        dest="ignore_source_ids",
        action="append",
        default=[],
        help="Explicit source-side bootstrap/base exception id.",
    )
    parser.add_argument(
        "--ignore-target-id",
        dest="ignore_target_ids",
        action="append",
        default=[],
        help="Explicit target-side bootstrap/base exception id.",
    )
    parser.add_argument(
        "--out-dir",
        dest="out_dir",
        type=Path,
        default=None,
        help="Override packet directory.",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Write only the manifest with references to the original files instead of copying them.",
    )
    parser.add_argument(
        "--command",
        dest="commands",
        action="append",
        default=[],
        help="Command string worth preserving in the packet manifest and summary.",
    )
    parser.add_argument(
        "--note",
        dest="notes",
        action="append",
        default=[],
        help="Short human note to preserve in the packet manifest and summary.",
    )
    args = parser.parse_args()

    packet_dir = args.out_dir or packets.default_packet_dir(
        kind=args.kind,
        profile=args.profile,
        account=args.account,
        source_profile=args.source_profile,
        source_category=args.source_category,
        target_profile=args.target_profile,
        target_account=args.target_account,
        window_start=args.window_start,
        window_end=args.window_end,
        label=args.label,
    )

    copy_files = not bool(args.manifest_only)
    input_records: list[dict[str, object]] = []
    input_records.extend(
        _collect_records(
            entries=args.inputs,
            section="inputs",
            packet_dir=packet_dir,
            copy_files=copy_files,
        )
    )
    input_records.extend(
        _collect_records(
            entries=args.input_human,
            section="inputs",
            packet_dir=packet_dir,
            copy_files=copy_files,
            acquisition_method="human_download",
        )
    )
    input_records.extend(
        _collect_records(
            entries=args.input_agent,
            section="inputs",
            packet_dir=packet_dir,
            copy_files=copy_files,
            acquisition_method="agent_download",
        )
    )
    input_records.extend(
        _collect_records(
            entries=args.input_derived,
            section="inputs",
            packet_dir=packet_dir,
            copy_files=copy_files,
            acquisition_method="derived_input",
        )
    )
    artifact_records = _collect_records(
        entries=args.artifacts,
        section="artifacts",
        packet_dir=packet_dir,
        copy_files=copy_files,
    )
    output_records = _collect_records(
        entries=args.outputs,
        section="outputs",
        packet_dir=packet_dir,
        copy_files=copy_files,
    )

    scope = {
        "profile": str(args.profile or "").strip(),
        "account": str(args.account or "").strip(),
        "source_profile": str(args.source_profile or "").strip(),
        "source_category": str(args.source_category or "").strip(),
        "target_profile": str(args.target_profile or "").strip(),
        "target_account": str(args.target_account or "").strip(),
        "window_start": str(args.window_start or "").strip(),
        "window_end": str(args.window_end or "").strip(),
        "label": str(args.label or "").strip(),
    }
    exceptions = {
        "ignore_source_ids": [str(v or "").strip() for v in args.ignore_source_ids if str(v or "").strip()],
        "ignore_target_ids": [str(v or "").strip() for v in args.ignore_target_ids if str(v or "").strip()],
    }
    manifest = packets.build_manifest(
        kind=args.kind,
        packet_dir=packet_dir,
        scope=scope,
        inputs=input_records,
        artifacts=artifact_records,
        outputs=output_records,
        exceptions=exceptions,
        commands=args.commands,
        notes=args.notes,
    )
    manifest_path = packets.write_manifest(packet_dir, manifest)
    summary_path = packets.write_summary(packet_dir, manifest)
    print(f"Packet directory: {packet_dir.resolve()}")
    print(f"Manifest: {manifest_path.resolve()}")
    print(f"Summary: {summary_path.resolve()}")
    print(
        "Counts: "
        f"inputs={len(input_records)} "
        f"artifacts={len(artifact_records)} "
        f"outputs={len(output_records)}"
    )


if __name__ == "__main__":
    main()
