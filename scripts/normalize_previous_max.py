# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import ynab_il_importer.context_config as context_config
import ynab_il_importer.normalize_runner as normalize_runner


CARD_SOURCE_KINDS = {"max", "leumi_card_html"}
RAW_ROOTS = {
    "max": "previous_max",
    "leumi_card_html": "previous_leumi_card",
}
SOURCE_EXTENSIONS = {
    "max": ".xlsx",
    "leumi_card_html": ".html",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize explicit previous card snapshots into canonical parquet artifacts."
        )
    )
    parser.add_argument("context", help="Context name, for example: family")
    parser.add_argument(
        "account_suffix",
        help=(
            "Account suffix folder under the inferred previous-card raw root, "
            "for example: x9922 or x0602"
        ),
    )
    parser.add_argument(
        "--source-id",
        default="",
        help="Declared card source id when a context has multiple card sources.",
    )
    parser.add_argument(
        "--kind",
        choices=sorted(CARD_SOURCE_KINDS),
        default="",
        help=(
            "Optional previous statement source kind override. By default this is "
            "inferred from the context's declared card source kind."
        ),
    )
    parser.add_argument(
        "--cycle",
        default="",
        help="Optional cycle stem to normalize, for example: 2026_03",
    )
    parser.add_argument(
        "--defaults",
        dest="defaults_path",
        type=Path,
        default=context_config.DEFAULTS_PATH,
        help="Defaults TOML path.",
    )
    parser.add_argument(
        "--contexts-root",
        dest="contexts_root",
        type=Path,
        default=context_config.CONTEXTS_ROOT,
        help="Contexts root directory.",
    )
    args = parser.parse_args()

    defaults = context_config.load_defaults(args.defaults_path)
    context = context_config.load_context(args.context, contexts_root=args.contexts_root)

    selected_sources = context_config.select_context_sources(
        context,
        source_id=args.source_id or None,
        allowed_kinds=CARD_SOURCE_KINDS,
    )
    if len(selected_sources) != 1:
        raise ValueError(
            f"Context {context.name!r} must resolve to exactly one card source for previous-card normalization, found {[source.id for source in selected_sources]}."
        )
    inferred_kind = selected_sources[0].kind
    source_kind = args.kind or inferred_kind
    raw_root_name = RAW_ROOTS[source_kind]
    source_extension = SOURCE_EXTENSIONS[source_kind]

    raw_dir = (defaults.raw_root / raw_root_name / args.account_suffix).resolve()
    out_dir = (defaults.derived_root / raw_root_name / args.account_suffix).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    fingerprint_log_path = (
        defaults.outputs_root / context.name / defaults.files.fingerprint_log
    ).resolve()
    fingerprint_log_path.parent.mkdir(parents=True, exist_ok=True)

    if args.cycle:
        candidates = [raw_dir / f"{args.cycle}{source_extension}"]
    else:
        candidates = sorted(raw_dir.glob(f"*{source_extension}"))

    if not candidates:
        raise FileNotFoundError(
            f"No previous {source_kind} snapshots found in {raw_dir}"
        )

    for source_path in candidates:
        if not source_path.exists():
            raise FileNotFoundError(f"Missing previous snapshot: {source_path}")
        out_path = normalize_runner.default_out_path(source_path, source_kind, out_dir)
        normalize_runner.normalize_one(
            source_path,
            source_kind,
            out_path,
            use_fingerprint_map=True,
            account_map_path=context.account_map_path,
            fingerprint_map_path=context.fingerprint_map_path,
            fingerprint_log_path=fingerprint_log_path,
        )


if __name__ == "__main__":
    main()
