from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "normalize_previous_max.py"
SPEC = importlib.util.spec_from_file_location(
    "normalize_previous_max_script", SCRIPT_PATH
)
assert SPEC is not None and SPEC.loader is not None
normalize_previous_max = importlib.util.module_from_spec(SPEC)
sys.modules["normalize_previous_max_script"] = normalize_previous_max
SPEC.loader.exec_module(normalize_previous_max)

import ynab_il_importer.context_config as context_config


def test_main_writes_previous_max_parquet(monkeypatch, tmp_path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("family")
    previous_dir = defaults.raw_root / "previous_max" / "x9922"
    previous_dir.mkdir(parents=True, exist_ok=True)
    source_path = previous_dir / "2026_03.xlsx"
    source_path.write_text("stub", encoding="utf-8")

    captured: dict[str, Path | str] = {}

    monkeypatch.setattr(
        normalize_previous_max.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        normalize_previous_max.context_config,
        "load_context",
        lambda *_args, **_kwargs: context,
    )
    monkeypatch.setattr(
        normalize_previous_max.normalize_runner,
        "normalize_one",
        lambda in_path, fmt, out_path, **_kwargs: captured.update(
            {
                "in_path": Path(in_path),
                "out_path": Path(out_path),
                "fmt": fmt,
            }
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["normalize_previous_max.py", "family", "x9922", "--cycle", "2026_03"],
    )

    normalize_previous_max.main()

    assert captured["fmt"] == "max"
    assert captured["in_path"] == source_path
    assert captured["out_path"] == (
        defaults.derived_root / "previous_max" / "x9922" / "2026_03_max_norm.parquet"
    )


def test_main_infers_leumi_card_html_from_context(monkeypatch, tmp_path) -> None:
    defaults = context_config.DefaultsConfig(
        raw_root=tmp_path / "raw",
        derived_root=tmp_path / "derived",
        paired_root=tmp_path / "paired",
        outputs_root=tmp_path / "outputs",
    )
    context = context_config.load_context("pilates")
    previous_dir = defaults.raw_root / "previous_leumi_card" / "x0602"
    previous_dir.mkdir(parents=True, exist_ok=True)
    source_path = previous_dir / "2026_03.html"
    source_path.write_text("stub", encoding="utf-8")

    captured: dict[str, Path | str] = {}

    monkeypatch.setattr(
        normalize_previous_max.context_config,
        "load_defaults",
        lambda *_args, **_kwargs: defaults,
    )
    monkeypatch.setattr(
        normalize_previous_max.context_config,
        "load_context",
        lambda *_args, **_kwargs: context,
    )
    monkeypatch.setattr(
        normalize_previous_max.normalize_runner,
        "normalize_one",
        lambda in_path, fmt, out_path, **_kwargs: captured.update(
            {
                "in_path": Path(in_path),
                "out_path": Path(out_path),
                "fmt": fmt,
            }
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["normalize_previous_max.py", "pilates", "x0602", "--cycle", "2026_03"],
    )

    normalize_previous_max.main()

    assert captured["fmt"] == "leumi_card_html"
    assert captured["in_path"] == source_path
    assert captured["out_path"] == (
        defaults.derived_root
        / "previous_leumi_card"
        / "x0602"
        / "2026_03_leumi_card_html_norm.parquet"
    )
