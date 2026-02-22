import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def test_import_package_and_fingerprint() -> None:
    import ynab_il_importer as pkg
    from ynab_il_importer.fingerprint import fingerprint_v0

    assert pkg.__version__
    fp = fingerprint_v0("SUPERSAL 1234567 Tel Aviv 23")
    assert isinstance(fp, str)
    assert "1234567" not in fp


def test_cli_help_runs() -> None:
    cli_testing = pytest.importorskip("typer.testing")
    cli_runner = cli_testing.CliRunner()
    from ynab_il_importer.cli import app

    result = cli_runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "parse-bank" in result.output
    assert "build-payee-map" in result.output
