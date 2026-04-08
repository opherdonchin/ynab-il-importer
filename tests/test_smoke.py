import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def test_import_package_and_fingerprint() -> None:
    import ynab_il_importer as pkg
    import ynab_il_importer.fingerprint as fingerprint

    assert pkg.__version__
    fp = fingerprint.fingerprint_v0("SUPERSAL 1234567 Tel Aviv 23")
    assert isinstance(fp, str)
    assert "1234567" not in fp


def test_cli_help_runs() -> None:
    cli_testing = pytest.importorskip("typer.testing")
    cli_runner = cli_testing.CliRunner()
    import ynab_il_importer.cli as cli

    result = cli_runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert "parse-leumi-xls" in result.output
    assert "parse-leumi" in result.output
    assert "parse-max" in result.output
