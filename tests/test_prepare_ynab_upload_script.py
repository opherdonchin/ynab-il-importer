import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "prepare_ynab_upload.py"
SPEC = importlib.util.spec_from_file_location("prepare_ynab_upload_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
prepare_ynab_upload_script = importlib.util.module_from_spec(SPEC)
sys.modules["prepare_ynab_upload_script"] = prepare_ynab_upload_script
SPEC.loader.exec_module(prepare_ynab_upload_script)


def test_build_parser_parses_approved_bool_values() -> None:
    parser = prepare_ynab_upload_script._build_parser()

    args = parser.parse_args(["--in", "reviewed.csv", "--approved", "true"])
    assert args.approved is True

    args = parser.parse_args(["--in", "reviewed.csv", "--approved", "false"])
    assert args.approved is False


def test_build_parser_rejects_invalid_approved_value() -> None:
    parser = prepare_ynab_upload_script._build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--in", "reviewed.csv", "--approved", "maybe"])
