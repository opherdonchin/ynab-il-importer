import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "review_app.py"
SPEC = importlib.util.spec_from_file_location("review_app_wrapper", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
review_app_wrapper = importlib.util.module_from_spec(SPEC)
sys.modules["review_app_wrapper"] = review_app_wrapper
SPEC.loader.exec_module(review_app_wrapper)


def test_build_streamlit_command_includes_control_dir_and_resume(tmp_path: Path) -> None:
    control_dir = tmp_path / "control"
    cmd = review_app_wrapper._build_streamlit_command(
        app_path=tmp_path / "app.py",
        input_path=tmp_path / "proposed.csv",
        output_path=tmp_path / "reviewed.csv",
        categories_path=tmp_path / "categories.csv",
        resume=str(tmp_path / "resume.csv"),
        control_dir=control_dir,
        port=8510,
    )

    assert "--server.port" in cmd
    assert "8510" in cmd
    assert "--control-dir" in cmd
    assert str(control_dir) in cmd
    assert "--resume" in cmd


def test_terminate_pid_windows_prefers_taskkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    taskkill_calls: list[list[str]] = []

    monkeypatch.setattr(review_app_wrapper.sys, "platform", "win32")
    monkeypatch.setattr(review_app_wrapper, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(
        review_app_wrapper.subprocess,
        "run",
        lambda cmd, **kwargs: taskkill_calls.append(cmd),
    )
    monkeypatch.setattr(review_app_wrapper, "_wait_for_exit", lambda pid, timeout_seconds: True)

    def _unexpected_os_kill(pid: int, sig: int) -> None:
        raise AssertionError("os.kill should not be used on Windows quit requests")

    monkeypatch.setattr(review_app_wrapper.os, "kill", _unexpected_os_kill)

    review_app_wrapper._terminate_pid(321)

    assert taskkill_calls == [["taskkill", "/PID", "321", "/T", "/F"]]


def test_wait_foreground_treats_interrupted_quit_as_clean_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    control_dir = tmp_path / "control"
    control_dir.mkdir(parents=True, exist_ok=True)
    review_app_wrapper._quit_request_path(control_dir).write_text("{}", encoding="utf-8")

    class _FakeProcess:
        pid = 654

        @staticmethod
        def poll() -> int | None:
            return None

    monkeypatch.setattr(
        review_app_wrapper,
        "_terminate_pid",
        lambda pid: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    assert review_app_wrapper._wait_foreground(_FakeProcess(), control_dir) == 0
