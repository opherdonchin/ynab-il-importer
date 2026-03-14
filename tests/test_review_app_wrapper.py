import importlib.util
import sys
from pathlib import Path


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
