import importlib.util
import sys
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "review_context.py"
SPEC = importlib.util.spec_from_file_location("review_context_script", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
review_context_script = importlib.util.module_from_spec(SPEC)
sys.modules["review_context_script"] = review_context_script
SPEC.loader.exec_module(review_context_script)


def test_rebase_reviewed_artifact_reconciles_onto_current_proposal(
    monkeypatch,
    tmp_path: Path,
) -> None:
    reviewed_path = tmp_path / "reviewed.parquet"
    proposal_path = tmp_path / "proposal.parquet"
    reviewed_path.write_text("", encoding="utf-8")
    proposal_path.write_text("", encoding="utf-8")

    old_reviewed = pl.DataFrame(
        {
            "transaction_id": ["old-only"],
            "reviewed": [True],
        }
    )
    new_proposed = pl.DataFrame(
        {
            "transaction_id": ["current-only"],
            "reviewed": [False],
        }
    )
    merged = pl.DataFrame(
        {
            "transaction_id": ["current-only"],
            "reviewed": [True],
        }
    )
    saved: dict[str, object] = {}

    monkeypatch.setattr(
        review_context_script.review_io,
        "load_review_artifact",
        lambda path: Path(path),
    )
    monkeypatch.setattr(
        review_context_script.review_io,
        "project_review_artifact_to_working_dataframe",
        lambda artifact: old_reviewed
        if Path(artifact) == reviewed_path
        else new_proposed,
    )
    monkeypatch.setattr(
        review_context_script.review_reconcile,
        "reconcile_reviewed_transactions",
        lambda old, new: (merged, {"direct_matches": 0, "fallback_matches": 0, "untouched_rows": 1}),
    )

    def _save(df: pl.DataFrame, path: Path) -> None:
        saved["df"] = df
        saved["path"] = path

    monkeypatch.setattr(
        review_context_script.review_io,
        "save_reviewed_transactions",
        _save,
    )

    review_context_script._rebase_reviewed_artifact(reviewed_path, proposal_path)

    assert saved["path"] == reviewed_path
    assert isinstance(saved["df"], pl.DataFrame)
    assert saved["df"]["transaction_id"].to_list() == ["current-only"]
    assert saved["df"]["reviewed"].to_list() == [True]
