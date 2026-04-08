from pathlib import Path

import pandas as pd
import polars as pl


def display_path(path: str | Path) -> str:
    return str(Path(path).resolve())


def wrote_message(path: str | Path, row_count: int | None = None) -> str:
    path_text = display_path(path)
    if row_count is None:
        return f"Wrote {path_text}"
    return f"Wrote {path_text} ({row_count} rows)"


def report_message(path: str | Path) -> str:
    return f"Report: {display_path(path)}"


def write_dataframe(df: pd.DataFrame | pl.DataFrame, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(df, pl.DataFrame):
        df.write_csv(output_path)
        return
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
