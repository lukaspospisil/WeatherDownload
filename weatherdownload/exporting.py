from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

ExportFormat = Literal["csv", "excel", "parquet", "mat"]
DEFAULT_OUTPUT_DIR = Path("outputs")


def export_table(table: pd.DataFrame, output_path: str | Path, format: ExportFormat) -> Path:
    destination = resolve_output_path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        table.to_csv(destination, index=False)
    elif format == "excel":
        _export_excel(table, destination)
    elif format == "parquet":
        _export_parquet(table, destination)
    elif format == "mat":
        _export_mat(table, destination)
    else:
        raise ValueError(f"Unsupported output format: {format}")

    return destination


def resolve_output_path(output_path: str | Path) -> Path:
    candidate = Path(output_path)
    if candidate.name == "":
        return candidate
    if candidate.is_absolute():
        return candidate
    if candidate.parent == Path('.'):
        return DEFAULT_OUTPUT_DIR / candidate
    return candidate


def _export_excel(table: pd.DataFrame, destination: Path) -> None:
    try:
        table.to_excel(destination, index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Excel export requires 'openpyxl'. Install it with 'pip install .[excel]'."
        ) from exc


def _export_parquet(table: pd.DataFrame, destination: Path) -> None:
    try:
        table.to_parquet(destination, index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Parquet export requires 'pyarrow'. Install it with 'pip install .[parquet]'."
        ) from exc


def _export_mat(table: pd.DataFrame, destination: Path) -> None:
    try:
        from scipy.io import savemat
    except ImportError as exc:
        raise RuntimeError(
            "MAT export requires 'scipy'. Install it with 'pip install .[mat]'."
        ) from exc

    payload = {
        column: [None if pd.isna(value) else value for value in table[column].tolist()]
        for column in table.columns
    }
    savemat(destination, {"table": payload})
