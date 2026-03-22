from __future__ import annotations

from datetime import date, datetime
from numbers import Real
from pathlib import Path
from typing import Literal

import numpy as np
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
        excel_table = _prepare_table_for_excel(table)
        excel_table.to_excel(destination, index=False)
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
        column: _to_matlab_array(table[column])
        for column in table.columns
    }
    savemat(destination, {"table": payload})


def _prepare_table_for_excel(table: pd.DataFrame) -> pd.DataFrame:
    prepared = table.copy()
    for column in prepared.columns:
        series = prepared[column]
        if isinstance(series.dtype, pd.DatetimeTZDtype):
            prepared[column] = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return prepared


def _to_matlab_array(series: pd.Series) -> np.ndarray:
    if pd.api.types.is_datetime64_any_dtype(series):
        return np.array([_serialize_datetime_like(value) for value in series.tolist()], dtype=object)

    if pd.api.types.is_bool_dtype(series):
        return np.array([False if pd.isna(value) else bool(value) for value in series.tolist()], dtype=bool)

    if pd.api.types.is_numeric_dtype(series):
        return np.array([np.nan if pd.isna(value) else float(value) for value in series.tolist()], dtype=np.float64)

    non_missing = [value for value in series.tolist() if not pd.isna(value)]
    if non_missing and all(isinstance(value, bool) for value in non_missing):
        return np.array([False if pd.isna(value) else bool(value) for value in series.tolist()], dtype=bool)
    if non_missing and all(isinstance(value, Real) and not isinstance(value, bool) for value in non_missing):
        return np.array([np.nan if pd.isna(value) else float(value) for value in series.tolist()], dtype=np.float64)
    if non_missing and all(isinstance(value, (pd.Timestamp, datetime, date)) for value in non_missing):
        return np.array([_serialize_datetime_like(value) for value in series.tolist()], dtype=object)

    return np.array([_serialize_object_like(value) for value in series.tolist()], dtype=object)


def _serialize_object_like(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return _serialize_datetime_like(value)
    return str(value)


def _serialize_datetime_like(value: object) -> str:
    if pd.isna(value):
        return ""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.isoformat()
    return timestamp.tz_convert("UTC").isoformat()

