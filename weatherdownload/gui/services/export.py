from __future__ import annotations

from pathlib import Path
import tempfile

import pandas as pd

from weatherdownload import export_table


EXPORT_FORMAT_LABELS = {
    "csv": "csv",
    "parquet": "parquet",
    "xlsx": "excel",
    "mat": "mat",
}

EXPORT_SUFFIXES = {
    "csv": ".csv",
    "parquet": ".parquet",
    "xlsx": ".xlsx",
    "mat": ".mat",
}

EXPORT_MIME_TYPES = {
    "csv": "text/csv",
    "parquet": "application/x-parquet",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "mat": "application/x-matlab-data",
}


def resolve_export_filename(filename: str, *, format_name: str) -> str:
    cleaned = (filename or "").strip() or "weatherdownload_export"
    path = Path(cleaned)
    target_suffix = EXPORT_SUFFIXES[format_name]
    if path.suffix.lower() == target_suffix:
        return path.name
    return f"{path.stem or path.name}{target_suffix}"


def mime_type_for_export(format_name: str) -> str:
    return EXPORT_MIME_TYPES[format_name]


def export_table_bytes(table: pd.DataFrame, *, format_name: str, filename: str) -> tuple[bytes, str]:
    internal_format = EXPORT_FORMAT_LABELS[format_name]
    resolved_name = resolve_export_filename(filename, format_name=format_name)
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / resolved_name
        destination = export_table(table, output_path=output_path, format=internal_format)
        return destination.read_bytes(), destination.name
