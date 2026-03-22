from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect WeatherDownload output files or bundle directories.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to a file or bundle directory.",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=10,
        help="Number of preview rows to print for tabular files. Defaults to 10.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    path = args.path
    if not path.exists():
        raise SystemExit(f"Path not found: {path}")

    if path.is_dir():
        inspect_directory(path, head=max(args.head, 0))
        return 0

    inspect_path(path, head=max(args.head, 0))
    return 0


def inspect_directory(path: Path, head: int) -> None:
    # Summarize the directory first, then inspect known bundle members if present.
    stat = path.stat()
    entries = sorted(path.iterdir(), key=lambda item: item.name.lower())

    print_basic_info(path, file_type="directory", size_bytes=stat.st_size, modified=stat.st_mtime)
    print(f"Entries: {len(entries)}")
    print()

    print("Directory contents:")
    if not entries:
        print("(empty)")
        return
    for entry in entries:
        entry_type = "dir" if entry.is_dir() else entry.suffix.lower() or "file"
        print(f"- {entry.name} [{entry_type}]")
    print()

    known_children = [
        path / "data_info.json",
        path / "stations.parquet",
        path / "series.parquet",
    ]
    supported_children = [child for child in known_children if child.exists()]
    if not supported_children:
        return

    print("Known bundle files:")
    for child in supported_children:
        print()
        inspect_path(child, head=head, indent="  ")


def inspect_path(path: Path, head: int, indent: str = "") -> None:
    # Dispatch by file extension so one utility can handle common output types.
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        inspect_table(path, reader="parquet", head=head, indent=indent)
        return
    if suffix == ".csv":
        inspect_table(path, reader="csv", head=head, indent=indent)
        return
    if suffix == ".json":
        inspect_json(path, indent=indent)
        return
    if suffix == ".mat":
        inspect_mat(path, indent=indent)
        return
    raise SystemExit(f"Unsupported file type: {path.suffix or '(no extension)'}")


def inspect_table(path: Path, reader: str, head: int, indent: str = "") -> None:
    # Load the table into pandas so we can print the same summary for CSV and Parquet.
    table = pd.read_parquet(path) if reader == "parquet" else pd.read_csv(path)
    stat = path.stat()

    print_basic_info(path, file_type=reader, size_bytes=stat.st_size, modified=stat.st_mtime, indent=indent)
    print(f"{indent}Rows: {len(table):,}")
    print(f"{indent}Columns: {len(table.columns)}")
    print()

    print(f"{indent}Columns:")
    print(f"{indent}{', '.join(table.columns.tolist()) if len(table.columns) else '(none)'}")
    print()

    dtype_table = pd.DataFrame(
        {
            "column": table.columns,
            "dtype": [str(dtype) for dtype in table.dtypes],
        }
    )
    print(f"{indent}Dtypes:")
    print(indent_lines(dtype_table.to_string(index=False), indent))
    print()

    missing_table = pd.DataFrame(
        {
            "column": table.columns,
            "missing": table.isna().sum().tolist(),
        }
    )
    print(f"{indent}Missing values per column:")
    print(indent_lines(missing_table.to_string(index=False), indent))
    print()

    memory_bytes = int(table.memory_usage(deep=True).sum())
    print(f"{indent}Estimated pandas memory usage: {memory_bytes:,} bytes")
    print()

    print(f"{indent}Preview (first {head} rows):")
    if head == 0:
        print(f"{indent}(preview disabled)")
    elif table.empty:
        print(f"{indent}(no rows)")
    else:
        print(indent_lines(table.head(head).to_string(index=False), indent))


def inspect_json(path: Path, indent: str = "") -> None:
    # Print a compact structural summary instead of dumping the full JSON payload.
    stat = path.stat()
    data = json.loads(path.read_text(encoding="utf-8"))

    print_basic_info(path, file_type="json", size_bytes=stat.st_size, modified=stat.st_mtime, indent=indent)
    print(f"{indent}Summary: {_json_summary(data)}")
    print()

    if isinstance(data, dict):
        print(f"{indent}Top-level keys:")
        keys = list(data.keys())
        print(f"{indent}{', '.join(keys) if keys else '(none)'}")
        print()
        print(f"{indent}Top-level structure:")
        for key in keys:
            print(f"{indent}- {key}: {_json_summary(data[key])}")
    elif isinstance(data, list):
        print(f"{indent}Top-level structure:")
        print(f"{indent}- list with {len(data)} item(s)")
        if data:
            print(f"{indent}- first item: {_json_summary(data[0])}")
    else:
        print(f"{indent}Value:")
        print(f"{indent}{repr(data)}")


def inspect_mat(path: Path, indent: str = "") -> None:
    try:
        from scipy.io import loadmat
    except ImportError as exc:
        raise SystemExit("MAT inspection requires scipy. Install optional dependencies with `pip install .[full]`.") from exc

    # Load top-level MATLAB variables and summarize each entry briefly.
    stat = path.stat()
    data = loadmat(path, squeeze_me=True, struct_as_record=False)
    keys = [key for key in data.keys() if not key.startswith("__")]

    print_basic_info(path, file_type="mat", size_bytes=stat.st_size, modified=stat.st_mtime, indent=indent)
    print(f"{indent}Top-level variables: {', '.join(keys) if keys else '(none)'}")
    print()

    print(f"{indent}Variable summary:")
    for key in keys:
        value = data[key]
        print(f"{indent}- {key}: {_mat_summary(value)}")
        preview = _mat_preview(value)
        if preview is not None:
            print(f"{indent}  preview: {preview}")


def print_basic_info(path: Path, file_type: str, size_bytes: int, modified: float, indent: str = "") -> None:
    print(f"{indent}Path: {path.resolve()}")
    print(f"{indent}Type: {file_type}")
    print(f"{indent}Size: {size_bytes:,} bytes")
    print(f"{indent}Modified: {pd.Timestamp(modified, unit='s').isoformat()}")


def indent_lines(text: str, indent: str) -> str:
    if not indent:
        return text
    return "\n".join(f"{indent}{line}" for line in text.splitlines())


def _json_summary(value: Any) -> str:
    if isinstance(value, dict):
        return f"object with {len(value)} key(s)"
    if isinstance(value, list):
        return f"array with {len(value)} item(s)"
    return type(value).__name__


def _mat_summary(value: Any) -> str:
    shape = getattr(value, "shape", None)
    if shape is not None:
        return f"{type(value).__name__}, shape={shape}"
    if isinstance(value, str):
        return "str"
    if isinstance(value, bytes):
        return f"bytes[{len(value)}]"
    return type(value).__name__


def _mat_preview(value: Any) -> str | None:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, (int, float, bool)):
        return repr(value)
    if hasattr(value, "shape") and getattr(value, "size", 0) <= 10:
        return repr(value.tolist() if hasattr(value, "tolist") else value)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
