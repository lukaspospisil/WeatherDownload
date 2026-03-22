from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print basic information about a Parquet file.",
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the Parquet file to inspect.",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=10,
        help="Number of preview rows to print. Defaults to 10.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    path = args.path
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    if not path.is_file():
        raise SystemExit(f"Not a file: {path}")

    table = pd.read_parquet(path)
    stat = path.stat()

    print(f"Path: {path.resolve()}")
    print(f"Size: {stat.st_size:,} bytes")
    print(f"Modified: {pd.Timestamp(stat.st_mtime, unit='s').isoformat()}")
    print(f"Rows: {len(table):,}")
    print(f"Columns: {len(table.columns)}")
    print()

    print("Columns:")
    print(", ".join(table.columns.tolist()) if len(table.columns) else "(none)")
    print()

    print("Dtypes:")
    dtype_table = pd.DataFrame(
        {
            "column": table.columns,
            "dtype": [str(dtype) for dtype in table.dtypes],
        }
    )
    print(dtype_table.to_string(index=False))
    print()

    print("Missing values per column:")
    missing_table = pd.DataFrame(
        {
            "column": table.columns,
            "missing": table.isna().sum().tolist(),
        }
    )
    print(missing_table.to_string(index=False))
    print()

    memory_bytes = int(table.memory_usage(deep=True).sum())
    print(f"Estimated pandas memory usage: {memory_bytes:,} bytes")
    print()

    preview_rows = max(args.head, 0)
    print(f"Preview (first {preview_rows} rows):")
    if preview_rows == 0:
        print("(preview disabled)")
    elif table.empty:
        print("(no rows)")
    else:
        print(table.head(preview_rows).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
