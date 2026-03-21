from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from .exporting import export_table
from .metadata import read_station_metadata
from .observations import download_observations
from .queries import ObservationQuery


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="weatherdownload",
        description="Work with CHMI weather data.",
    )
    subparsers = parser.add_subparsers(dest="resource")

    stations_parser = subparsers.add_parser("stations", help="Work with station metadata.")
    stations_subparsers = stations_parser.add_subparsers(dest="stations_command")
    metadata_parser = stations_subparsers.add_parser(
        "metadata",
        help="Read station metadata and print or export it.",
    )
    metadata_parser.add_argument("--format", choices=["screen", "csv", "excel", "parquet", "mat"], default="screen", help="Output format.")
    metadata_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    metadata_parser.add_argument("--source-url", default=None, help="Optional alternative CHMI metadata CSV URL.")
    metadata_parser.set_defaults(handler=handle_station_metadata)

    observations_parser = subparsers.add_parser("observations", help="Work with CHMI observations.")
    observations_subparsers = observations_parser.add_subparsers(dest="observations_command")
    daily_parser = observations_subparsers.add_parser("daily", help="Download daily historical_csv observations.")
    daily_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="CHMI WSI station_id. Can be provided multiple times.")
    daily_parser.add_argument("--element", action="append", required=True, dest="elements", help="Element code. Can be provided multiple times.")
    daily_parser.add_argument("--start-date", required=True, dest="start_date", help="Start date in YYYY-MM-DD format.")
    daily_parser.add_argument("--end-date", required=True, dest="end_date", help="End date in YYYY-MM-DD format.")
    daily_parser.add_argument("--format", choices=["screen", "csv", "excel", "parquet", "mat"], default="screen", help="Output format.")
    daily_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    daily_parser.set_defaults(handler=handle_daily_observations)

    return parser


def handle_station_metadata(args: argparse.Namespace) -> int:
    stations = read_station_metadata(source_url=args.source_url) if args.source_url else read_station_metadata()
    if args.format == "screen":
        print(_format_table(stations, metadata_view=True))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(stations, output_path=args.output, format=args.format)
    print(f"Exported station metadata to {destination}")
    return 0


def handle_daily_observations(args: argparse.Namespace) -> int:
    query = ObservationQuery(
        dataset_scope="historical_csv",
        resolution="daily",
        station_ids=args.station_ids,
        start_date=args.start_date,
        end_date=args.end_date,
        elements=args.elements,
    )
    observations = download_observations(query)
    if args.format == "screen":
        print(_format_table(observations, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(observations, output_path=args.output, format=args.format)
    print(f"Exported daily observations to {destination}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 1
    try:
        return args.handler(args)
    except KeyboardInterrupt:
        print("Operation cancelled by user.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _format_table(table: pd.DataFrame, metadata_view: bool) -> str:
    if table.empty:
        return "No data found."
    if metadata_view:
        display_columns = ['station_id', 'gh_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'begin_date', 'end_date']
        renamed = table.loc[:, display_columns]
        return renamed.to_string(index=False)
    return table.to_string(index=False)


if __name__ == "__main__":
    raise SystemExit(main())
