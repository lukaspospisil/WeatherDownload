from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from .availability import list_station_elements, list_station_paths, station_availability, station_supports
from .exporting import export_table
from .metadata import read_station_metadata
from .observations import download_observations
from .queries import ObservationQuery


OUTPUT_FORMATS = ["screen", "csv", "excel", "parquet", "mat"]


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
    metadata_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    metadata_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    metadata_parser.add_argument("--source-url", default=None, help="Optional alternative CHMI metadata CSV URL.")
    metadata_parser.set_defaults(handler=handle_station_metadata)

    availability_parser = stations_subparsers.add_parser(
        "availability",
        help="List implemented observation paths for a station.",
    )
    availability_parser.add_argument("--station-id", required=True, dest="station_id", help="CHMI WSI station_id.")
    availability_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    availability_parser.add_argument("--include-elements", action="store_true", help="Include supported elements in the output.")
    availability_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    availability_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    availability_parser.add_argument("--source-url", default=None, help="Optional alternative CHMI metadata CSV URL.")
    availability_parser.set_defaults(handler=handle_station_availability)

    supports_parser = stations_subparsers.add_parser(
        "supports",
        help="Check whether a station supports a dataset path.",
    )
    supports_parser.add_argument("--station-id", required=True, dest="station_id", help="CHMI WSI station_id.")
    supports_parser.add_argument("--dataset-scope", required=True, dest="dataset_scope", help="Dataset scope to check.")
    supports_parser.add_argument("--resolution", required=True, help="Resolution to check.")
    supports_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    supports_parser.add_argument("--source-url", default=None, help="Optional alternative CHMI metadata CSV URL.")
    supports_parser.set_defaults(handler=handle_station_supports)

    elements_parser = stations_subparsers.add_parser(
        "elements",
        help="List supported elements for an implemented station path.",
    )
    elements_parser.add_argument("--station-id", required=True, dest="station_id", help="CHMI WSI station_id.")
    elements_parser.add_argument("--dataset-scope", required=True, dest="dataset_scope", help="Dataset scope to inspect.")
    elements_parser.add_argument("--resolution", required=True, help="Resolution to inspect.")
    elements_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    elements_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    elements_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    elements_parser.add_argument("--source-url", default=None, help="Optional alternative CHMI metadata CSV URL.")
    elements_parser.set_defaults(handler=handle_station_elements)

    observations_parser = subparsers.add_parser("observations", help="Work with CHMI observations.")
    observations_subparsers = observations_parser.add_subparsers(dest="observations_command")

    tenmin_parser = observations_subparsers.add_parser("10min", help="Download narrow 10min historical_csv observations.")
    tenmin_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="CHMI WSI station_id. Can be provided multiple times.")
    tenmin_parser.add_argument("--element", action="append", required=True, dest="elements", help="Element code. Can be provided multiple times.")
    tenmin_parser.add_argument("--start", required=True, dest="start", help="Start datetime in ISO format.")
    tenmin_parser.add_argument("--end", required=True, dest="end", help="End datetime in ISO format.")
    tenmin_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    tenmin_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    tenmin_parser.set_defaults(handler=handle_tenmin_observations)

    daily_parser = observations_subparsers.add_parser("daily", help="Download daily historical_csv observations.")
    daily_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="CHMI WSI station_id. Can be provided multiple times.")
    daily_parser.add_argument("--element", action="append", required=True, dest="elements", help="Element code. Can be provided multiple times.")
    daily_parser.add_argument("--start-date", required=True, dest="start_date", help="Start date in YYYY-MM-DD format.")
    daily_parser.add_argument("--end-date", required=True, dest="end_date", help="End date in YYYY-MM-DD format.")
    daily_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    daily_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    daily_parser.set_defaults(handler=handle_daily_observations)

    return parser


def handle_station_metadata(args: argparse.Namespace) -> int:
    stations = _read_stations_for_cli(args)
    if args.format == "screen":
        print(_format_table(stations, metadata_view=True))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(stations, output_path=args.output, format=args.format)
    print(f"Exported station metadata to {destination}")
    return 0


def handle_station_availability(args: argparse.Namespace) -> int:
    stations = _read_stations_for_cli(args)
    if args.format == "screen":
        paths = list_station_paths(
            stations,
            args.station_id,
            active_on=args.active_on,
            include_elements=args.include_elements,
        )
        print(_format_table(paths, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    availability = station_availability(
        stations,
        station_ids=[args.station_id],
        active_on=args.active_on,
        implemented_only=True,
    )
    if not args.include_elements and "supported_elements" in availability.columns:
        availability = availability.drop(columns=["supported_elements"])
    destination = export_table(availability, output_path=args.output, format=args.format)
    print(f"Exported station availability to {destination}")
    return 0


def handle_station_supports(args: argparse.Namespace) -> int:
    stations = _read_stations_for_cli(args)
    supported = station_supports(
        stations,
        args.station_id,
        args.dataset_scope,
        args.resolution,
        active_on=args.active_on,
    )
    result = pd.DataFrame([
        {
            "station_id": args.station_id,
            "dataset_scope": args.dataset_scope,
            "resolution": args.resolution,
            "active_on": args.active_on,
            "supported": supported,
        }
    ])
    print(_format_table(result, metadata_view=False))
    return 0


def handle_station_elements(args: argparse.Namespace) -> int:
    stations = _read_stations_for_cli(args)
    elements = list_station_elements(
        stations,
        args.station_id,
        args.dataset_scope,
        args.resolution,
        active_on=args.active_on,
    )
    table = pd.DataFrame(
        [
            {
                "station_id": args.station_id,
                "dataset_scope": args.dataset_scope,
                "resolution": args.resolution,
                "element": element,
            }
            for element in elements
        ],
        columns=["station_id", "dataset_scope", "resolution", "element"],
    )
    if args.format == "screen":
        print(_format_table(table, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(table, output_path=args.output, format=args.format)
    print(f"Exported station elements to {destination}")
    return 0


def handle_tenmin_observations(args: argparse.Namespace) -> int:
    query = ObservationQuery(
        dataset_scope="historical_csv",
        resolution="10min",
        station_ids=args.station_ids,
        start=args.start,
        end=args.end,
        elements=args.elements,
    )
    observations = download_observations(query)
    if args.format == "screen":
        print(_format_table(observations, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(observations, output_path=args.output, format=args.format)
    print(f"Exported 10min observations to {destination}")
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


def _read_stations_for_cli(args: argparse.Namespace) -> pd.DataFrame:
    return read_station_metadata(source_url=args.source_url) if getattr(args, "source_url", None) else read_station_metadata()


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
