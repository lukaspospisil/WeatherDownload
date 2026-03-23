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
DEFAULT_COUNTRY = "CZ"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="weatherdownload",
        description="Work with country-aware weather data.",
    )
    subparsers = parser.add_subparsers(dest="resource")

    stations_parser = subparsers.add_parser("stations", help="Work with station metadata.")
    stations_subparsers = stations_parser.add_subparsers(dest="stations_command")
    metadata_parser = stations_subparsers.add_parser(
        "metadata",
        help="Read station metadata and print or export it.",
    )
    _add_country_argument(metadata_parser)
    metadata_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    metadata_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    metadata_parser.add_argument("--source-url", default=None, help="Optional provider-specific metadata URL override.")
    metadata_parser.set_defaults(handler=handle_station_metadata)

    availability_parser = stations_subparsers.add_parser(
        "availability",
        help="List implemented observation paths for a station.",
    )
    _add_country_argument(availability_parser)
    availability_parser.add_argument("--station-id", required=True, dest="station_id", help="Canonical station_id.")
    availability_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    availability_parser.add_argument("--include-elements", action="store_true", help="Include supported elements in the output.")
    availability_parser.add_argument("--include-mapping", action="store_true", help="Include canonical-to-raw element mapping in the output.")
    availability_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    availability_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    availability_parser.add_argument("--source-url", default=None, help="Optional provider-specific metadata URL override.")
    availability_parser.set_defaults(handler=handle_station_availability)

    supports_parser = stations_subparsers.add_parser(
        "supports",
        help="Check whether a station supports a dataset path.",
    )
    _add_country_argument(supports_parser)
    supports_parser.add_argument("--station-id", required=True, dest="station_id", help="Canonical station_id.")
    supports_parser.add_argument("--dataset-scope", required=True, dest="dataset_scope", help="Dataset scope to check.")
    supports_parser.add_argument("--resolution", required=True, help="Resolution to check.")
    supports_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    supports_parser.add_argument("--source-url", default=None, help="Optional provider-specific metadata URL override.")
    supports_parser.set_defaults(handler=handle_station_supports)

    elements_parser = stations_subparsers.add_parser(
        "elements",
        help="List supported elements for an implemented station path.",
    )
    _add_country_argument(elements_parser)
    elements_parser.add_argument("--station-id", required=True, dest="station_id", help="Canonical station_id.")
    elements_parser.add_argument("--dataset-scope", required=True, dest="dataset_scope", help="Dataset scope to inspect.")
    elements_parser.add_argument("--resolution", required=True, help="Resolution to inspect.")
    elements_parser.add_argument("--active-on", default=None, dest="active_on", help="Optional date filter in YYYY-MM-DD format.")
    elements_parser.add_argument("--include-mapping", action="store_true", help="Include canonical-to-raw element mapping in the output.")
    elements_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    elements_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    elements_parser.add_argument("--source-url", default=None, help="Optional provider-specific metadata URL override.")
    elements_parser.set_defaults(handler=handle_station_elements)

    observations_parser = subparsers.add_parser("observations", help="Work with observations.")
    observations_subparsers = observations_parser.add_subparsers(dest="observations_command")

    tenmin_parser = observations_subparsers.add_parser("10min", help="Download 10min observations.")
    _add_country_argument(tenmin_parser)
    tenmin_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="Canonical station_id. Can be provided multiple times.")
    tenmin_parser.add_argument("--element", action="append", required=True, dest="elements", help="Canonical or raw provider element code. Can be provided multiple times.")
    tenmin_parser.add_argument("--start", dest="start", help="Start datetime in ISO format.")
    tenmin_parser.add_argument("--end", dest="end", help="End datetime in ISO format.")
    tenmin_parser.add_argument("--all-history", action="store_true", dest="all_history", help="Download the full available history explicitly.")
    tenmin_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    tenmin_parser.add_argument("--layout", choices=["wide", "long"], default=None, help="Observation output layout. Defaults to wide for screen/csv/excel and long for parquet/mat.")
    tenmin_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    tenmin_parser.set_defaults(handler=handle_tenmin_observations)

    hourly_parser = observations_subparsers.add_parser("hourly", help="Download hourly observations.")
    _add_country_argument(hourly_parser)
    hourly_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="Canonical station_id. Can be provided multiple times.")
    hourly_parser.add_argument("--element", action="append", required=True, dest="elements", help="Canonical or raw provider element code. Can be provided multiple times.")
    hourly_parser.add_argument("--start", dest="start", help="Start datetime in ISO format.")
    hourly_parser.add_argument("--end", dest="end", help="End datetime in ISO format.")
    hourly_parser.add_argument("--all-history", action="store_true", dest="all_history", help="Download the full available history explicitly.")
    hourly_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    hourly_parser.add_argument("--layout", choices=["wide", "long"], default=None, help="Observation output layout. Defaults to wide for screen/csv/excel and long for parquet/mat.")
    hourly_parser.add_argument("--output", type=Path, help="Output file path. A bare filename is written under outputs/. Not used for 'screen'.")
    hourly_parser.set_defaults(handler=handle_hourly_observations)

    daily_parser = observations_subparsers.add_parser("daily", help="Download daily observations.")
    _add_country_argument(daily_parser)
    daily_parser.add_argument("--station-id", action="append", required=True, dest="station_ids", help="Canonical station_id. Can be provided multiple times.")
    daily_parser.add_argument("--element", action="append", required=True, dest="elements", help="Canonical or raw provider element code. Can be provided multiple times.")
    daily_parser.add_argument("--start-date", dest="start_date", help="Start date in YYYY-MM-DD format.")
    daily_parser.add_argument("--end-date", dest="end_date", help="End date in YYYY-MM-DD format.")
    daily_parser.add_argument("--all-history", action="store_true", dest="all_history", help="Download the full available history explicitly.")
    daily_parser.add_argument("--format", choices=OUTPUT_FORMATS, default="screen", help="Output format.")
    daily_parser.add_argument("--layout", choices=["wide", "long"], default=None, help="Observation output layout. Defaults to wide for screen/csv/excel and long for parquet/mat.")
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
            include_elements=args.include_elements or args.include_mapping,
            include_element_mapping=args.include_mapping,
            country=args.country,
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
        include_element_mapping=args.include_mapping,
        country=args.country,
    )
    if not args.include_elements and not args.include_mapping and "supported_elements" in availability.columns:
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
        country=args.country,
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
    if args.include_mapping:
        table = list_station_elements(
            stations,
            args.station_id,
            args.dataset_scope,
            args.resolution,
            active_on=args.active_on,
            country=args.country,
            include_mapping=True,
        )
    else:
        elements = list_station_elements(
            stations,
            args.station_id,
            args.dataset_scope,
            args.resolution,
            active_on=args.active_on,
            country=args.country,
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
    _validate_observation_mode(args, daily=False)
    query = ObservationQuery(
        country=args.country,
        dataset_scope=_default_dataset_scope(args.country),
        resolution="10min",
        station_ids=args.station_ids,
        start=args.start,
        end=args.end,
        all_history=args.all_history,
        elements=args.elements,
    )
    observations = _prepare_observation_output(
        download_observations(query, country=args.country),
        output_format=args.format,
        requested_layout=args.layout,
    )
    if args.format == "screen":
        print(_format_table(observations, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(observations, output_path=args.output, format=args.format)
    print(f"Exported 10min observations to {destination}")
    return 0


def handle_hourly_observations(args: argparse.Namespace) -> int:
    _validate_observation_mode(args, daily=False)
    query = ObservationQuery(
        country=args.country,
        dataset_scope=_default_dataset_scope(args.country),
        resolution="1hour",
        station_ids=args.station_ids,
        start=args.start,
        end=args.end,
        all_history=args.all_history,
        elements=args.elements,
    )
    observations = _prepare_observation_output(
        download_observations(query, country=args.country),
        output_format=args.format,
        requested_layout=args.layout,
    )
    if args.format == "screen":
        print(_format_table(observations, metadata_view=False))
        return 0
    if not args.output:
        raise SystemExit("Missing required --output for file export.")
    destination = export_table(observations, output_path=args.output, format=args.format)
    print(f"Exported hourly observations to {destination}")
    return 0


def handle_daily_observations(args: argparse.Namespace) -> int:
    _validate_observation_mode(args, daily=True)
    query = ObservationQuery(
        country=args.country,
        dataset_scope=_default_dataset_scope(args.country),
        resolution="daily",
        station_ids=args.station_ids,
        start_date=args.start_date,
        end_date=args.end_date,
        all_history=args.all_history,
        elements=args.elements,
    )
    observations = _prepare_observation_output(
        download_observations(query, country=args.country),
        output_format=args.format,
        requested_layout=args.layout,
    )
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


def _add_country_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--country",
        default=DEFAULT_COUNTRY,
        help="ISO 3166-1 alpha-2 country code. Defaults to CZ.",
    )


def _default_dataset_scope(country: str) -> str:
    return "historical" if country.strip().upper() == "DE" else "historical_csv"


def _read_stations_for_cli(args: argparse.Namespace) -> pd.DataFrame:
    return read_station_metadata(country=args.country, source_url=getattr(args, "source_url", None))


def _validate_observation_mode(args: argparse.Namespace, *, daily: bool) -> None:
    if daily:
        if args.all_history and (args.start_date is not None or args.end_date is not None):
            raise ValueError('--all-history cannot be used together with --start-date or --end-date.')
        if not args.all_history and (args.start_date is None or args.end_date is None):
            raise ValueError('Use either --all-history or both --start-date and --end-date.')
        return
    if args.all_history and (args.start is not None or args.end is not None):
        raise ValueError('--all-history cannot be used together with --start or --end.')
    if not args.all_history and (args.start is None or args.end is None):
        raise ValueError('Use either --all-history or both --start and --end.')


def _prepare_observation_output(
    observations: pd.DataFrame,
    *,
    output_format: str,
    requested_layout: str | None,
) -> pd.DataFrame:
    if observations.empty:
        return observations
    resolved_layout = requested_layout or _default_observation_layout(output_format)
    if resolved_layout == "long":
        return observations
    return _pivot_observations_wide(observations)


def _default_observation_layout(output_format: str) -> str:
    if output_format in {"screen", "csv", "excel"}:
        return "wide"
    return "long"


def _pivot_observations_wide(observations: pd.DataFrame) -> pd.DataFrame:
    if "observation_date" in observations.columns:
        row_key = "observation_date"
    elif "timestamp" in observations.columns:
        row_key = "timestamp"
    else:
        return observations

    index_columns = ["station_id"]
    if "gh_id" in observations.columns and observations["gh_id"].notna().any():
        index_columns.append("gh_id")
    index_columns.append(row_key)

    wide = observations.pivot_table(index=index_columns, columns="element", values="value", aggfunc="first").reset_index()
    wide.columns.name = None
    return wide


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
