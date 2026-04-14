from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from weatherdownload import ObservationQuery


CLI_DEFAULT_DATASET_SCOPE = {
    "AT": "historical",
    "BE": "historical",
    "CH": "historical",
    "CZ": "historical_csv",
    "DE": "historical",
    "DK": "historical",
    "HU": "historical",
    "NL": "historical",
    "PL": "historical",
    "SE": "historical",
    "SK": "recent",
}

CLI_RESOLUTION_COMMAND = {
    "daily": "daily",
    "1hour": "hourly",
    "10min": "10min",
}


@dataclass(frozen=True)
class CliCommandInfo:
    supported: bool
    command: str | None
    reason: str | None = None


def build_python_snippet(query: ObservationQuery) -> str:
    lines = [
        "from weatherdownload import ObservationQuery, download_observations",
        "",
        "query = ObservationQuery(",
        f'    country="{query.country}",',
        f'    dataset_scope="{query.dataset_scope}",',
        f'    resolution="{query.resolution}",',
        f"    station_ids={repr(list(query.station_ids))},",
        f"    elements={repr(list(query.elements or []))},",
    ]
    if query.all_history:
        lines.append("    all_history=True,")
    elif query.resolution == "daily":
        lines.append(f'    start_date="{_format_value(query.start_date)}",')
        lines.append(f'    end_date="{_format_value(query.end_date)}",')
    else:
        lines.append(f'    start="{_format_value(query.start)}",')
        lines.append(f'    end="{_format_value(query.end)}",')
    lines.extend(
        [
            ")",
            "",
            "observations = download_observations(query)",
        ]
    )
    return "\n".join(lines)


def build_cli_command_info(query: ObservationQuery) -> CliCommandInfo:
    expected_scope = CLI_DEFAULT_DATASET_SCOPE.get(query.country)
    if expected_scope != query.dataset_scope:
        return CliCommandInfo(
            supported=False,
            command=None,
            reason=(
                "The current CLI only supports each country's default observation dataset path. "
                f"This query uses dataset_scope '{query.dataset_scope}', but the CLI-compatible path for {query.country} is '{expected_scope}'."
            ),
        )
    resolution_command = CLI_RESOLUTION_COMMAND.get(query.resolution)
    if resolution_command is None:
        return CliCommandInfo(
            supported=False,
            command=None,
            reason=f"The current CLI does not expose a command for resolution '{query.resolution}'.",
        )
    parts = ["weatherdownload", "observations", resolution_command, "--country", query.country]
    for station_id in query.station_ids:
        parts.extend(["--station-id", station_id])
    for element in query.elements or []:
        parts.extend(["--element", element])
    if query.all_history:
        parts.append("--all-history")
    elif query.resolution == "daily":
        parts.extend(["--start-date", _format_value(query.start_date), "--end-date", _format_value(query.end_date)])
    else:
        parts.extend(["--start", _format_value(query.start), "--end", _format_value(query.end)])
    return CliCommandInfo(supported=True, command=" ".join(parts))


def build_cli_command(query: ObservationQuery) -> str | None:
    return build_cli_command_info(query).command


def _format_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
