# WeatherDownload

WeatherDownload is a Python library for working with CHMI weather datasets.

Today it focuses on station metadata from the official CHMI `historical_csv` metadata feed, while keeping a clean shape for future observation downloads.

## Current capabilities

- read station metadata into a pandas `DataFrame`
- use CHMI WSI as the canonical public `station_id`
- filter station metadata in memory
- export tabular data to `csv`, `xlsx`, `parquet`, and `mat`
- discover supported CHMI query dimensions before building download requests
- validate CHMI observation queries against the broader CHMI dataset structure
- download the first implemented paths: `historical_csv` + `daily` and `historical_csv` + `1hour`
- keep a simple CLI for metadata listing and export

## Canonical Station Identifier

The library uses CHMI `WSI` as the single canonical public station identifier.

- `station_id`: canonical CHMI WSI used across metadata and observations
- `gh_id`: secondary metadata field kept for cross-reference with CHMI station metadata

Public examples should use `station_id`.

## Library API

```python
from weatherdownload import (
    ObservationQuery,
    download_observations,
    export_table,
    filter_stations,
    list_dataset_scopes,
    list_resolutions,
    list_station_elements,
    list_station_paths,
    list_supported_elements,
    read_station_metadata,
    station_supports,
)

stations = read_station_metadata()
selected = filter_stations(
    stations,
    station_ids=["0-20000-0-11406"],
    active_on="2024-01-01",
)
export_table(selected, "stations.csv", format="csv")  # writes to outputs/stations.csv

query = ObservationQuery(
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    elements=["TMA", "TMI"],
)
observations = download_observations(query, station_metadata=selected)
```

## Station Filtering And Availability

Station filtering stays DataFrame-first and works entirely in memory on the metadata table.

```python
from weatherdownload import (
    filter_stations,
    list_station_elements,
    list_station_paths,
    read_station_metadata,
    station_supports,
)

stations = read_station_metadata()

western_active = filter_stations(
    stations,
    name_contains="vary",
    gh_ids=["L3KVAL01"],
    bbox=(12.8, 50.1, 13.0, 50.3),
    active_on="1955-01-01",
)

paths = list_station_paths(stations, "0-20000-0-11406", include_elements=True)
daily_elements = list_station_elements(stations, "0-20000-0-11406", "historical_csv", "daily")
supports_hourly = station_supports(stations, "0-20000-0-11406", "historical_csv", "1hour")
```

Current availability helpers are CHMI-specific and intentionally conservative: they report only implemented observation paths from the CHMI registry and optionally apply station lifecycle filtering via `active_on`.

## Supported Query Dimensions

Supported `dataset_scope` values:

- `now`
- `recent`
- `historical`
- `historical_csv`

Supported `resolution` values depend on `dataset_scope` and can be discovered via `list_resolutions(...)`.

Current downloader implementation support is narrower than the full CHMI capability registry. At the moment, the library implements `historical_csv` + `daily` and `historical_csv` + `1hour`.

Supported query dimensions are defined by an explicit CHMI registry layer. The registry describes broader CHMI capabilities, while downloader implementation support is narrower. Supported `elements` can be discovered via `list_supported_elements(...)`.

## Daily Query Semantics

Daily observations are treated as date-based data.

- prefer `start_date` and `end_date` for `resolution="daily"`
- `start_date` and `end_date` are inclusive
- `start` and `end` cannot be used together with `start_date` and `end_date`
- for `resolution="daily"`, `start` and `end` are rejected to avoid misleading time-of-day precision

## Hourly Query Semantics

Hourly observations are treated as timestamp-based data.

- use `start` and `end` for `resolution="1hour"`
- `start_date` and `end_date` are not used for hourly data
- normalized hourly timestamps are parsed from CHMI `DT` and kept as timezone-aware UTC pandas timestamps

## Hourly Observations Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    dataset_scope="historical_csv",
    resolution="1hour",
    station_ids=["0-20000-0-11406"],
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["E"],
)

hourly = download_observations(query)
```

Normalized hourly output schema:

- `station_id`: canonical CHMI WSI identifier
- `gh_id`: secondary station identifier from metadata, nullable when metadata are not provided
- `element`: observed element code
- `timestamp`: timezone-aware UTC pandas timestamp parsed from CHMI `DT`
- `value`: numeric observation value, nullable
- `flag`: source flag value, nullable
- `quality`: numeric quality code, nullable
- `dataset_scope`: constant `historical_csv`
- `resolution`: constant `1hour`

## Daily Observations Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="1865-06-01",
    end_date="1865-06-10",
    elements=["TMA"],
)

daily = download_observations(query)
```

Normalized daily output schema rationale:

- daily data are date-based, so the normalized API exposes `observation_date`
- the raw CHMI `DT` source field remains internal for parsing, but is not exposed as a fake high-precision timestamp
- `time_function` is preserved because it still describes the CHMI daily measurement convention

Normalized daily output schema:

- `station_id`: canonical CHMI WSI identifier
- `gh_id`: secondary station identifier from metadata
- `element`: observed element code
- `observation_date`: normalized daily date
- `time_function`: source `TIMEFUNC` value from CHMI daily CSV
- `value`: numeric observation value, nullable
- `flag`: source flag value, nullable
- `quality`: numeric quality code, nullable
- `dataset_scope`: constant `historical_csv`
- `resolution`: constant `daily`

## CLI

```powershell
weatherdownload stations metadata --format screen
weatherdownload stations metadata --format csv --output stations.csv
weatherdownload stations metadata --format excel --output reports/stations.xlsx
weatherdownload stations metadata --format parquet --output D:/data/stations.parquet
weatherdownload stations metadata --format mat --output stations.mat
weatherdownload stations availability --station-id 0-20000-0-11406
weatherdownload stations availability --station-id 0-20000-0-11406 --include-elements --format csv --output station-paths.csv
weatherdownload stations supports --station-id 0-20000-0-11406 --dataset-scope historical_csv --resolution daily
weatherdownload stations elements --station-id 0-20000-0-11406 --dataset-scope historical_csv --resolution daily
weatherdownload observations daily --station-id 0-20000-0-11406 --element TMA --start-date 1865-06-01 --end-date 1865-06-10
weatherdownload observations daily --station-id 0-20000-0-11406 --element TMA --start-date 1865-06-01 --end-date 1865-06-10 --format csv --output daily.csv
weatherdownload observations hourly --station-id 0-20000-0-11406 --element E --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
```

If `--output` is just a filename such as `stations.csv` or `daily.csv`, the file is written under `outputs/` by default.

Explicit relative paths such as `reports/stations.xlsx` and absolute paths such as `D:/data/stations.parquet` are used as provided. Missing parent directories are created automatically.

## Installation

```powershell
pip install .
```

Optional export dependencies:

```powershell
pip install .[full]
```

## Architecture

- `weatherdownload.metadata`: CHMI station metadata loading and filtering
- `weatherdownload.chmi_registry`: explicit CHMI dataset registry and typed dataset specs
- `weatherdownload.availability`: CHMI-specific station availability helpers backed by metadata + registry
- `weatherdownload.discovery`: discovery helpers backed by the CHMI registry
- `weatherdownload.chmi_daily`: daily historical_csv path mapping, download, parse, and normalization helpers
- `weatherdownload.chmi_hourly`: hourly historical_csv path mapping, download, parse, and normalization helpers
- `weatherdownload.observations`: narrow public observation downloader entrypoint
- `weatherdownload.exporting`: generic DataFrame export helpers
- `weatherdownload.queries`: query model and validation
- `weatherdownload.cli`: thin CLI wrapper over the library API

## Planned next steps

- add more implemented downloader paths beyond `historical_csv` + `daily` and `historical_csv` + `1hour`
- connect discovery helpers to concrete CHMI download endpoints more systematically
- add richer station filtering such as geographic bounds and dataset availability

