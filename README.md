# WeatherDownload

[![CI](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml/badge.svg)](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml)

WeatherDownload is a Python library for working with weather datasets through a country-aware provider layer.

Today it focuses on:

- `CZ` via CHMI
- `DE` via DWD metadata/discovery and the first `historical/daily` downloader path

The public API stays DataFrame-first and is designed to keep a stable shape across countries.

## Current capabilities

- read station metadata into a pandas `DataFrame`
- use a canonical public `station_id`
- filter station metadata in memory
- export tabular data to `csv`, `xlsx`, `parquet`, and `mat`
- discover supported dataset scopes, resolutions, and elements before building download requests
- validate observation queries against provider-specific capability registries
- download the first implemented paths: `CZ historical_csv` + `10min`, `CZ historical_csv` + `1hour`, `CZ historical_csv` + `daily`, and the first `DE historical` + `daily` path
- keep a simple CLI for metadata, discovery, and observation export

## Canonical Station Identifier

The library uses a normalized public `station_id` across providers.

- `CZ`: CHMI `WSI`
- `DE`: zero-padded DWD `Stations_id`

`gh_id` remains an optional secondary identifier and is nullable where a provider does not expose an equivalent field.

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

stations = read_station_metadata(country="CZ")
selected = filter_stations(
    stations,
    station_ids=["0-20000-0-11406"],
    active_on="2024-01-01",
)
export_table(selected, "stations.csv", format="csv")

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    elements=["tas_max", "tas_min"],
)
observations = download_observations(query, station_metadata=selected)
```

## Canonical Elements

WeatherDownload now exposes a canonical meteorological element vocabulary so users do not have to memorize provider-specific raw codes.

Why this exists:

- the same meteorological meaning should be requestable across countries
- users should not need to know CHMI and DWD code systems up front
- normalized outputs should still preserve source provenance

Preferred canonical names include:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

Provider-specific raw codes such as `TMA`, `SSV1H`, `TMK`, or `RSK` are still accepted for backward compatibility.

Normalized observation outputs preserve both identities:

- `element`: canonical meteorological element name
- `element_raw`: original provider-specific code

A fuller reference is available in [docs/canonical_elements.md](docs/canonical_elements.md).

### Daily Cross-Country Mapping

Current daily mapping for the implemented daily downloader paths:

| Canonical element | CZ raw | DE raw |
| --- | --- | --- |
| `tas_mean` | `T` | `TMK` |
| `tas_max` | `TMA` | `TXK` |
| `tas_min` | `TMI` | `TNK` |
| `wind_speed` | `F` | `FM` |
| `vapour_pressure` | `E` | `VPM` |
| `sunshine_duration` | `SSV` | `SDK` |
| `precipitation` | `SRA` | `RSK` |
| `pressure` | `P` | `PM` |
| `relative_humidity` | `RH` | `UPM` |

### Canonical Query Examples

The same canonical request style works across countries:

```python
from weatherdownload import ObservationQuery, download_observations

cz_query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "tas_max", "tas_min"],
)

cz_daily = download_observations(cz_query)
```

```python
from weatherdownload import ObservationQuery, download_observations

de_query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "precipitation"],
)

de_daily = download_observations(de_query)
```

Backward-compatible raw-code usage still works:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["TMK", "RSK"],
)

observations = download_observations(query)
```

## Discovery And Listing

Discovery helpers are canonical-first by default.

### `list_supported_elements(...)`

Default behavior returns canonical names:

```python
from weatherdownload import list_supported_elements

canonical = list_supported_elements(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
)
```

`provider_raw=True` returns raw provider codes:

```python
raw_codes = list_supported_elements(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    provider_raw=True,
)
```

`include_mapping=True` returns a mapping table:

```python
mapping = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    include_mapping=True,
)
```

Mapping columns:

- `element`: canonical name
- `element_raw`: preferred raw provider code
- `raw_elements`: all raw codes mapped to that canonical element for the selected path

### `list_station_elements(...)`

Default behavior returns canonical names for the selected station path:

```python
from weatherdownload import list_station_elements, read_station_metadata

stations = read_station_metadata(country="CZ")
elements = list_station_elements(
    stations,
    "0-20000-0-11406",
    "historical_csv",
    "daily",
)
```

`provider_raw=True` returns raw codes.

`include_mapping=True` returns a station-scoped mapping table with:

- `station_id`
- `dataset_scope`
- `resolution`
- `element`
- `element_raw`
- `raw_elements`

Station availability helpers follow the same idea:

- canonical names by default
- raw codes with `provider_raw=True`
- canonical-to-raw mapping with `include_element_mapping=True`

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

stations = read_station_metadata(country="CZ")

western_active = filter_stations(
    stations,
    name_contains="vary",
    gh_ids=["L3KVAL01"],
    bbox=(12.8, 50.1, 13.0, 50.3),
    active_on="1955-01-01",
)

paths = list_station_paths(stations, "0-20000-0-11406", include_elements=True)
daily_elements = list_station_elements(stations, "0-20000-0-11406", "historical_csv", "daily")
supports_daily = station_supports(stations, "0-20000-0-11406", "historical_csv", "daily")
```

## Supported Query Dimensions

Supported `dataset_scope` values include:

- `now`
- `recent`
- `historical`
- `historical_csv`

Supported `resolution` values depend on `dataset_scope` and can be discovered with `list_resolutions(...)`.

Current downloader implementation support is narrower than the full provider discovery surface. At the moment, the library implements `CZ historical_csv` + `10min`, `CZ historical_csv` + `1hour`, `CZ historical_csv` + `daily`, and the first `DE historical` + `daily` path.

## 10min Query Semantics

10min observations are timestamp-based.

- use `start` and `end` for `resolution="10min"`
- `start_date` and `end_date` are not used for 10min data
- normalized `timestamp` is parsed from the provider source and kept as a timezone-aware UTC pandas timestamp

## 10min Observations Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="10min",
    station_ids=["0-20000-0-11406"],
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T00:20:00Z",
    elements=["tas_mean", "soil_temperature_10cm"],
)

tenmin = download_observations(query)
```

Normalized 10min output schema:

- `station_id`: canonical public station identifier
- `gh_id`: secondary station identifier from metadata, nullable when metadata are not provided
- `element`: canonical meteorological element name
- `element_raw`: raw provider element code
- `timestamp`: timezone-aware UTC pandas timestamp
- `value`: numeric observation value, nullable
- `flag`: source flag value, nullable
- `quality`: numeric quality code, nullable
- `dataset_scope`: dataset scope used in the query
- `resolution`: constant `10min`

## DE Daily Query Semantics

For `country="DE"`, the first implemented DWD downloader path is `historical + daily` from the DWD daily `kl` archive files.

- use `start_date` and `end_date`
- normalized `station_id` is the zero-padded DWD station id
- normalized `observation_date` is parsed from DWD `MESS_DATUM`
- `time_function` is nullable because DWD daily files do not expose a CHMI-like `TIMEFUNC` field
- `gh_id` is nullable for DE

## Daily Query Semantics

Daily observations are date-based.

- prefer `start_date` and `end_date` for `resolution="daily"`
- `start_date` and `end_date` are inclusive
- `start` and `end` cannot be used together with `start_date` and `end_date`
- for `resolution="daily"`, `start` and `end` are rejected to avoid misleading time-of-day precision

## Hourly Query Semantics

Hourly observations are timestamp-based.

- use `start` and `end` for `resolution="1hour"`
- `start_date` and `end_date` are not used for hourly data
- normalized hourly timestamps are parsed from the provider source and kept as timezone-aware UTC pandas timestamps

## Hourly Observations Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="1hour",
    station_ids=["0-20000-0-11406"],
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["vapour_pressure", "pressure"],
)

hourly = download_observations(query)
```

Normalized hourly output schema:

- `station_id`: canonical public station identifier
- `gh_id`: secondary station identifier from metadata, nullable when metadata are not provided
- `element`: canonical meteorological element name
- `element_raw`: raw provider element code
- `timestamp`: timezone-aware UTC pandas timestamp
- `value`: numeric observation value, nullable
- `flag`: source flag value, nullable
- `quality`: numeric quality code, nullable
- `dataset_scope`: dataset scope used in the query
- `resolution`: constant `1hour`

## Daily Observations Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="1865-06-01",
    end_date="1865-06-10",
    elements=["tas_max"],
)

daily = download_observations(query)
```

Normalized daily output schema:

- `station_id`: canonical public station identifier
- `gh_id`: secondary station identifier from metadata
- `element`: canonical meteorological element name
- `element_raw`: raw provider element code
- `observation_date`: normalized daily date
- `time_function`: source time-function field when the provider exposes one, nullable otherwise
- `value`: numeric observation value, nullable
- `flag`: source flag value, nullable
- `quality`: numeric quality code, nullable
- `dataset_scope`: dataset scope used in the query
- `resolution`: constant `daily`

## CLI

The CLI is also canonical-first.

- `--element` accepts canonical names or raw provider codes
- `stations elements --include-mapping` shows canonical-to-raw mapping output
- `stations availability --include-mapping` includes per-path element mapping information

```powershell
weatherdownload stations metadata --country CZ --format screen
weatherdownload stations metadata --country DE --format screen
weatherdownload stations elements --country CZ --station-id 0-20000-0-11406 --dataset-scope historical_csv --resolution daily --include-mapping
weatherdownload stations availability --country DE --station-id 00044 --include-mapping
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --element tas_min --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element TMK --element RSK --start-date 2024-01-01 --end-date 2024-01-10
```

If `--output` is just a filename such as `stations.csv`, `tenmin.csv`, or `daily.csv`, the file is written under `outputs/` by default.

Explicit relative paths such as `reports/stations.xlsx` and absolute paths such as `D:/data/stations.parquet` are used as provided. Missing parent directories are created automatically.

## Examples

Minimal runnable example scripts are available under `examples/`.

- `examples/read_metadata.py`: load station metadata, apply a simple active-date filter, and print a small station table
- `examples/download_daily.py`: run a minimal daily query with canonical elements
- `examples/download_hourly.py`: run a minimal hourly query with canonical elements
- `examples/download_tenmin.py`: run a minimal 10min query with canonical elements
- `examples/station_availability.py`: inspect implemented station paths, canonical elements, and support checks before downloading
- `examples/download_fao.py`: prepare a CHMI daily dataset bundle with cache-aware download/build modes plus MAT or Parquet export for later FAO-related processing [more info](docs/download_fao.md)

Run them with:

```powershell
python examples/read_metadata.py
python examples/download_daily.py
python examples/download_hourly.py
python examples/download_tenmin.py
python examples/station_availability.py
python examples/download_fao.py --mode full --cache-dir outputs/fao_cache --export-format both --output outputs/fao_daily.mat --output-dir outputs/fao_daily_bundle
```

## Installation

```powershell
pip install .
```

Optional export dependencies:

```powershell
pip install .[full]
```

## Architecture

- `weatherdownload.metadata`: station metadata loading and filtering
- `weatherdownload.chmi_registry`: explicit CHMI dataset registry and typed dataset specs
- `weatherdownload.dwd_registry`: explicit DWD dataset registry and typed dataset specs
- `weatherdownload.elements`: canonical element vocabulary and raw-code mapping helpers
- `weatherdownload.availability`: provider-aware station availability helpers backed by metadata + registry
- `weatherdownload.discovery`: provider-aware discovery helpers backed by provider registries
- `weatherdownload.chmi_tenmin`: CZ 10min historical_csv path mapping, download, parse, and normalization helpers
- `weatherdownload.chmi_daily`: CZ daily historical_csv path mapping, download, parse, and normalization helpers
- `weatherdownload.chmi_hourly`: CZ hourly historical_csv path mapping, download, parse, and normalization helpers
- `weatherdownload.dwd_daily`: first DE daily historical path download, parse, and normalization helpers
- `weatherdownload.observations`: public observation downloader entrypoint
- `weatherdownload.exporting`: generic DataFrame export helpers
- `weatherdownload.queries`: query model and validation
- `weatherdownload.cli`: thin CLI wrapper over the library API

## Planned Next Steps

- broaden implemented CHMI and DWD downloader coverage
- improve packaging and release readiness
- extend the canonical element layer across additional provider paths

## MATLAB-Oriented Workflow

`examples/download_fao.py` builds a clean CHMI daily meteorological dataset for later MATLAB, R, or Python processing. It supports cache-aware `full`, `download`, and `build` modes, reuses cached `meta1`, `meta2`, and daily CSV inputs under `outputs/fao_cache` by default, applies fixed `TIMEFUNC` selection, keeps only complete E-based days, filters to stations with at least 3650 complete days by default, and can export either a MATLAB-oriented `.mat` bundle, a portable Parquet bundle directory, or both. The Parquet bundle contains `data_info.json`, `stations.parquet`, and a long-form `series.parquet`.

The example does not compute FAO, extraterrestrial radiation `Ra`, or any other derived variables.
