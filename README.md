# WeatherDownload

[![CI](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml/badge.svg)](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml)

<p align="right">
  <img src="docs/images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload is a DataFrame-first Python library for country-aware weather metadata, discovery, and observation downloads through a unified interface.

Current countries:

- `AT` via GeoSphere Austria
- `BE` via RMI/KMI open-data AWS platform
- `CZ` via CHMI
- `DE` via DWD
- `DK` via DMI open-data APIs
- `NL` via KNMI Data Platform
- `SK` via SHMU OpenDATA (experimental, limited to `recent / daily` station observations and minimal station discovery)

What stays stable across countries:

- the public API shape
- canonical `station_id`
- canonical meteorological element names
- normalized output schemas
- country selection with ISO 3166-1 alpha-2 codes such as `AT`, `BE`, `CZ`, `DE`, `DK`, `NL`, and `SK`

## Install

```powershell
pip install .
```

KNMI NetCDF support is included in the default install. NL usage also requires an API key in `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY`.

Optional export dependencies:

```powershell
pip install .[full]
```

## Quick Start

```python
from weatherdownload import ObservationQuery, download_observations, read_station_metadata

stations = read_station_metadata(country="CZ")

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "tas_max", "tas_min"],
)

daily = download_observations(query, station_metadata=stations)
```

The same API shape works for Belgium through the official RMI/KMI AWS daily, `1hour`, and `10min` layers:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="BE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["6414"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation", "sunshine_duration"],
)

daily = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="BE",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["6414"],
    start="2024-01-01T01:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

hourly = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="BE",
    dataset_scope="historical",
    resolution="10min",
    station_ids=["6414"],
    start="2024-01-01T00:10:00Z",
    end="2024-01-01T00:20:00Z",
    elements=["tas_mean", "pressure"],
)

tenmin = download_observations(query)
```

The same API shape also works for Denmark via the official DMI Climate Data `stationValue` collection for `daily` and `1hour`, and the official DMI Meteorological Observation API for `10min`:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DK",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["06180"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation", "sunshine_duration"],
)

daily = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DK",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["06180"],
    start="2024-01-01T01:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

hourly = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DK",
    dataset_scope="historical",
    resolution="10min",
    station_ids=["06180"],
    start="2024-01-01T00:10:00Z",
    end="2024-01-01T00:20:00Z",
    elements=["tas_mean", "pressure"],
)

tenmin = download_observations(query)
```

The same API shape works for Germany:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "precipitation"],
)

daily = download_observations(query)
```

The same API shape also works for Austria via the official GeoSphere Austria daily station dataset:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="AT",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["1"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation", "sunshine_duration"],
)

daily = download_observations(query)
```

The same API shape also works for the Netherlands via the official KNMI validated daily station dataset. Set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="NL",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["0-20000-0-06260"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation"],
)

daily = download_observations(query)
```

The experimental Slovakia slice currently focuses only on SHMU recent daily station observations and minimal station discovery derived from the same recent/daily payload.

Use explicit full-history mode only when you want the entire implemented station history:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["00044"],
    all_history=True,
    elements=["tas_mean"],
)

hourly = download_observations(query)
```

## CLI

```powershell
weatherdownload stations metadata --country AT --format screen
weatherdownload stations metadata --country BE --format screen
weatherdownload stations metadata --country CZ --format screen
weatherdownload stations metadata --country DE --format screen
weatherdownload stations metadata --country DK --format screen
weatherdownload stations metadata --country NL --format screen
weatherdownload stations metadata --country SK --format screen
weatherdownload observations daily --country AT --station-id 1 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country BE --station-id 6414 --element tas_mean --element precipitation --element sunshine_duration --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --element tas_min --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DK --station-id 06180 --element tas_mean --element precipitation --element sunshine_duration --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations hourly --country DK --station-id 06180 --element tas_mean --element pressure --start 2024-01-01T01:00:00Z --end 2024-01-01T02:00:00Z
weatherdownload observations 10min --country DK --station-id 06180 --element tas_mean --element pressure --start 2024-01-01T00:10:00Z --end 2024-01-01T00:20:00Z
weatherdownload observations daily --country NL --station-id 0-20000-0-06260 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country SK --station-id 11800 --element tas_max --element precipitation --start-date 2025-01-01 --end-date 2025-01-02
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --all-history
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element wind_speed --start 1999-12-31T22:00:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations hourly --country BE --station-id 6414 --element tas_mean --element pressure --start 2024-01-01T01:00:00Z --end 2024-01-01T02:00:00Z
weatherdownload observations 10min --country DE --station-id 00044 --element tas_mean --element relative_humidity --start 1999-12-31T22:50:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations 10min --country BE --station-id 6414 --element tas_mean --element pressure --start 2024-01-01T00:10:00Z --end 2024-01-01T00:20:00Z
weatherdownload observations 10min --country CZ --station-id 0-20000-0-11406 --element tas_mean --all-history
```

`--all-history` is explicit and mutually exclusive with `--start`/`--end` or `--start-date`/`--end-date`.

## Coverage Snapshot

| Country | Metadata | Discovery | Daily | 1hour | 10min |
| --- | --- | --- | --- | --- | --- |
| `AT` | Yes | Yes | Yes, narrow slice | No | No |
| `BE` | Yes | Yes | Yes, narrow slice | Yes, narrow slice | Yes, narrow slice |
| `CZ` | Yes | Yes | Yes | Yes | Yes |
| `DE` | Yes | Yes | Yes | Yes, narrow slice | Yes, narrow slice |
| `DK` | Yes | Yes | Yes, narrow slice | Yes, narrow slice | Yes, narrow slice |
| `NL` | Yes, API key required | Yes | Yes, narrow slice | No | No |
| `SK` | Experimental, probe-derived | Experimental | Yes, narrow slice | No | No |

Current intentionally narrow slices:

- `AT historical / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`
- `BE historical / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `BE historical / 1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `BE historical / 10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `DE historical / 1hour`: `tas_mean`, `relative_humidity`, `wind_speed`
- `DE historical / 10min`: `tas_mean`, `relative_humidity`, `wind_speed`
- `DK historical / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `DK historical / 1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `DK historical / 10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `NL historical / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`
- `SK recent / daily`: `tas_max`, `tas_min`, `sunshine_duration`, `precipitation`

DK scope limits for this pass:

- official DMI open-data APIs only
- Denmark only; Greenland and Faroe Islands differences are out of scope in this pass
- historical `daily` and `1hour` station observations use the DMI Climate Data `stationValue` collection
- station discovery uses the official DMI Climate Data `station` collection filtered to Denmark stations
- historical `10min` station observations use the DMI Meteorological Observation API `observation` collection and preserve the published `observed` timestamp in UTC
- WeatherDownload does not derive meteorological variables
- raw DMI `qcStatus` and `validity` are preserved in `flag` for Climate Data `daily` and `1hour`; `10min` `flag` stays null because the implemented metObs path does not expose QC/status fields
- no FAO computation and no derived meteorological variables

NL scope limits for this pass:

- KNMI Open Data API only
- historical daily validated station observations only
- hourly and EDR are out of scope
- no FAO computation and no FAO-related derived variables

BE scope limits for this pass:

- official RMI/KMI open-data platform only
- historical `daily`, `1hour`, and `10min` station observations only
- daily values are the official provider-side `aws_1day` aggregates from 10-minute data under the shared `resolution="daily"` path
- the documented daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- `1hour` values are the official provider-side `aws_1hour` aggregates from 10-minute data under the shared `resolution="1hour"` path
- the documented `1hour` grouping window is from `(H-1):10` to `H:00` for hour `H`
- 10-minute values come directly from the official `aws_10min` layer and WeatherDownload preserves the published timestamps
- source field windows stay provider-defined; 10-minute `pressure` is documented as a last-minute average and hourly `pressure` is the provider-side average of that field
- WeatherDownload does not recompute daily or hourly aggregates from Belgium 10-minute data
- raw `qc_flags` stay in `flag` as source text and normalized `quality` stays null
- no FAO computation and no derived meteorological variables

## Docs

- [Provider Model And Coverage](docs/providers.md)
- [GeoSphere Austria Provider Notes](docs/providers_at_geosphere.md)
- [RMI/KMI Belgium Provider Notes](docs/providers_be_rmi.md)
- [DMI Denmark Provider Notes](docs/providers_dk_dmi.md)
- [KNMI Netherlands Provider Notes](docs/providers_nl_knmi.md)
- [Canonical Elements](docs/canonical_elements.md)
- [Normalized Output Schemas](docs/output_schema.md)
- [Examples And Workflows](docs/examples.md)
- [MATLAB-Oriented FAO Workflow](docs/download_fao.md)
- [Experimental Slovakia Provider Notes](docs/providers_sk_experimental.md)
- [Changelog](docs/changelog.md)

## Public API Highlights

- `read_station_metadata(country=...)`
- `read_station_observation_metadata(country=...)`
- `list_dataset_scopes(country=...)`
- `list_resolutions(country=..., dataset_scope=...)`
- `list_supported_elements(country=..., dataset_scope=..., resolution=...)`
- `download_observations(...)`
- `filter_stations(...)`
- `station_availability(...)`
- `list_station_paths(...)`
- `list_station_elements(...)`

## Notes

- `station_id` is normalized across providers:
  - `AT`: GeoSphere Klima station id as string
  - `BE`: official RMI/KMI AWS station code from the `aws_station` metadata layer
  - `CZ`: CHMI `WSI`
  - `DE`: zero-padded DWD `Stations_id`
  - `DK`: official DMI `stationId` from the Climate Data `station` collection
  - `NL`: official KNMI station identifier from the station metadata CSV used by this provider
  - `SK`: SHMU `ind_kli` as string
- `gh_id` is optional and nullable when a provider does not expose an equivalent field
- outputs stay DataFrame-first
- provider-specific internals stay behind the provider layer
- `AT` support is currently limited to `historical / daily` via the official GeoSphere Austria `klima-v2-1d` station dataset
- `BE` support is currently limited to `historical / daily` via `aws_1day`, `historical / 1hour` via `aws_1hour`, and `historical / 10min` via `aws_10min`; daily and hourly values are provider-side aggregates, 10-minute values are preserved as published, and WeatherDownload does not recompute hourly or daily aggregates
- `DK` support is currently limited to Denmark `historical / daily` and `historical / 1hour` station observations via the DMI Climate Data `stationValue` path, `historical / 10min` observations via the DMI Meteorological Observation API `observation` path, and station discovery via the DMI Climate Data `station` collection; Greenland and Faroe Islands differences are intentionally out of scope for this pass
- `NL` support is currently limited to KNMI `historical / daily` validated station observations via the Open Data API; hourly and EDR are intentionally out of scope for this pass
- `SK` support is experimental, limited to `recent / daily`, and currently has incomplete probe-derived station metadata






