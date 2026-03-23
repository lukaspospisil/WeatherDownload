# WeatherDownload

[![CI](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml/badge.svg)](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml)

<p align="right">
  <img src="docs/images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload is a DataFrame-first Python library for country-aware weather metadata, discovery, and observation downloads.

Current countries:

- `CZ` via CHMI
- `DE` via DWD

What stays stable across countries:

- the public API shape
- canonical `station_id`
- canonical meteorological element names
- normalized output schemas
- country selection with ISO 3166-1 alpha-2 codes such as `CZ` and `DE`

## Install

```powershell
pip install .
```

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
weatherdownload stations metadata --country CZ --format screen
weatherdownload stations metadata --country DE --format screen
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --element tas_min --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --all-history
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element wind_speed --start 1999-12-31T22:00:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --all-history
weatherdownload observations 10min --country DE --station-id 00044 --element tas_mean --element relative_humidity --start 1999-12-31T22:50:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations 10min --country CZ --station-id 0-20000-0-11406 --element tas_mean --all-history
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --layout long --format parquet --output daily-long.parquet
```

`--all-history` is explicit and mutually exclusive with `--start`/`--end` or `--start-date`/`--end-date`.

Observation output layout defaults:

- `screen`, `csv`, `excel`: `wide`
- `parquet`, `mat`: `long`
- use `--layout long` or `--layout wide` to override the default

## Canonical Elements

WeatherDownload is canonical-first:

- canonical names such as `tas_mean`, `tas_max`, `wind_speed`, and `precipitation` are the preferred public interface
- raw provider codes are still accepted for backward compatibility
- normalized observation outputs preserve both:
  - `element`: canonical name
  - `element_raw`: original provider code

## Coverage Snapshot

| Country | Metadata | Discovery | Daily | 1hour | 10min |
| --- | --- | --- | --- | --- | --- |
| `CZ` | Yes | Yes | Yes | Yes | Yes |
| `DE` | Yes | Yes | Yes | Yes, narrow slice | Yes, narrow slice |

Current intentionally narrow slices:

- `DE historical / 1hour`: `tas_mean`, `relative_humidity`, `wind_speed`
- `DE historical / 10min`: `tas_mean`, `relative_humidity`, `wind_speed`

## Docs

- [Provider Model And Coverage](docs/providers.md)
- [Canonical Elements](docs/canonical_elements.md)
- [Normalized Output Schemas](docs/output_schema.md)
- [Examples And Workflows](docs/examples.md)
- [MATLAB-Oriented FAO Workflow](docs/download_fao.md)

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
  - `CZ`: CHMI `WSI`
  - `DE`: zero-padded DWD `Stations_id`
- `gh_id` is optional and nullable when a provider does not expose an equivalent field
- outputs stay DataFrame-first
- provider-specific internals stay behind the provider layer
