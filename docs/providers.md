# Provider Model And Coverage

WeatherDownload uses a provider layer so the public API can stay stable while provider-specific logic remains internal.

## Country Selection

Use ISO 3166-1 alpha-2 country codes:

- `CZ`
- `DE`

Examples:

```python
from weatherdownload import read_station_metadata, list_supported_elements

cz_stations = read_station_metadata(country="CZ")
de_stations = read_station_metadata(country="DE")

de_daily_elements = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
)
```

```powershell
weatherdownload stations metadata --country CZ
weatherdownload stations metadata --country DE
```

## Stable Public Model

The same public API shape is used across countries:

- `read_station_metadata(country=...)`
- `read_station_observation_metadata(country=...)`
- `list_dataset_scopes(country=...)`
- `list_resolutions(country=..., dataset_scope=...)`
- `list_supported_elements(country=..., dataset_scope=..., resolution=...)`
- `download_observations(...)`

What the library normalizes across providers:

- canonical `station_id`
- canonical element names
- normalized observation columns
- DataFrame-first return values

What stays provider-specific internally:

- source URLs
- file layouts
- raw element codes
- quality/flag conventions
- timestamp parsing rules

## Canonical Station Identifier

| Country | Normalized `station_id` |
| --- | --- |
| `CZ` | CHMI `WSI` |
| `DE` | zero-padded DWD `Stations_id` |

`gh_id` remains an optional secondary field and is nullable when a provider does not expose an equivalent identifier.

## Implemented Support vs Discovery Support

WeatherDownload distinguishes between:

- discovery support: the provider registry knows that a dataset path exists
- implemented support: the library can actually download and normalize that path

This matters because discovery can be broader than the current downloader coverage.

## Coverage Overview

| Country | Dataset scope | Resolution | Status | Notes |
| --- | --- | --- | --- | --- |
| `CZ` | `historical_csv` | `daily` | Implemented | CHMI daily downloader |
| `CZ` | `historical_csv` | `1hour` | Implemented | CHMI hourly downloader |
| `CZ` | `historical_csv` | `10min` | Implemented | CHMI 10min downloader |
| `DE` | `historical` | `daily` | Implemented | DWD daily `kl` path |
| `DE` | `historical` | `1hour` | Implemented | Narrow slice |
| `DE` | `historical` | `10min` | Implemented | Narrow slice |

## Current Narrow Slices

The following implemented paths are intentionally conservative:

### DE `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

### DE `historical / 10min`

Supported canonical elements:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

## Query Semantics

Daily paths are date-based:

- use `start_date` and `end_date`
- output uses `observation_date`

Subdaily paths are timestamp-based:

- use `start` and `end`
- output uses `timestamp`

For DWD subdaily paths:

- before `2000-01-01`, source timestamps are localized to `Europe/Berlin` and then converted to UTC
- from `2000-01-01` onward, source timestamps are treated as UTC directly

## CLI Notes

The CLI mirrors the provider model:

- `--country` defaults to `CZ`
- the same command shape works across countries
- unsupported combinations fail with a clear error

Examples:

```powershell
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element wind_speed --start 1999-12-31T22:00:00Z --end 2000-01-01T00:00:00Z
```
