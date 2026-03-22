# Normalized Output Schemas

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload keeps its public outputs normalized across providers.

This page documents the stable public columns, not the provider-specific raw files behind them.

## General Principles

- outputs are pandas `DataFrame` objects
- `station_id` is normalized across countries
- `gh_id` is optional and nullable when a provider does not expose it
- canonical elements are exposed in `element`
- provider provenance is preserved in `element_raw`

## Station Metadata Schema

Returned by `read_station_metadata(...)`.

| Column | Meaning |
| --- | --- |
| `station_id` | Canonical public station identifier |
| `gh_id` | Optional secondary station identifier |
| `begin_date` | Normalized station-validity start |
| `end_date` | Normalized station-validity end |
| `full_name` | Provider station name |
| `longitude` | Longitude |
| `latitude` | Latitude |
| `elevation_m` | Elevation in meters |

Notes:

- `CZ`: `station_id` is CHMI `WSI`
- `DE`: `station_id` is zero-padded DWD `Stations_id`

## Observation Metadata Schema

Returned by `read_station_observation_metadata(...)`.

| Column | Meaning |
| --- | --- |
| `obs_type` | Provider observation type |
| `station_id` | Canonical public station identifier |
| `begin_date` | Validity start |
| `end_date` | Validity end |
| `element` | Provider raw element code in metadata tables |
| `schedule` | Provider schedule field, nullable |
| `name` | Optional provider name/label |
| `description` | Optional provider description |
| `height` | Optional provider measurement height |

This table describes availability metadata, not normalized downloaded observations.

## Daily Observation Schema

Returned by `download_observations(...)` for `resolution="daily"`.

| Column | Meaning |
| --- | --- |
| `station_id` | Canonical public station identifier |
| `gh_id` | Optional secondary identifier |
| `element` | Canonical meteorological element name |
| `element_raw` | Raw provider element code |
| `observation_date` | Daily date |
| `time_function` | Provider time-function field when available, nullable otherwise |
| `value` | Observation value |
| `flag` | Provider flag, nullable |
| `quality` | Provider quality indicator, nullable |
| `dataset_scope` | Dataset scope used in the query |
| `resolution` | `daily` |

Daily semantics:

- use `start_date` and `end_date`
- output uses `observation_date`
- `time_function` is provider-dependent

For example:

- CHMI daily exposes `time_function`
- DWD daily keeps `time_function` nullable

## Hourly And 10min Observation Schema

Returned by `download_observations(...)` for `resolution="1hour"` and `resolution="10min"`.

| Column | Meaning |
| --- | --- |
| `station_id` | Canonical public station identifier |
| `gh_id` | Optional secondary identifier |
| `element` | Canonical meteorological element name |
| `element_raw` | Raw provider element code |
| `timestamp` | Timezone-aware UTC timestamp |
| `value` | Observation value |
| `flag` | Provider flag, nullable |
| `quality` | Provider quality indicator, nullable |
| `dataset_scope` | Dataset scope used in the query |
| `resolution` | `1hour` or `10min` |

Subdaily semantics:

- use `start` and `end`
- output uses `timestamp`
- timestamps are normalized to UTC

## `element` vs `element_raw`

WeatherDownload preserves both user-facing meaning and source provenance:

| Column | Meaning |
| --- | --- |
| `element` | Canonical name such as `tas_mean` or `wind_speed` |
| `element_raw` | Provider raw code such as `T`, `TMK`, `TT_TU`, or `FF_10` |

This lets users work with stable cross-country names without losing the original source code.

## `observation_date` vs `timestamp`

Use this rule of thumb:

- `observation_date` means date-based data
- `timestamp` means timestamp-based data

Examples:

- daily data -> `observation_date`
- hourly data -> `timestamp`
- 10min data -> `timestamp`

## DWD Subdaily Timestamp Rule

For the currently implemented DWD `historical / 1hour` and `historical / 10min` paths:

- before `2000-01-01`, source timestamps are localized to `Europe/Berlin` and then converted to UTC
- from `2000-01-01` onward, source timestamps are treated as UTC directly

## Example

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["00044"],
    start="1999-12-31T22:00:00Z",
    end="2000-01-01T00:00:00Z",
    elements=["tas_mean", "wind_speed"],
)

hourly = download_observations(query)
```
