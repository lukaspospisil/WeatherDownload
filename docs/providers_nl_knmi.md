# KNMI Netherlands Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload includes a conservative KNMI slice for the Netherlands through the existing unified provider interface.

## Scope

Country and paths:

- `country="NL"`
- `dataset_scope="historical"`
- `resolution="daily"`
- `resolution="1hour"`

## Official Source

WeatherDownload uses the official KNMI Open Data API only.

Source paths used in this pass:

- KNMI Open Data API
- dataset: `daily-in-situ-meteorological-observations-validated`
- dataset: `hourly-in-situ-meteorological-observations-validated`
- version: `1.0`

## Implemented Paths

Implemented public paths in this pass:

- `country="NL"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="NL"`, `dataset_scope="historical"`, `resolution="1hour"`

This pass intentionally does not implement:

- KNMI `10min` downloads
- KNMI EDR access paths
- FAO computation
- FAO-related or other derived meteorological variables such as ET0, extraterrestrial radiation, net radiation, psychrometric constant, or derived vapour pressure

## Authentication

KNMI Open Data API access requires an API key.

Supported environment variables:

- `WEATHERDOWNLOAD_KNMI_API_KEY`
- `KNMI_API_KEY`

Resolution order:

1. `WEATHERDOWNLOAD_KNMI_API_KEY`
2. `KNMI_API_KEY`

If neither variable is set, the NL provider fails early with a clear error message before attempting downloads.

## Station Metadata

Station discovery is source-backed and uses official KNMI station metadata files retrieved through the Open Data API.

Normalized identifier choice:

- `station_id` = official KNMI station identifier from the station metadata CSV used by this provider
- `gh_id` remains null because KNMI does not expose a matching field in this path

## Supported Canonical Elements

### `daily`

- `tas_mean` -> `TG`
- `tas_max` -> `TX`
- `tas_min` -> `TN`
- `precipitation` -> `RH`
- `sunshine_duration` -> `SQ`
- `wind_speed` -> `FG`
- `pressure` -> `PG`
- `relative_humidity` -> `UG`

### `1hour`

- `tas_mean` -> `T`
- `precipitation` -> `RH`
- `wind_speed` -> `FH`
- `relative_humidity` -> `U`
- `pressure` -> `P`
- `sunshine_duration` -> `SQ`

Elements not listed here stay behind the provider boundary until they are validated carefully.

## Time Semantics

Daily files:

- KNMI documents daily file timestamps as the end of the daily UTC interval
- WeatherDownload converts that end timestamp back to the represented `observation_date`

Hourly files:

- WeatherDownload preserves the published KNMI hourly file timestamp as the normalized UTC `timestamp`
- the KNMI hourly validated dataset mixes provider-defined hourly interval aggregates and end-of-interval sampled values depending on the element
- those source-defined meanings stay behind the provider layer and are not reinterpreted into a different public meteorological meaning

## Quality And Flags

Current handling is intentionally conservative:

- the upstream datasets are already the validated daily and validated hourly datasets
- this pass does not normalize extra KNMI quality semantics beyond that dataset choice
- normalized `quality` and raw `flag` therefore remain null unless future source-backed semantics are added

## Shared Interface Examples

Use the normal shared interface:

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

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="NL",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["0-20000-0-06260"],
    start="2024-01-01T01:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

hourly = download_observations(query)
```

## Known Limitations

- only `NL / historical / daily` and `NL / historical / 1hour` are implemented
- KNMI Open Data API access requires `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY`
- `10min` and EDR paths are intentionally out of scope in this pass
- `quality` and `flag` remain null because this pass does not normalize extra KNMI quality semantics beyond the validated dataset choice
- no FAO computation and no derived meteorological variables are added
