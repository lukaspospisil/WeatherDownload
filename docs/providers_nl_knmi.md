# KNMI Netherlands Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload now includes a conservative KNMI slice for the Netherlands through the existing unified provider interface.

## Scope

Country and path:

- `country="NL"`
- `dataset_scope="historical"`
- `resolution="daily"`

## Official Source

WeatherDownload uses the official KNMI Open Data API only.

Source path used in this pass:

- KNMI Open Data API
- dataset: `daily-in-situ-meteorological-observations-validated`
- version: `1.0`

## Implemented Paths

Implemented public path in this pass:

- `country="NL"`, `dataset_scope="historical"`, `resolution="daily"`

This pass intentionally does not implement:

- hourly KNMI downloads
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

Station discovery for this slice is source-backed and uses official KNMI station metadata files retrieved through the Open Data API.

Normalized identifier choice:

- `station_id` = official KNMI station identifier from the station metadata CSV used by this provider
- `gh_id` remains null because KNMI does not expose a matching field in this path

## Supported Canonical Elements

The current conservative mapping is:

- `tas_mean` -> `TG`
- `tas_max` -> `TX`
- `tas_min` -> `TN`
- `precipitation` -> `RH`
- `sunshine_duration` -> `SQ`
- `wind_speed` -> `FG`
- `pressure` -> `PG`
- `relative_humidity` -> `UG`

Elements intentionally not exposed in this first slice are left behind the provider boundary until they are validated carefully.

## Daily Observation Semantics

KNMI documents daily file timestamps as the end of the daily UTC interval. WeatherDownload converts that end timestamp back to the represented `observation_date`.

## Quality And Flags

Current handling is intentionally conservative:

- the upstream dataset is already the validated daily dataset
- this pass does not normalize any extra KNMI quality semantics beyond that dataset choice
- normalized `quality` and `flag` therefore remain null unless future source-backed semantics are added

## Shared Interface Example

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

observations = download_observations(query)
```

## Known Limitations

- only `NL / historical / daily` is implemented
- KNMI Open Data API access requires `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY`
- hourly and EDR paths are intentionally out of scope in this pass
- `quality` and `flag` remain null because this pass does not normalize extra KNMI quality semantics beyond the validated dataset choice
- no FAO computation and no derived meteorological variables are added
