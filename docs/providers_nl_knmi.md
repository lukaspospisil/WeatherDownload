# KNMI Netherlands Provider Notes

WeatherDownload now includes a conservative KNMI slice for the Netherlands through the existing unified provider interface.

## Implemented Scope

Country and path:

- `country="NL"`
- `dataset_scope="historical"`
- `resolution="daily"`

Source path used in this pass:

- KNMI Open Data API
- dataset: `daily-in-situ-meteorological-observations-validated`
- version: `1.0`

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

## Station Discovery

Station discovery for this slice is source-backed and uses official KNMI station metadata files retrieved through the Open Data API.

Normalized identifier choice:

- `station_id` = KNMI `WSI` / WIGOS station identifier
- `gh_id` remains null because KNMI does not expose a matching field in this path

## Canonical Elements In Scope

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

## Time And Quality Semantics

KNMI documents daily file timestamps as the end of the daily UTC interval. WeatherDownload converts that end timestamp back to the represented `observation_date`.

Current quality handling is intentionally conservative:

- the upstream dataset is already the validated daily dataset
- this pass does not normalize any extra KNMI quality semantics beyond that dataset choice
- normalized `quality` and `flag` therefore remain null unless future source-backed semantics are added
