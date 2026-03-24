# DMI Denmark Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Denmark slice implemented through the shared provider architecture.

## Scope In This Pass

Supported query shapes in the shared public API:

- `country="DK"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="DK"`, `dataset_scope="historical"`, `resolution="1hour"`

Out of scope in this pass:

- `10min` support
- FAO computation
- ET0 computation
- derived meteorological variables
- Greenland and Faroe Islands differences unless explicitly implemented later

## Official Sources

WeatherDownload uses official DMI open-data APIs only.

Implemented paths in this pass:

- DMI Climate Data API station collection: `station`
- DMI Climate Data API observations collection: `stationValue`

Related official DMI documentation that informed the conservative scope:

- DMI Climate Data documentation
- DMI Meteorological Observation API documentation

The implemented downloader path in this pass uses Climate Data only. Meteorological Observation API paths are not part of the public DK implementation yet.

## Station Metadata

`station_id` is the official DMI `stationId` from the Climate Data `station` collection, normalized as a string.

The current metadata path keeps only source-backed fields exposed by that collection:

- station id
- station name
- longitude / latitude from the published station geometry
- station height as `elevation_m`
- begin / end validity timestamps

The provider filters to Denmark stations using the source-backed `country = DNK` field.

`gh_id` remains null because this path does not expose an equivalent secondary identifier.

## Daily Observation Semantics

The current daily downloader uses the Climate Data `stationValue` collection with `timeResolution=day`.

For the mapped Denmark daily parameters in this pass:

- DMI documents them as local-day Denmark values
- WeatherDownload keeps that provider-defined daily meaning behind the provider layer
- `observation_date` is normalized from the source interval start in `Europe/Copenhagen`
- WeatherDownload does not derive or recompute meteorological variables

## Hourly Observation Semantics

The current hourly downloader uses the Climate Data `stationValue` collection with `timeResolution=hour`.

For the mapped Denmark hourly parameters in this pass:

- DMI documents hourly station values in UTC
- the source `from` and `to` fields define the exact interval covered by each value
- WeatherDownload preserves the source hourly interval meaning and normalizes `timestamp` from the published interval end `to` in UTC
- WeatherDownload does not derive or recompute meteorological variables

This pass is intentionally Denmark only. Greenland and Faroe Islands daily and hourly differences are out of scope until they are implemented and documented separately.

## Supported Canonical Elements

Current conservative daily mapping:

- `tas_mean` -> `mean_temp`
- `tas_max` -> `mean_daily_max_temp`
- `tas_min` -> `mean_daily_min_temp`
- `precipitation` -> `acc_precip`
- `wind_speed` -> `mean_wind_speed`
- `relative_humidity` -> `mean_relative_hum`
- `pressure` -> `mean_pressure`
- `sunshine_duration` -> `bright_sunshine`

Current conservative hourly mapping:

- `tas_mean` -> `mean_temp`
- `precipitation` -> `acc_precip`
- `wind_speed` -> `mean_wind_speed`
- `relative_humidity` -> `mean_relative_hum`
- `pressure` -> `mean_pressure`
- `sunshine_duration` -> `bright_sunshine`

These are mapped directly from documented source properties only.

## Quality And Flags

Current handling is intentionally conservative:

- `flag` carries raw source-backed `qcStatus` and `validity` as compact JSON text
- `quality` remains null in the normalized output
- WeatherDownload does not reinterpret DMI QC semantics in this pass

## Shared Interface Examples

Use the normal shared interface:

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

observations = download_observations(query)
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

observations = download_observations(query)
```

There is no Denmark-specific public workflow shape. Denmark is exposed through the same shared provider interface as the other countries.
