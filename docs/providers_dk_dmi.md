# DMI Denmark Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Denmark slice implemented through the shared provider architecture.

## Scope In This Pass

Supported query shapes in the shared public API:

- `country="DK"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="DK"`, `dataset_scope="historical"`, `resolution="1hour"`
- `country="DK"`, `dataset_scope="historical"`, `resolution="10min"`

Out of scope in this pass:

- FAO computation
- ET0 computation
- derived meteorological variables
- Greenland and Faroe Islands differences unless explicitly implemented later

## Official Sources

WeatherDownload uses official DMI open-data APIs only.

Implemented paths in this pass:

- DMI Climate Data API station collection: `station`
- DMI Climate Data API observations collection: `stationValue`
- DMI Meteorological Observation API observations collection: `observation`

Related official DMI documentation that informed the conservative scope:

- DMI Climate Data documentation
- DMI Meteorological Observation API documentation

The public DK implementation now uses two official DMI observation paths behind the provider layer:

- Climate Data `stationValue` for `daily` and `1hour`
- Meteorological Observation API `observation` for `10min`

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

The daily downloader uses the Climate Data `stationValue` collection with `timeResolution=day`.

For the mapped Denmark daily parameters in this pass:

- DMI documents them as local-day Denmark values
- WeatherDownload keeps that provider-defined daily meaning behind the provider layer
- `observation_date` is normalized from the source interval start in `Europe/Copenhagen`
- WeatherDownload does not derive or recompute meteorological variables

## Hourly Observation Semantics

The hourly downloader uses the Climate Data `stationValue` collection with `timeResolution=hour`.

For the mapped Denmark hourly parameters in this pass:

- DMI documents hourly station values in UTC
- the source `from` and `to` fields define the exact interval covered by each value
- WeatherDownload preserves the source hourly interval meaning and normalizes `timestamp` from the published interval end `to` in UTC
- WeatherDownload does not derive or recompute meteorological variables

## 10-Minute Observation Semantics

The 10-minute downloader uses the Meteorological Observation API `observation` collection.

For the mapped Denmark 10-minute parameters in this pass:

- DMI documents the metObs path as raw observation data, not quality-controlled Climate Data
- the source `observed` timestamp is preserved as the normalized `timestamp` in UTC
- mapped fields keep the provider-defined 10-minute meanings published by DMI, such as latest 10-minute wind speed, precipitation in the latest 10 minutes, and sunshine in the latest 10 minutes
- WeatherDownload does not recompute hourly or daily values from the 10-minute path
- WeatherDownload does not derive meteorological variables

This pass is intentionally Denmark only. Greenland and Faroe Islands differences are out of scope until they are implemented and documented separately.

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

Current conservative 10-minute mapping:

- `tas_mean` -> `temp_dry`
- `precipitation` -> `precip_past10min`
- `wind_speed` -> `wind_speed`
- `relative_humidity` -> `humidity`
- `pressure` -> `pressure`
- `sunshine_duration` -> `sun_last10min_glob`

These are mapped directly from documented source properties only.

## Quality And Flags

Current handling is intentionally conservative:

- `daily` and `1hour` Climate Data paths preserve raw source-backed `qcStatus` and `validity` in `flag`
- `10min` metObs observations do not currently expose source QC/status fields in the implemented path, so `flag` remains null
- `quality` remains null in the normalized output for all Denmark paths
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

observations = download_observations(query)
```

There is no Denmark-specific public workflow shape. Denmark is exposed through the same shared provider interface as the other countries.
