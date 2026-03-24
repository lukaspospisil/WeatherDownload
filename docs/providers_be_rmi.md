# RMI/KMI Belgium Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Belgium slice implemented through the shared provider architecture.

## Scope In This Pass

Supported query shape:

- `country="BE"`
- `dataset_scope="historical"`
- `resolution="daily"`

Out of scope in this pass:

- 10-minute downloads
- hourly downloads
- FAO computation
- ET0 computation
- derived meteorological variables

## Official Source

WeatherDownload uses the official RMI/KMI open-data platform only.

Reference documentation:

- AWS documentation: `https://opendata.meteo.be/documentation/?dataset=aws`
- station metadata layer: `aws_station`
- daily observation layer: `aws_1day`

## Station Metadata

`station_id` is the official RMI/KMI AWS station code from the `aws_station` metadata layer, normalized as a string.

The current metadata path is source-backed and keeps only conservative fields:

- station code
- station name
- longitude / latitude
- altitude
- begin / end validity timestamps

`gh_id` remains null because this provider path does not expose an equivalent secondary identifier.

## Daily Observation Semantics

The official AWS documentation states that daily data are aggregated from 10-minute data.

For the implemented `aws_1day` path:

- daily values are official provider-side aggregates published by RMI/KMI
- WeatherDownload does not recompute daily aggregates from 10-minute data
- provider-defined day grouping remains behind the provider layer
- `observation_date` follows the source daily timestamp date

The documented daily grouping is:

- for day `D`, use 10-minute values from `00:10` on day `D` to `00:00` on day `D+1`

## Supported Canonical Elements

Current conservative mapping:

- `tas_mean` -> `temp_avg`
- `tas_max` -> `temp_max`
- `tas_min` -> `temp_min`
- `precipitation` -> `precip_quantity`
- `wind_speed` -> `wind_speed_10m`
- `relative_humidity` -> `humidity_rel_shelter_avg`
- `pressure` -> `pressure`
- `sunshine_duration` -> `sun_duration`

These are mapped directly from documented `aws_1day` properties only.

## Quality And Flags

The source exposes a `qc_flags` field on daily features.

Current handling is intentionally conservative:

- `flag` carries the raw `qc_flags` string when present
- `quality` remains null
- WeatherDownload does not reinterpret provider QC semantics in this pass

## Shared Interface Example

Use the normal shared interface:

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

observations = download_observations(query)
```

There is no Belgium-specific public workflow shape. Belgium is exposed through the same shared provider interface as the other countries.
