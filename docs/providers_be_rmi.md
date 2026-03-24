# RMI/KMI Belgium Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Belgium slice implemented through the shared provider architecture.

## Scope In This Pass

Supported query shapes:

- `country="BE"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="BE"`, `dataset_scope="historical"`, `resolution="10min"`

Out of scope in this pass:

- hourly downloads
- FAO computation
- ET0 computation
- derived meteorological variables
- recomputing daily or hourly aggregates from subdaily data

## Official Source

WeatherDownload uses the official RMI/KMI open-data platform only.

Reference documentation:

- AWS documentation: `https://opendata.meteo.be/documentation/?dataset=aws`
- station metadata layer: `aws_station`
- daily observation layer: `aws_1day`
- 10-minute observation layer: `aws_10min`

## Station Metadata

`station_id` is the official RMI/KMI AWS station code from the `aws_station` metadata layer, normalized as a string.

The current metadata path keeps only source-backed fields exposed by that layer:

- station code
- station name
- longitude / latitude from the published station geometry
- altitude
- begin / end validity timestamps

The provider does not infer missing metadata beyond those published fields.

`gh_id` remains null because this provider path does not expose an equivalent secondary identifier.

## Daily Observation Semantics

The official AWS documentation states that `aws_1day` daily data are aggregated from 10-minute data.

For the implemented `aws_1day` path:

- daily values are official provider-side aggregates published by RMI/KMI
- WeatherDownload does not recompute daily aggregates from 10-minute data
- provider-defined day grouping remains behind the provider layer
- `observation_date` follows the source daily timestamp date

The documented daily grouping window is:

- for day `D`, use 10-minute values from `00:10` on day `D` to `00:00` on day `D+1`

## 10-Minute Observation Semantics

For the implemented `aws_10min` path:

- values come directly from the official provider-published 10-minute layer
- WeatherDownload preserves the published `timestamp` from `aws_10min` and does not reinterpret it into a different meteorological meaning
- the source documentation describes most mapped fields as covering the last 10 minutes
- the source documentation describes `PRESSURE` specifically as a station-level last-minute average published on the same 10-minute path
- WeatherDownload does not recompute hourly or daily aggregates from these 10-minute values

## Supported Canonical Elements

Current conservative daily mapping:

- `tas_mean` -> `temp_avg`
- `tas_max` -> `temp_max`
- `tas_min` -> `temp_min`
- `precipitation` -> `precip_quantity`
- `wind_speed` -> `wind_speed_10m`
- `relative_humidity` -> `humidity_rel_shelter_avg`
- `pressure` -> `pressure`
- `sunshine_duration` -> `sun_duration`

Current conservative 10-minute mapping:

- `tas_mean` -> `temp_dry_shelter_avg`
- `precipitation` -> `precip_quantity`
- `wind_speed` -> `wind_speed_10m`
- `relative_humidity` -> `humidity_rel_shelter_avg`
- `pressure` -> `pressure`
- `sunshine_duration` -> `sun_duration`

These are mapped directly from documented source properties only.

## Quality And Flags

The source exposes a `qc_flags` field on daily and 10-minute features.

Current handling is intentionally conservative:

- `flag` carries the raw `qc_flags` source text when present
- `quality` remains null in the normalized output
- WeatherDownload does not reinterpret provider QC semantics in this pass

## Shared Interface Example

Use the normal shared interface:

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

observations = download_observations(query)
```

There is no Belgium-specific public workflow shape. Belgium is exposed through the same shared provider interface as the other countries.
