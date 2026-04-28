# SMHI Sweden Provider Notes

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Sweden slice implemented through the shared provider architecture.

## Scope

Supported query shapes in the shared public API:

- `country="SE"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="SE"`, `dataset_scope="historical"`, `resolution="1hour"`

Out of scope in this pass:

- `10min` support
- FAO computation
- ET0 computation
- derived meteorological variables

## Official Source

WeatherDownload uses the official SMHI Meteorological Observations open-data API only.

Implemented source model in this pass:

- parameter listing: `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}.json`
- station listing for each supported parameter comes from the official `station` array in that parameter payload
- historical daily and hourly observations both use the official corrected-archive CSV path:
  `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv`

Why `10min` is not implemented:

- the official SMHI parameter catalog used by this provider clearly exposes daily and hourly historical paths
- the same catalog also exposes some 15-minute parameters such as precipitation amount `summa 15 min, 4 gĂĄnger/tim`
- some hourly parameters are explicitly documented as hourly outputs based on 10-minute sampling, for example wind speed `medelvĂ¤rde 10 min, 1 gĂĄng/tim`
- this is not the same as a true historical `resolution="10min"` source path
- WeatherDownload therefore does not implement Sweden `10min` by reinterpreting 15-minute or hourly data as 10-minute observations

The provider keeps SMHI's parameter / station / period model behind the provider layer.

## Station Metadata

`station_id` is the official SMHI station id from the parameter station listings used by this provider, normalized as a string.

The current metadata path keeps only source-backed fields exposed by those station listings:

- station id
- station name
- longitude / latitude
- height as `elevation_m`
- begin / end validity timestamps from the supported parameter station rows

`gh_id` remains null because this path does not expose an equivalent secondary identifier.

## Daily Observation Semantics

The daily downloader uses the official SMHI corrected-archive CSV path for each supported parameter and station.

Important source-backed semantics in this pass:

- WeatherDownload uses only `period="corrected-archive"` for the historical daily slice
- SMHI documents that corrected-archive excludes the latest three months while quality control is still in progress
- `observation_date` is taken from the published `Representativt dygn` column instead of being re-derived from the UTC interval bounds
- provider-defined interval windows differ by parameter and stay behind the provider layer
- WeatherDownload does not recompute or derive meteorological variables

## Hourly Observation Semantics

The hourly downloader also uses the official SMHI corrected-archive CSV path for each supported parameter and station.

Important source-backed semantics in this pass:

- WeatherDownload uses only `period="corrected-archive"` for the historical `1hour` slice
- SMHI documents that corrected-archive excludes the latest three months while quality control is still in progress
- `timestamp` is taken directly from the published `Datum` + `Tid (UTC)` columns in the hourly CSV
- WeatherDownload preserves that published UTC hour timestamp and does not reinterpret it into a different meteorological meaning
- provider-defined hourly field semantics stay behind the provider layer

## Supported Canonical Elements

Current conservative daily mapping:

- `tas_mean` -> `2`
- `tas_max` -> `20`
- `tas_min` -> `19`
- `precipitation` -> `5`

Current conservative hourly mapping:

- `tas_mean` -> `1`
- `wind_speed` -> `4`
- `relative_humidity` -> `6`
- `precipitation` -> `7`
- `pressure` -> `9`

These are mapped directly from documented SMHI parameters only.

## Quality And Flags

Current handling is intentionally conservative:

- raw SMHI `Kvalitet` codes are preserved in `flag`
- normalized `quality` remains null in the public output
- WeatherDownload does not reinterpret SMHI quality semantics in this pass

## Shared Interface Examples

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="SE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["98230"],
    start_date="1996-10-01",
    end_date="1996-10-03",
    elements=["tas_mean", "tas_max", "precipitation"],
)

observations = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="SE",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["98230"],
    start="2012-11-29T11:00:00Z",
    end="2012-11-29T13:00:00Z",
    elements=["tas_mean", "pressure"],
)

observations = download_observations(query)
```

## Known Limitations

- only the conservative `historical / daily` and `historical / 1hour` Sweden slices documented on this page are implemented
- Sweden `10min` is not implemented because the official SMHI path used by this provider does not verify a true historical 10-minute observation path with matching semantics
- official SMHI hourly outputs derived from 10-minute sampling and official 15-minute parameters are not treated as `resolution="10min"`
- corrected-archive excludes the latest three months by source design
- `quality` remains null and raw `Kvalitet` stays in `flag`
- no FAO computation and no derived meteorological variables are added
