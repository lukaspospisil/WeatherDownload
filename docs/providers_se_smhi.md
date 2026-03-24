# SMHI Sweden Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page documents the current conservative Sweden slice implemented through the shared provider architecture.

## Scope In This Pass

Supported query shape in the shared public API:

- `country="SE"`, `dataset_scope="historical"`, `resolution="daily"`

Out of scope in this pass:

- hourly support
- `10min` support
- FAO computation
- ET0 computation
- derived meteorological variables

## Official Source

WeatherDownload uses the official SMHI Meteorological Observations open-data API only.

Implemented source model in this pass:

- parameter listing: `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}.json`
- station listing for each supported daily parameter comes from the official `station` array in that parameter payload
- historical daily observations come from the official corrected archive CSV path:
  `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv`

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

## Supported Canonical Elements

Current conservative daily mapping:

- `tas_mean` -> `2`
- `tas_max` -> `20`
- `tas_min` -> `19`
- `precipitation` -> `5`

These are mapped directly from documented SMHI daily parameters only.

## Quality And Flags

Current handling is intentionally conservative:

- raw SMHI `Kvalitet` codes are preserved in `flag`
- normalized `quality` remains null in the public output
- WeatherDownload does not reinterpret SMHI quality semantics in this pass

## Known Limitations

- only the four conservative daily parameters above are implemented
- other daily SMHI parameters are intentionally out of scope until they are added explicitly
- corrected-archive excludes the latest three months by source design
- there is no Sweden-specific public workflow shape; Sweden uses the same shared provider interface as the other countries

## Shared Interface Example

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
