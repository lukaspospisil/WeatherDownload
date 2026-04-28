# Normalized Output Schemas

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload keeps its public outputs normalized across providers and countries.

This page documents the stable public columns, not the provider-specific raw files behind them.

## Where To Start

- Station/provider coverage and supported country paths: [Provider Model And Coverage](providers.md)
- Shared example entry points: [Examples And Workflows](examples.md)
- FAO-oriented fixed-shape daily packaging workflow: [FAO-Oriented Daily Input Packaging Workflow](download_fao.md)
- Project overview and install/CLI quick start: [README](../README.md)

## General Principles

- outputs are pandas `DataFrame` objects
- `station_id` is the canonical public station identifier for the selected country/provider path
- `gh_id` is optional and nullable when a provider does not expose it
- canonical elements are exposed in `element`
- provider provenance is preserved in `element_raw`
- raw provider quality or status information is preserved in `flag` when the source exposes it
- normalized `quality` is only populated where WeatherDownload has an explicit provider-specific normalization; otherwise it remains null
- `provider` is the preferred public input name for choosing the concrete source within a country
- `dataset_scope` records the provider-specific dataset/product/source selected by the query; it is not a universal cross-country category
- output tables keep the `dataset_scope` column name for backward compatibility
- the public schema stays stable even when countries expose different subsets of canonical elements
- unavailable fields are not synthesized by default; they remain null/missing in fixed-shape downstream packaging workflows

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

- `station_id` keeps a stable public identifier while preserving provider-backed station identity
- `gh_id` is present only for provider paths that expose a reliable secondary identifier
- metadata coverage can differ by country; unsupported metadata fields remain null rather than inferred
- the stable public station metadata schema does not add a separate `country` column; provider country stays part of the query context and may also be encoded in the provider's station identifier

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
| `value` | Source-backed observation value |
| `flag` | Raw provider flag or status field, nullable |
| `quality` | Normalized quality indicator, nullable when no cross-provider normalization is defined |
| `dataset_scope` | Dataset scope used in the query |
| `resolution` | `daily` |

Daily semantics:

- use `start_date` and `end_date`
- output uses `observation_date`
- `time_function` is provider-dependent
- `dataset_scope` keeps the provider-local public token used in the query, such as `historical_csv`, `historical`, `historical_klimat`, `recent`, or `ghcnd`
- countries can expose different daily element coverage while keeping the same column contract

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
| `value` | Source-backed observation value |
| `flag` | Raw provider flag or status field, nullable |
| `quality` | Normalized quality indicator, nullable when no cross-provider normalization is defined |
| `dataset_scope` | Dataset scope used in the query |
| `resolution` | `1hour` or `10min` |

Subdaily semantics:

- use `start` and `end`
- output uses `timestamp`
- `dataset_scope` still keeps the provider-local scope name used in the query
- timestamps are normalized to UTC
- countries can expose different subdaily element coverage while keeping the same column contract

Subdaily missing-data semantics:

- a supported `1hour` or `10min` field is returned as one row per `station_id` + `timestamp` + `element`
- if a supported field has no usable value for a particular row, `value` is null for that row
- `flag` may still carry raw provider QC or status text for that same row when the source exposes it
- `quality` may still remain null for that same row when WeatherDownload does not define a normalized quality mapping
- if a country or provider slice does not support a requested subdaily canonical field at all, that field is unavailable for that slice rather than represented by synthetic rows

## `flag`, `quality`, and missing values

WeatherDownload separates source provenance from any library-level normalization:

- `flag` keeps the raw provider-side flag, QC text, or status code when one is available from the source
- `quality` is reserved for normalized quality values only where the library defines a clear provider-specific mapping
- if a provider does not expose a usable raw flag, `flag` remains null
- if the library does not define a normalized quality mapping for a provider path, `quality` remains null
- if a country or resolution does not support a canonical field, that field is simply absent from the returned observations selection; downstream fixed-shape bundles may represent it as null/missing

For `1hour` and `10min` outputs, distinguish these cases explicitly:

| Situation | Interpretation |
| --- | --- |
| `value` present, `flag` null, `quality` null | observed value with no raw provider flag and no normalized quality mapping |
| `value` present, `flag` present, `quality` null | observed value with raw provider-side QC/status preserved, but no normalized quality mapping |
| `value` null for a returned row | the field is supported for that provider path, but the value is missing for that specific `station_id` / `timestamp` / `element` row |
| no rows for a requested canonical field because the provider path does not support it | the field is unavailable for that country / dataset scope / resolution slice |

Subdaily provider variability is expected:

- some countries implement only `daily`
- some countries implement `daily` plus `1hour` but not `10min`
- some countries expose fewer canonical subdaily fields than others
- some providers expose raw QC/status fields and others do not
- the normalized subdaily schema stays stable while unsupported fields remain unavailable and row-level missing values remain null

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
