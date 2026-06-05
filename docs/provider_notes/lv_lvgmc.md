# LVGMC Latvia

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Latvian Environment, Geology and Meteorology Centre / `lvgmc` daily provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `LV`
- provider: `lvgmc`
- resolution(s): `daily`

## Official source and identifiers

- source: Latvian Environment, Geology and Meteorology Centre (LVGMC)
- dataset: `Hidrometeorologiskie noverojumi`
- portal: `data.gov.lv`
- package id: `40d80be5-0c09-47c4-80f3-fad4bec19f33`
- license: `CC0-1.0`

Resource ids used by this implementation:

- stations: `c32c7afd-0d05-44fd-8b24-1de85b4bf11d`
- parameters: `38b462ac-08b9-4168-9d6e-cbaedc2e775d`
- meteorological archive hourly AVG/MIN/MAX/SUM: `ecc62e27-2071-483c-bca9-5e53d979faa8`

The factual archive resource `339f73e4-20cf-4cea-be65-dcfd4b3b742c` and the last-48-hours operational resource `17460efb-ae99-4d1d-8144-1068f184b05f` are not used by this daily provider path.

## Access method and archive scope

- access method: CKAN DataStore API
- metadata endpoint: `https://data.gov.lv/dati/api/action/datastore_search`
- observations endpoint: `https://data.gov.lv/dati/api/action/datastore_search_sql`
- WeatherDownload queries only the requested station, raw abbreviations, and bounded UTC interval
- the meteorological archive is a recent archive only and covers roughly the last 365 days
- this provider is not documented or exposed as a long-term historical climatology path

## Station identifiers and coordinates

- `station_id` is the official LVGMC `STATION_ID`
- WeatherDownload uses `GEOGR1` as WGS84 longitude and `GEOGR2` as WGS84 latitude
- only active stations are exposed on this path; in practice this means stations whose `END_DATE` remains on the open-ended `3999-12-31` style range

## Daily semantics

- `LV / lvgmc / daily` is a deterministic WeatherDownload daily product built from official recent LVGMC hourly archive records
- it is not an official precomputed daily climatological summary product
- the implementation uses the hourly AVG/MIN/MAX/SUM archive resource rather than downloading the full archive CSV

## UTC time-bucketing rule

LVGMC documents the hourly aggregated timestamp as the end of the represented hour. WeatherDownload therefore assigns each hourly record to:

- `hourly_period_date = (DATETIME - 1 hour).date()`

That means:

- `2026-01-02T00:00:00Z` belongs to `2026-01-01`
- `2026-01-01T00:00:00Z` belongs to `2025-12-31`

The CKAN query interval is therefore built as:

- `DATETIME > <date>T00:00:00Z`
- `DATETIME <= <date_plus_one>T00:00:00Z`

for the requested UTC day.

## Supported observed and aggregated elements

Raw-to-canonical mapping:

| Raw | Canonical | Source unit | WeatherDownload aggregation |
| --- | --- | --- | --- |
| `HTDRY` | `tas_mean` | `degC` | mean over non-null hourly values assigned to the UTC day |
| `HATMX` | `tas_max` | `degC` | max over non-null hourly values assigned to the UTC day |
| `HATMN` | `tas_min` | `degC` | min over non-null hourly values assigned to the UTC day |
| `HPRAB` | `precipitation` | `mm` | sum over non-null hourly values assigned to the UTC day |
| `HWNDS` | `wind_speed` | `m/s` | mean over non-null hourly values assigned to the UTC day |
| `HWSMX` | `wind_speed_max` | `m/s` | max over non-null hourly values assigned to the UTC day |
| `HRLH` | `relative_humidity` | `%` | mean over non-null hourly values assigned to the UTC day |
| `HPRSL` | `pressure` | `hPa` | mean over non-null hourly values assigned to the UTC day |
| `HSNOW` | `snow_depth` | `cm` | last non-null value assigned to the UTC day |

## Unsupported elements

- `vapour_pressure` is unsupported because this provider slice does not expose an observed vapour-pressure field
- `sunshine_duration` is unsupported because it is not exposed on this implemented slice
- `solar_radiation` is unsupported because it is not exposed on this implemented slice
- `wind_direction` is unsupported because this daily provider does not define a circular daily aggregation convention for it
- `cloud_cover` is not exposed because this implementation keeps the supported public LV slice conservative and does not publish it as a WeatherDownload element here

No provider-level derivation is added for any unsupported field above.

## FAO status

- `LV / lvgmc / daily` is not an observed FAO-ready provider path
- the shared workflow may still use the existing optional `--fill-missing allow-derived` mechanism where provider-independent derivation rules already apply
- this provider adds no LV-specific derived `vapour_pressure`

## Related references

- [Supported Capabilities](../supported_capabilities.md)
- [Provider Notes](README.md)
