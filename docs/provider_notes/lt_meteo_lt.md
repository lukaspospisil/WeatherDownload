# Meteo.lt Lithuania

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Lithuanian Hydrometeorological Service (LHMT) / `meteo_lt` daily provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `LT`
- provider: `meteo_lt`
- resolution(s): `daily`

## Station identifiers

- `station_id` is the official Meteo.lt station code, for example `vilniaus-ams`

## Official source and access method

- base endpoint: `https://api.meteo.lt/v1/`
- station list: `GET /stations`
- station observations range: `GET /stations/{station-code}/observations`
- station daily observed slice: `GET /stations/{station-code}/observations/{YYYY-MM-DD}`
- access method: public official Meteo.lt JSON API from LHMT

## License and API limits

- unless a source page states otherwise, the public Meteo.lt data are published for reuse under `CC BY-SA 4.0`
- source attribution to LHMT is required
- documented limits: `180 requests/minute` and `20,000 requests/day` per IP

## Station scope

- station discovery is driven by `GET /stations`
- WeatherDownload stores the official station code, station name, coordinates, and the station-level observation range from `GET /stations/{station-code}/observations`
- the API exposes meteorological station observations, not official daily climatological summary products

## Daily semantics

- WeatherDownload `LT / meteo_lt / daily` is a deterministic daily product built from the observed Meteo.lt intra-day records returned for one UTC date
- it is not an official LHMT daily climatological summary
- missing hourly values stay missing and are ignored by the relevant daily aggregate
- if a whole UTC date has no stored observations, the API returns `404` and WeatherDownload treats that day as no data

## Supported observed and aggregated elements

Raw-to-canonical mapping:

| Raw | Canonical | Source unit | WeatherDownload aggregation |
| --- | --- | --- | --- |
| `airTemperature` | `tas_mean` | `degC` | mean over non-null UTC-date observations |
| `airTemperature` | `tas_max` | `degC` | max over non-null UTC-date observations |
| `airTemperature` | `tas_min` | `degC` | min over non-null UTC-date observations |
| `precipitation` | `precipitation` | `mm` | sum over non-null hourly amounts |
| `windSpeed` | `wind_speed` | `m/s` | mean over non-null UTC-date observations |
| `windGust` | `wind_speed_max` | `m/s` | max over non-null UTC-date observations |
| `relativeHumidity` | `relative_humidity` | `%` | mean over non-null UTC-date observations |
| `seaLevelPressure` | `pressure` | `hPa` | mean over non-null UTC-date observations |
| `snowDepth` | `snow_depth` | `cm` | last non-null UTC reading of the day |
| `cloudCover` | `cloud_cover` | `%` | mean over non-null UTC-date observations |

Notes:

- units are used directly as published by the API; WeatherDownload does not rescale them
- `pressure` is mean sea-level pressure because the raw field is `seaLevelPressure`
- `conditionCode` is intentionally not exposed as a canonical element
- `windDirection` is intentionally not exposed because there is no provider-level circular daily aggregation convention wired here

## Unsupported or intentionally not derived

- `vapour_pressure` is unsupported because this Meteo.lt endpoint does not publish an observed vapour-pressure field
- `sunshine_duration` is unsupported because this Meteo.lt endpoint does not publish it
- `solar_radiation` is unsupported because this Meteo.lt endpoint does not publish it
- WeatherDownload does not derive provider-level replacements for any of the unsupported fields above

## FAO status

- `LT / meteo_lt / daily` is not currently an observed FAO-ready provider path
- the shared workflow may still derive `vapour_pressure` only through the existing optional workflow-level `--fill-missing allow-derived` rule when that rule is generally applicable elsewhere
- this provider does not add any LT-specific derivation logic

## Related references

- [Supported Capabilities](../supported_capabilities.md)
- [Provider Notes](README.md)
