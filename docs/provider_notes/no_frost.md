# MET Norway Frost

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current official MET Norway Frost provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `NO`
- provider: `frost`
- resolution(s): `daily`

## Station identifiers

- `station_id` is the Frost station/source id such as `SN18700`
- WeatherDownload normalizes Frost source ids like `SN18700:0` back to the base station id `SN18700`

## Source

- station metadata: `https://frost.met.no/sources/v0.jsonld`
- observations: `https://frost.met.no/observations/v0.jsonld`
- authentication: HTTP basic auth with the Frost client id as username and an empty password

## Supported observed daily elements

Raw-to-canonical mapping:

| Raw | Canonical |
| --- | --- |
| `mean(air_temperature P1D)` | `tas_mean` |
| `max(air_temperature P1D)` | `tas_max` |
| `min(air_temperature P1D)` | `tas_min` |
| `sum(precipitation_amount P1D)` | `precipitation` |
| `mean(wind_speed P1D)` | `wind_speed` |
| `surface_snow_thickness` | `snow_depth` |

Units and conversions:

- `degC` -> canonical degrees Celsius for `tas_mean`, `tas_max`, and `tas_min`
- `mm` -> canonical millimetres for `precipitation`
- `m/s` -> canonical metres per second for `wind_speed`
- `cm` -> canonical millimetres for `snow_depth` via `value * 10.0`; Frost documents coded `surface_snow_thickness` states in centimetres, for example `0` meaning snow depth less than `0.5 cm`

Observed-only notes:

- Frost documents daily `sum(precipitation_amount P1D)` value `-1` as `No precipitation`, with product-series converted value `0.0 mm`; this is a coded no-precipitation state, not a documented trace-precipitation code, so WeatherDownload normalizes it to canonical observed `0.0 mm` instead of leaving the value missing
- Frost coded snow-depth values such as `-3`, `-1`, and `0` are treated as coded source states rather than measured depths; WeatherDownload preserves the coded meaning in `flag` and leaves canonical `value` missing for those rows
- daily dates come from Frost `referenceTime`; `timeOffset` is preserved in `flag` metadata but does not shift the published observation date

## Not implemented (yet)

- `relative_humidity`, `pressure`, `sunshine_duration`, and `solar_radiation`, because this first pass keeps only mappings whose daily observed semantics and units were confirmed conservatively
- `vapour_pressure`
- any derived variables
- any FAO-specific shortcuts

## Authentication

- live Frost use requires `WEATHERDOWNLOAD_FROST_CLIENT_ID` or `FROST_CLIENT_ID`
- fixture-backed unit tests do not require credentials
- `NO / ghcnd / daily` remains available separately as the NOAA GHCN-Daily fallback path
