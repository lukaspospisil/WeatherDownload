# Ilmateenistus Estonia

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Estonian Environment Agency / ilmateenistus daily provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `EE`
- provider: `ilmateenistus`
- resolution(s): `daily`

## Station identifiers

- `station_id` is the official `jaam_kood` station code from the climate open-data service, for example `AJHARK01`

## Official source and access method

- station + element coverage metadata: `https://keskkonnaandmed.envir.ee/f_kliima_jaam_vaatlus`
- element metadata: `https://keskkonnaandmed.envir.ee/f_kliima_element`
- daily observations: `https://keskkonnaandmed.envir.ee/f_kliima_paev`
- access method: public official JSON endpoints from the Estonian Environment Agency climate open-data service

## Raw vs quality-checked

- WeatherDownload uses the climate open-data service rather than the raw daily web table
- the public ilmateenistus daily web page warns that its inline web values are not quality checked and points users to downloadable quality-checked climate data
- this implementation therefore uses the official climate/open-data daily endpoint, not the raw website table

## Station scope

- station discovery is driven by the official station/element coverage endpoint
- coverage is station-specific and element-specific; begin/end dates can differ by station and by raw element
- WeatherDownload stores the official station code, station name, coordinates, elevation, and aggregated metadata coverage range

## Supported observed elements

Raw-to-canonical mapping:

| Raw | Canonical | Source unit | Conversion |
| --- | --- | --- | --- |
| `DTA08` | `tas_mean` | `degC` | none |
| `DTAX` | `tas_max` | `degC` | none |
| `DTAN` | `tas_min` | `degC` | none |
| `DPREC` | `precipitation` | `mm` | none |
| `DWS08` | `wind_speed` | `m/s` | none |
| `DRH08` | `relative_humidity` | `%` | none |
| `DSDUR` | `sunshine_duration` | `h` | none |
| `DPA008` | `pressure` | `hPa` | none |
| `DSND` | `snow_depth` | `cm` | none |
| `DRQS` | `solar_radiation` | `MJ/m2` | none |

Notes:

- `pressure` is mean sea-level pressure because that is how the official source labels `DPA008`
- `snow_depth` is the reported daily value at `06:00 UTC`; WeatherDownload keeps it as observed `snow_depth` without reinterpretation
- `solar_radiation` is exposed because the official source publishes daily observed `DRQS` directly in `MJ/m2`
- unsupported or ambiguous fields stay unsupported rather than being guessed

## Unsupported or postponed

- `vapour_pressure` is not exposed because this daily source does not directly publish an unambiguous observed vapour-pressure field
- `wind_speed_max` is currently postponed even though a daily gust field exists; the current implementation stays conservative and focuses on the requested observed core
- forecasts, hourly recomputations, and web-table scraping are intentionally out of scope

## Missing-value handling

- null/empty numeric values are normalized to missing
- no extra trace or sentinel conversions are currently applied beyond generic missing handling because the official daily values already arrive as direct decimals in the tested source path

## FAO status

- not currently treated as an EE FAO-ready provider path
- observed daily temperature, precipitation, wind speed, humidity, sunshine duration, pressure, snow depth, and solar radiation are available, but no EE-specific FAO workflow integration is added in this change

## Related references

- [Supported Capabilities](../supported_capabilities.md)
- [Provider Notes](README.md)
