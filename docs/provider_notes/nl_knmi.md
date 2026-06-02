# KNMI Netherlands

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative Netherlands daily slice backed by the official KNMI public `daggegevens` interface.

## Provider identifiers

- country: `NL`
- provider: `knmi`
- resolution: `daily`

## Official source

- public endpoint: `https://www.daggegevens.knmi.nl/klimatologie/daggegevens`
- source owner: Royal Netherlands Meteorological Institute (KNMI)
- interface: public POST-backed daily CSV/text response with inline station metadata in the header
- license: KNMI Data Platform open data is published under `CC BY 4.0`; the KNMI site also states website content is generally reusable under `CC0` unless noted otherwise

## Station scope

- automatic weather stations listed in the official KNMI daily response header
- station identifiers are official KNMI numeric `STN` codes such as `260`
- `gh_id` remains null on this path
- WeatherDownload station discovery reads the station table embedded in the official daily response header

## Supported observed elements

- `tas_mean` via `TG`
- `tas_max` via `TX`
- `tas_min` via `TN`
- `precipitation` via `RH`
- `wind_speed` via `FG`
- `relative_humidity` via `UG`
- `pressure` via `PG`
- `sunshine_duration` via `SQ`
- `solar_radiation` via `Q`

## Unsupported or postponed elements

- `vapour_pressure` is not exposed even though KNMI documents daily vapour pressure products, because the published daily value is described as derived from hourly data rather than treated here as a direct observed provider element
- `EV24` is not exposed because it does not match a supported observed element in WeatherDownload
- no ambiguous calculated variables are mapped

## Unit conversions

- `TG`, `TX`, `TN`: source `0.1 degC` -> canonical `degC` via `value * 0.1`
- `RH`: source `0.1 mm` -> canonical `mm` via `value * 0.1`
- `FG`: source `0.1 m/s` -> canonical `m/s` via `value * 0.1`
- `PG`: source `0.1 hPa` -> canonical `hPa` via `value * 0.1`
- `SQ`: source `0.1 hour` -> canonical `hour` via `value * 0.1`
- `Q`: source `J/cm^2` -> canonical `MJ m^-2` via `value * 0.01`

## Missing values and trace handling

- blank KNMI fields stay missing
- `RH=-1` is documented by KNMI for precipitation below `0.05 mm`; WeatherDownload normalizes this trace precipitation code to observed `0.0 mm`
- `SQ=-1` is documented by KNMI for sunshine duration below `0.05 hour`; WeatherDownload normalizes this below-threshold code to observed `0.0 hour`

## FAO status

- not provider-level FAO-ready because observed `vapour_pressure` is unavailable on this path
- the shared workflow may still fill `vapour_pressure` only in explicit `--fill-missing allow-derived` mode from observed `tas_mean` plus observed `relative_humidity`
- that fallback remains workflow-level `derived_opt_in`, not provider data

## Notes

- the official response header includes station metadata and variable descriptions before the `STN,YYYYMMDD,...` data header
- no KNMI API key is required for this provider slice
- no provider-side derivation is performed beyond explicit source-unit normalization and documented trace handling
