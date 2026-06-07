# DWD Germany

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Deutscher Wetterdienst (DWD) daily and subdaily provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `DE`
- provider: `historical`
- resolution(s): `daily`, `1hour`, `10min`

`DE / ghcnd / daily` also remains available separately as the shared NOAA GHCN-Daily fallback.

## Source

- daily station metadata and daily observations:
  - `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/`
- hourly station metadata and observations:
  - `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/`
- 10-minute station metadata and observations:
  - `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/`

The implemented national daily slice is the DWD CDC `daily/kl/historical` climate-summary path.

## Supported daily elements

WeatherDownload currently exposes these observed daily canonical elements from the DWD national path:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `wind_speed`
- `wind_speed_max`
- `relative_humidity`
- `pressure`
- `sunshine_duration`
- `solar_radiation`

## Notes

- station ids are the native 5-digit DWD station ids such as `00003`
- the provider token is `historical` because the same DWD implementation family also exposes hourly and 10-minute archive paths
- this note covers the national DWD path, not the separate `DE / ghcnd / daily` fallback
