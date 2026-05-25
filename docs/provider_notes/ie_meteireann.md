# Met Eireann Ireland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Provider Identifier

- `country="IE"`
- `provider="meteireann"`
- `resolution="daily"`

This is an initial conservative Ireland slice built from official Met Eireann daily observation CSV files.

## Official Sources

- Daily data landing page: `https://www.met.ie/climate/available-data/daily-data`
- Historical-data landing page: `https://www.met.ie/climate/available-data/historical-data%C2%A0`
- Station metadata CSV family: `https://clidata.met.ie/cli/climate_data/webdata/StationDetails.csv`
- Daily station CSV pattern: `https://clidata.met.ie/cli/climate_data/webdata/dly{station_id}.csv`
- Current implemented station file: `https://clidata.met.ie/cli/climate_data/webdata/dly532.csv`
- Daily key / field descriptions: `https://opendata2.met.ie/opendata2/docs/KeyDaily.txt`

WeatherDownload currently exposes only the official daily observation CSV path. It does not use forecast APIs, third-party mirrors, or scraped HTML tables.

## Current Station Scope

The first public Ireland slice is intentionally narrow:

- station: `Dublin Airport`
- `station_id="532"`
- latitude: `53.42778`
- longitude: `-6.24083`
- elevation_m: `71.0`

Met Eireann publishes a broader daily station network, but this first WeatherDownload pass keeps the contract conservative until more station ids and semantics are validated one by one.

## Supported Observed Elements

Current observed daily elements:

- `tas_max <- maxtp` `[degC]`
- `tas_min <- mintp` `[degC]`
- `precipitation <- rain` `[mm]`
- `wind_speed <- wdsp` `[knots -> m/s]`
- `sunshine_duration <- sun` `[hours]`

Notes:

- `wdsp` is published by Met Eireann in knots. WeatherDownload converts it to canonical `wind_speed` in `m/s`.
- no derived values are introduced by the provider.
- `tas_mean`, `relative_humidity`, `vapour_pressure`, and `pressure` are not currently exposed by this initial slice.
- `cbl` is listed by the source as Mean CBL Pressure `[hpa]`, but it is intentionally not mapped yet because the pressure semantics are not clear enough for a conservative first contract.

## Date Semantics

- source daily rows are documented as `date: - 00 to 00 utc`
- WeatherDownload preserves the source-assigned dates exactly
- no date shifting is applied

## Limitations

- Dublin Airport only in this first WeatherDownload pass
- daily-only
- observed-only
- not FAO-ready
- `Rn/net radiation is not downloaded`
- hourly and monthly data may exist in official Met Eireann products, but they are not implemented in this pass

Because `tas_mean`, `relative_humidity`, `vapour_pressure`, and a pressure mapping are not exposed here, this provider should not be described as provider-level observed FAO-ready.
