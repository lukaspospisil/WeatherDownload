# Met Eireann Ireland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Provider Identifier

- `country="IE"`
- `provider="meteireann"`
- `resolution="daily"`

This is a conservative multi-station Ireland slice built from official Met Eireann daily observation CSV files.

## Official Sources

- Daily data landing page: `https://www.met.ie/climate/available-data/daily-data`
- Historical-data landing page: `https://www.met.ie/climate/available-data/historical-data%C2%A0`
- Station metadata CSV family: `https://clidata.met.ie/cli/climate_data/webdata/StationDetails.csv`
- Daily station CSV pattern: `https://clidata.met.ie/cli/climate_data/webdata/dly{station_id}.csv`
- Current implemented station file: `https://clidata.met.ie/cli/climate_data/webdata/dly532.csv`
- Daily key / field descriptions: `https://opendata2.met.ie/opendata2/docs/KeyDaily.txt`

WeatherDownload currently exposes only the official daily observation CSV path. It does not use forecast APIs, third-party mirrors, or scraped HTML tables.

## Current Station Scope

`IE / meteireann / daily` is now station-metadata-driven, but still conservative.

station ids are Met Eireann daily station numbers used in the `dly{station_id}.csv` file pattern.

The runtime station list is an audited validated daily station set checked against official `dly{station_id}.csv` files. WeatherDownload exposes only stations whose official daily CSV currently has a recognizable daily structure and the full raw column set needed for the current supported elements:

- `date`
- `maxtp`
- `mintp`
- `rain`
- `wdsp`
- `sun`

Currently exposed verified daily stations include:

- `518` `Shannon Airport`
- `532` `Dublin Airport`
- `1575` `Malin Head`
- `2275` `Valentia Observatory`
- `2375` `Belmullet`
- `3723` `Casement`
- `3904` `Cork Airport`
- `4935` `Knock Airport`

Dublin Airport remains `station_id="532"`.

Met Eireann publishes a broader station metadata table, but WeatherDownload currently keeps the public daily contract to a conservative verified subset whose daily CSV path, raw columns, and metadata semantics have been checked together. Stations that currently lack required raw columns such as `sun` are intentionally not exposed in this provider slice even if they appear in `StationDetails.csv`.

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
- `tas_mean`, `relative_humidity`, `vapour_pressure`, and `pressure` are not currently exposed by this provider slice.
- `cbl` is listed by the source as Mean CBL Pressure `[hpa]`, but it is intentionally not mapped yet because the pressure semantics are not clear enough for a conservative first contract.

## Date Semantics

- source daily rows are documented as `date: - 00 to 00 utc`
- WeatherDownload preserves the source-assigned dates exactly
- no date shifting is applied

## Limitations

- conservative verified multi-station subset rather than every historical station listed in `StationDetails.csv`
- daily-only
- observed-only
- not FAO-ready
- `Rn/net radiation is not downloaded`
- hourly and 10-minute support are not implemented in this pass
- monthly and other official Met Eireann products may exist separately, but they are outside this provider slice

Because `tas_mean`, `relative_humidity`, `vapour_pressure`, and a pressure mapping are not exposed here, this provider should not be described as provider-level observed FAO-ready.
