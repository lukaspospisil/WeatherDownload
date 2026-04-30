# ECCC GeoMet Canada

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the current `CA / eccc / daily` provider registration based on Environment and Climate Change Canada (ECCC) GeoMet climate daily observations.

## Provider identifiers

- country: `CA`
- provider: `eccc`
- resolution(s): `daily`, `1hour`

## Station identifiers

- `station_id` is the ECCC `CLIMATE_IDENTIFIER` (normalized as a string)
- this is distinct from the numeric `STN_ID` shown in some GeoMet payloads

## Supported data

First-slice canonical elements:

- `tas_mean` (`MEAN_TEMPERATURE`)
- `tas_max` (`MAX_TEMPERATURE`)
- `tas_min` (`MIN_TEMPERATURE`)
- `precipitation` (`TOTAL_PRECIPITATION`)

No other canonical elements are currently advertised on this provider.

Hourly first-slice canonical elements:

- `tas_mean` (`TEMP`)
- `relative_humidity` (`RELATIVE_HUMIDITY`)

Hourly exclusions in this pass:

- `STATION_PRESSURE`, `WIND_SPEED`, `PRECIP_AMOUNT`, and `DEW_POINT_TEMP` are intentionally not mapped yet (units/semantics need additional verification)

## Current implementation scope

This provider is registered and discoverable, and supports live ECCC GeoMet daily and hourly observation downloads:

- live GeoMet `climate-daily` fetching with pagination
- live GeoMet `climate-hourly` fetching with pagination (conservative first slice)
- local fixture/source_url-backed operation is still supported for tests and offline use

In practice this means:

- station metadata can be loaded from the live GeoMet `climate-stations` collection, or from a local GeoJSON FeatureCollection fixture via `read_station_metadata(country="CA", source_url="...")`
- daily observations can be downloaded either live (GeoMet API), or from a local fixture when the passed `station_metadata` were created from that fixture (the implementation reuses the fixture path stored in `station_metadata.attrs["source_url"]`)

Important limitations:

- GeoMet `climate-daily` is a documented subset collection, so station discovery should be treated as conservative (some stations may not appear, or may not have full coverage)
- not all requested elements are guaranteed for every station/date; missing values are simply omitted from the normalized observation rows

## Source references

Official GeoMet collections:

- stations: `https://api.weather.gc.ca/collections/climate-stations`
- daily observations: `https://api.weather.gc.ca/collections/climate-daily`

## Relationship to `CA / ghcnd / daily`

- `CA / ghcnd / daily` remains unchanged and available as a separate daily provider
- `eccc` is additive and does not replace the existing GHCN-Daily path
