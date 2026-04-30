# ECCC GeoMet Canada

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the current `CA / eccc / daily` provider registration based on Environment and Climate Change Canada (ECCC) GeoMet climate daily observations.

## Provider identifiers

- country: `CA`
- provider: `eccc`
- resolution(s): `daily`

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

## Current implementation scope

This provider is registered and discoverable, but the current implementation is intentionally narrow:

- fixture/source_url-backed only
- live GeoMet API fetching and pagination are not implemented yet

In practice this means:

- station metadata can be loaded from a local GeoJSON FeatureCollection fixture via `read_station_metadata(country="CA", source_url="...")`
- daily observations can be downloaded only when the passed `station_metadata` were created from such a local fixture (the implementation reuses the fixture path stored in `station_metadata.attrs["source_url"]`)

## Source references

Official GeoMet collections:

- stations: `https://api.weather.gc.ca/collections/climate-stations`
- daily observations: `https://api.weather.gc.ca/collections/climate-daily`

## Relationship to `CA / ghcnd / daily`

- `CA / ghcnd / daily` remains unchanged and available as a separate daily provider
- `eccc` is additive and does not replace the existing GHCN-Daily path
