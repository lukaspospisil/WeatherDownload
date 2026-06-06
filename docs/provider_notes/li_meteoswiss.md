# MeteoSwiss Liechtenstein

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current MeteoSwiss A1 automatic-weather-station slice used for Liechtenstein. It is a narrow official daily wrapper around the MeteoSwiss open-data station `VAD` (Vaduz, `station_canton = FL`), not a separate national Liechtenstein platform.

## Provider identifiers

- country: `LI`
- provider: `meteoswiss`
- resolution(s): `daily`

## Source

- official source: MeteoSwiss A1 automatic weather station open data
- collection: `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn`
- station items: `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/{station_id}`
- station metadata: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv`
- parameter metadata: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv`
- data inventory: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_datainventory.csv`

## Station scope

- current WeatherDownload scope is the official MeteoSwiss station `VAD`
- `station_id` is the official MeteoSwiss A1 `station_abbr`
- `gh_id` carries the official MeteoSwiss `station_wigos_id`
- the wrapper is intentionally station-filtered to the Liechtenstein entry rather than exposing the broader Swiss network under `LI`

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration`, `solar_radiation`

Unsupported or intentionally unmapped fields stay unsupported. In particular:

- no provider-side FAO derivation is added
- `open_water_evaporation` is unsupported
- `snow_depth` is not exposed from this automatic-station slice
- the visible daily `wcc006d0` field is a foehn index, not snow depth

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official MeteoSwiss values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- this is a GHCN-independent national daily path based on MeteoSwiss open data
- daily precipitation keeps the official A1 `6 UTC -> 6 UTC following day` semantics behind the provider layer
- `flag` and `quality` remain null on the implemented slice
- MeteoSwiss FAO reference evaporation parameters such as `erefaod0` exist in the metadata but are not mapped to `open_water_evaporation`
- `LI` does not use GHCND prefix `LI`, because in NOAA GHCN-Daily `LI` is Liberia

## Examples

```powershell
weatherdownload stations elements --country LI --provider meteoswiss --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
