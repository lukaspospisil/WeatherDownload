# MeteoSwiss Switzerland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current MeteoSwiss A1 automatic-weather-station slice used by WeatherDownload. It keeps the public provider model stable while staying explicit about what this source does and does not represent.

## Provider identifiers

- country: `CH`
- provider: `historical`
- backward-compatible `dataset_scope`: `historical`
- resolution(s): `daily`, `1hour`, `10min`

## Source

- official source: MeteoSwiss A1 automatic weather station open data
- collection: `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn`
- station items: `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/{station_id}`
- station metadata: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv`
- parameter metadata: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv`
- data inventory: `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_datainventory.csv`

## Station identifiers

- `station_id` is the official MeteoSwiss A1 `station_abbr`
- `gh_id` carries the official MeteoSwiss `station_wigos_id`
- hourly and 10-minute timestamps are treated as UTC, matching the published assets

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration`

Unsupported or ambiguous source fields stay unsupported. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official MeteoSwiss values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- only the MeteoSwiss A1 automatic weather station product is used
- daily precipitation keeps the official A1 `6 UTC -> 6 UTC following day` semantics behind the provider layer
- `flag` and `quality` remain null on the implemented slice
- `open_water_evaporation` is intentionally unsupported here
- MeteoSwiss FAO reference evaporation parameters such as `erefaod0` are not the same as measured open-water or pan evaporation and are therefore not mapped to `open_water_evaporation`

## Examples

```powershell
weatherdownload stations elements --country CH --provider historical --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
