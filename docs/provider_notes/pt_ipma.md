# IPMA Portugal

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative official IPMA station-observation slice implemented in WeatherDownload.

## Provider identifiers

- country: `PT`
- provider: `ipma`
- `provider`: `ipma`
- resolution(s): `1hour`

## Source

- station metadata: `https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json`
- recent station observations: `https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json`

This implementation is intentionally recent-only and uses the current IPMA station observation feed rather than the long climate series or municipality-level interpolation products.

## Station identifiers

- `station_id` is the official IPMA `idEstacao`, normalized and stored as a string
- station names come from `properties.localEstacao`
- coordinates come from `geometry.coordinates` in `[longitude, latitude]` order
- `gh_id` remains null on this path

## Supported data

Current source-backed mapping for `PT / ipma / 1hour`:

- `temperatura` -> `tas_mean`
- `precAcumulada` -> `precipitation`
- `intensidadeVento` -> `wind_speed`
- `humidade` -> `relative_humidity`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Time and missing-data handling

- the IPMA observation payload is effectively recent-only, covering roughly the last 24 hours with hourly updates
- timestamp keys are parsed as naive datetimes because the source uses `YYYY-mm-ddThh:mi` keys without an explicit timezone suffix
- source value `-99.0` is treated as missing
- missing keys inside a station observation object are also treated conservatively as missing rather than guessed

## Limitations and caveats

- only `PT / ipma / 1hour` is implemented in this slice
- no daily IPMA path is implemented here
- no long climate series or interpolated municipality CSV paths are implemented here
- `pressao` is intentionally not mapped in this first implementation because IPMA documents it as mean-sea-level pressure and this slice stays conservative
- `radiacao`, `idDireccVento`, and `intensidadeVentoKM` are intentionally unsupported
- `vapour_pressure`, `sunshine_duration`, and `open_water_evaporation` are unsupported
- this provider is observed-only, does not add derived values, and is not FAO-ready

## Examples

```powershell
weatherdownload stations elements --country PT --provider ipma --resolution 1hour --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
