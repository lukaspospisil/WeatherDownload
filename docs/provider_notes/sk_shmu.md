# SHMU Slovakia

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current experimental SHMU OpenDATA slice. The implementation is intentionally narrow and focuses on the recent operational daily JSON feed.

## Provider identifiers

- country: `SK`
- provider: `recent`
- backward-compatible `dataset_scope`: `recent`
- resolution(s): `daily`

## Source

- metadata JSON: `https://opendata.shmu.sk/meteorology/climate/recent/metadata/kli_inter_metadata.json`
- recent daily index: `https://opendata.shmu.sk/meteorology/climate/recent/data/daily/`
- monthly JSON files: `https://opendata.shmu.sk/meteorology/climate/recent/data/daily/YYYY-MM/kli-inter - YYYY-MM.json`

The current implementation treats these monthly JSON files as the only supported SHMU observation source.

## Station identifiers

- `station_id` is derived from the SHMU `ind_kli` field
- this provider does not currently expose an authoritative station metadata source with stable names, coordinates, or elevation
- `gh_id`, `full_name`, `longitude`, `latitude`, and `elevation_m` therefore remain null in current normalized station metadata
- `begin_date` and `end_date` are probe-derived from sampled recent payloads and should not be treated as authoritative historical coverage bounds

## Supported data

Current source-backed mapping for `SK / recent / daily`:

- `tas_max` -> `t_max`
- `tas_min` -> `t_min`
- `sunshine_duration` -> `sln_svit`
- `precipitation` -> `zra_uhrn`
- `open_water_evaporation` -> `voda_vypar`

`open_water_evaporation` is supported here only because the official metadata and payloads document `voda_vypar` as water evaporation in millimeters for the published daily interval. It is treated as measured water-surface evaporation, not as ET0, PET, or another modeled evapotranspiration field.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official SHMU values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- `SK` support is experimental
- only `recent / daily` is implemented
- station discovery and station observation metadata are probe-derived rather than backed by an authoritative station registry
- `flag` and `quality` remain null because the implemented feed does not expose clearly documented QC semantics
- validated historical SHMU climate support is not implemented

## Examples

```powershell
weatherdownload stations elements --country SK --provider recent --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
- [Slovakia Historical Daily Climate Investigation](../audits/sk_historical_investigation.md)
