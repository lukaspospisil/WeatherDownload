# Meteo.ad Andorra

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current official Meteo.ad daily climatology export used for Andorra. It is an official national daily path based on public observed station data from Meteo.ad, not a GHCN-Daily fallback.

## Provider identifiers

- country: `AD`
- provider: `meteo_ad`
- resolution(s): `daily`

## Source

- official source: Meteo.ad, Servei Meteorològic Nacional
- climatology landing page: `https://www.meteo.ad/climatologia`
- station detail pages: `https://www.meteo.ad/estacions/{station_id}`
- daily variable discovery endpoint: `https://www.meteo.ad/Climatologia/GetDadesMesuraEstacio`
- daily export endpoint: `https://www.meteo.ad/climatologia/list2xls`

The provider is based on the public climatology export flow exposed on the official site:

- station selection from the public climatology page
- station detail pages for coordinates, altitude, and stated measurement period
- the public daily variable list behind `mesura=0`
- the official downloadable workbook export used by the site itself

## Station scope

- `station_id` is the official Meteo.ad station code such as `99130011`
- `gh_id` is currently null on this provider
- the provider uses only stations that are publicly listed on the official climatology page and have a usable public daily-variable response

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `sunshine_duration`, `solar_radiation`

Unsupported or intentionally unmapped fields stay unsupported. In particular:

- no provider-side FAO derivation is added
- `pressure` is not exposed because it was not confirmed in the public daily climatology variable/export contract used here
- `vapour_pressure` is unsupported
- `snow_depth` is unsupported; the visible daily snow field is fresh snow (`Gruix de neu fresca`), not snow depth
- `wind_direction` is currently left unsupported even though directional daily fields are visible in the export
- `open_water_evaporation` is unsupported

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses observed official Meteo.ad daily export values with only conservative unit normalization where needed:

- `sunshine_duration`: source minutes -> canonical hours via `value / 60`
- `solar_radiation`: source `J m^-2` -> canonical `MJ m^-2` via `value / 1_000_000`
- temperature, precipitation, wind speed, and relative humidity use the official source values directly

## Limitations and caveats

- this path preserves official Meteo.ad daily export semantics; WeatherDownload does not reinterpret the source daily climatology window
- `flag` and `quality` remain null on the implemented slice
- this is an observed-data provider path, not a FAO-ready claim
- no provider-side derivation is added for `tas_mean`, `vapour_pressure`, sunshine/radiation alternatives, humidity alternatives, pressure, or wind
- `AD` does not use GHCND prefix `AD`; this implementation prefers the official national Meteo.ad source instead

## Examples

```powershell
weatherdownload stations elements --country AD --provider meteo_ad --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
