# GeoSphere Austria

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative GeoSphere Austria station-observation integration. The public WeatherDownload shape is standard; the source-specific behavior stays behind the provider layer.

## Provider identifiers

- country: `AT`
- provider: `historical`
- `provider`: `historical`
- resolution(s): `daily`, `1hour`, `10min`

## Source

- official source: GeoSphere Austria Dataset API
- dataset list: `https://dataset.api.hub.geosphere.at/v1/datasets`
- daily metadata: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d/metadata`
- daily data: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d`
- hourly metadata: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h/metadata`
- hourly data: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h`
- 10-minute metadata: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min/metadata`
- 10-minute data: `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min`

## Station identifiers

- `station_id` is the official GeoSphere Klima station ID, normalized as a string
- `gh_id` remains null on this path
- station metadata are normalized from official fields such as `name`, `lon`, `lat`, `altitude`, `valid_from`, and `valid_to`

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `solar_radiation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`
- `1hour`: `tas_mean`, `precipitation`, `solar_radiation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `solar_radiation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

Raw GeoSphere parameter names remain accepted for backward compatibility, but users should prefer canonical element names. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `daily`: `cglo_j` -> `solar_radiation`; source unit `J/cm²`; normalized unit `MJ m^-2`; conversion `value * 0.01`; semantics are the official calibrated 24-hour sum of global radiation
- `1hour`: `cglo` -> `solar_radiation`; source unit `W/m²`; normalized unit `MJ m^-2`; conversion `value * 0.0036`; semantics are calibrated hourly mean global irradiance converted to hourly interval energy
- `10min`: `cglo` -> `solar_radiation`; source unit `W/m²`; normalized unit `MJ m^-2`; conversion `value * 0.0006`; semantics are calibrated 10-minute mean global irradiance converted to 10-minute interval energy
- `sunshine_duration` remains separate as `so_h` on `daily` and `so` on `1hour` / `10min`
- `solar_radiation` is observed provider data normalized to `MJ m^-2`; no provider-side aggregation is performed

Quality companion fields are requested as `<raw_parameter>_flag` where available.

## Limitations and caveats

- `daily` is normalized as date-based data from the published daily timestamps
- `1hour` and `10min` preserve the published UTC timestamps
- the implemented path does not recompute hourly or daily aggregates from 10-minute data
- daily `quality` keeps the numeric `<raw_parameter>_flag` value, while hourly and 10-minute paths keep the raw flag text in `flag`
- no Austria-specific provider-side FAO workflow logic is implemented here; that stays in the example layer and this patch does not change FAO behavior

## Examples

```powershell
weatherdownload stations elements --country AT --provider historical --resolution daily --include-mapping
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="AT",
    provider="historical",
    resolution="1hour",
    station_ids=["1"],
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

observations = download_observations(query)
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
- [MATLAB-Oriented FAO Workflow](../download_fao.md)
