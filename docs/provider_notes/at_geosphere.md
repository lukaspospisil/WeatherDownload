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

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

Raw GeoSphere parameter names remain accepted for backward compatibility, but users should prefer canonical element names. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

The provider uses the official GeoSphere values directly and does not apply a special unit conversion layer beyond the shared WeatherDownload normalization.

Quality companion fields are requested as `<raw_parameter>_flag` where available.

## Limitations and caveats

- `daily` is normalized as date-based data from the published daily timestamps
- `1hour` and `10min` preserve the published UTC timestamps
- the implemented path does not recompute hourly or daily aggregates from 10-minute data
- daily `quality` keeps the numeric `<raw_parameter>_flag` value, while hourly and 10-minute paths keep the raw flag text in `flag`
- no Austria-specific provider-side FAO workflow logic is implemented here; that stays in the example layer

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
