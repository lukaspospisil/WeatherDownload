# RMI/KMI Belgium

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Belgium implementation built on the official RMI/KMI AWS open-data layers, with `BE / ghcnd / daily` kept as the fallback path for direct-prefix GHCN-Daily stations.

## Provider identifiers

- country: `BE`
- provider/resolution: `rmi / daily`
- provider/resolution: `historical / 1hour`
- provider/resolution: `historical / 10min`
- fallback daily path: `ghcnd / daily`

## Source

- official source: RMI/KMI open-data AWS platform
- documentation: `https://opendata.meteo.be/documentation/?dataset=aws`
- WFS service: `https://opendata.meteo.be/service/aws/ows`
- station metadata layer: `aws:aws_station`
- daily layer: `aws:aws_1day`
- hourly layer: `aws:aws_1hour`
- 10-minute layer: `aws:aws_10min`

## Station identifiers

- `station_id` is the official RMI/KMI AWS station code, normalized as a string
- station metadata come from the `aws:aws_station` layer on the national path
- `gh_id` remains null on both the national and Belgium `ghcnd` fallback paths

## Supported data

Current source-backed mapping includes:

- `rmi / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `pressure`, `sunshine_duration`
- `historical / 1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `historical / 10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `ghcnd / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth`

These mappings are taken directly from documented source properties. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `sun_duration` is published by RMI/KMI in minutes on `aws_1day`; WeatherDownload normalizes it to hours on `rmi / daily`
- other mapped national daily values are passed through in source units
- the current implementation does not expose `short_wave_from_sky_avg` or `sun_int_avg` as `solar_radiation`, because this pass keeps the daily mapping to directly confirmed semantics only

## Limitations and caveats

- `aws_1day` and `aws_1hour` are official provider-side aggregates; WeatherDownload does not recompute them from `aws_10min`
- `pressure` on `rmi / daily` is station-level pressure as published by the source, not sea-level pressure
- `flag` preserves raw `qc_flags` text when present
- `quality` remains null in this slice
- no FAO or derived meteorological variables are added at provider level

## Examples

```powershell
weatherdownload stations elements --country BE --provider rmi --resolution daily --include-mapping
weatherdownload stations elements --country BE --provider historical --resolution 1hour --include-mapping
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="BE",
    provider="rmi",
    resolution="daily",
    station_ids=["6414"],
    start_date="2024-01-01",
    end_date="2024-01-02",
    elements=["tas_mean", "wind_speed_max", "sunshine_duration"],
)

observations = download_observations(query)
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
