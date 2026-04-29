# RMI/KMI Belgium

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative Belgium slice built on the official RMI/KMI AWS open-data layers. The public interface stays aligned with the shared WeatherDownload provider model.

## Provider identifiers

- country: `BE`
- provider: `historical`
- `provider`: `historical`
- resolution(s): `daily`, `1hour`, `10min`

## Source

- official source: RMI/KMI open-data AWS platform
- documentation: `https://opendata.meteo.be/documentation/?dataset=aws`
- station metadata layer: `aws_station`
- daily layer: `aws_1day`
- hourly layer: `aws_1hour`
- 10-minute layer: `aws_10min`

## Station identifiers

- `station_id` is the official RMI/KMI AWS station code, normalized as a string
- station metadata come from the `aws_station` layer
- `gh_id` remains null on this path

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

These mappings are taken directly from documented source properties. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official source values directly and does not add a special conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- `aws_1day` and `aws_1hour` are official provider-side aggregates; WeatherDownload does not recompute them from `aws_10min`
- the documented daily grouping window runs from `00:10` on day `D` to `00:00` on day `D+1`
- the documented hourly grouping window runs from `(H-1):10` to `H:00`
- `flag` preserves raw `qc_flags` text when present
- `quality` remains null in this slice
- no FAO or derived meteorological variables are added

## Examples

```powershell
weatherdownload stations elements --country BE --provider historical --resolution 1hour --include-mapping
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="BE",
    provider="historical",
    resolution="1hour",
    station_ids=["6414"],
    start="2024-01-01T01:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

observations = download_observations(query)
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
