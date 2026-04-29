# DMI Denmark

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative Denmark slice built from official DMI open-data APIs. The public interface stays standard while the source-specific time semantics remain behind the provider layer.

## Provider identifiers

- country: `DK`
- provider: `historical`
- `provider`: `historical`
- resolution(s): `daily`, `1hour`, `10min`

## Source

- official sources: DMI Climate Data API and DMI Meteorological Observation API
- station collection: `station`
- climate observations: `stationValue`
- meteorological observations: `observation`

The current implementation uses `stationValue` for `daily` and `1hour`, and the meteorological observation API for `10min`.

## Station identifiers

- `station_id` is the official DMI `stationId`, normalized as a string
- station metadata come from the DMI `station` collection
- the provider filters to Denmark stations using the source-backed `country = DNK` field
- `gh_id` remains null on this path

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official DMI values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- daily values follow DMI local-day Denmark semantics and normalize to `observation_date`
- hourly values preserve the source hourly interval meaning and normalize `timestamp` from the interval end in UTC
- 10-minute values preserve the DMI metObs `observed` timestamp in UTC
- WeatherDownload does not derive hourly or daily values from the 10-minute path
- `daily` and `1hour` preserve raw `qcStatus` and `validity` in `flag`
- `10min` currently leaves `flag` null because the implemented path does not expose matching QC/status fields
- `quality` remains null for all Denmark paths
- Greenland and Faroe Islands differences remain out of scope

## Examples

```powershell
weatherdownload stations elements --country DK --provider historical --resolution 10min --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
