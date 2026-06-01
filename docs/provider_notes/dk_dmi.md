# DMI Denmark

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative Denmark slice built from official DMI open-data APIs. The public interface stays standard while the source-specific time semantics remain behind the provider layer.

## Provider identifiers

- country: `DK`
- provider: `dmi`
- resolution(s): `daily`

Related Denmark subdaily paths:

- provider: `historical`
- resolution(s): `1hour`, `10min`

## Source

- official sources: DMI Climate Data API and DMI Meteorological Observation API
- station collection: `station`
- climate observations: `stationValue`
- meteorological observations: `observation`

The current implementation uses `stationValue` for `DK / dmi / daily` and for the existing `DK / historical / 1hour` path. The Denmark `10min` path continues to use the Meteorological Observation API.

Authentication:

- current official DMI documentation says API keys are no longer required for `opendataapi.dmi.dk`
- WeatherDownload therefore does not require `WEATHERDOWNLOAD_DMI_API_KEY` or `DMI_API_KEY` for the implemented Denmark paths
- if DMI changes this policy in the future, old authenticated calls are documented as being ignored rather than required on the current platform

## Station identifiers

- `station_id` is the official DMI `stationId`, normalized as a string
- station metadata come from the DMI `station` collection
- the provider filters to Denmark stations using the source-backed `country = DNK` field
- `gh_id` remains null on this path

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`, `solar_radiation`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

Daily raw-to-canonical mapping:

- `mean_temp` -> `tas_mean`
- `max_temp_w_date` -> `tas_max`
- `min_temp` -> `tas_min`
- `acc_precip` -> `precipitation`
- `mean_wind_speed` -> `wind_speed`
- `mean_relative_hum` -> `relative_humidity`
- `mean_pressure` -> `pressure`
- `bright_sunshine` -> `sunshine_duration`
- `mean_radiation` -> `solar_radiation`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the documented official DMI stationValue units directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

- `mean_temp`, `max_temp_w_date`, `min_temp`: source `degC` -> canonical degrees Celsius
- `acc_precip`: source `mm` -> canonical millimetres
- `mean_wind_speed`: source `m/s` -> canonical metres per second
- `mean_relative_hum`: source `%` -> canonical percent
- `mean_pressure`: source `hPa` -> canonical hectopascals
- `bright_sunshine`: source daily `hours` -> canonical hours
- `mean_radiation`: source daily `MJ m^-2` -> canonical `solar_radiation` in `MJ m^-2`

## Limitations and caveats

- daily values follow DMI local-day Denmark semantics and normalize to `observation_date`
- hourly values preserve the source hourly interval meaning and normalize `timestamp` from the interval end in UTC
- 10-minute values preserve the DMI metObs `observed` timestamp in UTC
- WeatherDownload does not derive hourly or daily values from the 10-minute path
- `daily` and `1hour` preserve raw `qcStatus` and `validity` in `flag`
- `10min` currently leaves `flag` null because the implemented path does not expose matching QC/status fields
- `quality` remains null for all Denmark paths
- `mean_vapour_pressure` is intentionally unsupported here; WeatherDownload does not derive `vapour_pressure`
- `pot_evaporation_makkink` is intentionally unsupported because it is a derived potential evaporation product, not measured station-level `open_water_evaporation`
- Greenland and Faroe Islands differences remain out of scope

## Examples

```powershell
weatherdownload stations elements --country DK --provider historical --resolution 10min --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
