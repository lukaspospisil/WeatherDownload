# Supported Capabilities

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page is generated from the current provider registry and discovery APIs, then checked in tests so it stays aligned with the code.

Navigation:

- conceptual provider model: [Provider Model](providers.md)
- provider-specific source notes: [Provider Notes](provider_notes/README.md)
- canonical element semantics: [Canonical Elements](canonical_elements.md)

Preferred public selection model:

- think in terms of `country + provider + resolution + element`
- `provider` is the preferred public selector
- `dataset_scope` remains accepted as a backward-compatible alias and still appears in normalized output schemas

Programmatic discovery:

```python
from weatherdownload import list_dataset_scopes, list_providers, list_resolutions, list_supported_elements

list_dataset_scopes(country="CZ")  # compatibility alias
list_providers(country="CZ")
list_resolutions(country="US", provider="ghcnd")
list_supported_elements(country="US", provider="ghcnd", resolution="daily")
```

CLI note:

- the current CLI does not expose a country-wide capability-listing command
- `weatherdownload stations elements ...` is station-level inspection and requires `--station-id`
- for country-wide provider/resolution/element discovery, use the Python discovery functions above

Representative station-level CLI examples:

```powershell
weatherdownload stations elements --country CZ --station-id 0-20000-0-11406 --provider historical_csv --resolution daily
weatherdownload stations elements --country US --station-id USC00000001 --provider ghcnd --resolution daily
```

## Capability Table

| Country | Provider | Resolution | Supported canonical elements | Provider/source description | Important notes |
| --- | --- | --- | --- | --- | --- |
| `AT` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | GeoSphere Austria historical daily station observations | Single public provider for AT. |
| `AT` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | GeoSphere Austria historical hourly station observations | Single public provider for AT. |
| `AT` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | GeoSphere Austria historical 10-minute station observations | Single public provider for AT. |
| `BE` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS daily observations | Provider-side daily aggregates; raw QC stays in flag. |
| `BE` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS hourly observations | Provider-side hourly aggregates; raw QC stays in flag. |
| `BE` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS 10-minute observations | Raw 10-minute path; no derived daily/hourly recomputation. |
| `CA` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `CH` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical daily station observations | FAO reference evaporation exists on MeteoSwiss metadata but is intentionally not mapped to open_water_evaporation. |
| `CH` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical hourly station observations | FAO reference evaporation is not mapped to open_water_evaporation. |
| `CH` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical 10-minute station observations | FAO reference evaporation is not mapped to open_water_evaporation. |
| `CZ` | `historical_csv` | `daily` | `open_water_evaporation`, `vapour_pressure`, `wind_speed`, `snow_depth`, `pressure`, `relative_humidity`, `precipitation`, `sunshine_duration`, `tas_mean`, `tas_max`, `tas_min`, `wind_from_direction` | CHMI historical CSV daily observations | Measured open-water evaporation supported via raw VY. |
| `CZ` | `historical_csv` | `1hour` | `vapour_pressure`, `pressure`, `cloud_cover`, `past_weather_1`, `past_weather_2`, `sunshine_duration` | CHMI historical CSV hourly observations | Implemented CHMI historical CSV hourly path. |
| `CZ` | `historical_csv` | `10min` | `tas_mean`, `tas_max`, `tas_min`, `tas_period_max`, `soil_temperature_10cm`, `soil_temperature_100cm`, `sunshine_duration` | CHMI historical CSV 10-minute observations | Implemented CHMI historical CSV 10-minute path. |
| `DE` | `historical` | `daily` | `wind_speed_max`, `wind_speed`, `precipitation`, `precipitation_indicator`, `sunshine_duration`, `snow_depth`, `cloud_cover`, `vapour_pressure`, `pressure`, `tas_mean`, `relative_humidity`, `tas_max`, `tas_min`, `ground_temperature_min` | DWD historical daily station observations | Single public provider for DE. |
| `DE` | `historical` | `1hour` | `tas_mean`, `relative_humidity`, `wind_speed` | DWD historical hourly station observations | Single public provider for DE. |
| `DE` | `historical` | `10min` | `tas_mean`, `relative_humidity`, `wind_speed` | DWD historical 10-minute station observations | Single public provider for DE. |
| `DK` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | DMI historical daily station observations | Single public provider for DK. |
| `DK` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | DMI historical hourly station observations | Single public provider for DK. |
| `DK` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | DMI historical 10-minute station observations | Meteorological Observation API path for 10-minute data. |
| `FI` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `FR` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `HU` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration` | HungaroMet historical daily station observations | Generic HungaroMet archive path; open_water_evaporation not implemented. |
| `HU` | `historical` | `1hour` | `precipitation`, `tas_mean`, `pressure`, `relative_humidity`, `wind_speed` | HungaroMet historical hourly station observations | Generic HungaroMet archive path; open_water_evaporation not implemented. |
| `HU` | `historical` | `10min` | `precipitation`, `tas_mean`, `pressure`, `relative_humidity`, `wind_speed` | HungaroMet historical 10-minute station observations | Generic HungaroMet archive path; separate wind-only product exists. |
| `HU` | `historical_wind` | `10min` | `wind_speed`, `wind_speed_max` | HungaroMet historical 10-minute wind station observations | Separate HungaroMet wind-only 10-minute product. |
| `IT` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `MX` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `NL` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | KNMI validated daily in-situ observations | Requires KNMI API key. |
| `NL` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | KNMI validated hourly in-situ observations | Requires KNMI API key. |
| `NL` | `historical` | `10min` | `tas_mean`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | KNMI near-real-time 10-minute in-situ observations | Requires KNMI API key; near-real-time rather than validated historical product. |
| `NO` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `NZ` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; no EVAP/open_water_evaporation. |
| `PL` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration` | IMGW-PIB historical daily synop station observations | IMGW synop daily path; open_water_evaporation not implemented. |
| `PL` | `historical` | `1hour` | `tas_mean`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure` | IMGW-PIB historical hourly synop station observations | IMGW synop hourly path; open_water_evaporation not implemented. |
| `PL` | `historical_klimat` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | IMGW-PIB historical daily klimat station observations | Separate IMGW klimat daily path. |
| `SE` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | SMHI corrected-archive daily observations | Corrected-archive excludes latest three months. |
| `SE` | `historical` | `1hour` | `tas_mean`, `wind_speed`, `relative_humidity`, `precipitation`, `pressure` | SMHI corrected-archive hourly observations | Corrected-archive excludes latest three months. |
| `SK` | `recent` | `daily` | `tas_max`, `tas_min`, `sunshine_duration`, `precipitation`, `open_water_evaporation` | SHMU recent daily climatological stations | Experimental provider; measured water-surface evaporation supported via raw voda_vypar. |
| `US` | `ghcnd` | `daily` | `tas_max`, `tas_min`, `precipitation`, `open_water_evaporation` | NOAA GHCN-Daily | Raw GHCN station ids; inventory-driven station elements; measured open_water_evaporation supported via raw EVAP. |

## Open-Water Evaporation

Measured `open_water_evaporation` is currently supported only for:

- `CZ / historical_csv / daily` via CHMI raw `VY`
- `SK / recent / daily` via SHMU raw `voda_vypar`
- `US / ghcnd / daily` via NOAA GHCN-Daily raw `EVAP`

It is intentionally not supported for:

- `CA`, `MX`, `FI`, `FR`, `IT`, `NO`, and `NZ` on the current `ghcnd / daily` wrappers
- `CH` MeteoSwiss FAO reference evaporation fields, because they are not measured open-water or pan evaporation
- `HU`, `PL`, and other providers unless a measured open-water, pan, or evaporimeter variable is explicitly implemented

## Coverage Scope

- this table lists currently implemented provider/resolution paths
- it is derived from `list_providers()`, `list_resolutions()`, `list_supported_elements()`, and provider registry metadata
- station-level availability can still be narrower on inventory-driven providers such as GHCN-Daily
