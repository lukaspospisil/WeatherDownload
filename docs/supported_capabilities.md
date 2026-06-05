# Supported Capabilities

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page is generated from the current provider registry and discovery APIs, then checked in tests so it stays aligned with the code.

Navigation:

- conceptual provider model: [Provider Model](providers.md)
- provider-specific source notes: [Provider Notes](provider_notes/README.md)
- canonical element semantics: [Canonical Elements](canonical_elements.md)

Conceptual model:

- `country` selects the country
- `provider` selects the concrete data source or product within that country
- `resolution` selects the temporal resolution
- `element` selects the canonical meteorological variable
- provider values are provider-specific and are not globally standardized

Programmatic discovery:

```python
from weatherdownload import list_providers, list_resolutions, list_supported_elements

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
weatherdownload stations elements --country CZ --station-id EZM00011406 --provider ghcnd --resolution daily
weatherdownload stations elements --country US --station-id USC00000001 --provider ghcnd --resolution daily
```

## Capability Table

| Country | Provider | Resolution | Supported canonical elements | Provider/source description | Important notes |
| --- | --- | --- | --- | --- | --- |
| `AT` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix AU; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `AT` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `solar_radiation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | GeoSphere Austria historical daily station observations | National GeoSphere daily path; `ghcnd` is an additional daily provider. `cglo_j` is mapped to observed `solar_radiation` in canonical MJ m^-2 via `value * 0.01`; sunshine_duration remains separate. |
| `AT` | `historical` | `1hour` | `tas_mean`, `precipitation`, `solar_radiation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | GeoSphere Austria historical hourly station observations | Single public hourly provider for AT. `cglo` hourly mean irradiance is converted to observed `solar_radiation` interval energy in canonical MJ m^-2 via `value * 0.0036`; sunshine_duration remains separate; no FAO workflow change. |
| `AT` | `historical` | `10min` | `tas_mean`, `precipitation`, `solar_radiation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | GeoSphere Austria historical 10-minute station observations | Single public 10-minute provider for AT. `cglo` 10-minute mean irradiance is converted to observed `solar_radiation` interval energy in canonical MJ m^-2 via `value * 0.0006`; sunshine_duration remains separate; no provider-side aggregation. |
| `BE` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS daily observations | Provider-side daily aggregates; raw QC stays in flag. |
| `BE` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS hourly observations | Provider-side hourly aggregates; raw QC stays in flag. |
| `BE` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | RMI/KMI AWS 10-minute observations | Raw 10-minute path; no derived daily/hourly recomputation. |
| `BG` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `CA` | `eccc` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | Environment and Climate Change Canada GeoMet daily climate observations | Live GeoMet climate-daily fetching with pagination; station_id is ECCC CLIMATE_IDENTIFIER. Station discovery is conservative because climate-daily is a subset collection, and element coverage can vary by station/date. `ghcnd` remains available as a separate daily provider. |
| `CA` | `eccc` | `1hour` | `tas_mean`, `relative_humidity`, `wind_speed`, `precipitation`, `pressure` | Environment and Climate Change Canada GeoMet hourly climate observations | Live GeoMet climate-hourly fetching with pagination; station_id is ECCC CLIMATE_IDENTIFIER. Station discovery is conservative because climate-hourly is a subset collection, and element coverage can vary by station/time. WIND_SPEED is converted from km/h to m/s, and STATION_PRESSURE is converted from kPa to hPa. |
| `CA` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `CH` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SZ; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `CH` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical daily station observations | MeteoSwiss daily path; FAO reference evaporation exists but is intentionally not mapped to open_water_evaporation. |
| `CH` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical hourly station observations | FAO reference evaporation is not mapped to open_water_evaporation. |
| `CH` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`, `sunshine_duration` | MeteoSwiss A1 historical 10-minute station observations | FAO reference evaporation is not mapped to open_water_evaporation. |
| `CZ` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix EZ; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `CZ` | `historical_csv` | `daily` | `open_water_evaporation`, `vapour_pressure`, `wind_speed`, `snow_depth`, `pressure`, `relative_humidity`, `precipitation`, `sunshine_duration`, `tas_mean`, `tas_max`, `tas_min`, `wind_direction` | CHMI historical CSV daily observations | Measured open-water evaporation supported via raw VY. |
| `CZ` | `historical_csv` | `1hour` | `vapour_pressure`, `pressure`, `cloud_cover`, `past_weather_1`, `past_weather_2`, `sunshine_duration` | CHMI historical CSV hourly observations | Implemented CHMI historical CSV hourly path. |
| `CZ` | `historical_csv` | `10min` | `tas_mean`, `tas_max`, `tas_min`, `tas_period_max`, `soil_temperature_10cm`, `soil_temperature_100cm`, `sunshine_duration` | CHMI historical CSV 10-minute observations | Implemented CHMI historical CSV 10-minute path. |
| `DE` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix GM; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `DE` | `historical` | `daily` | `wind_speed_max`, `wind_speed`, `precipitation`, `precipitation_indicator`, `sunshine_duration`, `snow_depth`, `cloud_cover`, `vapour_pressure`, `pressure`, `tas_mean`, `relative_humidity`, `tas_max`, `tas_min`, `ground_temperature_min` | DWD historical daily station observations | National DWD daily path; `ghcnd` is an additional daily provider. |
| `DE` | `historical` | `1hour` | `tas_mean`, `relative_humidity`, `wind_speed` | DWD historical hourly station observations | Single public hourly provider for DE. |
| `DE` | `historical` | `10min` | `tas_mean`, `relative_humidity`, `wind_speed` | DWD historical 10-minute station observations | Single public 10-minute provider for DE. |
| `DK` | `dmi` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`, `solar_radiation` | DMI Climate Data daily stationValue observations | Official DMI climateData stationValue daily path; `ghcnd` remains available as a separate daily provider. `max_temp_w_date` and `min_temp` are used for daily tas_max/tas_min, and `mean_radiation` is mapped directly to observed `solar_radiation` in canonical MJ m^-2. No DMI API key is currently required. |
| `DK` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix DA; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `DK` | `historical` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | DMI historical hourly station observations | Single public hourly provider for DK. |
| `DK` | `historical` | `10min` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | DMI historical 10-minute station observations | Meteorological Observation API path for 10-minute data. |
| `EE` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `EE` | `ilmateenistus` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration`, `pressure`, `snow_depth`, `solar_radiation` | Estonian Environment Agency climate daily observations | Official Estonian climate daily JSON path; station_id is the official `jaam_kood`; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, relative_humidity, sunshine_duration, pressure, snow_depth, and solar_radiation only; values are used directly from source degC/mm/m s^-1/%/h/hPa/cm/MJ m^-2 units; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider. |
| `ES` | `aemet` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration` | AEMET OpenData daily climatological observations | Requires AEMET OpenData API key; observed-only daily slice; velmedia is converted from km/h to canonical m/s; hrMedia is mapped to canonical relative_humidity in percent; trace precipitation `Ip` is mapped to 0.0 mm; observed vapour_pressure is unavailable; shared FAO workflow compatibility exists only through explicit `--fill-missing allow-derived`, where vapour_pressure is workflow-layer `derived_opt_in` rather than observed provider data. |
| `FI` | `fmi` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | Finnish Meteorological Institute (FMI) Open Data WFS daily weather observations (timevaluepair) | Conservative first slice via WFS daily timevaluepair; station_id is FMI fmisid; station metadata is a conservative AWS+SYNOP subset; elevation_m is currently null; FMI daily snow is unit cm and is intentionally not mapped yet. |
| `FI` | `fmi` | `1hour` | `tas_mean`, `wind_speed`, `relative_humidity`, `pressure`, `precipitation` | Finnish Meteorological Institute (FMI) Open Data WFS hourly observations | Conservative first slice via WFS timevaluepair; station_id is FMI fmisid; station metadata is a conservative AWS+SYNOP subset; elevation_m is currently null; p_sea (MSL pressure) is mapped to canonical pressure; units may be absent inline in the XML payload. |
| `FI` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `FR` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `FR` | `meteo_france` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | Meteo-France daily climatological base RR-T-Vent observations | National Meteo-France daily RR-T-Vent slice; native 8-digit Meteo-France station ids; station-level availability comes from official station metadata; current RR-T-Vent RR/TN/TX/TM values are parsed as decimal mm/deg C without an extra /10 scaling step; no open_water_evaporation; tas_mean comes only from raw TM. |
| `GB` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `GB` | `metoffice_datahub` | `1hour` | `tas_mean`, `relative_humidity`, `wind_speed`, `wind_direction`, `pressure` | Met Office Weather DataHub Land Observations recent hourly station observations | Official Met Office Weather DataHub Land Observations hourly slice; station_id is the location geohash; last 48 hours only; live use requires API key via WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY or METOFFICE_DATAHUB_API_KEY; current conservative observed mapping exposes temperature -> tas_mean, humidity -> relative_humidity, wind_speed -> wind_speed, wind_direction compass points -> canonical degree wind_direction, and mslp -> pressure; no derived values; not FAO-ready. |
| `GR` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `HR` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `HU` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration` | HungaroMet historical daily station observations | Generic HungaroMet archive path; open_water_evaporation not implemented. |
| `HU` | `historical` | `1hour` | `precipitation`, `tas_mean`, `pressure`, `relative_humidity`, `wind_speed` | HungaroMet historical hourly station observations | Generic HungaroMet archive path; open_water_evaporation not implemented. |
| `HU` | `historical` | `10min` | `precipitation`, `tas_mean`, `pressure`, `relative_humidity`, `wind_speed` | HungaroMet historical 10-minute station observations | Generic HungaroMet archive path; separate wind-only product exists. |
| `HU` | `historical_wind` | `10min` | `wind_speed`, `wind_speed_max` | HungaroMet historical 10-minute wind station observations | Separate HungaroMet wind-only 10-minute product. |
| `IE` | `meteireann` | `daily` | `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `sunshine_duration` | Met Eireann official daily station CSV observations | Station-metadata-driven conservative multi-station slice from the official Met Eireann daily station CSV path; station ids are the Met Eireann daily station numbers used in dly{station_id}.csv; tas_max, tas_min, precipitation, and sunshine_duration are exposed directly, while wdsp is converted from knots to canonical m/s; cbl (Mean CBL Pressure) stays intentionally unmapped because its semantics are unclear; observed-only and not FAO-ready. |
| `IS` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `IT` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `LT` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `LT` | `meteo_lt` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `pressure`, `snow_depth`, `cloud_cover` | LHMT Meteo.lt meteorological station observations aggregated to daily UTC-date values | Official LHMT Meteo.lt meteorological station API slice aggregated by WeatherDownload from observed intra-day UTC records into deterministic daily UTC-date values. This is not an official climatological daily summary product. The current observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, wind_speed_max, relative_humidity, pressure, snow_depth, and cloud_cover only; unsupported vapour_pressure, sunshine_duration, and solar_radiation remain unavailable and are not provider-derived. `ghcnd` remains available as a separate daily provider. |
| `LU` | `asta` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration` | ASTA daily weather measurements from the Luxembourg agrometeorological station network | Official ASTA daily WFS slice with stable ASTA station ids such as AGM_022; current conservative implementation exposes observed tas_mean, tas_max, tas_min, precipitation, wind_speed, relative_humidity, and measured sunshine_duration; `avg_press` stays unmapped because the source labels it Relative Air Pressure (hPa), and ASTA still does not directly expose vapour_pressure; observed-only and not FAO-ready. |
| `LU` | `meteolux` | `daily` | `tas_max`, `tas_min`, `precipitation`, `sunshine_duration` | MeteoLux daily Findel observations from INSPIRE WFS plus official MeteoLux daily CSV sunshine duration | National MeteoLux daily Findel-only slice; tas_max, tas_min, and precipitation come from official INSPIRE WFS layers, while sunshine_duration comes from the official MeteoLux daily CSV DINS column in hours; precipitation days are source-defined as 06:00 UTC to 06:00 UTC of the following day; observed-only, not FAO-ready, and Rn/net radiation is not downloaded. |
| `LV` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `MX` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `NL` | `knmi` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`, `solar_radiation` | KNMI public daily station observations | Public daggegevens daily CSV path. TG/TX/TN, RH, FG, PG, and SQ are converted from source tenths to canonical units; RH=-1 and SQ=-1 are normalized to observed 0.0 for below-threshold trace values; Q is converted from J/cm^2 to canonical MJ m^-2 via value * 0.01; observed vapour_pressure is intentionally not exposed; compatible with shared FAO allow-derived only through tas_mean plus relative_humidity. |
| `NO` | `frost` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `snow_depth` | MET Norway Frost official daily station observations | Official MET Norway Frost daily station slice; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, and snow_depth only. Temperatures, precipitation, and wind speed are used directly from source degC/mm/m s^-1 units; snow depth is converted from source cm to canonical mm, and Frost coded precipitation value -1 is normalized to observed 0.0 mm. Live use requires WEATHERDOWNLOAD_FROST_CLIENT_ID or FROST_CLIENT_ID. No derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider. |
| `NO` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `NZ` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `PL` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration` | IMGW-PIB historical daily synop station observations | IMGW synop daily path; open_water_evaporation not implemented. |
| `PL` | `historical` | `1hour` | `tas_mean`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure` | IMGW-PIB historical hourly synop station observations | IMGW synop hourly path; open_water_evaporation not implemented. |
| `PL` | `historical_klimat` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | IMGW-PIB historical daily klimat station observations | Separate IMGW klimat daily path. |
| `PT` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `PT` | `ipma` | `1hour` | `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `solar_radiation` | IPMA recent hourly station observations | Official IPMA recent hourly station observations; station_id is IPMA idEstacao; timestamps are parsed as naive because the source keys do not include a timezone suffix; observed-only with tas_mean, precipitation, wind_speed, relative_humidity, and solar_radiation; radiacao is converted from kJ m^-2 to canonical solar_radiation in MJ m^-2 over the published hourly interval; pressure is intentionally not mapped; no derived values; not FAO-ready. |
| `RO` | `anm` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | ANM INSPIRE station-level daily CLIMAT observations | Official ANM INSPIRE/WFS station-level M201 CLIMAT daily slice; current conservative observed mapping exposes tas_mean, tas_max, tas_min, and precipitation only; temperature is converted from kelvin to canonical degrees Celsius; precipitation is used directly from source kg/m2 values; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider. |
| `RO` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `SE` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SW; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `SE` | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation` | SMHI corrected-archive daily observations | Corrected-archive excludes latest three months; `ghcnd` is an additional daily provider. |
| `SE` | `historical` | `1hour` | `tas_mean`, `wind_speed`, `relative_humidity`, `precipitation`, `pressure` | SMHI corrected-archive hourly observations | Corrected-archive excludes latest three months. |
| `SI` | `arso` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth`, `sunshine_duration` | ARSO station-level daily climate archive observations | Official ARSO WebMet archive daily station path; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, snow_depth, and sunshine_duration only; temperatures and precipitation are used directly from source degC/mm values; snow depth is converted from source cm to canonical mm; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider. |
| `SI` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA NCEI GHCN-Daily station observations |  |
| `SK` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` | NOAA GHCN-Daily | Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix LO; inventory-driven station elements; no EVAP/open_water_evaporation. |
| `SK` | `recent` | `daily` | `tas_max`, `tas_min`, `sunshine_duration`, `precipitation`, `open_water_evaporation` | SHMU recent daily climatological stations | Experimental provider; measured water-surface evaporation supported via raw voda_vypar. |
| `US` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth`, `open_water_evaporation` | NOAA GHCN-Daily | Raw GHCN station ids; inventory-driven station elements; measured open_water_evaporation supported via raw EVAP. |

## Open-Water Evaporation

Measured `open_water_evaporation` is currently supported only for:

- `CZ / historical_csv / daily` via CHMI raw `VY`
- `SK / recent / daily` via SHMU raw `voda_vypar`
- `US / ghcnd / daily` via NOAA GHCN-Daily raw `EVAP`

It is intentionally not supported for:

- `AT`, `CA`, `CH`, `CZ`, `DE`, `DK`, `FI`, `FR`, `GB`, `IT`, `MX`, `NO`, `NZ`, `PT`, `SE`, and `SK` on the current `ghcnd / daily` wrappers
- `PT / ipma / 1hour`, because this first IPMA slice does not expose measured open-water evaporation
- `CH` MeteoSwiss FAO reference evaporation fields, because they are not measured open-water or pan evaporation
- `HU`, `PL`, and other providers unless a measured open-water, pan, or evaporimeter variable is explicitly implemented

## Coverage Scope

- this table lists currently implemented provider/resolution paths
- it is derived from `list_providers()`, `list_resolutions()`, `list_supported_elements()`, and provider registry metadata
- station-level availability can still be narrower on inventory-driven providers such as GHCN-Daily
