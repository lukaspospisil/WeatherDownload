# Canonical Elements

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload exposes a canonical meteorological element vocabulary so users can request the same meteorological meaning across countries.

## Public Behavior

Canonical names are the preferred public interface.

Common examples:

- `tas_mean`
- `tas_max`
- `tas_min`
- `open_water_evaporation`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

Specific meaning note:

- `open_water_evaporation` means daily measured evaporation from an open water surface in `mm`
- it is intentionally not the generic name `evaporation`
- it is not `vapour_pressure`
- it is not reference evapotranspiration or ET0

Backward compatibility is preserved:

- canonical names are preferred
- raw provider codes are still accepted in queries

Normalized observation outputs preserve both identities:

- `element`: canonical element name
- `element_raw`: original provider-specific code

## Discovery Behavior

### `list_supported_elements(...)`

Default behavior returns canonical names:

```python
from weatherdownload import list_supported_elements

canonical = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
)
```

Raw-code view:

```python
raw_codes = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    provider_raw=True,
)
```

Mapping view:

```python
mapping = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    include_mapping=True,
)
```

Mapping columns:

- `element`
- `element_raw`
- `raw_elements`

### `list_station_elements(...)`

The same pattern applies at station level:

- canonical names by default
- raw codes with `provider_raw=True`
- canonical-to-raw mapping with `include_mapping=True`

## Cross-Country Daily Mapping

| Canonical element | CZ raw | DE raw | SK raw |
| --- | --- | --- | --- |
| `tas_mean` | `T` | `TMK` | |
| `tas_max` | `TMA` | `TXK` | `t_max` |
| `tas_min` | `TMI` | `TNK` | `t_min` |
| `wind_speed` | `F` | `FM` | |
| `vapour_pressure` | `E` | `VPM` | |
| `sunshine_duration` | `SSV` | `SDK` | `sln_svit` |
| `precipitation` | `SRA` | `RSK` | `zra_uhrn` |
| `pressure` | `P` | `PM` | |
| `relative_humidity` | `RH` | `UPM` | |
| `open_water_evaporation` | `VY` | | `voda_vypar` |

## Implemented Path Mappings

### CZ `historical_csv / daily`

| Canonical element | Raw code(s) |
| --- | --- |
| `tas_mean` | `T` |
| `tas_max` | `TMA` |
| `tas_min` | `TMI` |
| `open_water_evaporation` | `VY` |
| `wind_speed` | `F`, `WSPD` |
| `vapour_pressure` | `E` |
| `sunshine_duration` | `SSV` |
| `precipitation` | `SRA` |
| `pressure` | `P` |
| `relative_humidity` | `RH` |
| `wind_from_direction` | `WDIR` |
| `snow_depth` | `HS` |

`open_water_evaporation` on this CZ path is measured open-water-surface evaporation from CHMI raw code `VY`. It is not ET0 or another modeled evapotranspiration field.

### SK `recent / daily`

| Canonical element | Raw code(s) |
| --- | --- |
| `tas_max` | `t_max` |
| `tas_min` | `t_min` |
| `sunshine_duration` | `sln_svit` |
| `precipitation` | `zra_uhrn` |
| `open_water_evaporation` | `voda_vypar` |

`open_water_evaporation` on this SK path is measured water-surface evaporation from SHMU raw `voda_vypar` in `mm`. It is not ET0, PET, or another modeled evapotranspiration field.

Current support remains intentionally narrow after auditing additional official providers:

- `FR / Météo-France` remains unsupported because the verified daily evaporation-like fields on the public daily climatology source are Penman-Monteith ETP products, not measured open-water or pan evaporation
- `ES / AEMET` remains unsupported because this audit did not verify an official daily field definition proving a measured open-water or pan evaporation variable on the authenticated OpenData daily climatology endpoint
- `AU / BoM` remains unsupported for now because the official semantics fit `open_water_evaporation`, but this pass did not verify a clean automation-friendly public station-download contract suitable for a new provider without fragile scraping

Detailed audit notes:

- [France, Spain, And Australia Open-Water Evaporation Audit](providers_fr_es_au_open_water_evaporation_audit.md)

### CZ `historical_csv / 1hour`

| Canonical element | Raw code |
| --- | --- |
| `vapour_pressure` | `E` |
| `pressure` | `P` |
| `cloud_cover` | `N` |
| `past_weather_1` | `W1` |
| `past_weather_2` | `W2` |
| `sunshine_duration` | `SSV1H` |

### CZ `historical_csv / 10min`

| Canonical element | Raw code |
| --- | --- |
| `tas_mean` | `T` |
| `tas_max` | `TMA` |
| `tas_min` | `TMI` |
| `tas_period_max` | `TPM` |
| `soil_temperature_10cm` | `T10` |
| `soil_temperature_100cm` | `T100` |
| `sunshine_duration` | `SSV10M` |

### DE `historical / daily`

| Canonical element | Raw code |
| --- | --- |
| `tas_mean` | `TMK` |
| `tas_max` | `TXK` |
| `tas_min` | `TNK` |
| `wind_speed` | `FM` |
| `wind_speed_max` | `FX` |
| `vapour_pressure` | `VPM` |
| `sunshine_duration` | `SDK` |
| `precipitation` | `RSK` |
| `pressure` | `PM` |
| `relative_humidity` | `UPM` |
| `cloud_cover` | `NM` |
| `snow_depth` | `SHK_TAG` |
| `ground_temperature_min` | `TGK` |
| `precipitation_indicator` | `RSKF` |

### DE `historical / 1hour`

| Canonical element | Raw code |
| --- | --- |
| `tas_mean` | `TT_TU` |
| `relative_humidity` | `RF_TU` |
| `wind_speed` | `FF` |

### DE `historical / 10min`

| Canonical element | Raw code |
| --- | --- |
| `tas_mean` | `TT_10` |
| `relative_humidity` | `RF_10` |
| `wind_speed` | `FF_10` |

## Query Examples

Same canonical request style across countries:

```python
from weatherdownload import ObservationQuery, download_observations

cz_query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "tas_max", "tas_min"],
)

de_query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "precipitation"],
)
```

Raw-code backward-compatible usage:

```python
from weatherdownload import ObservationQuery

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["TMK", "RSK"],
)
```

CLI examples:

```powershell
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --element tas_min --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element TMK --element RSK --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element wind_speed --start 1999-12-31T22:00:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations 10min --country DE --station-id 00044 --element tas_mean --element relative_humidity --start 1999-12-31T22:50:00Z --end 2000-01-01T00:00:00Z
```
