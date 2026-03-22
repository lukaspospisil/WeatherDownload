# Canonical Meteorological Elements

WeatherDownload exposes a canonical, country-independent meteorological element vocabulary so users can write the same kinds of queries across providers and countries.

The goal is simple:

- users ask for stable meteorological meanings such as `tas_mean` or `precipitation`
- provider-specific raw codes stay available for backward compatibility and provenance
- normalized outputs preserve both the canonical identity and the original provider code

## Public Behavior

Canonical names are the preferred public interface.

Examples:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

Provider-specific raw codes such as `TMA`, `SSV1H`, `TMK`, or `RSK` are still accepted in queries for backward compatibility.

Normalized observation outputs preserve both identities:

- `element`: canonical meteorological element name
- `element_raw`: original provider-specific element code

## Current Mapping Coverage

The canonical element layer is currently implemented for these downloader paths:

- `CZ historical_csv / daily`
- `CZ historical_csv / 1hour`
- `CZ historical_csv / 10min`
- `DE historical / daily`

## Cross-Country Daily Mapping

This table shows the practical cross-country daily mapping for the currently implemented daily paths.

| Canonical element | CZ raw code | DE raw code |
| --- | --- | --- |
| `tas_mean` | `T` | `TMK` |
| `tas_max` | `TMA` | `TXK` |
| `tas_min` | `TMI` | `TNK` |
| `wind_speed` | `F` | `FM` |
| `vapour_pressure` | `E` | `VPM` |
| `sunshine_duration` | `SSV` | `SDK` |
| `precipitation` | `SRA` | `RSK` |
| `pressure` | `P` | `PM` |
| `relative_humidity` | `RH` | `UPM` |

## Path-Specific Mapping Notes

### CZ historical_csv / daily

| Canonical element | Raw code(s) |
| --- | --- |
| `tas_mean` | `T` |
| `tas_max` | `TMA` |
| `tas_min` | `TMI` |
| `wind_speed` | `F`, `WSPD` |
| `vapour_pressure` | `E` |
| `sunshine_duration` | `SSV` |
| `precipitation` | `SRA` |
| `pressure` | `P` |
| `relative_humidity` | `RH` |
| `wind_from_direction` | `WDIR` |
| `snow_depth` | `HS` |

### CZ historical_csv / 1hour

| Canonical element | Raw code |
| --- | --- |
| `vapour_pressure` | `E` |
| `pressure` | `P` |
| `cloud_cover` | `N` |
| `past_weather_1` | `W1` |
| `past_weather_2` | `W2` |
| `sunshine_duration` | `SSV1H` |

### CZ historical_csv / 10min

| Canonical element | Raw code |
| --- | --- |
| `tas_mean` | `T` |
| `tas_max` | `TMA` |
| `tas_min` | `TMI` |
| `tas_period_max` | `TPM` |
| `soil_temperature_10cm` | `T10` |
| `soil_temperature_100cm` | `T100` |
| `sunshine_duration` | `SSV10M` |

### DE historical / daily

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

## Python Examples

### Same canonical request shape across countries

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

cz_daily = download_observations(cz_query)
```

```python
from weatherdownload import ObservationQuery, download_observations

de_query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "precipitation"],
)

de_daily = download_observations(de_query)
```

### Raw-code backward compatibility

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["TMK", "RSK"],
)

observations = download_observations(query)
```

## Discovery And Listing

### `list_supported_elements(...)`

Default behavior returns canonical element names:

```python
from weatherdownload import list_supported_elements

canonical = list_supported_elements(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
)
```

`provider_raw=True` returns raw provider codes:

```python
raw_codes = list_supported_elements(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    provider_raw=True,
)
```

`include_mapping=True` returns a mapping table:

```python
mapping = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    include_mapping=True,
)
```

Mapping output columns:

- `element`: canonical name
- `element_raw`: preferred raw provider code
- `raw_elements`: list of raw provider codes for that canonical element

### `list_station_elements(...)`

Default behavior returns canonical element names for the selected station path:

```python
from weatherdownload import list_station_elements, read_station_metadata

stations = read_station_metadata(country="CZ")
elements = list_station_elements(
    stations,
    "0-20000-0-11406",
    "historical_csv",
    "daily",
)
```

`provider_raw=True` returns raw codes:

```python
raw_elements = list_station_elements(
    stations,
    "0-20000-0-11406",
    "historical_csv",
    "daily",
    provider_raw=True,
)
```

`include_mapping=True` returns a station-scoped mapping table:

```python
mapping = list_station_elements(
    stations,
    "0-20000-0-11406",
    "historical_csv",
    "daily",
    include_mapping=True,
)
```

Mapping output columns:

- `station_id`
- `dataset_scope`
- `resolution`
- `element`
- `element_raw`
- `raw_elements`

Station availability helpers follow the same idea:

- canonical names by default
- raw codes with `provider_raw=True`
- canonical-to-raw mapping with `include_element_mapping=True`

## CLI Behavior

`weatherdownload observations ... --element ...` accepts either canonical names or raw provider codes.

Examples:

```powershell
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --element tas_max --element tas_min --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element TMK --element RSK --start-date 2024-01-01 --end-date 2024-01-10
```

Station listing helpers can show mappings too:

```powershell
weatherdownload stations elements --country CZ --station-id 0-20000-0-11406 --dataset-scope historical_csv --resolution daily --include-mapping
weatherdownload stations availability --country DE --station-id 00044 --include-mapping
```
