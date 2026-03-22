# WeatherDownload Tutorial (English)

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

[Česká verze](tutorial.cs.md)

This tutorial is a practical guide to **WeatherDownload**. It is written as a step-by-step overview of common and advanced use cases, so that users can work with the library without studying the full source code first.

The goal is not only to show commands, but also to explain:

- the logic of the library,
- the difference between the general downloader and specialized workflow examples,
- what outputs look like,
- what users should expect from each command.

---

## 1. What the library does

WeatherDownload is a Python library for working with open meteorological datasets through one unified interface.

It currently supports:

- **CZ** via **CHMI**
- **DE** via **DWD**

The library can:

- load station metadata,
- load observation availability metadata,
- discover what is supported for a country / station / resolution,
- download meteorological observations,
- export tabular outputs to common formats,
- unify cross-country usage through **canonical elements**,
- provide workflow examples, such as preparing a dataset for later FAO calculations.

---

## 2. Core design idea

The library separates several layers:

1. **provider layer**  
   Hides country/provider-specific details (`CZ` / `DE`).

2. **canonical element layer**  
   Users do not need to know raw provider codes such as `TMA`, `TMK`, `RSK`, `SSV1H`, …  
   Instead, they can work with stable names such as:
   - `tas_mean`
   - `tas_max`
   - `tas_min`
   - `wind_speed`
   - `vapour_pressure`
   - `sunshine_duration`
   - `precipitation`
   - `pressure`
   - `relative_humidity`

3. **normalized output schema**  
   Outputs are kept as consistent as possible across countries.

4. **workflow examples**  
   Specific pipelines (for example, FAO dataset preparation) remain outside the core API.

This means the user can mostly vary:
- `country="CZ"` / `country="DE"`,
- station,
- resolution,
- canonical elements,

without having to relearn a new data model for each provider.

---

## 3. Installation

### 3.1 Basic installation

```bash
pip install .
```

### 3.2 Installation with all optional exports

```bash
pip install .[full]
```

This is recommended if you want to use:

- Excel export,
- Parquet export,
- MAT export.

---

## 4. First orientation: the two metadata layers

The library works with two different kinds of metadata:

### 4.1 `meta1` / station metadata

- `station_id`
- `full_name`
- `longitude`
- `latitude`
- `elevation_m`

### 4.2 `meta2` / station observation metadata

- `obs_type`
- `station_id`
- `begin_date`
- `end_date`
- `element`
- `schedule`

The distinction matters:

- `meta1` answers **what station this is and where it is located**,
- `meta2` answers **which observations are declared available and for which time span**.

---

## 5. First steps in the Python API

### 5.1 Load basic station metadata

```python
from weatherdownload import read_station_metadata

stations = read_station_metadata(country="CZ")
print(stations.head())
```

For Germany:

```python
stations = read_station_metadata(country="DE")
print(stations.head())
```

### 5.2 What output to expect

The output is a `pandas.DataFrame`, typically with normalized columns such as:

- `station_id`
- `gh_id`
- `begin_date`
- `end_date`
- `full_name`
- `longitude`
- `latitude`
- `elevation_m`

### 5.3 Load observation metadata

```python
from weatherdownload import read_station_observation_metadata

obs_meta = read_station_observation_metadata(country="CZ")
print(obs_meta.head())
```

For Germany:

```python
obs_meta = read_station_observation_metadata(country="DE")
print(obs_meta.head())
```

### 5.4 What output to expect

Again, the output is a `pandas.DataFrame`, now typically with columns such as:

- `obs_type`
- `station_id`
- `begin_date`
- `end_date`
- `element`
- `schedule`
- `name`
- `description`
- `height`

---

## 6. Filtering stations

### 6.1 Filter by `station_id`

```python
from weatherdownload import read_station_metadata, filter_stations

stations = read_station_metadata(country="CZ")
selected = filter_stations(stations, station_ids=["0-20000-0-11433"])
print(selected)
```

### 6.2 Filter by station name

```python
selected = filter_stations(stations, name_contains="kopisty")
```

### 6.3 Filter by bounding box

```python
selected = filter_stations(stations, bbox=(13.3, 50.4, 13.7, 50.7))
```

### 6.4 Filter by active date

```python
selected = filter_stations(stations, active_on="2024-01-01")
```

### 6.5 Filter by `gh_id`

```python
selected = filter_stations(stations, gh_ids=["L3CHEB01"])
```

---

## 7. Discovery: what the library knows and supports

### 7.1 Dataset scopes

```python
from weatherdownload import list_dataset_scopes

print(list_dataset_scopes(country="CZ"))
print(list_dataset_scopes(country="DE"))
```

### 7.2 Resolutions

```python
from weatherdownload import list_resolutions

print(list_resolutions(country="CZ"))
print(list_resolutions(country="DE"))
```

### 7.3 Supported elements in general

By default this returns **canonical names**:

```python
from weatherdownload import list_supported_elements

print(list_supported_elements(country="CZ", dataset_scope="historical_csv", resolution="daily"))
print(list_supported_elements(country="DE", dataset_scope="historical", resolution="daily"))
```

### 7.4 Raw provider codes instead of canonical names

```python
print(list_supported_elements(country="CZ", dataset_scope="historical_csv", resolution="daily", provider_raw=True))
```

### 7.5 Canonical-to-raw mapping

```python
mapping = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    include_mapping=True,
)
print(mapping)
```

### 7.6 What output to expect

- default mode: a list of canonical names,
- `provider_raw=True`: a list of raw provider codes,
- `include_mapping=True`: a table / `DataFrame` describing canonical ↔ raw mapping.

---

## 8. Availability: what is available for a specific station

### 8.1 Implemented paths for a station

```python
from weatherdownload import read_station_metadata, list_station_paths

stations = read_station_metadata(country="CZ")
paths = list_station_paths(stations, "0-20000-0-11433", include_elements=True, country="CZ")
print(paths)
```

### 8.2 Does a station support a path?

```python
from weatherdownload import station_supports

print(
    station_supports(
        stations,
        station_id="0-20000-0-11433",
        dataset_scope="historical_csv",
        resolution="daily",
        country="CZ",
    )
)
```

### 8.3 Elements for a station

```python
from weatherdownload import list_station_elements

elements = list_station_elements(
    stations,
    station_id="0-20000-0-11433",
    dataset_scope="historical_csv",
    resolution="daily",
    country="CZ",
)
print(elements)
```

### 8.4 Mapping for a station

```python
elements = list_station_elements(
    stations,
    station_id="00044",
    dataset_scope="historical",
    resolution="daily",
    country="DE",
    include_mapping=True,
)
print(elements)
```

---

## 9. Canonical elements vs raw provider codes

### 9.1 Preferred usage: canonical names

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

### 9.2 Backward compatibility: raw provider codes still work

For example:
- in CZ you can still use `TMA`, `T`, `SSV`, ...
- in DE you can still use `TMK`, `TXK`, `RSK`, ...

### 9.3 What output now contains

In normalized observation outputs, the current contract is:

- `element` = canonical name
- `element_raw` = original provider-specific code

Example:
- `element = tas_mean`
- `element_raw = TMK`

---

## 10. The general observations downloader

Important: the general downloader returns data in **long / tidy format**, not wide format.

This means:

- one variable = one row,
- the actual measurement goes into the `value` column.

For daily data, the output typically contains something like:

- `station_id`
- `gh_id`
- `element`
- `element_raw`
- `observation_date`
- `time_function`
- `value`
- `flag`
- `quality`
- `dataset_scope`
- `resolution`

This is **correct** and expected.

---

## 11. What the observations downloader output looks like

### 11.1 Daily observations – expected columns

Typical daily output contains:

- `station_id`
- `gh_id`
- `element`
- `element_raw`
- `observation_date`
- `time_function`
- `value`
- `flag`
- `quality`
- `dataset_scope`
- `resolution`

### 11.2 Hourly / 10min – expected columns

Typical subdaily output contains:

- `station_id`
- `gh_id`
- `element`
- `element_raw`
- `timestamp`
- `value`
- `flag`
- `quality`
- `dataset_scope`
- `resolution`

### 11.3 What this means in practice

If you download, for example:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`

you will **not** get four side-by-side columns in the general downloader output.
Instead:
- `element` identifies the variable,
- `value` holds the measurement.

---

## 12. Daily observations – CZ

### 12.1 Explicit date range

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --start-date 2024-01-01 --end-date 2024-01-31
```

### 12.2 Save to Parquet

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --start-date 2024-01-01 --end-date 2024-01-31 --format parquet --output kopisty_daily.parquet
```

### 12.3 Full available history

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --all-history
```

### 12.4 What output to expect

A long-format table where:
- `element` may be `tas_mean`, `tas_max`, `tas_min`, `wind_speed`,
- `element_raw` may be `T`, `TMA`, `TMI`, `F`,
- `value` contains the measurement.

---

## 13. Daily observations – DE

### 13.1 Daily temperature and precipitation in Germany

```bash
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
```

### 13.2 Full history

```bash
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --all-history
```

### 13.3 What is currently implemented for DE daily

The current narrow verified slice for `DE historical daily` includes canonical elements such as:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

### 13.4 What output to expect

Again, a long-format table:
- not separate columns such as `tas_mean`, `precipitation`, ...
- but rows where `element` identifies the variable and `value` contains the measurement.

---

## 14. Hourly observations – CZ

The current narrow slice for `CZ historical_csv 1hour` includes for example:

- `vapour_pressure`
- `pressure`
- `cloud_cover`
- `past_weather_1`
- `past_weather_2`
- `sunshine_duration`

Example:

```bash
weatherdownload observations hourly --country CZ --station-id 0-20000-0-11406 --element vapour_pressure --element pressure --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
```

### What output to expect

- `timestamp` instead of `observation_date`,
- canonical `element`,
- raw `element_raw`,
- measurement values in `value`.

---

## 15. 10-minute observations – CZ

The current narrow slice for `CZ historical_csv 10min` includes for example:

- `tas_mean`
- `tas_max`
- `tas_min`
- `soil_temperature_10cm`
- `soil_temperature_100cm`
- `sunshine_duration`

Example:

```bash
weatherdownload observations 10min --country CZ --station-id 0-20000-0-11406 --element tas_mean --element soil_temperature_10cm --start 2024-01-01T00:00:00Z --end 2024-01-01T00:20:00Z
```

---

## 16. Hourly observations – DE

The current narrow verified slice for `DE historical 1hour` includes:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

Example:

```bash
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element relative_humidity --element wind_speed --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
```

### 16.1 Important timestamp note for DE hourly

For DWD subdaily data, the implemented rule is:

- **before 2000-01-01**: timestamps are interpreted as `Europe/Berlin` local time and then converted to UTC,
- **from 2000-01-01 onward**: timestamps are treated directly as UTC.

Public output always uses timezone-aware UTC timestamps.

---

## 17. 10-minute observations – DE

The current narrow verified slice for `DE historical 10min` includes:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

Example:

```bash
weatherdownload observations 10min --country DE --station-id 00044 --element tas_mean --element relative_humidity --element wind_speed --start 2024-01-01T00:00:00Z --end 2024-01-01T00:20:00Z
```

---

## 18. Python API: observations

### 18.1 Daily query – CZ

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11433"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    elements=["tas_mean", "tas_max", "tas_min", "wind_speed"],
)

df = download_observations(query)
print(df.head())
```

### 18.2 Daily query – DE

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["00044"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation"],
)

df = download_observations(query)
print(df.head())
```

### 18.3 Full-history mode in the Python API

```python
query = ObservationQuery(
    country="CZ",
    dataset_scope="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11433"],
    elements=["tas_mean", "tas_max", "tas_min", "wind_speed"],
    all_history=True,
)
```

---

## 19. Exports

### 19.1 General tabular export

```python
from weatherdownload import export_table

export_table(df, "daily_output.parquet", format="parquet")
```

### 19.2 Note on daily/hourly/10min export shapes

General observation exports are **long-format**. This is good for:
- generic analysis,
- tidy data workflows,
- R / Python tabular processing.

---

## 20. The difference between the general observations downloader and the FAO workflow

### 20.1 `weatherdownload observations ...`
Returns **long-format** data:
- one row = one variable for one date/time,
- the actual measurement goes into `value`.

### 20.2 `examples/download_fao.py`
Returns a **wide-format workflow dataset**, prepared for later FAO work.

This means:
- one row per complete date,
- side-by-side canonical columns:
  - `Date`
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `wind_speed`
  - `vapour_pressure`
  - `sunshine_duration`

---

## 21. What `download_fao.py` output looks like

The FAO workflow does **not** return the general long-format downloader output.  
Instead, it creates a dedicated wide-format dataset bundle.

### 21.1 `parquet` export mode
A typical bundle directory contains:

- `data_info.json`
- `stations.parquet`
- `series.parquet`

### 21.2 What is inside `data_info.json`

- creation timestamp,
- target country,
- canonical variables,
- minimum required number of complete days,
- number of retained stations,
- `ProviderElementMapping`.

### 21.3 What is inside `stations.parquet`

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- screening summary

### 21.4 What is inside `series.parquet`

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- `Date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

### 21.5 What is guaranteed

After the final filter:
- only complete dates remain,
- canonical meteorological columns do not contain missing values.

---

## 22. MATLAB-oriented / FAO preparation workflow

The FAO example is a workflow layer on top of the library.

### 22.1 What it does
- loads station metadata,
- loads observation metadata,
- performs country-aware station screening,
- downloads required daily inputs,
- keeps only complete days,
- exports the final dataset.

### 22.2 What it does not do
- it does not compute FAO,
- it does not compute `Ra`,
- it does not compute other derived variables.

### 22.3 Canonical output variables

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

### 22.4 Provider provenance
Provider-specific mapping is preserved in:
- `dataInfo["ProviderElementMapping"]`

---

## 23. FAO example – basic commands

### 23.1 CZ, full pipeline, Parquet

```bash
python examples/download_fao.py --country CZ --mode full --export-format parquet
```

### 23.2 DE, full pipeline, Parquet

```bash
python examples/download_fao.py --country DE --mode full --export-format parquet
```

### 23.3 Download/cache only

```bash
python examples/download_fao.py --country CZ --mode download --cache-dir outputs/fao_cache
```

### 23.4 Build only from cache

```bash
python examples/download_fao.py --country CZ --mode build --cache-dir outputs/fao_cache --export-format both
```

---

## 24. FAO example modes

- `--mode full`
- `--mode download`
- `--mode build`

`full` = download + build + export  
`download` = cache only  
`build` = export from cache

---

## 25. Cache in the FAO example

Typical cache structure:

```text
outputs/fao_cache/
  meta1.csv
  meta2.csv
  daily/
    <station_id>/
      dly-<station_id>-<ELEMENT>.csv
```

Rules:
- if a file already exists, it is not downloaded again,
- `full` reuses cache,
- `download` prepares raw inputs,
- `build` allows offline build/export.

---

## 26. Progress reporting and silent mode in the FAO example

Progress reporting shows:
- screened candidate count,
- station progress,
- reuse/download summary,
- final summary.

Silent mode:

```bash
python examples/download_fao.py --silent
```

---

## 27. How to open and inspect outputs

### 27.1 Quick Parquet preview in Python

```bash
python -c "import pandas as pd; df = pd.read_parquet('outputs/kopisty_daily.parquet'); print(df.head()); print(df.columns.tolist())"
```

### 27.2 Universal inspection utility

```bash
python examples/inspect_file.py outputs/kopisty_daily.parquet
python examples/inspect_file.py outputs/fao_daily.mat
python examples/inspect_file.py outputs/fao_daily_bundle
```

### 27.3 What `inspect_file.py` should show

- path,
- file type,
- file size,
- last modification time,
- number of rows and columns,
- column names,
- data types,
- first rows preview,
- `.mat` structure summary,
- bundle-directory overview.

---

## 28. Typical use case: I want daily temperature and wind for one station

### CZ / Kopisty

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --all-history --format parquet --output kopisty_daily.parquet
```

### What to expect
You will get a **long-format** table. That means:
- temperature and wind will not appear as four side-by-side columns,
- instead, they appear as rows with different `element` values.

If you want a **wide-format** dataset for later modelling, use your own pivot or a workflow example.

---

## 29. Typical use case: I want a dataset ready for later FAO calculation

### CZ

```bash
python examples/download_fao.py --country CZ --mode full --export-format parquet
```

### DE

```bash
python examples/download_fao.py --country DE --mode full --export-format parquet
```

### What to expect
The result is a **wide-format dataset bundle**. The main data will be in `series.parquet` and will contain canonical columns:

- `Date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

---

## 30. Typical use case: I want to know what is available for a station and country

```bash
weatherdownload stations elements --country DE --station-id 00044 --dataset-scope historical --resolution daily --include-mapping
```

### What to expect
You get an overview of:
- canonical elements,
- raw provider codes,
- their mapping.

---

## 31. Common questions and typical mistakes

### “Why do I only see one element in the CSV?”
Because you only requested one element. Add more `--element`.

### “Where are the actual temperatures?”
In the general observations downloader they are in `value`, not in a dedicated `tas_mean` column.

### “Why do I not see tas_mean, tas_max, tas_min as separate columns?”
Because the general observations downloader returns long-format data.

### “I want everything available. Why do I need a date range?”
Use explicit `--all-history`.

### “The FAO example seems slow”
That is normal. Use `--mode download`, then `--mode build`, and optionally `--silent`.

---

## 32. Recommended workflow

### For exploration
Use:
- `stations metadata`
- `stations elements`
- `stations availability`
- `list_supported_elements(...)`
- `list_station_elements(...)`

### For general downloading
Use:
- `weatherdownload observations ...`

### For a specific workflow
Use:
- `examples/download_fao.py`

### For quick output inspection
Use:
- `examples/inspect_file.py`

---

## 33. Summary

The most important practical distinction is:

### A. General observations downloader
- universal,
- long-format,
- flexible,
- suitable for generic analysis.

### B. Workflow example (`download_fao.py`)
- specialized,
- wide-format,
- country-aware,
- suitable as a clean input dataset for later physical/model-based processing.
