# WeatherDownload Tutorial (čeština)

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

[English version](tutorial.en.md)

Tento tutoriál je praktický průvodce knihovnou **WeatherDownload**. Je napsaný jako „step by step“ přehled nejběžnějších i pokročilejších use-caseů, aby bylo možné knihovnu používat bez procházení celého zdrojového kódu.

Cílem není jen ukázat jednotlivé příkazy, ale i vysvětlit:

- jaká je logika knihovny,
- jak se liší obecný downloader od specializovaných workflow examples,
- jak vypadají výstupy,
- co může uživatel od konkrétního příkazu očekávat.

---

## 1. Co knihovna dělá

WeatherDownload je Python knihovna pro práci s otevřenými meteorologickými daty přes jednotné rozhraní.

Aktuálně podporuje:

- **CZ** přes poskytovatele **CHMI**
- **DE** přes poskytovatele **DWD**

Knihovna umí zejména:

- načíst metadata stanic,
- načíst metadata o dostupnosti pozorování,
- zjišťovat, co je pro danou zemi / stanici / rozlišení podporované,
- stahovat meteorologická pozorování,
- exportovat tabulky do běžných formátů,
- sjednotit práci s různými zeměmi přes **canonical elements**,
- poskytovat workflow examples nad daty, například přípravu denních meteorologických vstupů pro pozdější FAO-orientované zpracování.

---

## 2. Základní filozofie návrhu

Knihovna odděluje několik vrstev:

1. **provider layer**  
   Schovává specifika jednotlivých poskytovatelů dat (`CZ` / `DE`).

2. **canonical element layer**  
   Uživatel nemusí znát provider-specific kódy jako `TMA`, `TMK`, `RSK`, `SSV1H`, …  
   Místo toho pracuje s jednotnými názvy jako:
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
   Výstupy mají co nejvíc stejný tvar mezi zeměmi.

4. **workflow examples**  
   Specifické pipeline (např. příprava konkrétního datasetu) zůstávají mimo core API.

Tím pádem může uživatel mezi zeměmi měnit hlavně:
- `country="CZ"` / `country="DE"`,
- `dataset_scope`,
- stanici,
- rozlišení,
- canonical elementy,

ale nemusí pokaždé znovu objevovat nový způsob práce s daty.

Veřejný query model má tři oddělené dimenze:

- `country` vybírá kontext země / providera
- `dataset_scope` vybírá konkrétní provider-specific dataset, produkt nebo zdroj
- `resolution` vybírá časové rozlišení

Hodnoty `dataset_scope` nejsou globálně standardizované mezi zeměmi. Například `CZ / historical_csv`, `SK / recent`, `PL / historical_klimat` a `US / ghcnd` označují různé provider-specific zdrojové cesty.

---

## 3. Instalace

### 3.1 Základní instalace

```bash
pip install .
```

### 3.2 Instalace se všemi volitelnými exporty

```bash
pip install .[full]
```

To je doporučené, pokud chceš používat:

- Excel export,
- Parquet export,
- MAT export.

---

## 4. První orientace: jaké typy metadat existují

Knihovna pracuje se dvěma typy metadat:

### 4.1 `meta1` / station metadata
Základní metadata stanice, např.:

- `station_id`
- `full_name`
- `longitude`
- `latitude`
- `elevation_m`

### 4.2 `meta2` / station observation metadata
Metadata o dostupnosti měření, např.:

- `obs_type`
- `station_id`
- `begin_date`
- `end_date`
- `element`
- `schedule`

Rozdíl je důležitý:

- `meta1` říká **co je to za stanici a kde leží**,
- `meta2` říká **jaká měření a v jakém období jsou pro ni deklarovaně dostupná**.

---

## 5. První kroky v Python API

### 5.1 Načtení základních metadat stanic

```python
from weatherdownload import read_station_metadata

stations = read_station_metadata(country="CZ")
print(stations.head())
```

Pro Německo:

```python
stations = read_station_metadata(country="DE")
print(stations.head())
```

### 5.2 Co můžeš očekávat jako výstup

Výstup je `pandas.DataFrame` s normalizovanými sloupci, typicky:

- `station_id`
- `gh_id`
- `begin_date`
- `end_date`
- `full_name`
- `longitude`
- `latitude`
- `elevation_m`

### 5.3 Načtení observation metadata

```python
from weatherdownload import read_station_observation_metadata

obs_meta = read_station_observation_metadata(country="CZ")
print(obs_meta.head())
```

Pro Německo:

```python
obs_meta = read_station_observation_metadata(country="DE")
print(obs_meta.head())
```

### 5.4 Co můžeš očekávat jako výstup

Opět `pandas.DataFrame`, tentokrát typicky se sloupci:

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

## 6. Filtrování stanic

### 6.1 Filtrování podle `station_id`

```python
from weatherdownload import read_station_metadata, filter_stations

stations = read_station_metadata(country="CZ")
selected = filter_stations(stations, station_ids=["0-20000-0-11433"])
print(selected)
```

### 6.2 Filtrování podle názvu

```python
selected = filter_stations(stations, name_contains="kopisty")
```

### 6.3 Filtrování podle bounding boxu

```python
selected = filter_stations(stations, bbox=(13.3, 50.4, 13.7, 50.7))
```

### 6.4 Filtrování podle aktivity v čase

```python
selected = filter_stations(stations, active_on="2024-01-01")
```

### 6.5 Filtrování podle `gh_id`

```python
selected = filter_stations(stations, gh_ids=["L3CHEB01"])
```

### 6.6 Co můžeš očekávat jako výstup

Pořád `DataFrame` se stejným schématem jako `read_station_metadata()`, jen s menším počtem řádků.

---

## 7. Discovery: co knihovna zná a podporuje

### 7.1 Přehled dataset scopes

```python
from weatherdownload import list_dataset_scopes

print(list_dataset_scopes(country="CZ"))
print(list_dataset_scopes(country="DE"))
```

Poznámka k interpretaci:

- `dataset_scope` neznamená ve všech zemích totéž
- pojmenovává konkrétní provider source, který WeatherDownload používá za vybranou country path
- příklady: `CZ / historical_csv`, `CH / historical`, `HU / historical_wind`, `PL / historical_klimat`, `US / ghcnd`

### 7.2 Přehled rozlišení

```python
from weatherdownload import list_resolutions

print(list_resolutions(country="CZ"))
print(list_resolutions(country="DE"))
```

### 7.3 Jaké elementy jsou podporované obecně

Výchozí chování vrací **canonical names**:

```python
from weatherdownload import list_supported_elements

print(list_supported_elements(country="CZ", dataset_scope="historical_csv", resolution="daily"))
print(list_supported_elements(country="DE", dataset_scope="historical", resolution="daily"))
```

### 7.4 Provider raw kódy místo canonical names

```python
print(list_supported_elements(country="CZ", dataset_scope="historical_csv", resolution="daily", provider_raw=True))
```

### 7.5 Mapping canonical -> raw

```python
mapping = list_supported_elements(
    country="DE",
    dataset_scope="historical",
    resolution="daily",
    include_mapping=True,
)
print(mapping)
```

### 7.6 Co můžeš očekávat jako výstup

- výchozí režim: seznam canonical názvů,
- `provider_raw=True`: seznam raw provider kódů,
- `include_mapping=True`: tabulka / `DataFrame` s mappingem canonical ↔ raw.

---

## 8. Availability: co je dostupné pro konkrétní stanici

### 8.1 Přehled implementovaných path pro stanici

```python
from weatherdownload import read_station_metadata, list_station_paths

stations = read_station_metadata(country="CZ")
paths = list_station_paths(stations, "0-20000-0-11433", include_elements=True, country="CZ")
print(paths)
```

### 8.2 Ověření, zda stanice podporuje konkrétní path

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

### 8.3 Elementy pro konkrétní stanici

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

### 8.4 Mapping pro konkrétní stanici

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

### 9.1 Preferovaný způsob: canonical names

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

### 9.2 Zpětná kompatibilita: raw provider codes stále fungují

Například:
- v CZ můžeš stále použít `TMA`, `T`, `SSV`, ...
- v DE můžeš stále použít `TMK`, `TXK`, `RSK`, ...

### 9.3 Co se vrací ve výstupu

Ve normalized outputu nyní platí:

- `element` = canonical name
- `element_raw` = původní provider-specific kód

Příklad:
- `element = tas_mean`
- `element_raw = TMK`

---

## 10. Obecný observations downloader

Pozor: obecný downloader vrací data ve **long / tidy formátu**, ne ve wide formátu.

To znamená:

- jedna veličina = jeden řádek,
- hodnota je ve sloupci `value`.

Například pro daily data dostaneš něco jako:

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

To je **správně**. Není to chyba.

---

## 11. Jak vypadá výstup z observations downloaderu

### 11.1 Daily observations – očekávané sloupce

Typický denní výstup obsahuje:

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

### 11.2 Hourly / 10min – očekávané sloupce

Typický subdaily výstup obsahuje:

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

### 11.3 Co to znamená prakticky

Pokud si stáhneš například pro Kopisty:
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`

tak ve výstupu **nebudou 4 sloupce vedle sebe**, ale:
- ve sloupci `element` bude vždy jedna z těchto hodnot,
- a ve sloupci `value` bude samotná naměřená hodnota.

---

## 12. Daily observations – CZ

### 12.1 Explicitní časový interval

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --start-date 2024-01-01 --end-date 2024-01-31
```

### 12.2 Uložení do Parquetu

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --start-date 2024-01-01 --end-date 2024-01-31 --format parquet --output kopisty_daily.parquet
```

### 12.3 Celá dostupná historie

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --all-history
```

### 12.4 Co můžeš očekávat jako výstup

Long-format tabulku, kde:
- `element` bude například `tas_mean`, `tas_max`, `tas_min`, `wind_speed`,
- `element_raw` bude třeba `T`, `TMA`, `TMI`, `F`,
- `value` bude samotná hodnota.

---

## 13. Daily observations – DE

### 13.1 Daily teplota a srážky v Německu

```bash
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
```

### 13.2 Celá historie

```bash
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --all-history
```

### 13.3 Co je aktuálně implementované pro DE daily

První narrow verified slice pro `DE historical daily` zahrnuje canonical elementy jako:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `precipitation`
- `pressure`
- `relative_humidity`

### 13.4 Co můžeš očekávat jako výstup

Opět long-format tabulku, tedy:
- ne sloupce `tas_mean`, `precipitation`, ...
- ale řádky s `element` a hodnotami ve `value`.

---

## 14. Hourly observations – CZ

Aktuální konzervativni subset pro `CZ historical_csv 1hour` zahrnuje zejména:

- `vapour_pressure`
- `pressure`
- `cloud_cover`
- `past_weather_1`
- `past_weather_2`
- `sunshine_duration`

Příklad:

```bash
weatherdownload observations hourly --country CZ --station-id 0-20000-0-11406 --element vapour_pressure --element pressure --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
```

### Co můžeš očekávat jako výstup

- `timestamp` místo `observation_date`,
- canonical `element`,
- raw `element_raw`,
- vlastní hodnoty ve `value`.

---

## 15. 10-minute observations – CZ

Aktuální konzervativni subset pro `CZ historical_csv 10min` zahrnuje zejména:

- `tas_mean`
- `tas_max`
- `tas_min`
- `soil_temperature_10cm`
- `soil_temperature_100cm`
- `sunshine_duration`

Příklad:

```bash
weatherdownload observations 10min --country CZ --station-id 0-20000-0-11406 --element tas_mean --element soil_temperature_10cm --start 2024-01-01T00:00:00Z --end 2024-01-01T00:20:00Z
```

---

## 16. Hourly observations – DE

Aktuální narrow verified slice pro `DE historical 1hour` zahrnuje canonical elementy:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

Příklad:

```bash
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element relative_humidity --element wind_speed --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
```

### 16.1 Důležitá časová poznámka pro DE hourly
Pro DWD subdaily data je implementované pravidlo:

- **před 2000-01-01**: čas se interpretuje jako `Europe/Berlin` a pak převádí do UTC,
- **od 2000-01-01**: čas se bere přímo jako UTC.

Veřejný výstup používá vždy timezone-aware UTC timestamp.

---

## 17. 10-minute observations – DE

Aktuální narrow verified slice pro `DE historical 10min` zahrnuje canonical elementy:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

Příklad:

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

### 18.3 Full-history mode v Python API

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

## 19. Exporty

### 19.1 Obecný tabulkový export

```python
from weatherdownload import export_table

export_table(df, "daily_output.parquet", format="parquet")
```

### 19.2 Poznámka k daily/hourly/10min exportům
Obecné observations exporty jsou **long-format**. To je dobré pro:
- obecnou analýzu,
- tidy data workflow,
- R / Python tabulkové zpracování.

---

## 20. Rozdíl mezi obecným observations downloaderem a FAO workflow

### 20.1 `weatherdownload observations ...`
Vrací **long-format** data:
- jeden řádek = jedna veličina pro jedno datum / čas,
- hodnota je ve sloupci `value`.

### 20.2 `examples/workflows/download_fao.py`
Vrací **wide-format workflow dataset** pro přípravu denních meteorologických vstupů pro pozdější FAO-orientované zpracování.

To znamená:
- jeden datumový řádek,
- vedle sebe canonical sloupce:
  - `date`
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `wind_speed`
  - `vapour_pressure`
  - `sunshine_duration`

---

## 21. Jak vypadá výstup z `download_fao.py`

FAO workflow nevrací obecný long-format downloader output, ale specializovaný wide-format bundle denních meteorologických vstupů pro pozdější FAO-orientované zpracování.

### 21.1 Export režim `parquet`
Vznikne bundle adresář typicky obsahující:

- `data_info.json`
- `stations.parquet`
- `series.parquet`

### 21.2 Co je v `data_info.json`

- creation timestamp,
- target country,
- canonical variables,
- minimum required number of complete days,
- number of retained stations,
- `provider_element_mapping`.

### 21.3 Co je v `stations.parquet`

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- screening summary

### 21.4 Co je v `series.parquet`

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- `date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

### 21.5 Co je garantováno
Po finálním filtru:
- zůstávají jen kompletní dny,
- canonical meteorologické sloupce nemají missing hodnoty.

---

## 22. MATLAB-oriented / workflow pro přípravu FAO vstupů

FAO example je workflow layer nad knihovnou pro balení denních meteorologických vstupů.

### 22.1 Co dělá
- načte metadata stanic,
- načte observation metadata,
- udělá country-aware screening stanic,
- stáhne potřebné daily vstupy,
- ponechá jen kompletní dny,
- exportuje finální dataset.

### 22.2 Co nedělá
- nepočítá FAO-56 ET0,
- nepočítá `Ra`,
- nepočítá další derived variables.

### 22.3 Canonical výstupní variables
Finální exportované variables používají canonical names:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

### 22.4 Provider provenance
Provider-specific mapping je uložený v:
- `data_info["provider_element_mapping"]`

---

## 23. FAO example – základní příkazy

### 23.1 CZ, full pipeline, Parquet

```bash
python examples/workflows/download_fao.py --country CZ --mode full --export-format parquet
```

### 23.2 DE, full pipeline, Parquet

```bash
python examples/workflows/download_fao.py --country DE --mode full --export-format parquet
```

### 23.3 Jen download/cache

```bash
python examples/workflows/download_fao.py --country CZ --mode download --cache-dir outputs/fao_cache
```

### 23.4 Build pouze z cache

```bash
python examples/workflows/download_fao.py --country CZ --mode build --cache-dir outputs/fao_cache --export-format both
```

---

## 24. Režimy FAO example

- `--mode full`
- `--mode download`
- `--mode build`

`full` = download + build + export  
`download` = jen cache  
`build` = jen z cache

---

## 25. Cache v FAO example

Typická cache struktura:

```text
outputs/fao_cache/
  meta1.csv
  meta2.csv
  daily/
    <station_id>/
      dly-<station_id>-<ELEMENT>.csv
```

Princip:
- když soubor existuje, nestrhává se znovu,
- `full` reuse-ne cache,
- `download` připraví raw vstupy,
- `build` dovolí build/export bez sítě.

---

## 26. Progress reporting a silent mode ve FAO example

Progress reporting ukazuje:
- screened candidate count,
- station progress,
- reuse/download summary,
- final summary.

Tichý režim:

```bash
python examples/workflows/download_fao.py --silent
```

---

## 27. Jak otevřít a zkontrolovat výstupy

### 27.1 Rychlý pohled na Parquet v Pythonu

```bash
python -c "import pandas as pd; df = pd.read_parquet('outputs/kopisty_daily.parquet'); print(df.head()); print(df.columns.tolist())"
```

### 27.2 Univerzální inspect utility

```bash
python scripts/dev/inspect_file.py outputs/kopisty_daily.parquet
python scripts/dev/inspect_file.py outputs/fao_daily.cz.mat
python scripts/dev/inspect_file.py outputs/fao_daily.cz
```

### 27.3 Co můžeš od `scripts/dev/inspect_file.py` očekávat

- path,
- file type,
- file size,
- last modification time,
- počet řádků a sloupců,
- názvy sloupců,
- datové typy,
- preview prvních řádků,
- summary `.mat` struktur,
- přehled bundle adresáře.

---

## 28. Typický use-case: chci denní teplotu a vítr pro jednu stanici

### CZ / Kopisty

```bash
weatherdownload observations daily --country CZ --station-id 0-20000-0-11433 --element tas_mean --element tas_max --element tas_min --element wind_speed --all-history --format parquet --output kopisty_daily.parquet
```

### Co čekat
Dostaneš **long-format** tabulku. To znamená:
- teplota a vítr nebudou jako 4 sloupce vedle sebe,
- ale jako řádky s různými hodnotami `element`.

Pokud chceš **wide-format** dataset pro další modelování, použij vlastní pivot nebo workflow example.

---

## 29. Typický use-case: chci dataset připravený pro pozdější FAO-orientované zpracování

### CZ

```bash
python examples/workflows/download_fao.py --country CZ --mode full --export-format parquet
```

### DE

```bash
python examples/workflows/download_fao.py --country DE --mode full --export-format parquet
```

### Co čekat
Výsledkem je **wide-format dataset bundle**. Hlavní data budou v `series.parquet` a budou mít canonical sloupce:

- `date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

---

## 30. Typický use-case: chci vědět, co je pro stanici a zemi dostupné

```bash
weatherdownload stations elements --country DE --station-id 00044 --dataset-scope historical --resolution daily --include-mapping
```

### Co čekat
Dostaneš přehled:
- canonical elementů,
- raw provider kódů,
- jejich mapování.

---

## 31. Typické chyby a jak je chápat

### „Why do I only see one element in the CSV?“
Protože jsi stáhl jen jeden element. Přidej více `--element`.

### „Where are the actual temperatures?“
V obecném observations downloaderu jsou ve sloupci `value`, ne ve sloupci `tas_mean`.

### „Why do I not see tas_mean, tas_max, tas_min as separate columns?“
Protože obecný observations downloader vrací long-format data.

### „I want everything available, why do I need a date range?“
Použij explicitní `--all-history`.

### „FAO example seems slow“
To je normální. Použij `--mode download`, pak `--mode build`, případně `--silent`.

---

## 32. Doporučený pracovní styl

### Pro exploraci
Používej:
- `stations metadata`
- `stations elements`
- `stations availability`
- `list_supported_elements(...)`
- `list_station_elements(...)`

### Pro obecné stahování
Používej:
- `weatherdownload observations ...`

### Pro konkrétní workflow
Používej:
- `examples/workflows/download_fao.py`

### Pro rychlou kontrolu výstupu
Používej:
- `scripts/dev/inspect_file.py`

---

## 33. Shrnutí

Nejdůležitější praktická věc je rozlišovat:

### A. Obecný observations downloader
- univerzální,
- long-format,
- flexibilní,
- vhodný pro obecnou analýzu.

### B. Workflow example (`download_fao.py`)
- specializovaný,
- wide-format,
- country-aware,
- vhodný jako čistý dataset denních meteorologických vstupů pro pozdější FAO-orientované nebo jiné navazující fyzikální či modelovací kroky.






