# Meteo-France France

France currently exposes two daily providers:

- `provider="meteo_france"` for the national Meteo-France daily climatological base slice documented here
- `provider="ghcnd"` for the shared NOAA GHCN-Daily wrapper documented separately

## Provider identifiers

- country: `FR`
- provider: `meteo_france`
- resolution: `daily`

## Source

- official daily dataset: `https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes/`
- RR-T-Vent field dictionary: `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_descriptif_champs_RR-T-Vent.csv`
- station metadata dataset: `https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees/`
- station metadata file: `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/METADONNEES_STATION/fiches.json`

## Station identifiers

- `station_id` is the native 8-digit Meteo-France station identifier
- `gh_id` stays empty on this provider
- station-level element availability comes from official station metadata `parametres`, not from a full archive scan

## V1 scope

The first implementation slice uses only the official daily `RR-T-Vent` family.

Supported canonical mappings:

- `RR` -> `precipitation`
- `TN` -> `tas_min`
- `TX` -> `tas_max`
- `TM` -> `tas_mean`

## Units and normalization

- observed RR-T-Vent sample files already serialize these fields as decimal values in the canonical output units
- `RR` normalizes to `mm`
- `TN`, `TX`, and `TM` normalize to `deg C`
- missing source values become null normalized `value`
- raw quality columns such as `QRR` are not exposed in normalized `quality` in this v1 slice

Important boundary:

- `tas_mean` comes only from raw `TM`
- this provider does not derive `tas_mean` from `TN` and `TX`

## Unsupported in this slice

- `open_water_evaporation`
- `wind_speed`
- `sunshine_duration`
- `relative_humidity`
- `vapour_pressure`
- `snow_depth`
- ETP/PET/ET0 or Penman-Monteith style evaporation products

`open_water_evaporation` is intentionally unsupported here because the verified public daily Meteo-France evaporation-like fields are evapotranspiration products rather than measured open-water or pan evaporation.

## File selection strategy

- files are selected by station department and requested period bucket
- the current RR-T-Vent naming pattern is:
  - `Q_{department}_avant-1949_RR-T-Vent.csv.gz`
  - `Q_{department}_previous-1950-{YYYY}_RR-T-Vent.csv.gz`
  - `Q_{department}_latest-{YYYY}-{YYYY}_RR-T-Vent.csv.gz`
- WeatherDownload fetches only the department and period files needed for the requested station IDs and date range

## Examples

```powershell
weatherdownload stations elements --country FR --provider meteo_france --station-id 07005001 --resolution daily
```

```powershell
weatherdownload observations daily --country FR --provider meteo_france --station-id 07005001 --start-date 2025-01-01 --end-date 2025-01-02 --element tas_mean --element precipitation
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
