# Provider Model

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload uses a provider layer so the public API can stay stable while provider-specific logic remains internal.

## Navigation

- Current capability matrix: [Supported Capabilities](supported_capabilities.md)
- Practical usage patterns: [Examples And Workflows](examples.md)
- Normalized output columns: [Normalized Output Schemas](output_schema.md)
- Canonical element semantics: [Canonical Elements](canonical_elements.md)

## Public Model

Think in terms of:

- `country + provider + resolution + element`

Public discovery and download APIs use the same shape across countries:

- `read_station_metadata(country=...)`
- `read_station_observation_metadata(country=...)`
- `list_providers(country=...)`
- `list_dataset_scopes(country=...)`
- `list_resolutions(country=..., provider=...)`
- `list_supported_elements(country=..., provider=..., resolution=...)`
- `download_observations(...)`

Public query dimensions:

- `country` selects the country/provider context
- `provider` is the preferred public selector for the concrete source path
- `dataset_scope` remains accepted as a backward-compatible alias for `provider`
- `resolution` selects the temporal resolution within that provider path

Compatibility notes:

- normalized output tables still expose the `dataset_scope` column for backward compatibility
- provider values are provider-local names, not a universal cross-country taxonomy
- similar-looking names such as `historical`, `recent`, or `ghcnd` should always be interpreted within the selected country/provider path

Examples:

| `country` | `provider` | Meaning |
| --- | --- | --- |
| `CZ` | `historical_csv` | CHMI historical CSV source |
| `CH` | `historical` | MeteoSwiss historical station-data path |
| `HU` | `historical_wind` | HungaroMet wind-only 10-minute product |
| `PL` | `historical_klimat` | IMGW daily klimat path |
| `US` | `ghcnd` | NOAA GHCN-Daily source |

## Provider Families

WeatherDownload currently uses two broad provider families:

### National Providers

These use country-specific public source contracts and naming:

- `AT`, `BE`, `CH`, `CZ`, `DE`, `DK`, `HU`, `NL`, `PL`, `SE`, `SK`

Country-specific details such as raw codes, URLs, units, QC fields, and source caveats live in the provider notes linked below.

### Shared GHCN-Daily Wrappers

These are thin country wrappers around the shared implementation in `weatherdownload/providers/ghcnd/`:

- `US`, `CA`, `MX`, `FI`, `FR`, `IT`, `NO`, `NZ`

Shared GHCN-Daily characteristics:

- `provider="ghcnd"`
- currently implemented only for `resolution="daily"`
- wrappers keep raw GHCN station IDs as `station_id`
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`
- the wrappers stay thin while the parser/downloader logic lives in the shared GHCN provider package

## How To Discover Support

Use the discovery APIs when you need the current supported options:

```python
from weatherdownload import (
    list_dataset_scopes,
    list_providers,
    list_resolutions,
    list_supported_elements,
)

list_dataset_scopes(country="CZ")  # compatibility alias
list_providers(country="US")
list_resolutions(country="US", provider="ghcnd")
list_supported_elements(country="US", provider="ghcnd", resolution="daily")
```

The complete current matrix is documented in [Supported Capabilities](supported_capabilities.md).

## Open-Water Evaporation Boundary

The semantic boundary for `open_water_evaporation` is:

- measured open-water-surface evaporation
- evaporation-pan evaporation
- evaporimeter evaporation

It is not:

- ET0
- PET
- FAO reference evaporation
- modeled evaporation

Current support is intentionally narrow. The authoritative current availability list is in [Supported Capabilities](supported_capabilities.md), and the semantic mapping details are in [Canonical Elements](canonical_elements.md).

Important constraints that should stay clear across the docs:

- `CZ / historical_csv / daily` is supported via CHMI raw `VY`
- `SK / recent / daily` is supported via SHMU raw `voda_vypar`
- `US / ghcnd / daily` is supported via NOAA raw `EVAP`
- `CA`, `MX`, `FI`, `FR`, `IT`, `NO`, and `NZ` GHCN wrappers do not expose `open_water_evaporation`
- `CH` FAO reference evaporation is not `open_water_evaporation`

## CLI Notes

The CLI mirrors the provider model:

- `--country` selects the country
- `--provider` is the preferred selector
- `--dataset-scope` remains a backward-compatible alias

Examples:

```powershell
weatherdownload observations daily --country CZ --provider historical_csv --station-id 0-20000-0-11406 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country US --provider ghcnd --station-id USC00000001 --element tas_max --start-date 2020-05-01 --end-date 2020-05-03
weatherdownload stations elements --country CH --station-id AIG --provider historical --resolution daily
```

For the full current list of provider/resolution/element combinations, use [Supported Capabilities](supported_capabilities.md).

## Provider Notes

- [GeoSphere Austria Provider Notes](providers_at_geosphere.md)
- [RMI/KMI Belgium Provider Notes](providers_be_rmi.md)
- [MeteoSwiss Switzerland Provider Notes](providers_ch_meteoswiss.md)
- [DMI Denmark Provider Notes](providers_dk_dmi.md)
- [HungaroMet Hungary Provider Notes](providers_hu_hungaromet.md)
- [KNMI Netherlands Provider Notes](providers_nl_knmi.md)
- [IMGW-PIB Poland Provider Notes](providers_pl_imgw.md)
- [SMHI Sweden Provider Notes](providers_se_smhi.md)
- [Experimental Slovakia Provider Notes](providers_sk_experimental.md)
- [NOAA / GHCN-Daily Provider Notes](providers_us_noaa_ghcnd.md)
- [NOAA / GHCN-Daily Canada Provider Notes](providers_ca_noaa_ghcnd.md)
- [NOAA / GHCN-Daily Mexico Provider Notes](providers_mx_noaa_ghcnd.md)
- [NOAA / GHCN-Daily Direct-Prefix Country Wrapper Notes](providers_ghcnd_direct_prefix_wrappers.md)
- [France, Spain, And Australia Open-Water Evaporation Audit](providers_fr_es_au_open_water_evaporation_audit.md)
