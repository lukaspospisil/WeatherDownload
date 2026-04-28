# WeatherDownload

[![CI](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml/badge.svg)](https://github.com/lukaspospisil/WeatherDownload/actions/workflows/ci.yml)

<p align="right">
  <img src="docs/images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload is a DataFrame-first Python library for country-aware weather metadata, discovery, and source-backed observation downloads through a unified interface. It keeps the public API, canonical `station_id`, canonical element names, and normalized output schemas stable across providers while leaving unsupported fields missing instead of silently deriving them.

## Install

```powershell
pip install .
```

Optional export dependencies:

```powershell
pip install .[full]
```

`NL` also requires `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` for KNMI Open Data API access.

## Quick Start

Python:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="CZ",
    provider="historical_csv",
    resolution="daily",
    station_ids=["0-20000-0-11406"],
    start_date="2024-01-01",
    end_date="2024-01-10",
    elements=["tas_mean", "tas_max", "tas_min"],
)

observations = download_observations(query)
```

CLI:

```powershell
weatherdownload stations metadata --country HU --format screen
weatherdownload observations daily --country DE --provider historical --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country HU --station-id 13704 --element tas_mean --element pressure --start 2026-01-01T00:00:00Z --end 2026-01-01T01:00:00Z
weatherdownload observations 10min --country NL --station-id 0-20000-0-06260 --element tas_mean --element pressure --start 2024-01-01T09:10:00Z --end 2024-01-01T09:20:00Z
```

Conceptual model:

- `country` selects the country/provider context
- `provider` is the preferred public name for the concrete provider-specific dataset, product, or source
- `dataset_scope` is kept as a backward-compatible alias for `provider`
- `resolution` selects the temporal resolution
- provider values are not globally standardized across countries

Examples:

| `country` | `provider` | Meaning |
| --- | --- | --- |
| `CZ` | `historical_csv` | CHMI OpenData `historical_csv` product |
| `CA` | `ghcnd` | NOAA GHCN-Daily source |
| `SK` | `recent` | SHMU recent daily JSON source |
| `HU` | `historical_wind` | HungaroMet special 10-minute wind product |
| `PL` | `historical_klimat` | IMGW daily klimat source |
| `US` | `ghcnd` | NOAA GHCN-Daily source |

Compatibility note:

- existing Python and CLI usage with `dataset_scope` or `--dataset-scope` remains valid
- normalized output tables still keep the `dataset_scope` column for backward compatibility

See [Provider Model And Coverage](docs/providers.md) for the full country-by-country matrix and exact supported scope names.

## Coverage Snapshot

| Country | `daily` | `1hour` | `10min` | `download_fao` | Status |
| --- | --- | --- | --- | --- | --- |
| `AT` | Yes* | Yes* | Yes* | Yes | Stable |
| `BE` | Yes* | Yes* | Yes* | Yes | Stable |
| `CA` | Yes* | No | No | No | Stable |
| `CH` | Yes* | Yes* | Yes* | Yes | Stable |
| `CZ` | Yes | Yes | Yes | Yes | Stable |
| `DE` | Yes | Yes* | Yes* | Yes | Stable |
| `DK` | Yes* | Yes* | Yes* | Yes | Stable |
| `HU` | Yes* | Yes* | Yes* | Yes | Stable |
| `NL` | Yes* | Yes* | Yes* | Yes | Stable |
| `PL` | Yes* | Yes* | No | Yes | Stable |
| `SE` | Yes* | Yes* | No | Yes | Stable |
| `SK` | Yes* | No | No | No | Experimental |
| `US` | Yes* | No | No | No | Stable |

`Yes*` means the path is implemented, but element coverage or dataset scope is intentionally conservative for that provider slice. See [Provider Model And Coverage](docs/providers.md) and the provider-specific notes for exact limits.

Hungary also exposes a separate wind-only `historical_wind / 10min` capability alongside the generic `historical / 10min` path; see the [HungaroMet Hungary Provider Notes](docs/providers_hu_hungaromet.md) for details.

In the FAO-prep workflow, Poland can optionally supplement missing daily `wind_speed` and `vapour_pressure` from official hourly IMGW synop observations via `--fill-missing allow-hourly-aggregate`; see [FAO-Oriented Daily Input Packaging Workflow](docs/download_fao.md) for the explicit provenance rules.

## Supported Countries

- `AT` via GeoSphere Austria
- `BE` via RMI/KMI open-data AWS platform
- `CA` via NOAA NCEI GHCN-Daily (currently limited to `ghcnd / daily`)
- `CH` via MeteoSwiss A1 automatic weather stations
- `CZ` via CHMI
- `DE` via DWD
- `DK` via DMI open-data APIs
- `HU` via HungaroMet open data on `odp.met.hu`
- `NL` via KNMI Data Platform
- `PL` via IMGW-PIB public synop archives (`daily` and `1hour`) plus a separate daily `klimat` scope
- `SE` via SMHI Meteorological Observations API
- `SK` via SHMU OpenDATA (experimental, currently limited to `recent / daily`)
- `US` via NOAA NCEI GHCN-Daily (currently limited to `ghcnd / daily`)

## Where To Start

- Provider coverage, supported elements, dataset scopes, and country caveats: [Provider Model And Coverage](docs/providers.md)
- Shared usage examples and workflow entry points: [Examples And Workflows](docs/examples.md)
- Normalized station and observation schemas: [Normalized Output Schemas](docs/output_schema.md)
- FAO-oriented daily packaging example: [FAO-Oriented Daily Input Packaging Workflow](docs/download_fao.md)

## Docs Index

- [Provider Model And Coverage](docs/providers.md)
- [Examples And Workflows](docs/examples.md)
- [Normalized Output Schemas](docs/output_schema.md)
- [Canonical Elements](docs/canonical_elements.md)
- [GeoSphere Austria Provider Notes](docs/providers_at_geosphere.md)
- [RMI/KMI Belgium Provider Notes](docs/providers_be_rmi.md)
- [NOAA / GHCN-Daily Canada Provider Notes](docs/providers_ca_noaa_ghcnd.md)
- [MeteoSwiss Switzerland Provider Notes](docs/providers_ch_meteoswiss.md)
- [DMI Denmark Provider Notes](docs/providers_dk_dmi.md)
- [HungaroMet Hungary Provider Notes](docs/providers_hu_hungaromet.md)
- [KNMI Netherlands Provider Notes](docs/providers_nl_knmi.md)
- [IMGW-PIB Poland Provider Notes](docs/providers_pl_imgw.md)
- [SMHI Sweden Provider Notes](docs/providers_se_smhi.md)
- [Experimental Slovakia Provider Notes](docs/providers_sk_experimental.md)
- [NOAA / GHCN-Daily Provider Notes](docs/providers_us_noaa_ghcnd.md)
- [Changelog](docs/changelog.md)

## Stable Cross-Country Invariants

- public API shape stays the same across providers
- `station_id` stays canonical per provider path
- canonical meteorological element names stay shared across countries
- normalized output schemas stay stable and DataFrame-first
- missing variables stay missing by default instead of being silently derived
