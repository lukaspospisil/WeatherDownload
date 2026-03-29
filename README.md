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
    dataset_scope="historical_csv",
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
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country HU --station-id 13704 --element tas_mean --element pressure --start 2026-01-01T00:00:00Z --end 2026-01-01T01:00:00Z
weatherdownload observations 10min --country NL --station-id 0-20000-0-06260 --element tas_mean --element pressure --start 2024-01-01T09:10:00Z --end 2024-01-01T09:20:00Z
```

## Coverage Snapshot

| Country | `daily` | `1hour` | `10min` | `download_fao` | Status |
| --- | --- | --- | --- | --- | --- |
| `AT` | Yes* | Yes* | Yes* | Yes | Stable |
| `BE` | Yes* | Yes* | Yes* | Yes | Stable |
| `CZ` | Yes | Yes | Yes | Yes | Stable |
| `DE` | Yes | Yes* | Yes* | Yes | Stable |
| `DK` | Yes* | Yes* | Yes* | Yes | Stable |
| `HU` | Yes* | Yes* | Yes* | Yes | Stable |
| `NL` | Yes* | Yes* | Yes* | Yes | Stable |
| `SE` | Yes* | Yes* | No | Yes | Stable |
| `SK` | Yes* | No | No | No | Experimental |

`Yes*` means the path is implemented, but element coverage or dataset scope is intentionally conservative for that provider slice. See [Provider Model And Coverage](docs/providers.md) and the provider-specific notes for exact limits.

Hungary also exposes a separate wind-only `historical_wind / 10min` capability alongside the generic `historical / 10min` path; see the [HungaroMet Hungary Provider Notes](docs/providers_hu_hungaromet.md) for details.

## Supported Countries

- `AT` via GeoSphere Austria
- `BE` via RMI/KMI open-data AWS platform
- `CZ` via CHMI
- `DE` via DWD
- `DK` via DMI open-data APIs
- `HU` via HungaroMet open data on `odp.met.hu`
- `NL` via KNMI Data Platform
- `SE` via SMHI Meteorological Observations API
- `SK` via SHMU OpenDATA (experimental, currently limited to `recent / daily`)

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
- [DMI Denmark Provider Notes](docs/providers_dk_dmi.md)
- [HungaroMet Hungary Provider Notes](docs/providers_hu_hungaromet.md)
- [KNMI Netherlands Provider Notes](docs/providers_nl_knmi.md)
- [SMHI Sweden Provider Notes](docs/providers_se_smhi.md)
- [Experimental Slovakia Provider Notes](docs/providers_sk_experimental.md)
- [Changelog](docs/changelog.md)

## Stable Cross-Country Invariants

- public API shape stays the same across providers
- `station_id` stays canonical per provider path
- canonical meteorological element names stay shared across countries
- normalized output schemas stay stable and DataFrame-first
- missing variables stay missing by default instead of being silently derived
