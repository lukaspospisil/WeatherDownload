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
- `provider` selects the concrete provider-specific dataset, product, or source
- `resolution` selects the temporal resolution
- provider values are not globally standardized across countries

Examples:

| `country` | `provider` | Meaning |
| --- | --- | --- |
| `CA` | `ghcnd` | NOAA GHCN-Daily source |
| `CZ` | `ghcnd` | NOAA GHCN-Daily wrapper with raw GHCN station ids |
| `CZ` | `historical_csv` | CHMI OpenData `historical_csv` product |
| `FI` | `ghcnd` | NOAA GHCN-Daily source |
| `FR` | `ghcnd` | NOAA GHCN-Daily source |
| `IT` | `ghcnd` | NOAA GHCN-Daily source |
| `MX` | `ghcnd` | NOAA GHCN-Daily source |
| `NO` | `ghcnd` | NOAA GHCN-Daily source |
| `NZ` | `ghcnd` | NOAA GHCN-Daily source |
| `SK` | `recent` | SHMU recent daily JSON source |
| `HU` | `historical_wind` | HungaroMet special 10-minute wind product |
| `PL` | `historical_klimat` | IMGW daily klimat source |
| `US` | `ghcnd` | NOAA GHCN-Daily source |

Notes:

- `provider` is the only public selector name in Python and CLI
- normalized output tables use the `provider` column
- the shared NOAA GHCN-Daily implementation lives in `weatherdownload/providers/ghcnd/`, while `US`, `CA`, `MX`, `CZ`, `FI`, `FR`, `IT`, `NO`, and `NZ` stay thin country wrappers with raw GHCN station ids preserved as `station_id`
- GHCN-Daily station support is inventory-driven, so not every station exposes `tas_mean` (`TAVG`) or `snow_depth` (`SNWD`); inspect a specific station with `weatherdownload stations elements --country US --provider ghcnd --station-id USC00000001 --resolution daily`

See [Supported Capabilities](docs/supported_capabilities.md) for the generated country/provider/resolution/element overview, and [Provider Model](docs/providers.md) for the conceptual explanation.

## Where To Start

- Provider model and terminology: [Provider Model](docs/providers.md)
- Generated current capability table from the registry/discovery APIs: [Supported Capabilities](docs/supported_capabilities.md)
- Shared usage examples and workflow entry points: [Examples And Workflows](docs/examples.md)
- Normalized station and observation schemas: [Normalized Output Schemas](docs/output_schema.md)
- FAO-oriented daily packaging example: [FAO-Oriented Daily Input Packaging Workflow](docs/download_fao.md)

## Docs Index

- [Provider Model](docs/providers.md)
- [Supported Capabilities](docs/supported_capabilities.md)
- [Provider Notes](docs/provider_notes/README.md)
- [Examples And Workflows](docs/examples.md)
- [Normalized Output Schemas](docs/output_schema.md)
- [Canonical Elements](docs/canonical_elements.md)
- [Changelog](docs/changelog.md)

Provider-specific notes are indexed in [Provider Notes](docs/provider_notes/README.md).

## Stable Cross-Country Invariants

- public API shape stays the same across providers
- `station_id` stays canonical per provider path
- canonical meteorological element names stay shared across countries
- normalized output schemas stay stable and DataFrame-first
- missing variables stay missing by default instead of being silently derived
