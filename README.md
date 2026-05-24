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
weatherdownload stations metadata --country HU --provider historical --format screen
weatherdownload observations daily --country DE --provider historical --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country HU --station-id 13704 --element tas_mean --element pressure --start 2026-01-01T00:00:00Z --end 2026-01-01T01:00:00Z
weatherdownload observations 10min --country NL --station-id 0-20000-0-06260 --element tas_mean --element pressure --start 2024-01-01T09:10:00Z --end 2024-01-01T09:20:00Z
```

Conceptual model:

- `country` selects the country
- `provider` selects the concrete data source or product within that country
- `resolution` selects the temporal resolution
- `element` selects the canonical meteorological variable

For the authoritative current matrix of implemented country/provider/resolution/element paths, see [Supported Capabilities](docs/supported_capabilities.md).

## Daily Data Coverage In Europe

<p align="center">
  <img src="docs/assets/europe_daily_coverage_map.svg"
       alt="Daily data coverage in Europe"
       width="900">
</p>

The map shows European countries for which WeatherDownload currently implements daily meteorological observation downloads. It is not a FAO-readiness map and does not imply that all variables are available at all stations.

- Dark green - national daily downloader implemented
- Light green - daily data available via GHCN-Daily
- Red - attempted, but no reliable daily support yet
- Gray - not attempted yet

Some non-European countries may also be supported, but they are not shown because this map focuses on Europe.

## Documentation

- conceptual provider model and terminology: [Provider Model](docs/providers.md)
- generated current capability table (checked in tests): [Supported Capabilities](docs/supported_capabilities.md)
- provider-specific source notes: [Provider Notes](docs/provider_notes/README.md)
- practical usage examples: [Examples And Workflows](docs/examples.md)
- normalized station and observation schemas: [Normalized Output Schemas](docs/output_schema.md)

