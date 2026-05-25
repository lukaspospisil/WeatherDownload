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

## Data Coverage

WeatherDownload currently implements daily meteorological observation downloads
for several European countries. The map below shows daily-data implementation
status in Europe.

<p align="center">
  <img src="docs/assets/europe_daily_coverage_map.svg"
       alt="Daily data coverage in Europe"
       width="900">
</p>

This is daily-data coverage, not FAO-readiness coverage, and it does not imply
that all variables are available at all stations. Neutral land outside the
European coverage set is shown only as geographic context.

Legend: dark green = national daily downloader, light green = GHCN-Daily,
red = attempted but no reliable daily support yet, gray = not attempted yet,
very light gray = context land outside the coverage classification.

For daily, hourly, and 10-minute coverage maps, see
[Data Coverage](docs/data_coverage.md).

## Documentation

- conceptual provider model and terminology: [Provider Model](docs/providers.md)
- generated current capability table (checked in tests): [Supported Capabilities](docs/supported_capabilities.md)
- European data coverage maps: [Data Coverage](docs/data_coverage.md)
- provider-specific source notes: [Provider Notes](docs/provider_notes/README.md)
- practical usage examples: [Examples And Workflows](docs/examples.md)
- MAPE 2026 CZ FAO comparison workflow: [MAPE 2026 CZ FAO Workflow](docs/download_fao_mape2026.md)
- normalized station and observation schemas: [Normalized Output Schemas](docs/output_schema.md)

