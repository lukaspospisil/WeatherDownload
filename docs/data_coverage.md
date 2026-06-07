# Data coverage

These maps show WeatherDownload implementation status for European meteorological observation downloads.

- They are not FAO-readiness maps.
- They do not imply that all variables are available at all stations.
- They do not imply broad element coverage in every official provider; for example, `BG / nimh / daily` is an intentionally narrow official daily path that currently exposes only `precipitation` and `snow_depth`.
- They reflect WeatherDownload implementation status, not general public data availability in each country.
- They distinguish implemented download coverage from documented official-provider audit status where that audit status is known.
- Non-European land inside the current viewport is shown only as neutral geographic context and is not part of the coverage classification.

## Daily data coverage in Europe

<p align="center">
  <img src="assets/europe_daily_coverage_map.svg"
       alt="Daily data coverage in Europe"
       width="900">
</p>

- Fill color shows implemented daily download coverage.
- Orange outline means an official-provider audit was attempted and documented as failed, while daily data are still available through fallback coverage.
- Dark green - national daily downloader implemented
- Light green - daily data available via GHCN-Daily fallback
- Orange - investigated with a research note, but no safe implemented daily provider exists
- Red - attempted in the project-status override sense, but no reliable daily support yet
- Gray - not attempted yet and no research note is linked
- Very light gray - geographic context outside the European coverage classification

This is daily-data coverage, not FAO-readiness coverage. It does not imply that all variables are available at all stations, and it does not imply that fallback-based countries have official national providers. `GHCN-Daily` fallback coverage is distinct from official national providers. `Research-note-only` means the country was investigated but no safe provider was implemented. `Fallback + research note` means WeatherDownload can still download daily data there, but the official national-provider Gate 0 check failed.

## Hourly data coverage in Europe

<p align="center">
  <img src="assets/europe_hourly_coverage_map.svg"
       alt="Hourly data coverage in Europe"
       width="900">
</p>

- Fill color shows implemented hourly download coverage.
- Dark green - national hourly downloader implemented
- Red - attempted, but no reliable hourly support yet
- Gray - not attempted yet
- Very light gray - geographic context outside the European coverage classification

## 10-minute data coverage in Europe

<p align="center">
  <img src="assets/europe_10min_coverage_map.svg"
       alt="10-minute data coverage in Europe"
       width="900">
</p>

- Fill color shows implemented 10-minute download coverage.
- Dark green - national 10-minute downloader implemented
- Red - attempted, but no reliable 10-minute support yet
- Gray - not attempted yet
- Very light gray - geographic context outside the European coverage classification
