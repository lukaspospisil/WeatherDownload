# Data coverage

These maps show WeatherDownload implementation status for European meteorological observation downloads.

- They are not FAO-readiness maps.
- They do not imply that all variables are available in all countries or at all stations.
- National daily coverage means an official national daily downloader exists, not that every official provider exposes a broad variable set; for example, `BG / nimh / daily` is intentionally narrow and currently exposes only `precipitation` and `snow_depth`.
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
- Orange outline means an official national daily-provider audit was attempted and failed, while daily fallback data still exist.
- Dark green - national daily downloader implemented
- Light green - daily data available via GHCN-Daily fallback
- Orange - investigated with a research note, but no safe implemented daily provider exists
- Red - attempted in the project-status override sense, but no reliable daily support yet
- Gray - not attempted yet and no research note is linked
- Very light gray - geographic context outside the European coverage classification

This is daily-data coverage, not FAO-readiness coverage. It does not imply that all variables are available at all stations, and it does not imply that fallback-based countries have official national providers. `GHCN-Daily` fallback coverage is distinct from official national provider coverage. `Research-note-only` means the country was investigated but no safe provider was implemented. `Fallback + research note` means WeatherDownload can still download daily data there, but the official national-provider Gate 0 check failed.

### Researched Unresolved Countries

These countries have research notes but no implemented reliable daily downloader:

| Country | Research note |
| --- | --- |
| `KV` | [kv_ihmk_research.md](provider_notes/kv_ihmk_research.md) |
| `MC` | [mc_monaco_research.md](provider_notes/mc_monaco_research.md) |
| `SM` | [sm_san_marino_research.md](provider_notes/sm_san_marino_research.md) |
| `VA` | [va_vatican_research.md](provider_notes/va_vatican_research.md) |

### GHCN-Daily Fallback With Failed Official Audit

These countries still have daily data through `GHCN-Daily`, but an official national-provider Gate 0 audit failed:

| Country | Coverage | Research note |
| --- | --- | --- |
| `AL` | `ghcnd_daily` | [al_igjeum_research.md](provider_notes/al_igjeum_research.md) |
| `BA` | `ghcnd_daily` | [ba_meteo_bih_research.md](provider_notes/ba_meteo_bih_research.md) |
| `BY` | `ghcnd_daily` | [by_belhydromet_research.md](provider_notes/by_belhydromet_research.md) |
| `CY` | `ghcnd_daily` | [cy_meteo_cy_research.md](provider_notes/cy_meteo_cy_research.md) |
| `GB` | `ghcnd_daily` | [gb_metoffice_daily_research.md](provider_notes/gb_metoffice_daily_research.md) |
| `GR` | `ghcnd_daily` | [gr_hnms_research.md](provider_notes/gr_hnms_research.md) |
| `HR` | `ghcnd_daily` | [hr_dhmz_research.md](provider_notes/hr_dhmz_research.md) |
| `IT` | `ghcnd_daily` | [it_ispra_scia_research.md](provider_notes/it_ispra_scia_research.md) |
| `MD` | `ghcnd_daily` | [md_meteo_md_research.md](provider_notes/md_meteo_md_research.md) |
| `ME` | `ghcnd_daily` | [me_zhms_research.md](provider_notes/me_zhms_research.md) |
| `MK` | `ghcnd_daily` | [mk_uhmr_research.md](provider_notes/mk_uhmr_research.md) |
| `MT` | `ghcnd_daily` | [mt_malta_metoffice_research.md](provider_notes/mt_malta_metoffice_research.md) |
| `PT` | `ghcnd_daily` | [pt_ipma_research.md](provider_notes/pt_ipma_research.md) |
| `RS` | `ghcnd_daily` | [rs_rhmz_research.md](provider_notes/rs_rhmz_research.md) |
| `TR` | `ghcnd_daily` | [tr_mgm_research.md](provider_notes/tr_mgm_research.md) |
| `UA` | `ghcnd_daily` | [ua_ukrhydromet_research.md](provider_notes/ua_ukrhydromet_research.md) |

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
