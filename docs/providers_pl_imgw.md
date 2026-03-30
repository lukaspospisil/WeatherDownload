# IMGW-PIB Poland Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Poland as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `PL`
- provider: IMGW-PIB public meteorological archive
- implemented daily families:
  - `historical / daily` backed by `dobowe / synop`
  - `historical_klimat / daily` backed by `dobowe / klimat`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes

## Architectural Decision

`dobowe / klimat` is not merged into the existing synop-backed `historical / daily` slice.

It is exposed as a separate PL-specific dataset scope because it is a different IMGW station family with different archive grouping, different publication cadence, and a smaller daily field set. Treating it as the same public slice as `dobowe / synop` would blur discovery and make `PL / historical / daily` misleading for stations and elements that are only available in one family.

## Official Source Paths Used

- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/Opis.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_nag³ówek.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/k_d_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/k_d_nag³ówek.csv`

## Canonical Mapping

Supported canonical daily elements for `historical / daily` (`dobowe / synop`):

- `tas_mean` -> `STD`
- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `SMDB`
- `sunshine_duration` -> `USL`

Supported canonical daily elements for `historical_klimat / daily` (`dobowe / klimat`):

- `tas_mean` -> `STD`
- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `SMDB`

Rejected candidates after inspecting the official `k_d_format.txt` field list:

- `TMNG` was not mapped because IMGW defines it as minimum daily air temperature near the ground, which is not clearly the same as the repo's existing `ground_temperature_min` meaning
- `ROOP` was not mapped because it is precipitation form/type (`S/W/ `), not an existing canonical daily element
- `PKSN` is intentionally left unsupported in the library even though the source publishes it, to keep the practical exposed daily element core more uniform across countries and provider slices
- `sunshine_duration` remains unsupported because the official daily klimat product still does not publish `USL` or another clearly equivalent sunshine field

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

## Station Identifier And Output Semantics

- `station_id` is the official 5-character IMGW station code from `wykaz_stacji.csv`, normalized as string
- `gh_id` carries the longer official IMGW station code published alongside that 5-character identifier
- daily queries stay date-based through `start_date` and `end_date`
- normalized daily outputs keep the shared WeatherDownload schema
- raw IMGW daily status fields such as `WTMAX`, `WTMIN`, `WSTD`, `WSMDB`, and `WUSL` stay in `flag` when present in the source family
- normalized `quality` stays null in this pass
- the provider uses deterministic archive URLs and does not depend on scraping directory listings to build download targets

## Archive Shape

The official `dobowe / synop` archive uses three source-backed file patterns, and WeatherDownload keeps that logic behind the public `historical / daily` path:

- current year: monthly all-station ZIP archives such as `{year}_{month:02d}_s.zip`
- years `2001` through the previous year: one station-year ZIP archive per station such as `{year}_{station_code}_s.zip`
- years before `2001`: five-year station ZIP archives such as `{bucket_start}_{bucket_end}_{station_code}_s.zip`

The official `dobowe / klimat` archive uses a different source-backed pattern, and WeatherDownload exposes that distinction through the separate `historical_klimat / daily` path:

- years `2001` and later: monthly all-station ZIP archives such as `{year}_{month:02d}_k.zip`
- years before `2001`: yearly all-station ZIP archives inside five-year bucket directories, such as `{bucket_start}_{bucket_end}/{year}_k.zip`

## FAO-Prep Workflow Note

`examples/workflows/download_fao.py` now uses only the synop-backed `historical / daily` slice for Poland. It prepares a daily meteorological input bundle for later FAO-oriented workflows, does not compute FAO-56 ET0, keeps `wind_speed` and `vapour_pressure` missing in the current PL branch, and leaves station coordinates and elevation missing because the implemented official IMGW station list does not provide clean source-backed values for those fields.

## Current Limits

- `historical / daily` remains synop-only and unchanged
- `historical_klimat / daily` exposes only the clearly mappable practical core subset: `tas_mean`, `tas_max`, `tas_min`, and `precipitation`
- `terminowe`, `miesieczne`, and `opad` remain intentionally unsupported in this pass
- hourly and 10-minute Poland support are not implemented
- station coordinates, elevation, and validity dates are not available from the implemented official station list and therefore stay missing in normalized station metadata
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no daily values are recomputed from other IMGW products

## Next Safe Extension

The next low-risk extension would be to inspect whether any additional official IMGW daily families can be represented honestly as separate PL-specific dataset scopes without blurring the current synop and klimat distinctions.

