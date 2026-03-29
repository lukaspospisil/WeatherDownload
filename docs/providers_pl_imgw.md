# IMGW-PIB Poland Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Poland as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `PL`
- provider: IMGW-PIB public meteorological archive
- station family: `dobowe / synop` only
- dataset scopes: `historical`
- resolutions: `daily`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes

## Official Source Paths Used

- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/Opis.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_nagłówek.csv`

The current implementation uses only the official IMGW-PIB daily synop archive layout and does not mix in `klimat`, `opad`, `terminowe`, `miesieczne`, or API-only live endpoints.

## Canonical Mapping

Supported canonical daily elements in this pass:

- `tas_mean` -> `STD`
- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `SMDB`
- `sunshine_duration` -> `USL`

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

## Station Identifier And Output Semantics

- `station_id` is the official 5-character IMGW station code from `wykaz_stacji.csv`, normalized as string
- `gh_id` carries the longer official IMGW station code published alongside that 5-character identifier
- daily queries stay date-based through `start_date` and `end_date`
- normalized daily outputs keep the shared WeatherDownload schema
- raw IMGW daily status fields such as `WTMAX`, `WTMIN`, `WSTD`, `WSMDB`, and `WUSL` stay in `flag`
- normalized `quality` stays null in this pass
- the provider uses deterministic archive URLs for completed years and current-year months; it does not depend on scraping directory listings to build download targets

## Archive Shape

The official `dobowe / synop` archive uses three source-backed file patterns, and the provider handles them internally behind the shared `historical / daily` public path:

- current year: monthly all-station ZIP archives such as `{year}_{month:02d}_s.zip`
- years `2001` through the previous year: one station-year ZIP archive per station such as `{year}_{station_code}_s.zip`
- years before `2001`: five-year station ZIP archives such as `{bucket_start}_{bucket_end}_{station_code}_s.zip`

WeatherDownload keeps that archive logic internal so the public API still uses the normal country / dataset scope / resolution / date-range query model.

## Current Limits

- only the official `dobowe / synop` family is implemented in this pass
- `dobowe / klimat` remains intentionally unsupported here because it would broaden source-family semantics without being required for the first stable slice
- `terminowe`, `miesieczne`, and `opad` remain intentionally unsupported in this pass
- hourly and 10-minute Poland support are not implemented
- station coordinates, elevation, and validity dates are not available from the implemented official station list and therefore stay missing in normalized station metadata
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no daily values are recomputed from other IMGW products

## Next Safe Extension

The next low-risk extension would be to inspect `dobowe / klimat` as a separate official daily family and add it only if it can be represented honestly without blurring the current synop-backed `historical / daily` slice.
