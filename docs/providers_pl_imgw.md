# IMGW-PIB Poland Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Poland as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `PL`
- provider: IMGW-PIB public meteorological archive
- implemented source families:
  - `historical / daily` backed by `dobowe / synop`
  - `historical / 1hour` backed by `terminowe / synop`
  - `historical_klimat / daily` backed by `dobowe / klimat`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes
- hourly downloads: yes

## Architectural Decision

`terminowe / synop` is exposed as `historical / 1hour`, not as a separate PL-specific dataset scope.

That is the cleanest fit for the existing WeatherDownload architecture because it is the same official IMGW synop station family as the implemented `historical / daily` slice, just published at subdaily cadence. In contrast, `dobowe / klimat` remains separate as `historical_klimat / daily` because it is a different IMGW station family with different archive grouping and a smaller field set.

## Official Source Paths Used

- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/Opis.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/s_d_nagďż˝ďż˝wek.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/s_t_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/s_t_nagďż˝ďż˝wek.csv`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/k_d_format.txt`
- `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/k_d_nagďż˝ďż˝wek.csv`
- `https://danepubliczne.imgw.pl/pl/apiinfo`
- `https://danepubliczne.imgw.pl/api/data/meteo`

## Canonical Mapping

Supported canonical daily elements for `historical / daily` (`dobowe / synop`):

- `tas_mean` -> `STD`
- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `SMDB`
- `sunshine_duration` -> `USL`

Rejected daily synop candidates after inspecting the official `s_d_format.txt` field list:

- `FF10` was not mapped because IMGW defines it as duration of wind `>=10 m/s` in hours, not daily mean wind speed
- `FF15` was not mapped because IMGW defines it as duration of wind `>15 m/s` in hours, not daily maximum wind speed
- no daily `relative_humidity` field is published in the implemented synop family
- no daily `vapour_pressure` field is published in the implemented synop family

Supported canonical hourly elements for `historical / 1hour` (`terminowe / synop`):

- `tas_mean` -> `TEMP`
- `wind_speed` -> `FWR`
- `wind_speed_max` -> `PORW`
- `relative_humidity` -> `WLGW`
- `vapour_pressure` -> `CPW`
- `pressure` -> `PPPS`

Rejected hourly synop candidates after inspecting the official `s_t_format.txt` field list:

- `WO6G` was not mapped because IMGW defines it as precipitation over 6 hours, not hourly precipitation
- `USLN` was left unsupported in this first hourly slice because the official documentation says the hour-label semantics changed in March 2015 and are tied to local solar time rather than a simple shared UTC-hour convention
- `TPTR` was left unsupported because this pass keeps the exposed subdaily element core limited to the already used cross-country practical set
- ambiguous cloud, snow, present-weather, and coded-state fields remain unsupported

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
- hourly queries stay timestamp-based through `start` and `end`
- normalized daily and hourly outputs keep the shared WeatherDownload schemas
- raw IMGW status fields stay in `flag` when present in the implemented source family
- normalized `quality` stays null in this pass
- latitude and longitude are enriched only when the official IMGW `api/data/meteo` feed exposes an exact `kod_stacji == gh_id` match for the station
- unmatched stations keep null `latitude` and `longitude`
- `elevation_m` and validity dates remain missing because the implemented official IMGW metadata sources do not publish clean joinable values for them
- the provider uses deterministic archive URLs and does not depend on scraping directory listings to build download targets

## Timestamp And Quality Caveats

- `historical / 1hour` timestamps are built from the published `ROK`, `MC`, `DZ`, and `GG` fields and are treated as UTC in the implemented slice because the archive is structured as synoptic term observations and the official format files do not document a competing local civil-time convention
- this UTC treatment is an implementation assumption from the official source structure, not a provider-side conversion to a new meteorological meaning
- the official `s_t_format.txt` notes that hourly `USLN` changed labeling semantics in March 2015 and uses local solar time, which is why this first hourly slice leaves `sunshine_duration` unsupported
- the official `Opis.txt` warns that psychrometric data for terms `01,02,04,05,07,08,10,11,13,14,16,17,19,20,22,23` before 1994 are of questionable quality; this caveat is relevant to humidity-related hourly fields such as `WLGW`, `CPW`, and `TPTR`

## Why `10min` Is Still Unsupported

I inspected the official IMGW public archive families and the official public API listings to check whether Poland has a clean meteorological `10min` path that fits the shared WeatherDownload model.

The clean answer is still no for this provider pass:

- the implemented official IMGW archive tree exposes `dobowe` and `terminowe` families cleanly for the supported daily and hourly synop slices
- the official `api/data/meteo` feed does publish some latest-value fields whose names include `10min`, such as `opad_10min_data` and `wiatr_poryw_10min_data`
- but that feed is not a clean historical `10min` observation archive and does not expose one row = one shared 10-minute timestamp across variables
- instead, the official feed carries separate variable-specific timestamps like `temperatura_powietrza_data`, `wiatr_srednia_predkosc_data`, `wilgotnosc_wzgledna_data`, `opad_10min_data`, and `wiatr_poryw_10min_data`, and those timestamps can differ within the same station row

Because of that source shape, exposing `api/data/meteo` as `PL / 10min` would be misleading. It would force WeatherDownload to invent a fake common `timestamp` or to silently mix variables observed at different moments into one normalized 10-minute record.

This provider therefore leaves `PL / 10min` unsupported until IMGW exposes a clean official 10-minute meteorological archive or a source-backed feed with one honest subdaily timestamp model.

## Archive Shape

The official `dobowe / synop` archive uses three source-backed file patterns, and WeatherDownload keeps that logic behind the public `historical / daily` path:

- current year: monthly all-station ZIP archives such as `{year}_{month:02d}_s.zip`
- years `2001` through the previous year: one station-year ZIP archive per station such as `{year}_{station_code}_s.zip`
- years before `2001`: five-year station ZIP archives such as `{bucket_start}_{bucket_end}_{station_code}_s.zip`

The official `terminowe / synop` archive uses a simpler hourly station-archive pattern, and WeatherDownload keeps that logic behind the public `historical / 1hour` path:

- years `2001` and later: one station-year ZIP archive per station such as `{year}_{station_code}_s.zip`
- years before `2001`: five-year station ZIP archives such as `{bucket_start}_{bucket_end}_{station_code}_s.zip`

The official `dobowe / klimat` archive uses a different source-backed pattern, and WeatherDownload exposes that distinction through the separate `historical_klimat / daily` path:

- years `2001` and later: monthly all-station ZIP archives such as `{year}_{month:02d}_k.zip`
- years before `2001`: yearly all-station ZIP archives inside five-year bucket directories, such as `{bucket_start}_{bucket_end}/{year}_k.zip`

## FAO-Prep Workflow Note

`examples/workflows/download_fao.py` uses the synop-backed `historical / daily` slice by default for Poland. It prepares a daily meteorological input bundle for later FAO-oriented workflows, does not compute FAO-56 ET0, keeps `wind_speed` and `vapour_pressure` missing in the default PL daily branch because the official synop daily fields `FF10` and `FF15` are duration-of-threshold wind indicators rather than wind-speed observations and the implemented daily IMGW families do not publish daily relative humidity or vapour pressure, and may include station latitude/longitude only when the official IMGW `api/data/meteo` feed exposes an exact station-code match. `elevation_m` still remains missing because no clean official joinable source was found for the implemented slices.

With the explicit workflow fill policy `--fill-missing allow-hourly-aggregate`, the FAO example may supplement:

- daily `wind_speed` from official `historical / 1hour` IMGW `wind_speed`
- daily `vapour_pressure` from official `historical / 1hour` IMGW `vapour_pressure`

The current workflow uses arithmetic means over the UTC calendar day and requires at least 18 hourly observations before filling either daily field. Supplemented values are marked explicitly as `aggregated_hourly_opt_in` in workflow provenance outputs. This stays in the example layer only; it does not change provider semantics and does not hide that these fields are aggregated from hourly observations.

The `historical / 1hour` slice adds official subdaily observations only. It does not compute FAO-56 ET0, and any workflow-layer daily supplementation remains explicit and documented rather than hidden inside the provider.

## Current Limits

- `historical / daily` remains synop-only and unchanged
- `historical / 1hour` is conservative and currently exposes only `tas_mean`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, and `pressure`
- `historical_klimat / daily` exposes only the clearly mappable practical core subset: `tas_mean`, `tas_max`, `tas_min`, and `precipitation`
- `terminowe / synop` is not exposed as hidden provider-side daily aggregation; the current FAO example may use it only through the explicit opt-in `allow-hourly-aggregate` workflow policy with documented provenance
- `10min` remains intentionally unsupported because the official IMGW public surfaces inspected in this pass do not expose a clean historical meteorological 10-minute dataset with one honest shared timestamp model
- `miesieczne` and `opad` remain intentionally unsupported in this pass
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no daily values are recomputed from other IMGW products

## Next Safe Extension

The next low-risk extension would be either:

- an official IMGW historical 10-minute meteorological archive or feed with one clean timestamp model per row, if IMGW publishes one later
- or further explicit workflow-layer daily aggregation from the implemented `historical / 1hour` synop slice when that aggregation is documented separately and kept distinct from source-observed daily values

