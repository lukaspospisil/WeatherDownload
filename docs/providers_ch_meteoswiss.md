# MeteoSwiss Switzerland Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Switzerland as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `CH`
- provider: MeteoSwiss A1 automatic weather stations only
- dataset scopes: `historical`
- resolutions: `daily`, `1hour`, `10min`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes
- hourly downloads: yes
- 10-minute downloads: yes

## Official Source Paths Used

- `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn`
- `https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/{station_id}`
- `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv`
- `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv`
- `https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_datainventory.csv`

The implementation uses only the official MeteoSwiss A1 automatic weather station collection and the station assets linked from that STAC collection.

## Canonical Mapping

Supported canonical daily elements in this pass:

- `tas_mean` -> `tre200d0`
- `tas_max` -> `tre200dx`
- `tas_min` -> `tre200dn`
- `precipitation` -> `rre150d0`
- `wind_speed` -> `fkl010d0`
- `wind_speed_max` -> `fkl010d1`
- `relative_humidity` -> `ure200d0`
- `vapour_pressure` -> `pva200d0`
- `pressure` -> `prestad0`
- `sunshine_duration` -> `sre000d0`

Supported canonical hourly elements in this pass:

- `tas_mean` -> `tre200h0`
- `precipitation` -> `rre150h0`
- `wind_speed` -> `fkl010h0`
- `wind_speed_max` -> `fkl010h1`
- `relative_humidity` -> `ure200h0`
- `vapour_pressure` -> `pva200h0`
- `pressure` -> `prestah0`
- `sunshine_duration` -> `sre000h0`

Supported canonical 10-minute elements in this pass:

- `tas_mean` -> `tre200s0`
- `precipitation` -> `rre150z0`
- `wind_speed` -> `fkl010z0`
- `wind_speed_max` -> `fkl010z1`
- `relative_humidity` -> `ure200s0`
- `vapour_pressure` -> `pva200s0`
- `pressure` -> `prestas0`
- `sunshine_duration` -> `sre000z0`

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

`open_water_evaporation` is intentionally unsupported on the implemented MeteoSwiss A1 slice. The official `ogd-smn_meta_parameters.csv` currently exposes FAO reference-evaporation parameters such as `erefaod0`, but those are reference evapotranspiration style products, not measured open-water or pan evaporation.

## Station Identifier And Output Semantics

- `station_id` is the official MeteoSwiss A1 `station_abbr`, normalized as string
- `gh_id` carries the official MeteoSwiss `station_wigos_id`
- daily queries stay date-based through `start_date` and `end_date`
- hourly and 10-minute queries stay timestamp-based through `start` and `end`
- normalized daily and subdaily outputs keep the shared WeatherDownload schemas
- raw `flag` stays null on the implemented MeteoSwiss A1 slice
- normalized `quality` stays null in this pass
- hourly and 10-minute timestamps are treated as UTC because the official MeteoSwiss A1 open-data assets publish UTC reference timestamps
- daily precipitation keeps the official MeteoSwiss A1 `6 UTC -> 6 UTC following day` daily window semantics behind the provider layer
- station pressure is mapped from the official MeteoSwiss A1 station-pressure fields (`prestad0`, `prestah0`, `prestas0`)

## Current Limits

- only the official MeteoSwiss A1 automatic weather station product is used in this pass
- Swiss A2, A3, homogeneous climate-series products, and other MeteoSwiss collections remain intentionally unsupported
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no hourly data are recomputed from 10-minute files
- no daily aggregates are recomputed from subdaily source files
- raw radiation and other non-cleanly mappable A1 fields remain unsupported

## Next Safe Extension

The next low-risk extension would be to evaluate one additional MeteoSwiss ground-station product family, such as A2 precipitation stations, only if it can be represented honestly as a separate provider slice without weakening the current public model.
