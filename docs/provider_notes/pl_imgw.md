# IMGW-PIB Poland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current IMGW-PIB public archive integration for Poland. It keeps the shared provider model intact while making the source-family boundaries explicit.

## Provider identifiers

- country: `PL`
- provider: `historical`, `historical_klimat`
- backward-compatible `dataset_scope`: `historical`, `historical_klimat`
- resolution(s):
  - `historical`: `daily`, `1hour`
  - `historical_klimat`: `daily`

## Source

- archive description: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/Opis.txt`
- station list: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/wykaz_stacji.csv`
- synop daily archive: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/`
- synop hourly archive: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/`
- klimat daily archive: `https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/`
- API references: `https://danepubliczne.imgw.pl/pl/apiinfo`, `https://danepubliczne.imgw.pl/api/data/meteo`

## Station identifiers

- `station_id` is the official 5-character IMGW station code from `wykaz_stacji.csv`
- `gh_id` carries the longer official IMGW station code published alongside that identifier
- latitude and longitude are enriched only when the official `api/data/meteo` feed exposes an exact `kod_stacji == gh_id` match
- `elevation_m` and validity dates remain unavailable on the implemented official metadata paths

## Supported data

Current source-backed mapping includes:

- `historical / daily` (`dobowe / synop`): `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`
- `historical / 1hour` (`terminowe / synop`): `tas_mean`, `wind_speed`, `wind_speed_max`, `relative_humidity`, `vapour_pressure`, `pressure`
- `historical_klimat / daily` (`dobowe / klimat`): `tas_mean`, `tas_max`, `tas_min`, `precipitation`

Unsupported or ambiguous source fields stay unsupported rather than being guessed. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official IMGW values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- `terminowe / synop` is exposed as `historical / 1hour`, not as a separate PL-specific provider token
- `historical_klimat / daily` stays separate because it is a different station family with different coverage and field semantics
- hourly timestamps are built from `ROK`, `MC`, `DZ`, and `GG` and treated as UTC in the implemented slice
- `quality` remains null; raw status fields stay in `flag` when present
- `10min` remains intentionally unsupported because the inspected official IMGW public surfaces do not expose one honest historical 10-minute timestamp model across variables
- `open_water_evaporation` is intentionally unsupported because the implemented official field lists do not publish a clearly documented measured open-water, pan, or evaporimeter evaporation field

## Examples

```powershell
weatherdownload stations elements --country PL --provider historical --resolution 1hour --include-mapping
```

```powershell
weatherdownload stations elements --country PL --provider historical_klimat --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
- [MATLAB-Oriented FAO Workflow](../download_fao.md)
