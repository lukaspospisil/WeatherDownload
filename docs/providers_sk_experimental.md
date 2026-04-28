# Experimental Slovakia Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Scope

The Slovakia provider is currently an experimental SHMU OpenDATA integration.

Its scope is intentionally limited to:

- country: `SK`
- provider: SHMU OpenDATA
- preferred public selector: `provider="recent"`
- backward-compatible alias: `dataset_scope="recent"`
- station observations only
- dataset scope: `recent`
- resolution: `daily`

This provider does not currently implement a validated historical climate download path.

## Official Source

WeatherDownload uses the public SHMU OpenDATA paths documented below for this experimental slice.

## Implemented Paths

Implemented SHMU endpoints:

- metadata description JSON:
  - `https://opendata.shmu.sk/meteorology/climate/recent/metadata/kli_inter_metadata.json`
- recent daily data index:
  - `https://opendata.shmu.sk/meteorology/climate/recent/data/daily/`
- recent daily monthly files:
  - `https://opendata.shmu.sk/meteorology/climate/recent/data/daily/YYYY-MM/kli-inter - YYYY-MM.json`

The current implementation treats these monthly JSON files as the only supported SHMU observation source.

Implemented public path in this pass:

- `country="SK"`, `dataset_scope="recent"`, `resolution="daily"`
- `country="SK"`, `provider="recent"`, `resolution="daily"`

## Supported Canonical Elements

Current canonical-to-raw mappings for `SK / recent / daily`:

- `tas_max` -> `t_max`
- `tas_min` -> `t_min`
- `sunshine_duration` -> `sln_svit`
- `precipitation` -> `zra_uhrn`
- `open_water_evaporation` -> `voda_vypar`

`open_water_evaporation` on this SHMU path is mapped only because the official metadata JSON documents `voda_vypar` as water evaporation in `mm`, and the current monthly JSON payloads expose that raw field directly. It is treated as measured water-surface evaporation for the published 7:00 to 7:00 daily interval, not as ET0, PET, or another modeled evapotranspiration field.

Other SHMU raw fields remain unmapped unless their semantics are explicitly verified.

## Station Metadata

The current SHMU provider does not expose an authoritative station metadata source equivalent to the CHMI and DWD station metadata layers.

As a result, `read_station_metadata(country="SK")` currently derives only minimal station coverage from sampled observation payloads.

Currently unavailable as authoritative normalized metadata:

- station name
- latitude
- longitude
- elevation
- secondary identifier analogous to `gh_id`

Current minimal station discovery behavior:

- `station_id`: derived from SHMU `ind_kli`
- `begin_date`: derived per station from that station's minimum `datum` in the current sampled `recent / daily` payload
- `end_date`: derived per station from that station's maximum `datum` in the current sampled `recent / daily` payload
- `gh_id`: null
- `full_name`: null
- `longitude`: null
- `latitude`: null
- `elevation_m`: null

These `begin_date` and `end_date` values describe only the coverage visible in the currently sampled recent payload. They are not authoritative historical station coverage bounds.

`read_station_observation_metadata(country="SK")` is also probe-derived from sampled payloads plus SHMU metadata JSON. It is useful for discovery, but it is not an authoritative availability registry.

## Quality And Flags

Current handling is intentionally conservative:

- `flag` is always null in the implemented `SK / recent / daily` path
- `quality` is always null in the implemented `SK / recent / daily` path
- WeatherDownload does not infer SHMU QC semantics that are not explicitly documented in the current source path

## Shared Interface Example

Use the normal shared interface:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="SK",
    provider="recent",
    resolution="daily",
    station_ids=["11800"],
    start_date="2025-01-01",
    end_date="2025-01-02",
    elements=["tas_max", "precipitation"],
)

observations = download_observations(query)
```

## Known Limitations

The current SHMU provider has the following explicit limitations:

- `SK` support is experimental
- only `recent / daily` is implemented
- only the five canonical elements listed above are implemented
- `all_history` is not implemented
- daily files are discovered from directory listings and monthly JSON file names

Operational distinction:

- implemented now: recent operational daily JSON data from SHMU OpenDATA
- not implemented yet: validated historical climate data with a stable long-term archive contract

## Open Questions

The following points remain unresolved and should be validated before broadening the provider:

- whether SHMU exposes an authoritative station registry with stable names and coordinates in a public machine-readable form
- whether SHMU provides a distinct validated historical climate archive separate from the `recent` operational feed
- whether additional SHMU daily fields have semantics that are precise enough for canonical mapping without local derivation
- whether SHMU exposes documented quality or flag fields for the recent daily JSON feed
- whether the `now / aws1min` file name timestamp encodes snapshot time, publication time, or another operational marker

## What Is Intentionally Not Implemented Yet

The following are intentionally out of scope for the current SHMU provider:

- SHMU hourly downloading
- SHMU 10-minute or 1-minute downloading
- SHMU full-history mode
- SHMU FAO workflow integration
- guessed metadata enrichment from unofficial sources
- canonical mappings for fields whose semantics are still ambiguous

## Next Implementation Milestones

Maintainer-oriented next steps, in order:

Reference investigation note:

- [Slovakia Historical Daily Climate Investigation](providers_sk_historical_investigation.md)


1. Identify and validate an authoritative SHMU station metadata source.
2. Add a validated historical SHMU climate path if a stable public source exists.
3. Revisit quality and flag normalization once SHMU field semantics are documented.
4. Revisit subdaily SHMU support only after timestamp semantics are confirmed.
5. Expand canonical element mappings only after the raw field semantics are explicitly verified.

