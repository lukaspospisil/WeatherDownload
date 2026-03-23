# Slovakia Historical Daily Climate Investigation

Investigation date: 2026-03-23

## Summary

A stable machine-readable SHMU open-data path for validated historical daily climate observations could not be verified from the currently public SHMU sources that were probed.

As of this investigation:

- the public SHMU climate file tree exposes only `now/` and `recent/`
- the obvious `historical/` climate paths return `404 Not Found`
- the public SHMU CKAN catalog does not expose a validated historical daily climate dataset entry that can be tied to station observations

This means the next Slovakia milestone is currently blocked on source discovery and source validation, not on parser implementation.

## Exact Access Paths Checked

Public SHMU climate root:

- `https://opendata.shmu.sk/meteorology/climate/`

Observed result:

- Apache index listing with only:
  - `now/`
  - `recent/`

Direct historical path probes:

- `https://opendata.shmu.sk/meteorology/climate/historical/`
- `https://opendata.shmu.sk/meteorology/climate/historical/data/`
- `https://opendata.shmu.sk/meteorology/climate/historical/metadata/`

Observed result:

- all three returned `404 Not Found`

Reference recent-daily metadata path that is currently public:

- `https://opendata.shmu.sk/meteorology/climate/recent/metadata/kli_inter_metadata.json`

This is useful as a schema reference for the operational recent feed, but it is not evidence of a validated historical dataset.

SHMU CKAN catalog probed:

- dataset index page:
  - `https://opendata.shmu.dcmc.sk/dataset`
- package list API:
  - `https://opendata.shmu.dcmc.sk/api/3/action/package_list`
- organization package search:
  - `https://opendata.shmu.dcmc.sk/api/3/action/package_search?fq=organization%3Ashmu&rows=200&fl=name,title,notes,resources,tags,metadata_modified`

Observed result:

- the SHMU CKAN catalog exposes many datasets, but the returned package list is dominated by code lists and reference tables
- no validated historical daily climate station-observation dataset was identified in the catalog responses inspected during this investigation

## Delivery Format

Validated historical daily climate format:

- not verified

Publicly verified SHMU climate formats today:

- `recent / daily`: monthly JSON files plus JSON metadata
- `now`: public tree exists, but not part of this milestone

Because no public validated historical daily path was confirmed, the validated historical delivery format remains unknown.

## Station Identifier Stability

Validated historical station identifier scheme:

- not verified

What is source-backed today for the operational recent daily feed:

- station identifier field: `ind_kli`
- current WeatherDownload normalization: `station_id = ind_kli` as string

This may or may not carry over to a validated historical dataset. There is not enough public evidence yet to treat that as a stable historical-provider contract.

## Date Field Stability

Validated historical daily date field:

- not verified

What is source-backed today for the operational recent daily feed:

- date field: `datum`
- format observed: ISO-like `YYYY-MM-DD`

Again, this is only verified for the recent operational feed.

## Daily Element Coverage

Validated historical daily element inventory:

- not verified

Current source-backed reference from the recent-daily metadata JSON includes raw daily fields such as:

- `t_max`
- `t_min`
- `sln_svit`
- `zra_uhrn`
- `t7`, `t14`, `t21`
- `tlak_vod_par7`, `tlak_vod_par14`, `tlak_vod_par21`
- `vlh_rel7`, `vlh_rel14`, `vlh_rel21`
- `vie_smer7`, `vie_rych7`, `vie_smer14`, `vie_rych14`, `vie_smer21`, `vie_rych21`
- `tlak7`, `tlak14`, `tlak21`
- `obl7`, `obl14`, `obl21`
- additional precipitation, snow, fog, soil-surface, and weather-phenomena fields

Comparison against the current canonical schema:

Clearly source-backed from the recent feed:

- `tas_max` <- `t_max`
- `tas_min` <- `t_min`
- `sunshine_duration` <- `sln_svit`
- `precipitation` <- `zra_uhrn`

Not source-backed yet for a validated historical daily path:

- `tas_mean`
- `wind_speed`
- `vapour_pressure`
- `pressure`
- `relative_humidity`

Those may be derivable from term-based fields in the recent metadata, but that would be an implementation decision, not a source-backed validated-historical finding.

## Station Metadata Availability

An authoritative validated-historical station metadata source could not be identified during this investigation.

Current SHMU recent station discovery remains minimal and probe-derived from observation payloads. That is not sufficient to claim validated historical station metadata coverage.

## Update Cadence

Validated historical daily update cadence:

- not verified

Current source-backed cadence only for the recent operational feed:

- monthly JSON files under `recent/data/daily/YYYY-MM/`

That cadence should not be assumed for any future validated historical dataset.

## Likely Implementation Risks

1. No verified public historical endpoint.
   Without a stable URL contract, implementation would be speculative.

2. No verified historical metadata resource.
   Station names, coordinates, elevation, and authoritative coverage remain unresolved.

3. Field semantics may differ from the recent operational feed.
   Reusing `recent` field names for `historical` without source confirmation would be unsafe.

4. Update cadence and partitioning are unknown.
   The provider cannot safely assume monthly JSON, yearly files, or any specific archive layout.

5. Canonical mapping coverage is incomplete.
   Only a subset of daily canonical elements is currently source-backed from the public recent metadata.

## Recommendation For The Next Milestone

Before implementing a Slovakia validated historical daily provider path, first identify at least one authoritative public source that provides all of the following:

- a stable machine-readable historical daily access path
- a documented or inspectable historical element inventory
- a stable station identifier contract
- an authoritative station metadata source or explicit metadata limitations

Until that source is found, the SHMU provider should remain limited to the current experimental `SK / recent / daily` slice.
