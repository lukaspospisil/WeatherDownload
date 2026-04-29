# FR Meteo-France Daily Provider Design

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note proposes a conservative first implementation slice for a France national provider based on the official Meteo-France daily climatological base files.

Scope for this note:

- country `FR`
- one new France national provider design only
- no implementation in this pass
- no schema changes
- no change to existing `provider="ghcnd"` behavior

## Recommendation Summary

- Recommended provider token: `meteo_france`
- Recommended first resolution: `daily`
- Recommended first-slice canonical elements:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `precipitation`
  - optional for the same first implementation if file strategy stays simple: `wind_speed`
- Explicitly out of scope for the first slice:
  - `open_water_evaporation`
  - derived evapotranspiration products
  - `sunshine_duration`
  - `relative_humidity`
  - `vapour_pressure`
  - `snow_depth`

The main design decision is to start from the `RR-T-Vent` daily file family because it already contains the clearest and most useful WeatherDownload mappings without requiring a second parameter-family download.

## Current Architecture Fit

The existing provider architecture already supports this cleanly:

- `weatherdownload.providers.base.WeatherProvider` expects:
  - station metadata reader
  - station observation metadata reader
  - dataset spec registry
  - observation downloader
- country providers may expose multiple provider tokens for the same country
- `weatherdownload.providers.ghcnd.mixed` already supports a mixed national-plus-GHCN country model
- `weatherdownload.core.queries.ObservationQuery` and the CLI already resolve by:
  - `country`
  - `provider`
  - `resolution`

This means France can move from:

- current: `FR` with `provider="ghcnd"`

to:

- `provider="meteo_france"` for national daily data
- `provider="ghcnd"` preserved as the existing fallback

without changing the public model.

## Source Structure

### Official daily dataset

Official source:

- Meteo-France daily climatological base dataset on data.gouv.fr
- URL: https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes/

Observed structure from the dataset page and documentation files:

- public compressed CSV downloads
- files are split by:
  - department
  - period lot
  - parameter family
- update cadence:
  - annual for history before 1950
  - monthly for 1950 through year minus two
  - daily for the last two years
- dataset documentation exposes at least two field dictionaries:
  - `Q_descriptif_champs_RR-T-Vent.csv`
  - `Q_descriptif_champs_autres-parametres.csv`

### Daily field families relevant here

`Q_descriptif_champs_RR-T-Vent.csv` includes:

- `RR`
- `TN`
- `TX`
- `TM`
- `FFM`
- other wind-extreme fields not needed for the first slice

`Q_descriptif_champs_autres-parametres.csv` includes:

- `INST`
- `UM`
- `TSVM`
- `HNEIGEF`
- `NEIGETOTX`
- `NEIGETOT06`
- `ETPMON`
- `ETPGRILLE`
- other fields not needed for the first slice

### Station metadata dataset

Official source:

- Meteo-France station metadata on data.gouv.fr
- URL: https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees/

Observed structure:

- official GeoJSON metadata dataset
- large full file plus smaller subset file
- dataset is intended to expose station characteristics for mainland and overseas stations

Additional field hints from the related station-metadata ecosystem on data.gouv.fr strongly suggest the core fields include:

- 8-digit Meteo-France station identifier
- station name / long name
- latitude
- longitude
- altitude
- opening date
- closing date
- department identifier
- daily/hourly/minutely capability flags

For the first implementation, we should design against the minimal metadata fields needed by WeatherDownload and verify exact field names during implementation with a small fixture.

## Provider Token

Recommended token:

- `provider="meteo_france"`

Why this token is the best fit:

- it identifies the concrete national source, not just a generic daily product
- it leaves room for later France products if needed
- it matches existing WeatherDownload style better than a product-only label such as `daily_climatology`
- it is clearer to users than a French-language token like `base_quotidienne`

Alternatives considered:

- `daily_climatology`
  - too generic if France later adds multiple Meteo-France daily products
- `climatology_daily`
  - awkward and still generic
- `base_quotidienne`
  - source-authentic but less consistent with the repo's mostly source-oriented provider names

## First-Slice Supported Elements

Recommended first slice:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`

Recommended optional fifth element only if we keep the same single-file-family strategy:

- `wind_speed`

Why these are safe:

- all are present in the `RR-T-Vent` daily field dictionary
- all are direct raw observations or direct daily summaries
- unit conversions are clear
- they do not require cross-file derivation

Deferred elements:

- `sunshine_duration`
  - raw field `INST` looks clear, but it lives in the second parameter-family file
- `relative_humidity`
  - raw field `UM` looks clear, but also lives in the second file family
- `vapour_pressure`
  - raw field `TSVM` looks promising, but also lives in the second file family
- `snow_depth`
  - snow fields exist, but should wait for exact semantic choice between fresh snow and total snow-on-ground
- `open_water_evaporation`
  - not supported; available evapotranspiration products are not equivalent

## Raw-to-Canonical Mapping

Recommended first-slice mapping table:

| Canonical element | Raw field | Source definition | Raw unit | Canonical unit | Conversion | Missing handling |
| --- | --- | --- | --- | --- | --- | --- |
| `precipitation` | `RR` | 24-hour precipitation amount assigned to day J | mm and 1/10 | mm | divide by 10 | provider missing/null sentinel to `NA` |
| `tas_min` | `TN` | daily minimum air temperature under shelter | deg C and 1/10 | deg C | divide by 10 | provider missing/null sentinel to `NA` |
| `tas_max` | `TX` | daily maximum air temperature under shelter | deg C and 1/10 | deg C | divide by 10 | provider missing/null sentinel to `NA` |
| `tas_mean` | `TM` | daily mean of hourly air temperatures under shelter | deg C and 1/10 | deg C | divide by 10 | provider missing/null sentinel to `NA` |
| `wind_speed` | `FFM` | daily mean wind speed from 10-minute means at 10 m | m/s and 1/10 | m/s | divide by 10 | provider missing/null sentinel to `NA` |

Important boundaries:

- `tas_mean` should map only from raw `TM`
- do not derive `tas_mean` from `TN` and `TX`
- do not map `TNTXM` to `tas_mean` in the first slice
- do not map `ETPMON` or `ETPGRILLE` to `open_water_evaporation`
- do not map `HNEIGEF` or `NEIG` to snowfall because no canonical snowfall element exists

### Deferred but plausible later mappings

These look promising but should stay out of the first slice:

| Canonical element | Raw field | Reason deferred |
| --- | --- | --- |
| `sunshine_duration` | `INST` | second parameter-family file required |
| `relative_humidity` | `UM` | second file family required |
| `vapour_pressure` | `TSVM` | second file family required |
| `snow_depth` | `NEIGETOT06` or `NEIGETOTX` | semantic choice should be explicit before implementation |

## Station Metadata Strategy

### Station ID convention

Use the native 8-digit Meteo-France station identifier as `station_id`.

Why:

- it is official
- it is already stable and unique in Meteo-France materials
- it avoids inventing a synthetic WeatherDownload identifier
- it keeps local provider station IDs separate from GHCN station IDs

`gh_id` strategy:

- leave `gh_id` empty unless a verified crosswalk is available
- do not attempt fuzzy GHCN matching in the first slice

### Metadata fields to expose

Use normal WeatherDownload station metadata columns:

- `station_id`
- `gh_id`
- `begin_date`
- `end_date`
- `full_name`
- `longitude`
- `latitude`
- `elevation_m`

Suggested additional provider-local fields can remain internal unless needed later:

- department code
- station open/closed flag
- daily capability flag

### Which stations to include

Recommendation:

- include only stations that are marked as daily-capable and are public in the official metadata, if those flags are present and reliable
- do not include every Meteo-France station if the metadata contain stations with no daily product

This keeps station discovery aligned with the first supported product.

### Opening and closing dates

Recommendation:

- map station open date to `begin_date`
- map station close date to `end_date`
- if a station is open and no end date is present, normalize to an open-ended current timestamp convention already used in the repo

### Duplicate names and station changes

Recommendation:

- treat `station_id` as the identity, not `full_name`
- duplicate names are acceptable
- if a station changes name over time, the metadata record should still follow the current official station identifier

## Download Strategy

### First implementation strategy

Recommendation:

- first implementation should consume the `RR-T-Vent` family only
- use:
  - station metadata dataset for station discovery
  - one or more daily observation `csv.gz` files for downloads

### How files should be selected

Recommended runtime behavior:

- use station metadata first to determine the station's department code
- resolve the relevant department file set
- fetch only the period files that overlap the requested date range

This is preferable to downloading a nationwide archive.

### Historical vs recent segmentation

The source is already segmented by period and update cycle. The implementation should mirror that instead of trying to create a synthetic single archive.

### Caching

Recommendation:

- do not require a persistent local cache for v1
- do support local `source_url` overrides for fixtures and controlled debugging
- if multiple files must be fetched for one request, use in-memory request/session reuse within a single call

If real-world performance later becomes a problem, add a dedicated cache in a separate pass.

### File sizes and repeated downloads

The dataset includes files ranging from small KB-scale resources to multi-MB `csv.gz` files. A conservative first slice should avoid repeated huge downloads by:

- selecting only the needed department
- selecting only the needed period lots
- filtering rows by station ID and date as early as possible during parsing

### Local source override

Recommendation:

- keep the existing `source_url` override pattern for tests
- for FR this likely needs to accept:
  - a local metadata file path
  - a local directory root containing a small fixture set for the daily files

That pattern is already common in other providers and is the safest route for tests.

## Availability Strategy

### Provider-level support

For `list_supported_elements(country='FR', provider='meteo_france', resolution='daily')`:

- return only first-slice canonical elements
- optionally include mapping to raw fields from the registry

### Station metadata

Use the official station metadata dataset as the primary source.

### Station observation metadata

Recommendation:

- build station observation metadata from station metadata plus provider-level supported raw fields
- do not scan full national observation history to infer per-station availability in v1

Why this is acceptable for the first slice:

- the source claims all parameters in the file family are provided for the relevant meteorological stations
- a first conservative implementation can treat `RR-T-Vent` support as station-family support for the selected daily-capable stations

Important caveat:

- this is weaker than the inventory-driven GHCN model
- if implementation testing shows many station-level gaps by element inside the same family, the design should switch to a more explicit inventory strategy before release

### Station elements and station finder

Initial strategy:

- `list_station_elements` for `provider='meteo_france'` should return the first-slice canonical set for included FR daily stations
- `find_stations_with_elements` should work from this provider-level station-family assumption, not from full file scans

This keeps the first slice small and aligned with current architecture.

### Observation downloads

Downloads should:

- accept native `station_id`
- resolve the relevant department/period files
- read matching rows for requested stations and dates
- normalize only the requested canonical elements

## Testing Strategy

Recommended fixtures:

- small FR station metadata fixture
  - a few stations
  - include daily-capable and non-daily-capable examples if the metadata support that distinction
- small FR daily observation fixture for the `RR-T-Vent` family
  - ideally compressed `csv.gz`
  - a few rows for:
    - `RR`
    - `TN`
    - `TX`
    - `TM`
    - `FFM`
- optional tiny mapping/dictionary fixture
  - only if parser logic depends on local copies of descriptive files

Recommended tests:

- provider registration for `FR`
  - `provider='meteo_france'`
  - `provider='ghcnd'` still present
- supported elements
  - first-slice canonical set only
- station metadata parsing
  - 8-digit station ID preserved
  - coordinates and elevation parsed
  - open/close dates normalized
- station elements
  - daily-capable stations expose the expected first-slice elements
- observation parsing
  - station/date filtering
  - raw-to-canonical reshaping
- unit conversion
  - all tenths-based conversions divide by 10
- missing values
  - provider sentinel values normalize to `NA`
- CLI
  - `weatherdownload stations elements --country FR --provider meteo_france --resolution daily ...`
  - `weatherdownload observations daily --country FR --provider meteo_france ...`
- public contracts
  - output schema unchanged

## Documentation Strategy

If implementation proceeds later, add:

- provider note:
  - `docs/provider_notes/fr_meteo_france.md`
- supported capabilities update
- examples update with:
  - `country='FR'`
  - `provider='meteo_france'`
  - `resolution='daily'`

Provider note should explicitly state:

- first slice is daily only
- native 8-digit Meteo-France station IDs are used
- first slice does not support `open_water_evaporation`
- `tas_mean` comes from raw `TM`, not from derived `TN`/`TX`

## Main Risks

### Medium risk: dataset partitioning details

The main unresolved implementation detail is the exact file naming and URL resolution for:

- department partition
- period partition
- parameter-family partition

This should be validated with one or two real sample resource URLs during implementation before the parser is written.

### Medium risk: exact station metadata field names

The official station metadata dataset is clearly suitable, but the exact GeoJSON property names still need a fixture-backed check.

### Medium risk: station-level availability assumption

The smallest design assumes that daily-capable Meteo-France stations in the selected product family expose the same first-slice variables. If real sample files contradict that, station observation metadata may need a stronger per-station inventory pass.

### Low risk: canonical mappings for the first slice

The first-slice mappings for `RR`, `TN`, `TX`, `TM`, and optionally `FFM` are straightforward and low-risk.

## Recommended Implementation Order

1. Add FR registry/spec for `provider='meteo_france'`, `resolution='daily'`
2. Parse official station metadata into standard WeatherDownload station metadata columns
3. Add FR daily fixture directory and local-source override strategy
4. Implement daily `RR-T-Vent` parser with:
   - `RR`
   - `TN`
   - `TX`
   - `TM`
   - optionally `FFM`
5. Add mixed FR provider wrapper preserving `ghcnd`
6. Add tests and docs

## Sources

- Meteo-France daily dataset:
  - https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes/
- Daily field dictionary:
  - https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_descriptif_champs_RR-T-Vent.csv
- Other-parameters field dictionary:
  - https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_descriptif_champs_autres-parametres.csv
- Station metadata dataset:
  - https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees/
