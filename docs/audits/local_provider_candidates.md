# Local Provider Candidate Audit

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note audits official national or local sources for countries that are currently covered mainly or only through NOAA GHCN-Daily wrappers.

Scope:

- CA / Canada
- FI / Finland
- FR / France
- IT / Italy
- NO / Norway
- NZ / New Zealand
- MX / Mexico

Constraints for this pass:

- audit only
- no provider implementation in this note
- no new canonical elements assumed
- `provider="ghcnd"` remains the current fallback where already implemented

## Architecture Fit

The current provider architecture is already compatible with a mixed national-plus-GHCN model:

- `weatherdownload.providers.base.WeatherProvider` expects station metadata, station observation metadata, dataset specs, and an observation downloader.
- `weatherdownload.providers.ghcnd.mixed` already provides helpers for combining a national provider with `provider="ghcnd"` under one country.
- Existing countries such as `AT`, `CH`, `CZ`, and `SK` demonstrate that pattern.

This means a new country-specific provider can be added conservatively while preserving the existing GHCN path.

## Candidate Summary

| Country | Classification | Official source | Format / access | Credentials | Notes |
| --- | --- | --- | --- | --- | --- |
| `FR` | `ready_to_implement` | Meteo-France daily climatological base data | Bulk `csv.gz` files by department and period on data.gouv.fr | No | Best first target for a daily-only slice. |
| `FI` | `promising_but_needs_design` | Finnish Meteorological Institute Open Data | WFS 2.0 / stored queries / XML-GML | No | Official and strong, but WFS parsing and stored-query design add complexity. |
| `CA` | `promising_but_needs_design` | Environment and Climate Change Canada GeoMet climate collections | OGC API / GeoJSON-style collections | No | Official and clean, but documented station subset limits need design care. |
| `NO` | `promising_but_needs_design` | MET Norway Frost API | REST JSON API | Yes, client ID | Excellent API, but authentication makes it a less frictionless first target. |
| `MX` | `promising_but_needs_design` | CONAGUA SIH climatological stations | Per-station CSV historical files + station catalog | No clear login seen | Promising bulk access, but schema and stability need deeper design validation. |
| `NZ` | `not_ready` | NIWA National Climate Database / DataHub / legacy CliFlo | Account-gated portal, delayed archive access | Yes, account/login | Public automation story is not clean enough for this library right now. |
| `IT` | `no_clean_source_found` | No clear single national daily station archive verified | Fragmented / unclear | Unclear | Do not implement without a better official national source. |

## Country Notes

### `FR` / France

Source:

- Meteo-France daily climatological base dataset on data.gouv.fr
- Meteo-France station metadata on data.gouv.fr

Evidence:

- Daily climatological data are published as compressed CSV downloads by department and period.
- Update cadence is documented: annual for pre-1950 history, monthly for 1950 through year minus two, and daily for the last two years.
- Station metadata are published separately as GeoJSON.
- Licensing is open and the source is official.

Likely canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- likely `wind_speed`
- likely `vapour_pressure`
- likely `sunshine_duration`

Station metadata:

- yes
- dedicated metadata dataset
- explicit 8-digit station identifiers documented in the metadata dataset

Implementation complexity:

- moderate
- bulk-file parser and dataset partitioning logic
- likely easiest first daily-only implementation among the audited countries

Main risks:

- file volume
- field dictionary validation still needed before coding
- per-station element availability may need station-observation metadata derived from files rather than a separate inventory endpoint

### `FI` / Finland

Source:

- Finnish Meteorological Institute Open Data

Evidence:

- Official open data WFS service with stored queries.
- Time-series observations are exposed through WFS requests.
- Daily and monthly station-specific values are part of the published open-data set catalog.
- The service is machine-readable and public.
- Request limits are documented.

Likely canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- likely `wind_speed`
- likely `vapour_pressure`
- possibly `snow_depth`

Station metadata:

- likely yes through station query patterns / WFS metadata

Station ID convention:

- needs design validation from actual station query responses

Implementation complexity:

- medium to high
- WFS/stored-query discovery and XML-GML handling are the main cost

Main risks:

- parsing complexity
- selecting the right stored queries for station metadata vs daily data
- daily archive semantics need to be pinned down before implementation

### `CA` / Canada

Source:

- Environment and Climate Change Canada GeoMet climate collections

Evidence:

- Official climate station metadata collection exists.
- Official daily climate observation collection exists.
- Daily collection exposes clear fields for mean/max/min temperature, precipitation, snow on ground, snowfall, and wind gust information.
- The station collection includes station metadata and daily/hourly coverage dates.

Likely canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `snow_depth`
- possibly `wind_speed` via daily gust-only fields is not a clean match for the current canonical `wind_speed`

Station metadata:

- yes
- clear station metadata collection

Station ID convention:

- likely `CLIMATE_IDENTIFIER` and/or `STN_ID`
- needs a deliberate public station ID choice

Implementation complexity:

- medium
- API is clean, but station identity, paging, and completeness limits need design work

Main risks:

- the published daily collection explicitly states that only a subset of total stations is shown due to size limitations
- that may conflict with WeatherDownload's preference for complete station discovery

### `NO` / Norway

Source:

- MET Norway Frost API

Evidence:

- Official REST API for historical weather and climate data.
- `sources/` provides source metadata.
- `observations/availableTimeSeries/` provides station-level element availability.
- `observations/` returns data in JSON.
- A client ID is required even for open data access.

Likely canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `snow_depth`
- likely `wind_speed`
- possibly `vapour_pressure`

Station metadata:

- yes via `sources/`

Station ID convention:

- Frost `sourceId`, typically `SN<number>` or `SN<number>:0`

Implementation complexity:

- medium
- the API itself is strong, but element mapping and auth UX need careful design

Main risks:

- credential requirement adds friction
- some data selection requires handling `timeOffset`, `timeResolution`, and default sensor filtering carefully

### `MX` / Mexico

Source:

- CONAGUA SIH climatological station pages
- CONAGUA open-data station catalog

Evidence:

- Official station catalog is published.
- The SIH climate page lists a climatological station catalog plus a large number of station-specific historical CSV files.
- This is promising for daily station ingestion without HTML scraping.

Likely canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- possibly `vapour_pressure`
- possibly `wind_speed`
- possibly `open_water_evaporation`, but this should not be assumed without schema verification

Station metadata:

- yes, at least a station catalog exists

Station ID convention:

- station mnemonic codes in SIH historical file names
- catalog key relationship still needs validation

Implementation complexity:

- medium to high
- many per-station files and schema normalization work

Main risks:

- unclear file schema consistency
- unclear update pattern
- unclear whether a separate observation inventory exists or must be inferred from files

### `NZ` / New Zealand

Source:

- NIWA National Climate Database / DataHub
- legacy CliFlo documentation

Evidence:

- Archived raw daily, hourly, and 10-minute data exist.
- Current access is through DataHub and requires login/account creation.
- Documentation says archived station data are about one month old in the public download flow, while more current API access is available by request through commercial arrangements.
- Older CliFlo access also required user login/subscription.

Classification rationale:

- the source is official, but the public automation path is account-gated and mixed with commercial-access language
- this is not a clean fit for a zero-credential default WeatherDownload provider

### `IT` / Italy

Source:

- no single clean national daily station archive was verified in this pass

Evidence:

- official meteorological and civil-protection sites were found, but not a clearly documented nationwide public daily station archive with stable machine-readable bulk access suitable for this library

Classification rationale:

- do not implement until a single official national source is verified
- regional fragmentation is the main likely blocker

## Recommendation

### Best first target: `FR` / Meteo-France daily climatological base data

Why:

- official
- public
- machine-readable
- no authentication
- daily observations are already organized as downloadable bulk files
- station metadata are available separately
- several existing WeatherDownload canonical elements should map cleanly
- the first implementation can stay narrow: daily only, conservative element mapping, and no new semantics

Suggested first slice:

- country `FR`
- provider token based on the source naming, to be chosen during design
- resolution `daily`
- conservative elements only:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `precipitation`
- station metadata from the official station metadata dataset
- station-level element availability derived conservatively

### Runner-up targets

1. `FI` / FMI Open Data
   Official and high-quality, but WFS/stored-query parsing is a bigger design investment than a bulk CSV source.

2. `CA` / ECCC GeoMet climate collections
   Very promising and architecturally modern, but the documented station subset limitation needs careful evaluation before it becomes a discovery-friendly provider.

3. `NO` / MET Norway Frost API
   Excellent API and likely strong metadata coverage, but requiring a client ID makes it slightly less frictionless as the first new provider addition.

## Sources

- MET Norway Frost API: https://frost.met.no/
- Frost usage/authentication concepts: https://frost.met.no/howto.html
- Finnish Meteorological Institute open data manual: https://en.ilmatieteenlaitos.fi/open-data-manual-data-catalog
- FMI time-series data manual: https://en.ilmatieteenlaitos.fi/open-data-manual-time-series-data
- FMI WFS services: https://en.ilmatieteenlaitos.fi/open-data-manual-fmi-wfs-services
- Meteo-France daily climatological base data: https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes/
- Meteo-France station metadata: https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees/
- ECCC GeoMet climate daily observations: https://api.weather.gc.ca/collections/climate-daily?f=html
- ECCC GeoMet climate stations: https://api.weather.gc.ca/collections/climate-stations?f=html
- NIWA National Climate Database: https://niwa.co.nz/climate-and-weather/national-climate-database
- NIWA DataHub CLIDB note: https://data.niwa.co.nz/pages/clidb-on-datahub
- CONAGUA SIH climatological stations: https://sih.conagua.gob.mx/climas.html
- CONAGUA station catalog dataset: https://www.datos.gob.mx/dataset/estaciones_sistema_informacion_hidrologica
