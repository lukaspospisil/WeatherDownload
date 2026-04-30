# CA ECCC Daily Provider Design

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note evaluates a conservative national Canada provider based on the official Environment and Climate Change Canada GeoMet climate API.

Scope for this note:

- country `CA`
- provider `eccc`
- resolution `daily`
- design only
- no runtime changes

## Recommendation Summary

- Recommended provider token: `eccc`
- Recommended first resolution: `daily`
- Recommended first-slice supported elements:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `precipitation`
- Optional later fifth element, only after a small follow-up validation pass:
  - `snow_depth`

Recommendation: this looks implementable as a small and conservative daily provider, but only if we accept one important limitation up front:

- GeoMet climate daily data are explicitly a subset of total Canadian climate stations, not the full national archive inventory.

That makes the implementation reasonably small, but not fully complete as a national station-discovery source.

## Current Architecture Fit

This fits the existing WeatherDownload model cleanly:

- `weatherdownload.providers.base.WeatherProvider` already expects:
  - station metadata reader
  - station observation metadata reader
  - dataset spec registry
  - observation downloader
- mixed national-plus-`ghcnd` country patterns already exist via `weatherdownload.providers.ghcnd.mixed`
- `CA` already has `provider="ghcnd"` for `daily`, so `provider="eccc"` would be an additive national path rather than a replacement

So the public model can stay:

- `country="CA"`
- `provider="eccc"`
- `resolution="daily"`

with the current `CA / ghcnd / daily` path preserved as a fallback.

## Stable Endpoints

### 1. Stable station metadata endpoint

Use the GeoMet climate-stations collection:

- collection root:
  - `https://api.weather.gc.ca/collections/climate-stations`
- station items:
  - `https://api.weather.gc.ca/collections/climate-stations/items?f=json`
- single station item:
  - `https://api.weather.gc.ca/collections/climate-stations/items/{CLIMATE_IDENTIFIER}?f=json`

Why this is the right metadata source:

- it is the official GeoMet station collection for climate observations
- live items include station identity, name, geometry, elevation, timezone, and daily/hourly/monthly coverage dates
- the item `id` in the live response matches `CLIMATE_IDENTIFIER`

### 2. Stable daily observations endpoint

Use the GeoMet climate-daily collection:

- collection root:
  - `https://api.weather.gc.ca/collections/climate-daily`
- daily items:
  - `https://api.weather.gc.ca/collections/climate-daily/items?f=json`

Conservative query shape for production use:

- `https://api.weather.gc.ca/collections/climate-daily/items?f=json&CLIMATE_IDENTIFIER={station_id}&datetime={start_date}/{end_date}&limit={n}`

Alternative advanced query shape also documented by ECCC:

- `?filter=properties.CLIMATE_IDENTIFIER='{station_id}' AND properties.LOCAL_DATE BETWEEN 'YYYY-MM-DD 00:00:00' AND 'YYYY-MM-DD 00:00:00'`

For a first implementation, the simple property-plus-`datetime` form is the safer default. CQL2 can stay as a fallback if GeoMet behavior differs for some station/date combinations.

## Station Identifier Choice

### 3. What `station_id` should WeatherDownload use?

Use:

- `station_id = CLIMATE_IDENTIFIER`

Do not use:

- `STN_ID` as the public station identifier

Why `CLIMATE_IDENTIFIER` is the best choice:

- ECCC's climate technical documentation defines the climate identifier as a permanent unique identifier for the observing site
- live GeoMet station items use `id = CLIMATE_IDENTIFIER`
- live daily GeoMet rows carry both `CLIMATE_IDENTIFIER` and `STN_ID`, which suggests `STN_ID` is an alternate/internal key rather than the primary public identity
- `CLIMATE_IDENTIFIER` is already string-safe and stable for CLI/API usage

Notes:

- official climate identifiers are documented as 7-digit numbers
- WeatherDownload should still normalize them as strings

## Raw-to-Canonical Mapping

### 4. Daily fields that map cleanly

Recommended first slice:

| Canonical element | ECCC daily field | Notes |
| --- | --- | --- |
| `tas_mean` | `MEAN_TEMPERATURE` | direct daily mean temperature |
| `tas_max` | `MAX_TEMPERATURE` | direct daily max temperature |
| `tas_min` | `MIN_TEMPERATURE` | direct daily min temperature |
| `precipitation` | `TOTAL_PRECIPITATION` | direct daily total precipitation |

Optional later mapping:

| Canonical element | ECCC daily field | Status |
| --- | --- | --- |
| `snow_depth` | `SNOW_ON_GROUND` | plausible and likely clean, but defer unless explicitly validated in implementation |

Conservative boundaries:

- `tas_mean` should map only from `MEAN_TEMPERATURE`
- do not derive `tas_mean` from `MAX_TEMPERATURE` and `MIN_TEMPERATURE`
- do not map `TOTAL_SNOW` because there is no current canonical snowfall element
- do not map gust fields to `wind_speed`; the exposed fields are extreme gust direction/speed, not a clean daily mean wind speed

## Units

### 5. Are units already canonical?

For the first four recommended fields, the GeoMet daily API appears already canonical:

- `MEAN_TEMPERATURE`, `MAX_TEMPERATURE`, `MIN_TEMPERATURE`: degrees C in the GeoMet API surface
- `TOTAL_PRECIPITATION`: mm in the GeoMet API surface

So for:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`

the conservative design is:

- no numeric unit conversion

Why this is a reasonable conclusion:

- live GeoMet JSON/CSV examples expose decimal values directly, for example `TOTAL_PRECIPITATION=3.4`
- ECCC's OGC API documentation uses climate-daily examples with temperature thresholds expressed in degrees C and precipitation thresholds expressed in mm

For `snow_depth`:

- `SNOW_ON_GROUND` appears to align with snow depth / snow on ground semantics
- ECCC's climate technical documentation and climate normals material use `cm` for snow-on-ground depth
- that looks usable as-is if WeatherDownload accepts this provider-specific unit alignment for `snow_depth`

Because snow-depth semantics and unit expectations are more likely to surprise users than the temperature/precipitation fields, it is still safer to defer `snow_depth` from the first slice.

## Station Discovery Completeness

### 6. Is the daily API complete enough for station discovery?

Short answer:

- no, not by itself

Details:

- the official ECCC GeoMet climate-daily documentation explicitly says only a subset of total stations is shown due to size limitations
- the same climate product family is therefore not a complete replacement for the full historical climate archive inventory
- `climate-stations` is the right station-discovery endpoint within GeoMet, but it should still be treated as the metadata companion for the GeoMet climate subset, not as proof of full national completeness

Recommendation:

- use `climate-stations` for station metadata and station discovery for this provider
- document clearly that `CA / eccc / daily` exposes the GeoMet subset, while `CA / ghcnd / daily` remains available as a separate fallback path

## Metadata Strategy

- Source station metadata from `climate-stations`
- Normalize:
  - `station_id` from `CLIMATE_IDENTIFIER`
  - `full_name` from `STATION_NAME`
  - `latitude` and `longitude` from geometry
  - `elevation_m` from `ELEVATION`
  - `begin_date` from `DLY_FIRST_DATE`
  - `end_date` from `DLY_LAST_DATE`
- Leave `gh_id` empty unless a verified crosswalk is added later

This is a small and safe strategy because the live station payload already contains the core fields WeatherDownload usually needs.

## Download Strategy

- Query `climate-daily/items`
- Filter by:
  - `CLIMATE_IDENTIFIER={station_id}`
  - `datetime={start}/{end}`
- Page with `limit` and `offset` or the returned `next` link as needed
- Normalize one row per station-day-element into the standard daily observation schema
- Preserve provider flags from:
  - `MEAN_TEMPERATURE_FLAG`
  - `MAX_TEMPERATURE_FLAG`
  - `MIN_TEMPERATURE_FLAG`
  - `TOTAL_PRECIPITATION_FLAG`
  - later, if enabled, `SNOW_ON_GROUND_FLAG`

Why this stays small:

- no archive unpacking
- no per-element endpoint fanout
- no unit scaling for the first slice
- one metadata collection plus one observations collection

## Main Risks And Blockers

- Main blocker to calling this a full national provider: GeoMet climate daily is an official subset, not the full station universe
- Station-level element availability may vary because daily data come from both daily climate stations and hourly stations
- `snow_depth` is likely workable via `SNOW_ON_GROUND`, but it deserves a narrow implementation-time validation before being advertised
- The API can return large result sets, so paging needs to be implemented carefully
- `STN_ID` should not leak into the public interface by accident; the provider should standardize on `CLIMATE_IDENTIFIER`

## Final Judgment

### 7. Would a first implementation be small and safe?

Yes, with a narrow definition of success.

This is a good conservative first implementation candidate if we keep the scope to:

- `provider="eccc"`
- `resolution="daily"`
- `station_id = CLIMATE_IDENTIFIER`
- first-slice elements:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `precipitation`

and if we explicitly document:

- GeoMet climate daily is subset-based station coverage
- `snow_depth` is deferred unless validated during implementation

Under those constraints, the implementation should be materially smaller and safer than bulk-file or authentication-heavy national providers.

## Sources

- ECCC GeoMet climate stations collection:
  - `https://api.weather.gc.ca/collections/climate-stations?f=html`
- ECCC GeoMet climate daily collection:
  - `https://api.weather.gc.ca/collections/climate-daily?f=html`
- ECCC GeoMet climate daily schema:
  - `https://api.weather.gc.ca/collections/climate-daily/schema?f=html`
- ECCC GeoMet climate open-data readme:
  - `https://eccc-msc.github.io/open-data/msc-data/climate_obs/readme_climateobs_en/`
- ECCC GeoMet OGC API technical documentation:
  - `https://eccc-msc.github.io/open-data/msc-geomet/ogc_api_en/`
- ECCC climate technical documentation PDF:
  - `https://climate.weather.gc.ca/doc/Technical_Documentation.pdf`

## Changed Files

- `docs/audits/ca_eccc_design.md`
