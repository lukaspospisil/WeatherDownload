# FI / fmi (FMI Open Data WFS) Design Note

Goal: evaluate whether Finnish Meteorological Institute (FMI) Open Data can be implemented as a conservative national provider:

- `country="FI"`
- `provider="fmi"`
- `resolution="daily"` or `resolution="1hour"`

Design-only audit (no runtime changes).

## Official Endpoint

Recommended base endpoint (capabilities entrypoint):

- `https://opendata.fmi.fi/wfs?request=GetCapabilities`

Source (FMI "Addresses of machine-readable interfaces"):

- `https://en.ilmatieteenlaitos.fi/open-source-code`

## Observations Stored Queries

FMI's Open Data WFS uses stored queries (`storedquery_id=...`) with WFS `GetFeature` requests:

- `https://en.ilmatieteenlaitos.fi/open-data-manual-time-series-data`
- `https://en.ilmatieteenlaitos.fi/open-data-manual-fmi-wfs-services`

Weather observations are documented in the FMI Open Data manual as being available in two formats, `multipointcoverage` and `timevaluepair`, and the manual references these weather stored queries:

- `fmi::observations::weather::timevaluepair` (time series)
- `fmi::observations::weather::multipointcoverage` (time series)

Source (example usage and parameter examples):

- `https://en.ilmatieteenlaitos.fi/open-data-manual-wfs-examples-and-guidelines`

Daily-aggregate stored queries likely exist, but should be treated as discoverable details (confirm from the service's `ListStoredQueries` response):

- `https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=listStoredQueries`

## Station Identifiers

FMI uses a numeric station identifier `fmisid`, used as a request parameter to target a station. This is the best candidate for WeatherDownload's canonical `station_id` for an `FI / fmi` provider.

Source (example with `fmisid` and station discovery via `fmi::ef::stations`):

- `https://en.ilmatieteenlaitos.fi/open-data-manual-wfs-examples-and-guidelines`

## Response Format

FMI's time series observations are delivered via WFS `GetFeature` as XML/GML payloads. The Open Data manual highlights two result encodings:

- `timevaluepair` (INSPIRE compatible)
- `multipointcoverage` (more compact)

Source:

- `https://en.ilmatieteenlaitos.fi/open-data-manual-time-series-data`

## Clean First-Slice Mappings

### 1hour (recommended first implementation)

The FMI WFS examples show `parameters=ws_10min,t2m` for the **weather timevaluepair** stored query. That makes a conservative first slice for `resolution="1hour"`:

- `tas_mean` from `t2m` (2m air temperature)
- `wind_speed` from `ws_10min` (10-minute mean wind speed)

Source:

- `https://en.ilmatieteenlaitos.fi/open-data-manual-wfs-examples-and-guidelines`

Units for hourly parameters should be taken from the WFS response (`uom` / observedProperty descriptions). Do not hardcode conversions until verified against the payload.

### daily (plausible follow-up once the daily stored query is confirmed)

FMI documents daily observation semantics (mean/max/min temperature, daily precipitation, daily snow depth) and explicitly states daily precipitation is in millimetres.

Source:

- `https://en.ilmatieteenlaitos.fi/guidance-to-observations`

FMI also documents common daily measurands and units (on the "gridded observations" page), which align well with WeatherDownload canonical elements:

- `Tday` (daily mean temp, degC)
- `Tmax` (daily maximum temp, degC)
- `Tmin` (daily minimum temp, degC)
- `RRday` (daily precipitation sum, mm)
- `Snow` (snow depth, cm)
- plus `Rh` (%), `Psea` (hPa) as daily aggregates/measurands

Source:

- `https://en.ilmatieteenlaitos.fi/gridded-observations-on-aws-s3`

Conservative daily first slice (if exposed by the WFS daily query):

- `tas_mean`, `tas_max`, `tas_min`, `precipitation`
- `snow_depth` only if the WFS query exposes it clearly

## Proposed Raw-to-Canonical Mapping Table

| FMI field / measurand | Canonical element | Resolution | Unit | Notes |
| --- | --- | --- | --- | --- |
| `t2m` | `tas_mean` | `1hour` | (from WFS response) | Example weather `timevaluepair` parameter in FMI docs. |
| `ws_10min` | `wind_speed` | `1hour` | (from WFS response) | Example weather `timevaluepair` parameter in FMI docs. |
| `Tday` | `tas_mean` | `daily` | degC | FMI daily measurand + unit (confirm WFS daily query exposure). |
| `Tmax` | `tas_max` | `daily` | degC | FMI daily measurand + unit (confirm WFS daily query exposure). |
| `Tmin` | `tas_min` | `daily` | degC | FMI daily measurand + unit (confirm WFS daily query exposure). |
| `RRday` | `precipitation` | `daily` | mm | FMI daily measurand + unit (confirm WFS daily query exposure). |
| `Snow` | `snow_depth` | `daily` | cm | FMI daily measurand + unit (confirm WFS daily query exposure). |
| `Rh` | `relative_humidity` | `daily` | % | Possible daily aggregate measurand (confirm WFS daily query exposure). |
| `Psea` | `pressure` | `daily` | hPa | Possible daily aggregate measurand (confirm WFS daily query exposure). |

## Station Metadata Strategy (Proposed)

Use FMI "Environmental Monitoring Facility" discovery:

1. Identify the relevant station network(s) from `fmi::ef::networks`.
2. Fetch stations via `fmi::ef::stations` (supports `starttime`/`endtime` in FMI examples).
3. Normalize to WeatherDownload station metadata with `station_id = fmisid` (string), coordinates, and name.

Source:

- `https://en.ilmatieteenlaitos.fi/open-data-manual-wfs-examples-and-guidelines`

## Download Strategy (Proposed)

1. Use `GetFeature` with `storedquery_id=fmi::observations::weather::timevaluepair`.
2. Request by `fmisid` (preferred) and a conservative parameter list: `t2m,ws_10min`.
3. Set `timestep=60` (minutes) and use `starttime`/`endtime` to bound the request.

Source:

- `https://en.ilmatieteenlaitos.fi/open-data-manual-wfs-examples-and-guidelines`

## Main Risks / Blockers

- XML/GML parsing: even `timevaluepair` requires careful, namespace-aware XML parsing; `multipointcoverage` is more compact but structurally more complex.
- Daily support: depends on confirming the daily stored query IDs and which measurand names they expose (use `ListStoredQueries` / `DescribeStoredQueries`).
- Units: hourly units should be read from the payload (`uom`) before committing to any conversions.

## Recommendation Summary

- Recommended provider token: `fmi`
- Best first resolution: `1hour` (via `fmi::observations::weather::timevaluepair`)
- First-slice elements (hourly): `tas_mean`, `wind_speed`
- Daily follow-up elements (once confirmed on WFS): `tas_mean`, `tas_max`, `tas_min`, `precipitation`, optional `snow_depth`

