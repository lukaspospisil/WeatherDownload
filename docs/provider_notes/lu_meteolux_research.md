# Luxembourg Data Source Investigation

This is a historical research note. It records the investigation that led to the current Luxembourg provider slices and keeps pointers for possible future extensions.

This note records a research pass for Luxembourg extensions beyond the first Luxembourg implementation that existed at the time:

- country: `LU`
- provider: `meteolux`
- resolution: `daily`
- station: `Luxembourg/Findel Airport`
- station_id: `0-20000-0-06590`
- observed elements at that time: `tas_max`, `tas_min`, `precipitation`

That earlier MeteoLux-only implementation was conservative, observed-only, daily-only, and not FAO-ready.

## Current implemented support

WeatherDownload has since moved beyond the initial slice that motivated this note.

Current Luxembourg support now includes:

- `LU / meteolux / daily`
- `LU / asta / daily`

Current MeteoLux support:

- station: `Luxembourg/Findel Airport`
- station_id: `0-20000-0-06590`
- observed elements: `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`
- `tas_max`, `tas_min`, and `precipitation` come from the official MeteoLux INSPIRE WFS daily Findel Airport layers
- `sunshine_duration` comes from the official MeteoLux daily CSV `DINS` column in hours

Current ASTA support:

- multi-station network with stable ids such as `AGM_022`
- observed elements: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration`
- source family: official ASTA INSPIRE WFS daily observations plus official ASTA station metadata

The original implemented MeteoLux layers documented during this research pass were:

- `MF.PointTimeSeriesObservation_Daily_FindelAirport_maxtemperature`
- `MF.PointTimeSeriesObservation_Daily_FindelAirport_mintemperature`
- `MF.PointTimeSeriesObservation_Daily_FindelAirport_totalprecipitation`

Source family:

- MeteoLux on `data.public.lu`
- INSPIRE WFS base: `https://wms.inspire.geoportail.lu/geoserver/mf/wfs`

## Candidate sources

### 1. MeteoLux daily CSV for Findel Airport

Official source:

- dataset page: `https://data.public.lu/en/datasets/daily-meteorological-parameters-luxembourg-findel-airport-wmo-id-06590/`
- stable resource URL family: `https://data.public.lu/en/datasets/r/a67bd8c0-b036-4761-b161-bdab272302e5`

Protocol:

- CSV download

Open-data status:

- `CC0` on the dataset page

Update cadence:

- dataset page shows regular updates
- the resource is a historical file named `06590-dailyparams-luxfindel-1947-ongoing.csv`

Stations:

- one station only: Luxembourg/Findel Airport

Station identifiers:

- station is described as WMO `06590`
- the current WeatherDownload `station_id` `0-20000-0-06590` can still be kept as the stable public identifier

Time range:

- `1947-ongoing` according to the resource name and dataset description

Resolution:

- daily

Variables visible from the official CSV header and dataset description:

- `DXT` daily maximum air temperature
- `DNT` daily minimum air temperature
- `DRR06_06 (mm)` daily precipitation amount
- `DINS (Hours)` daily sunshine duration by observer

Units:

- `degC` for `DXT` and `DNT`
- `mm`
- `hours`

Observed or derived:

- observed provider data

Station metadata:

- dataset page includes latitude, longitude, station elevation, and station pressure elevation

Date/time semantics:

- source description is unusually explicit:
- `DXT`: `23:46 UTC (D-1)` to `23:45 UTC (D)`
- `DNT`: `23:46 UTC (D-1)` to `23:45 UTC (D)`
- `DRR06_06`: `05:46 UTC (D)` to `05:45 UTC (D+1)`
- `DINS`: daily sunshine duration by observer

Stability for automated downloading:

- good
- single stable CSV resource
- easy to fixture

Fixture-backed testing feasibility:

- high
- a few CSV rows are enough

Implementation difficulty:

- low

Recommendation:

- `implement`

Comments:

- this turned out to be the clearest immediate extension of the initial provider
- it is now implemented in WeatherDownload as observed `sunshine_duration` from the official MeteoLux CSV
- it still did not make the MeteoLux slice FAO-ready by itself because `tas_mean`, `wind_speed`, and `vapour_pressure` remain unavailable there

### 2. MeteoLux monthly climatological products

Official sources:

- monthly parameters page: `https://data.public.lu/en/datasets/monthly-meteorological-parameters-luxembourg-findel-airport-wmo-id-06590/`
- monthly INSPIRE page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-monthly-weather-measurements-at-luxembourg-findel-airport-1/`
- MeteoLux organization page: `https://data.public.lu/en/organizations/meteolux/`

Protocol:

- TXT / GML / WFS metadata pages

Open-data status:

- `CC0`

Stations:

- one station only: Luxembourg/Findel Airport

Resolution:

- monthly, not daily

Variables:

- monthly pages indicate variables such as mean temperature, precipitation, air pressure, sea-level pressure, and total solar duration

Observed or derived:

- observed climatological aggregates

Suitability for WeatherDownload:

- limited for the current scope because WeatherDownload currently focuses on station observations at daily/hourly/10min resolutions

Implementation difficulty:

- medium

Recommendation:

- `maybe later`

Comments:

- potentially useful as a future monthly capability, but not a natural next step for the current daily/hourly/10min matrix

### 3. MeteoLux hourly data based on monthly review documents

Official source:

- `https://data.public.lu/en/datasets/monthly-weather-reviews-based-on-hourly-data-luxembourg-findel-airport-wmo-id-06590-wigos-id-0-20000-0-06590/`

Protocol:

- documentation links to MeteoLux web pages and PDFs

Open-data status:

- `CC0` on the metadata page, but the actual content exposed here is documentation-style output

Stations:

- one station only: Luxembourg/Findel Airport

Resolution:

- described as hourly in the reports, but not exposed as a straightforward raw historical hourly observation feed

Variables mentioned in the documentation set:

- hourly mean air temperature
- hourly precipitation
- hourly sunshine duration
- hourly relative humidity
- hourly wind gusts / directions

Observed or derived:

- based on observed hourly data, but published as monthly reviews rather than a clean raw station API

Station metadata:

- station details are present

Date/time semantics:

- not documented as a machine-friendly raw hourly API on these pages

Stability for automated downloading:

- weak for WeatherDownload

Fixture-backed testing feasibility:

- poor if implemented by scraping or parsing human-facing documents

Implementation difficulty:

- high

Recommendation:

- `reject`

Comments:

- this is not a good WeatherDownload source in its current exposed form
- it is official, but it is not a clean structured raw observation product

### 4. ASTA INSPIRE WFS weather station network

Official source:

- daily page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-of-asta-1/`
- hourly page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-hourly-weather-measurements-of-asta/`
- live latest-hour page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-live-weather-measurements-of-latest-hour-of-asta-8/`
- station metadata page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-spatial-sampling-features-location-of-weather-stations-managed-by-asta/`
- WFS base: `https://wms.inspire.geoportail.lu/geoserver/mf/wfs`

Protocol:

- INSPIRE WFS / GML

Open-data status:

- `CC0`

Update cadence:

- dataset pages show recent updates
- hourly live page says it is updated each hour

Stations:

- the pages describe a network of ASTA weather stations
- the station-metadata page is official and structured
- one portal snippet describes `41` stations

Station identifiers:

- feature identifiers such as `AGM_012`, `AGM_021`, `AGM_022` appear in official responses
- `name_descr` values include station names such as `Grevenmacher`, `Steinsel`, and `Arsdorf`

Time range:

- daily: maximum of about `2 years`
- hourly: maximum of about `1 year`
- live latest-hour: latest hour only

Resolutions:

- daily
- hourly
- live latest-hour

Variables visible from the official WFS capabilities:

- air temperature at several heights: `avg_ta200`, `avg_ta020`, `avg_ta005`
- min/max air temperature variants
- precipitation: `sum_nn050`
- pressure: `avg_press`
- relative humidity: `avg_rh200`
- sunshine duration: `sum_ssd`
- global radiation: `sum_gs200`
- wind speed: `avg_wv1000`, `avg_wv200`, `max_wv1000max`
- PAR / radiation-related variable: `avg_parpfd200`
- soil temperature variables: `avg_tb005`, `avg_tb015`, `avg_tb030`, `avg_tb050`, `avg_tb100`
- leaf wetness, snow height, and several agro-climatic day counters
- daily-only variables also include `sum_pen` and `kwb`, which would need semantic review before any public WeatherDownload mapping

Units:

- not cleanly documented in the capability names themselves
- likely inferable from source metadata or domain semantics, but would need explicit verification before implementation

Observed or derived:

- mixed
- some variables are clearly observed station measurements
- some are ambiguous or domain-specific and should not be exposed without review

Station metadata:

- structured spatial-sampling dataset exists
- suitable for latitude/longitude and likely station elevation extraction

Date/time semantics:

- daily responses expose `day` and `datetime`
- hourly pages explicitly say data are shown in local wintertime (`GMT+1`)
- real WFS hourly features currently include both `datetime` and a separate `time` field such as `01:00:00+01:00`

Stability for automated downloading:

- promising
- official, structured, and queryable through WFS

Fixture-backed testing feasibility:

- good

Implementation difficulty:

- medium

Recommendation at the time:

- `implement` for a conservative subset later

Comments:

- ASTA was the strongest candidate for broader Luxembourg support
- it offers many stations, daily and hourly data, and variables that are much closer to FAO-readiness than the current MeteoLux Findel slice
- the main caution is semantic review:
- variable names are provider-specific
- units need explicit confirmation
- hourly timezone handling must be designed carefully

Outcome:

- a conservative ASTA daily provider is now implemented in WeatherDownload
- hourly ASTA is still future work

### 5. AGE INSPIRE WFS weather station network

Official source:

- daily page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-of-age/`
- hourly page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-hourly-weather-measurements-of-age-1/`
- station metadata page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-spatial-sampling-features-location-of-weather-stations-managed-by-age/`
- WFS base: `https://wms.inspire.geoportail.lu/geoserver/mf/wfs`

Protocol:

- INSPIRE WFS / GML

Open-data status:

- `CC0`

Update cadence:

- recent portal updates

Stations:

- `18` weather stations according to the official pages

Station identifiers:

- official responses contain identifiers like `AGE_050` and `AGE_063`
- `name_descr` values include stations such as `Bigonville` and `Reichlange`

Time range:

- daily: about `2 years`
- hourly: about `1 year`

Resolutions:

- daily
- hourly
- monthly
- yearly

Variables visible from the official WFS capabilities:

- `avg_ta200`
- `sum_gs200`
- `max_snow_h`
- `sum_nn050`
- `avg_press`
- `avg_rh200`

Units:

- not fully explicit in the capability names alone
- would need verification in provider documentation or sample products

Observed or derived:

- mostly observed station variables

Station metadata:

- structured spatial-sampling dataset exists

Date/time semantics:

- daily responses expose `day` and `datetime`
- hourly pages explicitly say data are shown in local wintertime (`GMT+1`)
- real WFS hourly features also expose a separate `time` field such as `01:00:00+01:00`

Stability for automated downloading:

- promising

Fixture-backed testing feasibility:

- good

Implementation difficulty:

- medium

Recommendation:

- `maybe later`

Comments:

- AGE is official and structured, but it is a smaller network and its variable set is narrower than ASTA
- it is probably a better second-step Luxembourg provider than a first-step expansion target

### 6. Official Luxembourg 10-minute meteorological observations

Investigation result:

- no official structured `10min` meteorological observation feed was found in the reviewed MeteoLux, ASTA, AGE, `data.public.lu`, or INSPIRE WFS materials
- the WFS capability structure reviewed during this investigation exposes daily, hourly, monthly, yearly, and ASTA live latest-hour products, but not a 10-minute time-series product

Recommendation:

- `reject for now`

Implementation difficulty:

- not applicable until an official structured source is found

## Findings

### Implementable now at the time

- extend Luxembourg support with the official MeteoLux daily CSV so Findel can add observed `sunshine_duration`
- add a separate ASTA provider later for multi-station Luxembourg daily and hourly observations after a careful variable/unit review

### Potentially implementable later

- AGE daily/hourly as a second Luxembourg provider
- monthly MeteoLux climatological products if WeatherDownload ever wants explicit monthly support

### Rejected or unsuitable for now

- MeteoLux monthly PDF-style hourly review documents as a raw observation source
- Luxembourg 10-minute support, because no official structured public source was found in this pass

## More Daily Stations

Yes.

The clearest official structured source for more Luxembourg daily stations is not MeteoLux itself, but ASTA and AGE:

- ASTA daily WFS weather measurements across a larger station network
- AGE daily WFS weather measurements across 18 stations

## More Daily Variables

Yes, with caveats.

For MeteoLux Findel specifically:

- the official daily CSV adds `sunshine_duration`

For broader Luxembourg networks:

- ASTA and AGE expose additional daily variables including pressure, relative humidity, precipitation, sunshine duration, radiation, and wind-related measurements

Still missing or unclear for FAO-readiness:

- MeteoLux Findel daily public sources reviewed here do not clearly expose observed daily `tas_mean`, `wind_speed`, `vapour_pressure`, and `sunshine_duration` together in one stable machine-friendly daily feed
- ASTA looks much closer to FAO-readiness than MeteoLux Findel, but the exact canonical mapping and unit verification would need a careful implementation pass

## Hourly Support

Yes, likely.

Official structured hourly WFS sources exist for:

- ASTA
- AGE

The best hourly implementation candidate is ASTA because:

- it has broader station coverage
- it exposes a larger variable set
- it is official and structured

The main risk is time semantics:

- the official metadata explicitly say the hourly data are shown in local wintertime (`GMT+1`)
- WeatherDownload would need a clear timestamp policy and fixture tests before advertising support

## 10-Minute Support

No official structured `10min` source was found in this investigation.

That does not prove such data do not exist institutionally, only that no clean public official structured product was identified in the reviewed portals and WFS capabilities.

## FAO Readiness

Luxembourg could move closer to FAO-readiness from official observed data, but not from the current MeteoLux Findel WFS slice alone.

Current MeteoLux Findel support at the time:

- `tas_max`
- `tas_min`
- `precipitation`

Likely immediate extension from official MeteoLux CSV at the time:

- `sunshine_duration`

Still missing for a conservative FAO-ready daily bundle:

- `tas_mean`
- `wind_speed`
- `vapour_pressure` or a clearly mappable equivalent

ASTA was the most promising official source for a future FAO-oriented Luxembourg path because it appeared to expose:

- temperature
- precipitation
- wind
- humidity
- sunshine duration
- pressure
- radiation-related variables

But that should be a separate implementation decision, not a silent extension of the current MeteoLux Findel slice.

## Historical Recommended Next Implementation Step

At the time of this research pass, one clear next step stood out:

- extend the existing `LU / meteolux / daily` provider with observed `sunshine_duration` from the official MeteoLux daily CSV resource

Why this is the best next step:

- official
- structured
- single-station scope matches the current provider
- easy to fixture-test
- low implementation risk
- improves the current Luxembourg slice without changing its conservative character

That next step has since been completed. After that, the next substantial Luxembourg project was:

- a separate `ASTA` provider with a conservative subset of daily observations first, and hourly only after timezone semantics and units are verified in code and tests

That ASTA daily provider is now implemented. Remaining possible extensions from this note are:

- ASTA hourly WFS after timestamp-policy review
- AGE daily and hourly WFS as a separate Luxembourg provider family
- pressure review if ASTA `avg_press` semantics become clear enough for a conservative mapping
- continued monitoring for any official structured Luxembourg `10min` source, which was not found in this investigation
