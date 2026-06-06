from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from weatherdownload import list_providers, list_resolutions, list_supported_countries, list_supported_elements
from weatherdownload.providers import get_provider


OUTPUT_PATH = Path('docs/supported_capabilities.md')


@dataclass(frozen=True)
class CapabilityRow:
    country: str
    provider: str
    resolution: str
    elements: tuple[str, ...]
    source_description: str
    notes: str


SOURCE_DESCRIPTIONS: dict[tuple[str, str, str], str] = {
    ('AT', 'historical', 'daily'): 'GeoSphere Austria historical daily station observations',
    ('AT', 'historical', '1hour'): 'GeoSphere Austria historical hourly station observations',
    ('AT', 'historical', '10min'): 'GeoSphere Austria historical 10-minute station observations',
    ('AL', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('AT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('BE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily station observations',
    ('BE', 'historical', '1hour'): 'RMI/KMI AWS hourly observations',
    ('BE', 'historical', '10min'): 'RMI/KMI AWS 10-minute observations',
    ('BE', 'rmi', 'daily'): 'RMI/KMI AWS daily observations',
    ('CA', 'eccc', 'daily'): 'Environment and Climate Change Canada GeoMet daily climate observations',
    ('CA', 'eccc', '1hour'): 'Environment and Climate Change Canada GeoMet hourly climate observations',
    ('CA', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('CH', 'historical', 'daily'): 'MeteoSwiss A1 historical daily station observations',
    ('CH', 'historical', '1hour'): 'MeteoSwiss A1 historical hourly station observations',
    ('CH', 'historical', '10min'): 'MeteoSwiss A1 historical 10-minute station observations',
    ('CH', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('CZ', 'historical_csv', 'daily'): 'CHMI historical CSV daily observations',
    ('CZ', 'historical_csv', '1hour'): 'CHMI historical CSV hourly observations',
    ('CZ', 'historical_csv', '10min'): 'CHMI historical CSV 10-minute observations',
    ('CZ', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('DE', 'historical', 'daily'): 'DWD historical daily station observations',
    ('DE', 'historical', '1hour'): 'DWD historical hourly station observations',
    ('DE', 'historical', '10min'): 'DWD historical 10-minute station observations',
    ('DE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('DK', 'dmi', 'daily'): 'DMI Climate Data daily stationValue observations',
    ('DK', 'historical', '1hour'): 'DMI historical hourly station observations',
    ('DK', 'historical', '10min'): 'DMI historical 10-minute station observations',
    ('DK', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('EE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('EE', 'ilmateenistus', 'daily'): 'Estonian Environment Agency climate daily observations',
    ('ES', 'aemet', 'daily'): 'AEMET OpenData daily climatological observations',
    ('FI', 'fmi', 'daily'): 'Finnish Meteorological Institute (FMI) Open Data WFS daily weather observations (timevaluepair)',
    ('FI', 'fmi', '1hour'): 'Finnish Meteorological Institute (FMI) Open Data WFS hourly observations',
    ('FI', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('FR', 'meteo_france', 'daily'): 'Meteo-France daily climatological base RR-T-Vent observations',
    ('FR', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('GB', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('GB', 'metoffice_datahub', '1hour'): 'Met Office Weather DataHub Land Observations recent hourly station observations',
    ('HU', 'historical', 'daily'): 'HungaroMet historical daily station observations',
    ('HU', 'historical', '1hour'): 'HungaroMet historical hourly station observations',
    ('HU', 'historical', '10min'): 'HungaroMet historical 10-minute station observations',
    ('HU', 'historical_wind', '10min'): 'HungaroMet historical 10-minute wind station observations',
    ('IE', 'meteireann', 'daily'): 'Met Eireann official daily station CSV observations',
    ('IT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('LT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('LT', 'meteo_lt', 'daily'): 'LHMT Meteo.lt meteorological station observations aggregated to daily UTC-date values',
    ('LV', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('LV', 'lvgmc', 'daily'): 'LVGMC recent meteorological archive observations aggregated to daily UTC-date values',
    ('MT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('LU', 'asta', 'daily'): 'ASTA daily weather measurements from the Luxembourg agrometeorological station network',
    ('LU', 'meteolux', 'daily'): 'MeteoLux daily Findel observations from INSPIRE WFS plus official MeteoLux daily CSV sunshine duration',
    ('MX', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('NL', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('NL', 'knmi', 'daily'): 'KNMI public daily station observations',
    ('NO', 'frost', 'daily'): 'MET Norway Frost official daily station observations',
    ('NO', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('NZ', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('PL', 'historical', 'daily'): 'IMGW-PIB historical daily synop station observations',
    ('PL', 'historical', '1hour'): 'IMGW-PIB historical hourly synop station observations',
    ('PL', 'historical_klimat', 'daily'): 'IMGW-PIB historical daily klimat station observations',
    ('PT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('PT', 'ipma', '1hour'): 'IPMA recent hourly station observations',
    ('RO', 'anm', 'daily'): 'ANM INSPIRE station-level daily CLIMAT observations',
    ('SI', 'arso', 'daily'): 'ARSO station-level daily climate archive observations',
    ('SE', 'historical', 'daily'): 'SMHI corrected-archive daily observations',
    ('SE', 'historical', '1hour'): 'SMHI corrected-archive hourly observations',
    ('SE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('SK', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('SK', 'recent', 'daily'): 'SHMU recent daily climatological stations',
    ('US', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
}

NOTES: dict[tuple[str, str, str], str] = {
    ('AL', 'ghcnd', 'daily'): 'Direct-prefix GHCN fallback for Albania only; raw GHCN station ids with prefix AL; inventory-driven station elements; unsupported wind, humidity, pressure, vapour_pressure, sunshine_duration, and solar_radiation are not provider-derived; not FAO-ready.',
    ('AT', 'historical', 'daily'): 'National GeoSphere daily path; `ghcnd` is an additional daily provider. `cglo_j` is mapped to observed `solar_radiation` in canonical MJ m^-2 via `value * 0.01`; sunshine_duration remains separate.',
    ('AT', 'historical', '1hour'): 'Single public hourly provider for AT. `cglo` hourly mean irradiance is converted to observed `solar_radiation` interval energy in canonical MJ m^-2 via `value * 0.0036`; sunshine_duration remains separate; no FAO workflow change.',
    ('AT', 'historical', '10min'): 'Single public 10-minute provider for AT. `cglo` 10-minute mean irradiance is converted to observed `solar_radiation` interval energy in canonical MJ m^-2 via `value * 0.0006`; sunshine_duration remains separate; no provider-side aggregation.',
    ('AT', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix AU; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('BE', 'ghcnd', 'daily'): 'Direct-prefix ghcnd fallback for Belgium stations only.',
    ('BE', 'historical', '1hour'): 'Provider-side hourly aggregates; raw QC stays in flag.',
    ('BE', 'historical', '10min'): 'Raw 10-minute path; no derived daily/hourly recomputation.',
    ('BE', 'rmi', 'daily'): 'Provider-side daily aggregates; raw QC stays in flag; sunshine minutes normalized to hours.',
    ('CA', 'eccc', 'daily'): 'Live GeoMet climate-daily fetching with pagination; station_id is ECCC CLIMATE_IDENTIFIER. Station discovery is conservative because climate-daily is a subset collection, and element coverage can vary by station/date. `ghcnd` remains available as a separate daily provider.',
    ('CA', 'eccc', '1hour'): 'Live GeoMet climate-hourly fetching with pagination; station_id is ECCC CLIMATE_IDENTIFIER. Station discovery is conservative because climate-hourly is a subset collection, and element coverage can vary by station/time. WIND_SPEED is converted from km/h to m/s, and STATION_PRESSURE is converted from kPa to hPa.',
    ('CA', 'ghcnd', 'daily'): 'Raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('CH', 'historical', 'daily'): 'MeteoSwiss daily path; FAO reference evaporation exists but is intentionally not mapped to open_water_evaporation.',
    ('CH', 'historical', '1hour'): 'FAO reference evaporation is not mapped to open_water_evaporation.',
    ('CH', 'historical', '10min'): 'FAO reference evaporation is not mapped to open_water_evaporation.',
    ('CH', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SZ; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('CZ', 'historical_csv', 'daily'): 'Measured open-water evaporation supported via raw VY.',
    ('CZ', 'historical_csv', '1hour'): 'Implemented CHMI historical CSV hourly path.',
    ('CZ', 'historical_csv', '10min'): 'Implemented CHMI historical CSV 10-minute path.',
    ('CZ', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix EZ; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('DE', 'historical', 'daily'): 'National DWD daily path; `ghcnd` is an additional daily provider.',
    ('DE', 'historical', '1hour'): 'Single public hourly provider for DE.',
    ('DE', 'historical', '10min'): 'Single public 10-minute provider for DE.',
    ('DE', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix GM; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('DK', 'dmi', 'daily'): 'Official DMI climateData stationValue daily path; `ghcnd` remains available as a separate daily provider. `max_temp_w_date` and `min_temp` are used for daily tas_max/tas_min, and `mean_radiation` is mapped directly to observed `solar_radiation` in canonical MJ m^-2. No DMI API key is currently required.',
    ('DK', 'historical', '1hour'): 'Single public hourly provider for DK.',
    ('DK', 'historical', '10min'): 'Meteorological Observation API path for 10-minute data.',
    ('DK', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix DA; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('EE', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('EE', 'ilmateenistus', 'daily'): 'Official Estonian climate daily JSON path; station_id is the official `jaam_kood`; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, relative_humidity, sunshine_duration, pressure, snow_depth, and solar_radiation only; values are used directly from source degC/mm/m s^-1/%/h/hPa/cm/MJ m^-2 units; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider.',
    ('ES', 'aemet', 'daily'): 'Requires AEMET OpenData API key; observed-only daily slice; velmedia is converted from km/h to canonical m/s; hrMedia is mapped to canonical relative_humidity in percent; trace precipitation `Ip` is mapped to 0.0 mm; observed vapour_pressure is unavailable; shared FAO workflow compatibility exists only through explicit `--fill-missing allow-derived`, where vapour_pressure is workflow-layer `derived_opt_in` rather than observed provider data.',
    ('FI', 'fmi', 'daily'): 'Conservative first slice via WFS daily timevaluepair; station_id is FMI fmisid; station metadata is a conservative AWS+SYNOP subset; elevation_m is currently null; FMI daily snow is unit cm and is intentionally not mapped yet.',
    ('FI', 'fmi', '1hour'): 'Conservative first slice via WFS timevaluepair; station_id is FMI fmisid; station metadata is a conservative AWS+SYNOP subset; elevation_m is currently null; p_sea (MSL pressure) is mapped to canonical pressure; units may be absent inline in the XML payload.',
    ('FI', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('FR', 'meteo_france', 'daily'): 'National Meteo-France daily RR-T-Vent slice; native 8-digit Meteo-France station ids; station-level availability comes from official station metadata; current RR-T-Vent RR/TN/TX/TM values are parsed as decimal mm/deg C without an extra /10 scaling step; no open_water_evaporation; tas_mean comes only from raw TM.',
    ('FR', 'ghcnd', 'daily'): 'Shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('GB', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('GB', 'metoffice_datahub', '1hour'): 'Official Met Office Weather DataHub Land Observations hourly slice; station_id is the location geohash; last 48 hours only; live use requires API key via WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY or METOFFICE_DATAHUB_API_KEY; current conservative observed mapping exposes temperature -> tas_mean, humidity -> relative_humidity, wind_speed -> wind_speed, wind_direction compass points -> canonical degree wind_direction, and mslp -> pressure; no derived values; not FAO-ready.',
    ('HU', 'historical', 'daily'): 'Generic HungaroMet archive path; open_water_evaporation not implemented.',
    ('HU', 'historical', '1hour'): 'Generic HungaroMet archive path; open_water_evaporation not implemented.',
    ('HU', 'historical', '10min'): 'Generic HungaroMet archive path; separate wind-only product exists.',
    ('HU', 'historical_wind', '10min'): 'Separate HungaroMet wind-only 10-minute product.',
    ('IE', 'meteireann', 'daily'): 'Station-metadata-driven conservative multi-station slice from the official Met Eireann daily station CSV path; station ids are the Met Eireann daily station numbers used in dly{station_id}.csv; tas_max, tas_min, precipitation, and sunshine_duration are exposed directly, while wdsp is converted from knots to canonical m/s; cbl (Mean CBL Pressure) stays intentionally unmapped because its semantics are unclear; observed-only and not FAO-ready.',
    ('IT', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('LT', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('LT', 'meteo_lt', 'daily'): 'Official LHMT Meteo.lt meteorological station API slice aggregated by WeatherDownload from observed intra-day UTC records into deterministic daily UTC-date values. This is not an official climatological daily summary product. The current observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, wind_speed_max, relative_humidity, pressure, snow_depth, and cloud_cover only; unsupported vapour_pressure, sunshine_duration, and solar_radiation remain unavailable and are not provider-derived. `ghcnd` remains available as a separate daily provider.',
    ('LV', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('LV', 'lvgmc', 'daily'): 'Official LVGMC recent meteorological archive slice queried through the CKAN DataStore API. WeatherDownload aggregates hourly AVG/MIN/MAX/SUM records into deterministic UTC-date daily values using the documented period-end rule `hourly_period_date = DATETIME - 1 hour`. This is not an official climatological daily summary product. The current observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, wind_speed_max, relative_humidity, pressure, and snow_depth only; unsupported vapour_pressure, sunshine_duration, solar_radiation, wind_direction, and cloud_cover are not provider-derived. `ghcnd` remains available as a separate daily provider.',
    ('MT', 'ghcnd', 'daily'): 'Direct-prefix GHCN fallback for Malta only; raw GHCN station ids with prefix MT; inventory-driven station elements; unsupported wind, humidity, pressure, vapour_pressure, sunshine_duration, and solar_radiation are not provider-derived; not FAO-ready.',
    ('LU', 'asta', 'daily'): 'Official ASTA daily WFS slice with stable ASTA station ids such as AGM_022; current conservative implementation exposes observed tas_mean, tas_max, tas_min, precipitation, wind_speed, relative_humidity, and measured sunshine_duration; `avg_press` stays unmapped because the source labels it Relative Air Pressure (hPa), and ASTA still does not directly expose vapour_pressure; observed-only and not FAO-ready.',
    ('LU', 'meteolux', 'daily'): 'National MeteoLux daily Findel-only slice; tas_max, tas_min, and precipitation come from official INSPIRE WFS layers, while sunshine_duration comes from the official MeteoLux daily CSV DINS column in hours; precipitation days are source-defined as 06:00 UTC to 06:00 UTC of the following day; observed-only, not FAO-ready, and Rn/net radiation is not downloaded.',
    ('MX', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('NL', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('NL', 'knmi', 'daily'): 'Public daggegevens daily CSV path via HTTP POST with fmt=csv. TG/TX/TN, FG/FXX, PG, RH, and SQ are converted from source tenths to canonical units; RH=-1 and SQ=-1 are normalized to observed 0.0 for below-threshold trace values; Q is converted from J/cm^2 to canonical MJ m^-2 via value * 0.01; cloud_cover is intentionally skipped because KNMI NG octa semantics are not yet exposed through a stable repository conversion contract; observed vapour_pressure is intentionally not exposed; `ghcnd` remains available as a separate daily provider; compatible with shared FAO allow-derived only through tas_mean plus relative_humidity.',
    ('NO', 'frost', 'daily'): 'Official MET Norway Frost daily station slice; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, wind_speed, and snow_depth only. Temperatures, precipitation, and wind speed are used directly from source degC/mm/m s^-1 units; snow depth is converted from source cm to canonical mm, and Frost coded precipitation value -1 is normalized to observed 0.0 mm. Live use requires WEATHERDOWNLOAD_FROST_CLIENT_ID or FROST_CLIENT_ID. No derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider.',
    ('NO', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('NZ', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('PL', 'historical', 'daily'): 'IMGW synop daily path; open_water_evaporation not implemented.',
    ('PL', 'historical', '1hour'): 'IMGW synop hourly path; open_water_evaporation not implemented.',
    ('PL', 'historical_klimat', 'daily'): 'Separate IMGW klimat daily path.',
    ('PT', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('PT', 'ipma', '1hour'): 'Official IPMA recent hourly station observations; station_id is IPMA idEstacao; timestamps are parsed as naive because the source keys do not include a timezone suffix; observed-only with tas_mean, precipitation, wind_speed, relative_humidity, and solar_radiation; radiacao is converted from kJ m^-2 to canonical solar_radiation in MJ m^-2 over the published hourly interval; pressure is intentionally not mapped; no derived values; not FAO-ready.',
    ('RO', 'anm', 'daily'): 'Official ANM INSPIRE/WFS station-level M201 CLIMAT daily slice; current conservative observed mapping exposes tas_mean, tas_max, tas_min, and precipitation only; temperature is converted from kelvin to canonical degrees Celsius; precipitation is used directly from source kg/m2 values; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider.',
    ('SI', 'arso', 'daily'): 'Official ARSO WebMet archive daily station path; current conservative observed mapping exposes tas_mean, tas_max, tas_min, precipitation, snow_depth, and sunshine_duration only; temperatures and precipitation are used directly from source degC/mm values; snow depth is converted from source cm to canonical mm; no derived values; not FAO-ready; `ghcnd` remains available as a separate daily provider.',
    ('SE', 'historical', 'daily'): 'Corrected-archive excludes latest three months; `ghcnd` is an additional daily provider.',
    ('SE', 'historical', '1hour'): 'Corrected-archive excludes latest three months.',
    ('SE', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SW; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('SK', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix LO; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('SK', 'recent', 'daily'): 'Experimental provider; measured water-surface evaporation supported via raw voda_vypar.',
    ('US', 'ghcnd', 'daily'): 'Raw GHCN station ids; inventory-driven station elements; measured open_water_evaporation supported via raw EVAP.',
}


def _collect_rows() -> list[CapabilityRow]:
    rows: list[CapabilityRow] = []
    for country in list_supported_countries():
        weather_provider = get_provider(country)
        for spec in weather_provider.list_implemented_dataset_specs():
            provider = spec.provider
            resolution = spec.resolution
            elements = tuple(list_supported_elements(country=country, provider=provider, resolution=resolution))
            key = (country, provider, resolution)
            rows.append(
                CapabilityRow(
                    country=country,
                    provider=provider,
                    resolution=resolution,
                    elements=elements,
                    source_description=SOURCE_DESCRIPTIONS.get(key, getattr(spec, 'label', weather_provider.name)),
                    notes=NOTES.get(key, ''),
                )
            )
    resolution_order = {'daily': 0, '1hour': 1, '10min': 2}
    return sorted(rows, key=lambda row: (row.country, row.provider, resolution_order.get(row.resolution, 99), row.resolution))


def render_supported_capabilities_markdown() -> str:
    rows = _collect_rows()
    lines = [
        '# Supported Capabilities',
        '',
        '<p align="right">',
        '  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">',
        '</p>',
        '',
        'This page is generated from the current provider registry and discovery APIs, then checked in tests so it stays aligned with the code.',
        '',
        'Navigation:',
        '',
        '- conceptual provider model: [Provider Model](providers.md)',
        '- provider-specific source notes: [Provider Notes](provider_notes/README.md)',
        '- canonical element semantics: [Canonical Elements](canonical_elements.md)',
        '',
        'Conceptual model:',
        '',
        '- `country` selects the country',
        '- `provider` selects the concrete data source or product within that country',
        '- `resolution` selects the temporal resolution',
        '- `element` selects the canonical meteorological variable',
        '- provider values are provider-specific and are not globally standardized',
        '',
        'Programmatic discovery:',
        '',
        '```python',
        'from weatherdownload import list_providers, list_resolutions, list_supported_elements',
        '',
        'list_providers(country="CZ")',
        'list_resolutions(country="US", provider="ghcnd")',
        'list_supported_elements(country="US", provider="ghcnd", resolution="daily")',
        '```',
        '',
        'CLI note:',
        '',
        '- the current CLI does not expose a country-wide capability-listing command',
        '- `weatherdownload stations elements ...` is station-level inspection and requires `--station-id`',
        '- for country-wide provider/resolution/element discovery, use the Python discovery functions above',
        '',
        'Representative station-level CLI examples:',
        '',
        '```powershell',
        'weatherdownload stations elements --country CZ --station-id 0-20000-0-11406 --provider historical_csv --resolution daily',
        'weatherdownload stations elements --country CZ --station-id EZM00011406 --provider ghcnd --resolution daily',
        'weatherdownload stations elements --country US --station-id USC00000001 --provider ghcnd --resolution daily',
        '```',
        '',
        '## Capability Table',
        '',
        '| Country | Provider | Resolution | Supported canonical elements | Provider/source description | Important notes |',
        '| --- | --- | --- | --- | --- | --- |',
    ]
    for row in rows:
        elements = ', '.join(f'`{element}`' for element in row.elements)
        lines.append(
            f"| `{row.country}` | `{row.provider}` | `{row.resolution}` | {elements} | {row.source_description} | {row.notes} |"
        )

    lines.extend([
        '',
        '## Open-Water Evaporation',
        '',
        'Measured `open_water_evaporation` is currently supported only for:',
        '',
        '- `CZ / historical_csv / daily` via CHMI raw `VY`',
        '- `SK / recent / daily` via SHMU raw `voda_vypar`',
        '- `US / ghcnd / daily` via NOAA GHCN-Daily raw `EVAP`',
        '',
        'It is intentionally not supported for:',
        '',
        '- `AT`, `CA`, `CH`, `CZ`, `DE`, `DK`, `FI`, `FR`, `GB`, `IT`, `MX`, `NO`, `NZ`, `PT`, `SE`, and `SK` on the current `ghcnd / daily` wrappers',
        '- `PT / ipma / 1hour`, because this first IPMA slice does not expose measured open-water evaporation',
        '- `CH` MeteoSwiss FAO reference evaporation fields, because they are not measured open-water or pan evaporation',
        '- `HU`, `PL`, and other providers unless a measured open-water, pan, or evaporimeter variable is explicitly implemented',
        '',
        '## Coverage Scope',
        '',
        '- this table lists currently implemented provider/resolution paths',
        '- it is derived from `list_providers()`, `list_resolutions()`, `list_supported_elements()`, and provider registry metadata',
        '- station-level availability can still be narrower on inventory-driven providers such as GHCN-Daily',
    ])
    return '\n'.join(lines) + '\n'


def write_supported_capabilities_doc(output_path: Path = OUTPUT_PATH) -> Path:
    output_path.write_text(render_supported_capabilities_markdown(), encoding='utf-8')
    return output_path


if __name__ == '__main__':
    destination = write_supported_capabilities_doc()
    print(f'Wrote {destination}')
