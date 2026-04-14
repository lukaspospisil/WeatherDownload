from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from weatherdownload.elements import raw_to_canonical_map_for_spec
from weatherdownload.fao import FINAL_SERIES_COLUMNS
from weatherdownload.providers import get_provider, normalize_country_code


FAO_CANONICAL_ELEMENTS = tuple(FINAL_SERIES_COLUMNS)
FILL_MISSING_CHOICES = ('none', 'allow-derived', 'allow-hourly-aggregate')
PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS = 18

CZ_TIMEFUNC_BY_CANONICAL = {
    'tas_mean': 'AVG',
    'wind_speed': 'AVG',
    'vapour_pressure': 'AVG',
    'tas_max': '20:00',
    'tas_min': '20:00',
    'sunshine_duration': '00:00',
}
AT_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
BE_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
DK_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
CH_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
HU_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
PL_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'sunshine_duration')
NL_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration')
SE_REQUIRED_OBSERVED_ELEMENTS = ('tas_mean', 'tas_max', 'tas_min')

AT_ASSUMPTIONS = {
    'wind_height_handling': (
        'wind_speed uses GeoSphere daily vv_mittel as delivered. This shared workflow does not convert wind speed to FAO 2 m wind speed, '
        'because the dataset metadata used here do not provide a normalized measurement height contract for that conversion.'
    ),
    'pressure_usage': (
        'GeoSphere daily pressure is available in the provider, but it is intentionally not included in this shared FAO-preparation bundle. '
        'The current cross-country workflow exports only the common daily core inputs used downstream in the existing sample workflow.'
    ),
    'relative_humidity_interpretation': (
        'GeoSphere daily rf_mittel exists in the provider, but this shared workflow does not use it to derive new variables. '
        'vapour_pressure therefore remains empty for Austria in the shared bundle.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses GeoSphere daily so_h as observed sunshine duration in hours. '
        'This shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
AT_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['tl_mittel'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tlmax'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tlmin'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['vv_mittel'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Austria daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'sunshine_duration': {'raw_codes': ['so_h'], 'selection_rule': None, 'status': 'observed'},
}

BE_ASSUMPTIONS = {
    'observed_inputs_only': 'The Belgium branch packages only source-backed daily observations from the RMI/KMI provider. The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.',
    'provider_daily_aggregation': 'Belgium daily values come from the official provider-side aws_1day aggregation. This shared workflow does not recompute daily values from 10-minute data.',
    'vapour_pressure_availability': 'The current Belgium daily provider path does not expose observed vapour_pressure for this shared workflow. The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.',
    'pressure_usage': 'Belgium daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.',
    'sunshine_duration_to_radiation': 'sunshine_duration uses observed Belgium daily sunshine duration only. The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
}
BE_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['temp_avg'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['temp_max'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['temp_min'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['wind_speed_10m'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Belgium daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'sunshine_duration': {'raw_codes': ['sun_duration'], 'selection_rule': None, 'status': 'observed'},
}

DK_ASSUMPTIONS = {
    'observed_inputs_only': 'The Denmark branch packages only source-backed daily observations from the DMI provider. The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.',
    'denmark_only_scope': 'This shared workflow uses the current Denmark-only DMI daily provider slice. Greenland and Faroe Islands differences are out of scope in this pass.',
    'vapour_pressure_availability': 'The current Denmark daily provider path does not expose observed vapour_pressure for this shared workflow. The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.',
    'pressure_usage': 'Denmark daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.',
    'sunshine_duration_to_radiation': 'sunshine_duration uses observed Denmark daily sunshine duration only. The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
}
DK_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['mean_temp'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['mean_daily_max_temp'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['mean_daily_min_temp'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['mean_wind_speed'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Denmark daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'sunshine_duration': {'raw_codes': ['bright_sunshine'], 'selection_rule': None, 'status': 'observed'},
}

CH_ASSUMPTIONS = {
    'observed_inputs_only': 'The Switzerland branch packages source-backed daily observations from the MeteoSwiss A1 provider slice for later FAO-oriented processing. It does not compute FAO-56 ET0.',
    'vapour_pressure_availability': 'Observed daily vapour_pressure is available from the implemented MeteoSwiss A1 daily provider path and is used directly when present.',
    'fallback_policy': 'If the shared allow-derived policy is enabled and observed vapour_pressure is missing on some rows, the existing shared fallback from observed tas_mean plus observed relative_humidity may be used. No CH-specific derivation logic is introduced.',
    'sunshine_duration_to_radiation': 'Observed daily sunshine_duration is exported as input only. The workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
    'daily_precipitation_window': 'MeteoSwiss daily precipitation semantics remain provider-defined. This FAO-prep workflow does not reinterpret or recompute them.',
}
CH_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['tre200d0'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tre200dx'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tre200dn'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['fkl010d0'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': ['pva200d0'], 'selection_rule': None, 'status': 'observed'},
    'sunshine_duration': {'raw_codes': ['sre000d0'], 'selection_rule': None, 'status': 'observed'},
}

HU_ASSUMPTIONS = {
    'observed_inputs_only': 'The Hungary branch packages only source-backed daily observations from the HungaroMet provider. The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables by default.',
    'vapour_pressure_availability': 'The current Hungary daily provider path does not expose observed vapour_pressure for this shared workflow. The shared workflow leaves vapour_pressure empty unless the optional shared allow-derived fill policy is enabled.',
    'relative_humidity_helper': 'Hungary daily relative_humidity is available from the current provider slice and may be used only by the existing shared allow-derived fallback rule for vapour_pressure. No provider-specific derivation logic is added.',
    'sunshine_duration_to_radiation': 'sunshine_duration uses observed HungaroMet daily sunshine duration only. The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
}
HU_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['t'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tx'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tn'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['fs'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Hungary daily provider path. The shared workflow leaves this field empty by default and may fill it only through the existing opt-in shared fallback rule.'},
    'sunshine_duration': {'raw_codes': ['f'], 'selection_rule': None, 'status': 'observed'},
}

PL_ASSUMPTIONS = {
    'observed_inputs_only': 'The Poland branch packages only source-backed daily observations from the IMGW-PIB synop provider slice for later FAO-oriented processing. It does not compute FAO-56 ET0.',
    'wind_speed_availability': 'The current Poland synop daily provider path does not expose observed wind_speed for this shared workflow. The shared workflow leaves wind_speed empty instead of substituting another IMGW product or recomputing it from other resolutions.',
    'vapour_pressure_availability': 'The current Poland synop daily provider path does not expose observed vapour_pressure for this shared workflow. The shared workflow leaves vapour_pressure empty, and the existing allow-derived fallback cannot be used because this provider slice does not expose observed daily relative_humidity here.',
    'sunshine_duration_to_radiation': 'Observed IMGW daily sunshine_duration is exported as input only. This workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
    'station_metadata_limits': 'The implemented official IMGW station list provides the canonical station identifiers and names used here, but not clean source-backed coordinates or elevation for this provider slice. Those metadata fields therefore remain missing in the normalized station table and exported FAO-prep bundle.',
}
PL_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['STD'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['TMAX'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['TMIN'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Poland synop daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Poland synop daily provider path. The shared workflow leaves this field empty, and the existing shared fallback cannot be used because relative_humidity is unavailable in this slice.'},
    'sunshine_duration': {'raw_codes': ['USL'], 'selection_rule': None, 'status': 'observed'},
}
PL_HOURLY_PROVIDER_ELEMENT_MAPPING = {
    'wind_speed': {'raw_codes': ['FWR'], 'selection_rule': f'UTC daily arithmetic mean from hourly observations when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly values are available', 'status': 'aggregated_hourly_opt_in', 'notes': 'Opt-in only. Filled from official IMGW historical/1hour wind_speed observations; never used by default.'},
    'vapour_pressure': {'raw_codes': ['CPW'], 'selection_rule': f'UTC daily arithmetic mean from hourly observations when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly values are available', 'status': 'aggregated_hourly_opt_in', 'notes': 'Opt-in only. Filled from official IMGW historical/1hour vapour_pressure observations; never used by default.'},
}

NL_ASSUMPTIONS = {
    'observed_inputs_only': 'The Netherlands branch packages only source-backed daily observations from the KNMI provider. The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.',
    'vapour_pressure_availability': 'KNMI NL historical daily support in this pass does not expose observed vapour_pressure in the provider path used here. The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.',
    'pressure_usage': 'KNMI daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.',
    'sunshine_duration_to_radiation': 'sunshine_duration uses observed KNMI daily sunshine duration only. The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.',
}
NL_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['TG'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['TX'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['TN'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['FG'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Netherlands daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'sunshine_duration': {'raw_codes': ['SQ'], 'selection_rule': None, 'status': 'observed'},
}

SE_ASSUMPTIONS = {
    'observed_inputs_only': 'The Sweden branch packages only source-backed daily observations from the SMHI provider. The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.',
    'corrected_archive_limit': 'The current Sweden daily provider path uses the official SMHI corrected-archive daily CSV path. That source excludes the latest three months while quality control is still in progress.',
    'wind_speed_availability': 'The current Sweden daily provider path used by this shared workflow does not expose observed daily wind_speed. The shared workflow leaves wind_speed empty instead of deriving it or substituting hourly data.',
    'vapour_pressure_availability': 'The current Sweden daily provider path does not expose observed vapour_pressure for this shared workflow. The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.',
    'sunshine_duration_availability': 'The current Sweden daily provider path used by this shared workflow does not expose observed daily sunshine_duration. The shared workflow leaves sunshine_duration empty instead of estimating it from other fields.',
}
SE_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['2'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['20'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['19'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'vapour_pressure': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
    'sunshine_duration': {'raw_codes': [], 'selection_rule': None, 'status': 'unavailable', 'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.'},
}


@dataclass(frozen=True)
class FaoCountryConfig:
    country: str
    dataset_scope: str
    resolution: str
    obs_types: tuple[str, ...]
    canonical_to_raw: dict[str, tuple[str, ...]]
    raw_to_canonical: dict[str, str]
    time_function_by_canonical: dict[str, str]
    required_complete_elements: tuple[str, ...]
    query_elements: tuple[str, ...]
    provider_element_mapping: dict[str, dict[str, Any]]
    assumptions: dict[str, str]
    dataset_type: str
    source: str
    hourly_dataset_scope: str | None = None
    hourly_resolution: str | None = None
    hourly_query_elements: tuple[str, ...] = ()


def build_observed_provider_element_mapping(canonical_to_raw: dict[str, tuple[str, ...]], time_function_by_canonical: dict[str, str]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for canonical_name in FINAL_SERIES_COLUMNS:
        mapping[canonical_name] = {'raw_codes': list(canonical_to_raw[canonical_name]), 'selection_rule': time_function_by_canonical.get(canonical_name), 'status': 'observed'}
    return mapping


def get_fao_country_config(country: str | None, *, fill_missing: str = 'none') -> FaoCountryConfig:
    normalized_country = normalize_country_code(country)
    try:
        provider = get_provider(normalized_country)
        daily_spec = provider.get_dataset_spec('historical_csv' if normalized_country == 'CZ' else 'historical', 'daily')
    except Exception as exc:
        raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.') from exc
    if fill_missing not in FILL_MISSING_CHOICES:
        raise ValueError(f'Unsupported fill policy: {fill_missing}')

    canonical_to_raw = daily_spec.canonical_elements or {}
    raw_to_provider_canonical = raw_to_canonical_map_for_spec(daily_spec)
    if normalized_country == 'AT':
        query_elements = AT_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'BE':
        query_elements = BE_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'CH':
        query_elements = FAO_CANONICAL_ELEMENTS
    elif normalized_country == 'DK':
        query_elements = DK_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'HU':
        query_elements = HU_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'PL':
        query_elements = PL_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'NL':
        query_elements = NL_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'SE':
        query_elements = SE_REQUIRED_OBSERVED_ELEMENTS
    else:
        query_elements = FAO_CANONICAL_ELEMENTS
    query_elements = tuple(query_elements)
    if fill_missing == 'allow-derived' and normalized_country in {'AT', 'BE', 'CH', 'DK', 'HU', 'NL'}:
        query_elements = tuple(dict.fromkeys([*query_elements, 'relative_humidity']))

    selected_canonical_to_raw: dict[str, tuple[str, ...]] = {}
    raw_to_canonical: dict[str, str] = {}
    for canonical_name in query_elements:
        raw_codes = canonical_to_raw.get(canonical_name, ())
        if not raw_codes:
            raise ValueError(f'FAO preparation example requires daily canonical element {canonical_name!r} for country {normalized_country}.')
        selected_canonical_to_raw[canonical_name] = tuple(raw_codes)
    for raw_code, canonical_name in raw_to_provider_canonical.items():
        if canonical_name in selected_canonical_to_raw:
            raw_to_canonical[raw_code.upper()] = canonical_name

    if normalized_country == 'CZ':
        return FaoCountryConfig('CZ', 'historical_csv', 'daily', ('DLY',), selected_canonical_to_raw, raw_to_canonical, dict(CZ_TIMEFUNC_BY_CANONICAL), FAO_CANONICAL_ELEMENTS, query_elements, build_observed_provider_element_mapping(selected_canonical_to_raw, dict(CZ_TIMEFUNC_BY_CANONICAL)), {}, 'CHMI observed daily input bundle prepared for later FAO workflow packaging', 'CHMI OpenData historical_csv metadata and daily observations')
    if normalized_country == 'DE':
        return FaoCountryConfig('DE', 'historical', 'daily', ('DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, FAO_CANONICAL_ELEMENTS, query_elements, build_observed_provider_element_mapping(selected_canonical_to_raw, {}), {}, 'DWD observed daily input bundle prepared for later FAO workflow packaging', 'DWD CDC historical daily metadata and observations')
    if normalized_country == 'AT':
        return FaoCountryConfig('AT', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, AT_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(AT_PROVIDER_ELEMENT_MAPPING), dict(AT_ASSUMPTIONS), 'GeoSphere Austria observed daily input bundle prepared for later FAO workflow packaging', 'GeoSphere Austria Dataset API station historical daily klima-v2-1d')
    if normalized_country == 'BE':
        return FaoCountryConfig('BE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, BE_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(BE_PROVIDER_ELEMENT_MAPPING), dict(BE_ASSUMPTIONS), 'RMI/KMI Belgium observed daily input bundle prepared for later FAO workflow packaging', 'RMI/KMI open-data platform aws_1day daily station observations')
    if normalized_country == 'CH':
        return FaoCountryConfig('CH', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, CH_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(CH_PROVIDER_ELEMENT_MAPPING), dict(CH_ASSUMPTIONS), 'MeteoSwiss Switzerland observed daily input bundle prepared for later FAO workflow packaging', 'MeteoSwiss A1 automatic weather stations historical daily observations')
    if normalized_country == 'DK':
        return FaoCountryConfig('DK', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, DK_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(DK_PROVIDER_ELEMENT_MAPPING), dict(DK_ASSUMPTIONS), 'DMI Denmark observed daily input bundle prepared for later FAO workflow packaging', 'DMI Climate Data station and stationValue daily station observations for Denmark')
    if normalized_country == 'HU':
        return FaoCountryConfig('HU', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, HU_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(HU_PROVIDER_ELEMENT_MAPPING), dict(HU_ASSUMPTIONS), 'HungaroMet Hungary observed daily input bundle prepared for later FAO workflow packaging', 'HungaroMet open data daily station observations from odp.met.hu')
    if normalized_country == 'PL':
        provider_mapping = dict(PL_PROVIDER_ELEMENT_MAPPING)
        assumptions = dict(PL_ASSUMPTIONS)
        dataset_type = 'IMGW-PIB Poland observed daily input bundle prepared for later FAO workflow packaging'
        source = 'IMGW-PIB public daily synop station observations from the official archive'
        hourly_query_elements: tuple[str, ...] = ()
        if fill_missing == 'allow-hourly-aggregate':
            provider_mapping = {
                **provider_mapping,
                'wind_speed': dict(PL_HOURLY_PROVIDER_ELEMENT_MAPPING['wind_speed']),
                'vapour_pressure': dict(PL_HOURLY_PROVIDER_ELEMENT_MAPPING['vapour_pressure']),
            }
            assumptions.update(
                {
                    'hourly_supplement_policy': 'The explicit allow-hourly-aggregate policy may supplement missing daily wind_speed and vapour_pressure from official IMGW historical/1hour synop observations. This remains a daily FAO-input-preparation workflow and does not compute FAO-56 ET0.',
                    'hourly_aggregation_boundary': 'Poland hourly supplementation groups normalized IMGW historical/1hour timestamps by UTC calendar day in this first workflow slice.',
                    'hourly_aggregation_threshold': f'Daily hourly supplements are filled only when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations are available for that day; otherwise the field stays missing.',
                }
            )
            dataset_type = 'IMGW-PIB Poland daily input bundle with opt-in hourly supplementation prepared for later FAO workflow packaging'
            source = 'IMGW-PIB public daily synop station observations plus opt-in official historical/1hour synop supplementation from the official archive'
            hourly_query_elements = ('wind_speed', 'vapour_pressure')
        return FaoCountryConfig('PL', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, PL_REQUIRED_OBSERVED_ELEMENTS, query_elements, provider_mapping, assumptions, dataset_type, source, 'historical' if hourly_query_elements else None, '1hour' if hourly_query_elements else None, hourly_query_elements)
    if normalized_country == 'NL':
        return FaoCountryConfig('NL', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, NL_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(NL_PROVIDER_ELEMENT_MAPPING), dict(NL_ASSUMPTIONS), 'KNMI observed daily input bundle prepared for later FAO workflow packaging', 'KNMI Open Data API validated daily in-situ meteorological observations')
    if normalized_country == 'SE':
        return FaoCountryConfig('SE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, SE_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(SE_PROVIDER_ELEMENT_MAPPING), dict(SE_ASSUMPTIONS), 'SMHI Sweden observed daily input bundle prepared for later FAO workflow packaging', 'SMHI Meteorological Observations corrected-archive daily station observations')
    raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.')


def list_supported_fao_countries() -> list[str]:
    supported: list[str] = []
    for country in ['CZ', 'DE', 'AT', 'BE', 'CH', 'DK', 'HU', 'PL', 'NL', 'SE']:
        try:
            get_fao_country_config(country)
        except Exception:
            continue
        supported.append(country)
    return supported
