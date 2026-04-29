from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

import pandas as pd

from .elements import element_mapping_dict_for_spec, element_mapping_for_spec, supported_elements_for_spec
from .metadata import STATION_METADATA_COLUMNS, filter_stations, read_station_metadata, read_station_observation_metadata
from .queries import normalize_provider_scope

AVAILABILITY_COLUMNS = [
    'station_id',
    'gh_id',
    'dataset_scope',
    'resolution',
    'implemented',
    'supported_elements',
]
FIND_STATIONS_COLUMNS = [
    'station_id',
    'gh_id',
    'full_name',
    'longitude',
    'latitude',
    'elevation_m',
    'begin_date',
    'end_date',
    'provider',
    'dataset_scope',
    'resolution',
    'matched_elements',
    'missing_requested_elements',
    'matching_begin_date',
    'matching_end_date',
]


def station_availability(
    stations: pd.DataFrame,
    station_ids: Sequence[str] | None = None,
    active_on: date | datetime | str | None = None,
    implemented_only: bool = True,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_element_mapping: bool = False,
) -> pd.DataFrame:
    from ..providers import get_provider

    provider = get_provider(country)
    filtered = filter_stations(stations, station_ids=station_ids, active_on=active_on)
    specs = provider.list_implemented_dataset_specs() if implemented_only else []
    rows: list[dict[str, object]] = []

    for station in filtered.itertuples(index=False):
        for spec in specs:
            station_supported_elements = _station_supported_elements_for_spec(
                stations,
                station.station_id,
                spec,
                provider_raw=provider_raw,
            )
            if station_supported_elements is None:
                station_supported_elements = supported_elements_for_spec(spec, provider_raw=provider_raw)
            if not station_supported_elements:
                continue
            row = {
                'station_id': station.station_id,
                'gh_id': station.gh_id,
                'dataset_scope': spec.dataset_scope,
                'resolution': spec.resolution,
                'implemented': spec.implemented,
                'supported_elements': station_supported_elements,
            }
            if include_element_mapping:
                station_mapping = _station_supported_element_mapping_for_spec(stations, station.station_id, spec)
                row['supported_element_mapping'] = station_mapping if station_mapping is not None else element_mapping_dict_for_spec(spec)
            rows.append(row)

    columns = AVAILABILITY_COLUMNS + (['supported_element_mapping'] if include_element_mapping else [])
    return pd.DataFrame.from_records(rows, columns=columns)


def station_supports(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str | None,
    resolution: str,
    active_on: date | datetime | str | None = None,
    country: str = 'CZ',
    provider: str | None = None,
) -> bool:
    from ..providers import get_provider

    weather_provider = get_provider(country)
    normalized_scope = normalize_provider_scope(dataset_scope=dataset_scope, provider=provider)
    spec = weather_provider.get_dataset_spec(normalized_scope, resolution)
    if not spec.implemented:
        return False
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
    )
    return not availability[
        (availability['dataset_scope'] == spec.dataset_scope)
        & (availability['resolution'] == spec.resolution)
    ].empty


def list_station_paths(
    stations: pd.DataFrame,
    station_id: str,
    active_on: date | datetime | str | None = None,
    include_elements: bool = False,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_element_mapping: bool = False,
) -> pd.DataFrame:
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
        provider_raw=provider_raw,
        include_element_mapping=include_element_mapping,
    )
    if availability.empty:
        return availability
    if include_elements or include_element_mapping:
        return availability.reset_index(drop=True)
    return availability.drop(columns=['supported_elements']).reset_index(drop=True)


def list_station_elements(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str | None,
    resolution: str,
    active_on: date | datetime | str | None = None,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_mapping: bool = False,
    provider: str | None = None,
):
    from ..providers import get_provider

    normalized_scope = normalize_provider_scope(dataset_scope=dataset_scope, provider=provider)
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
        provider_raw=provider_raw,
    )
    matches = availability[
        (availability['dataset_scope'] == normalized_scope)
        & (availability['resolution'] == resolution.strip())
    ]
    if matches.empty:
        if include_mapping:
            return pd.DataFrame(columns=['station_id', 'dataset_scope', 'resolution', 'element', 'element_raw', 'raw_elements'])
        return []
    if include_mapping:
        weather_provider = get_provider(country)
        spec = weather_provider.get_dataset_spec(normalized_scope, resolution)
        station_mapping = _station_mapping_frame_for_spec(stations, station_id, spec)
        mapping = station_mapping.copy() if station_mapping is not None else element_mapping_for_spec(spec).copy()
        mapping.insert(0, 'resolution', resolution.strip())
        mapping.insert(0, 'dataset_scope', normalized_scope)
        mapping.insert(0, 'station_id', station_id)
        return mapping.reset_index(drop=True)
    return list(matches.iloc[0]['supported_elements'])


def find_stations_with_elements(
    *,
    elements: Sequence[str],
    resolution: str,
    country: str = 'CZ',
    dataset_scope: str | None = None,
    provider: str | None = None,
    station_ids: Sequence[str] | None = None,
    active_on: date | datetime | str | None = None,
    source_url: str | None = None,
    stations: pd.DataFrame | None = None,
    observation_metadata: pd.DataFrame | None = None,
    observation_metadata_source_url: str | None = None,
    timeout: int = 60,
) -> pd.DataFrame:
    from ..providers import get_provider

    weather_provider = get_provider(country)
    resolved_provider = _resolve_provider_for_station_search(
        weather_provider,
        country=country,
        resolution=resolution,
        dataset_scope=dataset_scope,
        provider=provider,
    )
    spec = weather_provider.get_dataset_spec(resolved_provider, resolution)
    requested_elements = _normalize_requested_canonical_elements(elements, spec)
    if not requested_elements:
        raise ValueError('Use at least one supported canonical or raw element.')

    stations_table = stations if stations is not None else read_station_metadata(country=country, source_url=source_url, timeout=timeout)
    filtered_stations = filter_stations(stations_table, station_ids=station_ids, active_on=active_on)
    if filtered_stations.empty:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    observation_table = observation_metadata
    if observation_table is None:
        observation_table = read_station_observation_metadata(
            country=country,
            source_url=observation_metadata_source_url,
            timeout=timeout,
        )

    station_matches = _find_station_matches_from_observation_metadata(
        filtered_stations,
        observation_table,
        spec,
        requested_elements=requested_elements,
        active_on=active_on,
    )
    if station_matches is None:
        station_matches = _find_station_matches_from_station_availability(
            filtered_stations,
            spec,
            country=country,
            requested_elements=requested_elements,
            active_on=active_on,
        )
    if station_matches.empty:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    station_matches.insert(8, 'provider', resolved_provider)
    station_matches.insert(9, 'dataset_scope', resolved_provider)
    station_matches.insert(10, 'resolution', spec.resolution)
    return station_matches.loc[:, FIND_STATIONS_COLUMNS].reset_index(drop=True)


def _station_provider_raw_elements_by_path(stations: pd.DataFrame) -> dict[tuple[str, str], dict[str, list[str]]]:
    return stations.attrs.get('station_provider_raw_elements_by_path', {})


def _station_raw_elements_for_spec(stations: pd.DataFrame, station_id: str, spec) -> list[str] | None:
    mapping = _station_provider_raw_elements_by_path(stations)
    station_map = mapping.get((spec.dataset_scope, spec.resolution))
    if station_map is None:
        return None
    raw_elements = station_map.get(str(station_id))
    if raw_elements is None:
        return []
    return list(raw_elements)


def _station_supported_elements_for_spec(
    stations: pd.DataFrame,
    station_id: str,
    spec,
    *,
    provider_raw: bool,
) -> list[str] | None:
    raw_elements = _station_raw_elements_for_spec(stations, station_id, spec)
    if raw_elements is None:
        return None
    if provider_raw:
        return raw_elements
    mapping = element_mapping_for_spec(spec)
    return [
        str(row.element)
        for row in mapping.itertuples(index=False)
        if str(row.element_raw) in raw_elements
    ]


def _station_supported_element_mapping_for_spec(
    stations: pd.DataFrame,
    station_id: str,
    spec,
) -> dict[str, list[str]] | None:
    mapping_frame = _station_mapping_frame_for_spec(stations, station_id, spec)
    if mapping_frame is None:
        return None
    return {
        str(row.element): list(row.raw_elements)
        for row in mapping_frame.itertuples(index=False)
    }


def _station_mapping_frame_for_spec(stations: pd.DataFrame, station_id: str, spec) -> pd.DataFrame | None:
    raw_elements = _station_raw_elements_for_spec(stations, station_id, spec)
    if raw_elements is None:
        return None
    mapping = element_mapping_for_spec(spec)
    if mapping.empty:
        return mapping
    filtered = mapping[mapping['element_raw'].isin(raw_elements)].copy()
    return filtered.reset_index(drop=True)


def _resolve_provider_for_station_search(
    weather_provider,
    *,
    country: str,
    resolution: str,
    dataset_scope: str | None,
    provider: str | None,
) -> str:
    if dataset_scope is not None or provider is not None:
        return normalize_provider_scope(dataset_scope=dataset_scope, provider=provider)

    normalized_country = country.strip().upper()
    matching_scopes = sorted({
        spec.dataset_scope
        for spec in weather_provider.list_implemented_dataset_specs()
        if spec.resolution == resolution.strip()
    })
    if len(matching_scopes) == 1:
        return matching_scopes[0]
    if len(matching_scopes) > 1:
        choices = ', '.join(matching_scopes)
        raise ValueError(
            f"Multiple providers support country='{normalized_country}' and resolution='{resolution.strip()}': {choices}. "
            'Use provider=... (preferred) or dataset_scope=... explicitly.'
        )
    raise ValueError(f"Unsupported resolution '{resolution.strip()}' for country '{normalized_country}'.")


def _normalize_requested_canonical_elements(elements: Sequence[str], spec) -> list[str]:
    mapping = element_mapping_for_spec(spec)
    canonical_elements = supported_elements_for_spec(spec, provider_raw=False)
    raw_to_canonical: dict[str, str] = {}
    for row in mapping.itertuples(index=False):
        raw_to_canonical[str(row.element_raw).casefold()] = str(row.element)
        for raw_code in getattr(row, 'raw_elements', []):
            raw_to_canonical[str(raw_code).casefold()] = str(row.element)

    normalized: list[str] = []
    seen: set[str] = set()
    unsupported: list[str] = []
    for item in elements:
        cleaned = str(item).strip()
        if not cleaned:
            continue
        canonical = None
        if cleaned.lower() in canonical_elements:
            canonical = cleaned.lower()
        else:
            canonical = raw_to_canonical.get(cleaned.casefold())
        if canonical is None:
            unsupported.append(cleaned)
            continue
        if canonical not in seen:
            seen.add(canonical)
            normalized.append(canonical)
    if unsupported:
        supported = ', '.join(canonical_elements)
        unsupported_joined = ', '.join(unsupported)
        raise ValueError(
            f"Unsupported elements for provider='{spec.dataset_scope}' and resolution='{spec.resolution}': {unsupported_joined}. "
            f"Supported canonical elements: {supported}."
        )
    return normalized


def _find_station_matches_from_observation_metadata(
    stations: pd.DataFrame,
    observation_metadata: pd.DataFrame | None,
    spec,
    *,
    requested_elements: list[str],
    active_on: date | datetime | str | None,
) -> pd.DataFrame | None:
    if observation_metadata is None or observation_metadata.empty:
        return None
    required_columns = {'station_id', 'element'}
    if not required_columns.issubset(observation_metadata.columns):
        return None

    filtered = observation_metadata.copy()
    filtered['station_id'] = filtered['station_id'].astype(str)
    filtered = filtered[filtered['station_id'].isin(stations['station_id'].astype(str))]
    if filtered.empty:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    resolution_filtered = _filter_observation_metadata_for_resolution(filtered, spec.resolution)
    raw_elements = set(supported_elements_for_spec(spec, provider_raw=True))
    resolution_filtered = resolution_filtered[resolution_filtered['element'].astype(str).isin(raw_elements)].copy()
    if resolution_filtered.empty:
        return None

    mapping = element_mapping_for_spec(spec)
    raw_to_canonical: dict[str, str] = {}
    for row in mapping.itertuples(index=False):
        canonical = str(row.element)
        raw_to_canonical[str(row.element_raw)] = canonical
        for raw_code in getattr(row, 'raw_elements', []):
            raw_to_canonical[str(raw_code)] = canonical
    resolution_filtered['canonical_element'] = resolution_filtered['element'].astype(str).map(raw_to_canonical)
    resolution_filtered = resolution_filtered[resolution_filtered['canonical_element'].notna()].copy()
    if resolution_filtered.empty:
        return None

    resolution_filtered['begin_ts'] = pd.to_datetime(resolution_filtered.get('begin_date'), utc=True, errors='coerce')
    resolution_filtered['end_ts'] = pd.to_datetime(resolution_filtered.get('end_date'), utc=True, errors='coerce')
    per_element = (
        resolution_filtered.groupby(['station_id', 'canonical_element'], as_index=False)
        .agg(begin_ts=('begin_ts', 'min'), end_ts=('end_ts', 'max'))
    )
    grouped = []
    requested_set = set(requested_elements)
    for station_id, group in per_element.groupby('station_id', sort=True):
        available = [element for element in requested_elements if element in set(group['canonical_element'].tolist())]
        missing = [element for element in requested_elements if element not in set(available)]
        matching_begin = pd.NaT
        matching_end = pd.NaT
        if not missing:
            requested_group = group[group['canonical_element'].isin(requested_set)]
            if not requested_group.empty:
                matching_begin = requested_group['begin_ts'].max()
                matching_end = requested_group['end_ts'].min()
                if pd.notna(matching_begin) and pd.notna(matching_end) and matching_begin > matching_end:
                    missing = list(requested_elements)
                    available = []
        grouped.append(
            {
                'station_id': str(station_id),
                'matched_elements': available,
                'missing_requested_elements': missing,
                'matching_begin_ts': matching_begin,
                'matching_end_ts': matching_end,
            }
        )
    matches = pd.DataFrame.from_records(
        grouped,
        columns=['station_id', 'matched_elements', 'missing_requested_elements', 'matching_begin_ts', 'matching_end_ts'],
    )
    matches = matches[matches['missing_requested_elements'].apply(lambda values: len(values) == 0)].copy()
    if matches.empty:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    if active_on is not None:
        active_ts = pd.Timestamp(active_on)
        if active_ts.tzinfo is None:
            active_ts = active_ts.tz_localize('UTC')
        else:
            active_ts = active_ts.tz_convert('UTC')
        begin_ok = matches['matching_begin_ts'].isna() | (matches['matching_begin_ts'] <= active_ts)
        end_ok = matches['matching_end_ts'].isna() | (matches['matching_end_ts'] >= active_ts)
        matches = matches[begin_ok & end_ok].copy()
        if matches.empty:
            return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    station_metadata = stations.reindex(columns=STATION_METADATA_COLUMNS)
    merged = station_metadata.merge(matches, on='station_id', how='inner')
    merged['matching_begin_date'] = _format_timestamp_series(merged['matching_begin_ts'])
    merged['matching_end_date'] = _format_timestamp_series(merged['matching_end_ts'])
    return merged.drop(columns=['matching_begin_ts', 'matching_end_ts']).reset_index(drop=True)


def _find_station_matches_from_station_availability(
    stations: pd.DataFrame,
    spec,
    *,
    country: str,
    requested_elements: list[str],
    active_on: date | datetime | str | None,
) -> pd.DataFrame:
    availability = station_availability(
        stations,
        station_ids=stations['station_id'].astype(str).tolist(),
        active_on=active_on,
        implemented_only=True,
        country=country,
    )
    availability = availability[
        (availability['dataset_scope'] == spec.dataset_scope)
        & (availability['resolution'] == spec.resolution)
    ].copy()
    if availability.empty:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)

    rows: list[dict[str, object]] = []
    for row in availability.itertuples(index=False):
        supported = list(row.supported_elements)
        matched = [element for element in requested_elements if element in supported]
        missing = [element for element in requested_elements if element not in supported]
        if missing:
            continue
        rows.append(
            {
                'station_id': row.station_id,
                'matched_elements': matched,
                'missing_requested_elements': missing,
                'matching_begin_date': pd.NA,
                'matching_end_date': pd.NA,
            }
        )
    if not rows:
        return pd.DataFrame(columns=FIND_STATIONS_COLUMNS)
    matches = pd.DataFrame.from_records(
        rows,
        columns=['station_id', 'matched_elements', 'missing_requested_elements', 'matching_begin_date', 'matching_end_date'],
    )
    station_metadata = stations.reindex(columns=STATION_METADATA_COLUMNS)
    return station_metadata.merge(matches, on='station_id', how='inner').reset_index(drop=True)


def _filter_observation_metadata_for_resolution(observation_metadata: pd.DataFrame, resolution: str) -> pd.DataFrame:
    if 'obs_type' not in observation_metadata.columns:
        return observation_metadata
    tokens_by_resolution = {
        'daily': ('DLY', 'DAILY'),
        '1hour': ('HLY', 'HOURLY', '1H', '1HR'),
        '10min': ('10M', '10MIN'),
    }
    tokens = tokens_by_resolution.get(resolution.strip())
    if not tokens:
        return observation_metadata
    obs_type = observation_metadata['obs_type'].astype(str).str.upper()
    mask = obs_type.apply(lambda value: any(token in value for token in tokens))
    filtered = observation_metadata[mask].copy()
    if filtered.empty:
        return observation_metadata
    return filtered


def _format_timestamp_series(series: pd.Series) -> pd.Series:
    formatted = pd.Series(pd.NA, index=series.index, dtype='object')
    non_null = series.notna()
    if non_null.any():
        formatted.loc[non_null] = pd.to_datetime(series.loc[non_null], utc=True).dt.strftime('%Y-%m-%d')
    return formatted


