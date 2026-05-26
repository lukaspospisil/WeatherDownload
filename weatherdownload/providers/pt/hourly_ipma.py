from __future__ import annotations

import pandas as pd

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .ipma_parser import (
    IPMA_NORMALIZED_SUBDAILY_COLUMNS,
    normalize_ipma_hourly_observations,
    normalize_ipma_interval_value,
    read_text_from_source,
)
from .metadata import read_station_metadata_ipma
from .registry import get_dataset_spec


def download_hourly_observations_ipma(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'ipma' or query.resolution != '1hour':
        raise UnsupportedQueryError("The IPMA Portugal downloader supports only provider='ipma' and resolution='1hour'.")
    if query.all_history:
        raise UnsupportedQueryError("The IPMA Portugal downloader does not support all_history for provider='ipma'.")
    if not query.elements:
        raise UnsupportedQueryError('The IPMA Portugal hourly downloader requires at least one element.')
    if query.start is None or query.end is None:
        raise UnsupportedQueryError('The IPMA Portugal hourly downloader requires start and end timestamps.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ipma(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No IPMA Portugal station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No IPMA Portugal station metadata found for station_id: {', '.join(missing_station_ids)}")

    spec = get_dataset_spec('ipma', '1hour')
    observation_text = read_text_from_source(spec.data_url or '', timeout=timeout)
    normalized = normalize_ipma_hourly_observations(
        observation_text,
        station_ids=query.station_ids,
        raw_elements=query.elements or [],
        provider=query.provider,
        resolution=query.resolution,
        start=query.start,
        end=query.end,
        station_metadata=metadata_table,
    )
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')

    element_columns = canonicalize_element_series(normalized['element_raw'], query)
    normalized['element'] = element_columns['element']
    normalized['element_raw'] = element_columns['element_raw']
    normalized['value'] = [
        normalize_ipma_interval_value(str(raw_code), value)
        for raw_code, value in zip(normalized['element_raw'], normalized['value'], strict=False)
    ]
    return normalized.loc[:, IPMA_NORMALIZED_SUBDAILY_COLUMNS].sort_values(
        ['station_id', 'timestamp', 'element'],
        kind='stable',
    ).reset_index(drop=True)
