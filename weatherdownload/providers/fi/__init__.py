from __future__ import annotations

from ...errors import UnsupportedQueryError
from .metadata import read_station_metadata_ghcnd, read_station_observation_metadata_ghcnd
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'precipitation',
)


def _download_observations(*args, **kwargs):
    from .observations import download_daily_observations_ghcnd

    query = args[0] if args else kwargs.get('query')
    _assert_supported_query(query)
    return download_daily_observations_ghcnd(*args, **kwargs)


def _assert_supported_query(query) -> None:
    if query is None:
        raise UnsupportedQueryError('NOAA GHCN-Daily provider requires an ObservationQuery.')
    if getattr(query, 'country', '').strip().upper() != 'FI':
        raise UnsupportedQueryError("NOAA GHCN-Daily provider supports only country='FI'.")
    if getattr(query, 'dataset_scope', None) != 'ghcnd':
        raise UnsupportedQueryError("NOAA GHCN-Daily provider supports only dataset_scope='ghcnd'.")
    if getattr(query, 'resolution', None) != 'daily':
        raise UnsupportedQueryError("NOAA GHCN-Daily provider supports only resolution='daily'.")


PROVIDER = WeatherProvider(
    country_code='FI',
    name='NOAA NCEI GHCN-Daily',
    read_station_metadata=read_station_metadata_ghcnd,
    read_station_observation_metadata=read_station_observation_metadata_ghcnd,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('FI',),
    supported_dataset_scopes=('ghcnd',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
)
