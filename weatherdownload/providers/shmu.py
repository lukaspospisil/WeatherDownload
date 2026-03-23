from __future__ import annotations

from ..errors import UnsupportedQueryError
from ..shmu_metadata import read_station_metadata_shmu, read_station_observation_metadata_shmu
from ..shmu_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from .base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'sunshine_duration',
    'precipitation',
)


def _download_observations(*args, **kwargs):
    from ..observations import _download_observations_shmu

    query = args[0] if args else kwargs.get('query')
    _assert_supported_query(query)
    return _download_observations_shmu(*args, **kwargs)


def _assert_supported_query(query) -> None:
    if query is None:
        raise UnsupportedQueryError("Experimental SHMU provider requires an ObservationQuery.")
    if getattr(query, 'country', '').strip().upper() != 'SK':
        raise UnsupportedQueryError("Experimental SHMU provider supports only country='SK'.")
    if getattr(query, 'dataset_scope', None) != 'recent':
        raise UnsupportedQueryError("Experimental SHMU provider supports only dataset_scope='recent'.")
    if getattr(query, 'resolution', None) != 'daily':
        raise UnsupportedQueryError("Experimental SHMU provider supports only resolution='daily'.")


PROVIDER = WeatherProvider(
    country_code='SK',
    name='SHMU (experimental)',
    read_station_metadata=read_station_metadata_shmu,
    read_station_observation_metadata=read_station_observation_metadata_shmu,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('SK',),
    supported_dataset_scopes=('recent',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=True,
)
