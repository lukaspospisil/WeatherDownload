from __future__ import annotations

from ...errors import UnsupportedQueryError
from .ghcnd import (
    download_daily_observations_ghcnd,
    read_station_metadata_ghcnd,
    read_station_observation_metadata_ghcnd,
)
from .metadata import read_station_metadata_shmu, read_station_observation_metadata_shmu
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider
from ..ghcnd.mixed import (
    build_mixed_observation_downloader,
    build_mixed_station_metadata_reader,
    build_mixed_station_observation_metadata_reader,
)

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'sunshine_duration',
    'precipitation',
    'open_water_evaporation',
)


def _download_national_observations(*args, **kwargs):
    from ...observations import _download_observations_shmu

    query = args[0] if args else kwargs.get('query')
    _assert_supported_national_query(query)
    return _download_observations_shmu(*args, **kwargs)


def _assert_supported_national_query(query) -> None:
    if query is None:
        raise UnsupportedQueryError("Experimental SHMU provider requires an ObservationQuery.")
    if getattr(query, 'country', '').strip().upper() != 'SK':
        raise UnsupportedQueryError("Experimental SHMU provider supports only country='SK'.")
    if getattr(query, 'dataset_scope', None) != 'recent':
        raise UnsupportedQueryError("Experimental SHMU provider supports only dataset_scope='recent'.")
    if getattr(query, 'resolution', None) != 'daily':
        raise UnsupportedQueryError("Experimental SHMU provider supports only resolution='daily'.")


_read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_shmu,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
_read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_shmu,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
_download_observations = build_mixed_observation_downloader(
    download_national_observations=_download_national_observations,
    download_ghcnd_observations=download_daily_observations_ghcnd,
)


PROVIDER = WeatherProvider(
    country_code='SK',
    name='SHMU (experimental) + NOAA GHCN-Daily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('SK',),
    supported_dataset_scopes=('ghcnd', 'recent'),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=True,
)
