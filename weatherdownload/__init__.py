"""Public API for working with country-specific weather providers."""

from .availability import list_station_elements, list_station_paths, station_availability, station_supports
from .providers.cz.registry import ChmiDatasetSpec, get_dataset_spec
from .discovery import list_dataset_scopes, list_resolutions, list_supported_elements
from .errors import DatasetNotImplementedError, DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .exporting import export_table
from .metadata import DEFAULT_META1_URL, DEFAULT_META2_URL, filter_stations, read_station_metadata, read_station_observation_metadata
from .observations import download_observations
from .providers import get_provider, list_supported_countries, normalize_country_code
from .queries import ObservationQuery, QueryValidationError, validate_observation_query

__all__ = [
    'ChmiDatasetSpec', 'DEFAULT_META1_URL', 'DEFAULT_META2_URL', 'DatasetNotImplementedError', 'DownloadError', 'EmptyResultError', 'ObservationQuery', 'QueryValidationError',
    'StationNotFoundError', 'UnsupportedQueryError', 'download_observations', 'export_table', 'filter_stations', 'get_dataset_spec', 'get_provider',
    'list_dataset_scopes', 'list_resolutions', 'list_station_elements', 'list_station_paths', 'list_supported_countries', 'list_supported_elements', 'normalize_country_code',
    'read_station_metadata', 'read_station_observation_metadata', 'station_availability', 'station_supports', 'validate_observation_query',
]

