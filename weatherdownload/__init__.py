"""Public API for working with CHMI weather data."""

from .availability import list_station_elements, list_station_paths, station_availability, station_supports
from .chmi_registry import ChmiDatasetSpec, get_dataset_spec
from .discovery import list_dataset_scopes, list_resolutions, list_supported_elements
from .errors import DatasetNotImplementedError, DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .exporting import export_table
from .metadata import DEFAULT_META1_URL, filter_stations, read_station_metadata
from .observations import download_observations
from .queries import ObservationQuery, QueryValidationError, validate_observation_query

__all__ = [
    'ChmiDatasetSpec', 'DEFAULT_META1_URL', 'DatasetNotImplementedError', 'DownloadError', 'EmptyResultError', 'ObservationQuery', 'QueryValidationError',
    'StationNotFoundError', 'UnsupportedQueryError', 'download_observations', 'export_table', 'filter_stations', 'get_dataset_spec',
    'list_dataset_scopes', 'list_resolutions', 'list_station_elements', 'list_station_paths', 'list_supported_elements', 'read_station_metadata',
    'station_availability', 'station_supports', 'validate_observation_query',
]
