"""Public API for working with CHMI weather data."""

from .discovery import list_dataset_scopes, list_resolutions, list_supported_elements
from .errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .exporting import export_table
from .metadata import DEFAULT_META1_URL, filter_stations, read_station_metadata
from .observations import download_observations
from .queries import ObservationQuery, QueryValidationError, validate_observation_query

__all__ = [
    'DEFAULT_META1_URL', 'DownloadError', 'EmptyResultError', 'ObservationQuery', 'QueryValidationError',
    'StationNotFoundError', 'UnsupportedQueryError', 'download_observations', 'export_table', 'filter_stations',
    'list_dataset_scopes', 'list_resolutions', 'list_supported_elements', 'read_station_metadata',
    'validate_observation_query',
]
