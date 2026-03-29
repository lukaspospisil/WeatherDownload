from __future__ import annotations


class WeatherDownloadError(Exception):
    """Base exception for WeatherDownload library errors."""


class UnsupportedQueryError(WeatherDownloadError, ValueError):
    """Raised when a query combination is structurally unsupported by a downloader."""


class DatasetNotImplementedError(WeatherDownloadError, NotImplementedError):
    """Raised when a CHMI dataset combination is valid but not implemented by the library yet."""


class StationNotFoundError(WeatherDownloadError, LookupError):
    """Raised when no source files can be found for requested stations."""


class EmptyResultError(WeatherDownloadError, ValueError):
    """Raised when a valid request returns no observation rows."""


class DownloadError(WeatherDownloadError, RuntimeError):
    """Raised when a remote CHMI download fails."""

