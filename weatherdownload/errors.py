from __future__ import annotations


class WeatherDownloadError(Exception):
    """Base exception for WeatherDownload library errors."""


class UnsupportedQueryError(WeatherDownloadError, ValueError):
    """Raised when a query combination is not supported by an implemented downloader."""


class StationNotFoundError(WeatherDownloadError, LookupError):
    """Raised when no source files can be found for requested stations."""


class EmptyResultError(WeatherDownloadError, ValueError):
    """Raised when a valid request returns no observation rows."""


class DownloadError(WeatherDownloadError, RuntimeError):
    """Raised when a remote CHMI download fails."""
