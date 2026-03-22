from __future__ import annotations

from .base import WeatherProvider
from .chmi import PROVIDER as CHMI_PROVIDER
from .dwd import PROVIDER as DWD_PROVIDER

PROVIDERS: dict[str, WeatherProvider] = {
    'CZ': CHMI_PROVIDER,
    'DE': DWD_PROVIDER,
}


def normalize_country_code(country: str | None = None) -> str:
    if country is None:
        return 'CZ'
    normalized = country.strip().upper()
    if not normalized:
        return 'CZ'
    return normalized


def get_provider(country: str | None = None) -> WeatherProvider:
    normalized = normalize_country_code(country)
    provider = PROVIDERS.get(normalized)
    if provider is None:
        supported = ', '.join(sorted(PROVIDERS))
        raise ValueError(f"Unsupported country code: {country}. Supported countries: {supported}")
    return provider


def list_supported_countries() -> list[str]:
    return sorted(PROVIDERS)
