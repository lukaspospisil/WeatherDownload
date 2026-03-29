from __future__ import annotations

from .base import WeatherProvider
from .be import PROVIDER as BE_PROVIDER
from .ch import PROVIDER as CH_PROVIDER
from .chmi import PROVIDER as CHMI_PROVIDER
from .dk import PROVIDER as DK_PROVIDER
from .dwd import PROVIDER as DWD_PROVIDER
from .geosphere import PROVIDER as GEOSPHERE_PROVIDER
from .hu import PROVIDER as HU_PROVIDER
from .knmi import PROVIDER as KNMI_PROVIDER
from .pl import PROVIDER as PL_PROVIDER
from .se import PROVIDER as SE_PROVIDER
from .shmu import PROVIDER as SHMU_PROVIDER

PROVIDERS: dict[str, WeatherProvider] = {
    'AT': GEOSPHERE_PROVIDER,
    'BE': BE_PROVIDER,
    'CH': CH_PROVIDER,
    'CZ': CHMI_PROVIDER,
    'DE': DWD_PROVIDER,
    'DK': DK_PROVIDER,
    'HU': HU_PROVIDER,
    'NL': KNMI_PROVIDER,
    'PL': PL_PROVIDER,
    'SE': SE_PROVIDER,
    'SK': SHMU_PROVIDER,
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

