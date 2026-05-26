from __future__ import annotations

from importlib import import_module

from .at import PROVIDER as GEOSPHERE_PROVIDER
from .base import WeatherProvider
from .be import PROVIDER as BE_PROVIDER
from .bg import PROVIDER as BG_GHCND_PROVIDER
from .ca import PROVIDER as CA_GHCND_PROVIDER
from .ch import PROVIDER as CH_PROVIDER
from .cz import PROVIDER as CHMI_PROVIDER
from .de import PROVIDER as DWD_PROVIDER
from .dk import PROVIDER as DK_PROVIDER
from .ee import PROVIDER as EE_GHCND_PROVIDER
from .es import PROVIDER as ES_PROVIDER
from .fi import PROVIDER as FI_GHCND_PROVIDER
from .fr import PROVIDER as FR_GHCND_PROVIDER
from .gb import PROVIDER as GB_PROVIDER
from .gr import PROVIDER as GR_GHCND_PROVIDER
from .hr import PROVIDER as HR_GHCND_PROVIDER
from .hu import PROVIDER as HU_PROVIDER
from .ie import PROVIDER as IE_PROVIDER
from .it import PROVIDER as IT_GHCND_PROVIDER
from .lt import PROVIDER as LT_GHCND_PROVIDER
from .lu import PROVIDER as LU_PROVIDER
from .lv import PROVIDER as LV_GHCND_PROVIDER
from .mx import PROVIDER as MX_GHCND_PROVIDER
from .nl import PROVIDER as KNMI_PROVIDER
from .no import PROVIDER as NO_GHCND_PROVIDER
from .nz import PROVIDER as NZ_GHCND_PROVIDER
from .pl import PROVIDER as PL_PROVIDER
from .pt import PROVIDER as PT_PROVIDER
from .ro import PROVIDER as RO_GHCND_PROVIDER
from .se import PROVIDER as SE_PROVIDER
from .si import PROVIDER as SI_GHCND_PROVIDER
from .sk import PROVIDER as SHMU_PROVIDER
from .us import PROVIDER as US_GHCND_PROVIDER

IS_GHCND_PROVIDER = import_module('weatherdownload.providers.is').PROVIDER

PROVIDERS: dict[str, WeatherProvider] = {
    'AT': GEOSPHERE_PROVIDER,
    'BE': BE_PROVIDER,
    'BG': BG_GHCND_PROVIDER,
    'CA': CA_GHCND_PROVIDER,
    'CH': CH_PROVIDER,
    'CZ': CHMI_PROVIDER,
    'DE': DWD_PROVIDER,
    'DK': DK_PROVIDER,
    'EE': EE_GHCND_PROVIDER,
    'ES': ES_PROVIDER,
    'FI': FI_GHCND_PROVIDER,
    'FR': FR_GHCND_PROVIDER,
    'GB': GB_PROVIDER,
    'GR': GR_GHCND_PROVIDER,
    'HR': HR_GHCND_PROVIDER,
    'HU': HU_PROVIDER,
    'IE': IE_PROVIDER,
    'IS': IS_GHCND_PROVIDER,
    'IT': IT_GHCND_PROVIDER,
    'LT': LT_GHCND_PROVIDER,
    'LU': LU_PROVIDER,
    'LV': LV_GHCND_PROVIDER,
    'MX': MX_GHCND_PROVIDER,
    'NL': KNMI_PROVIDER,
    'NO': NO_GHCND_PROVIDER,
    'NZ': NZ_GHCND_PROVIDER,
    'PL': PL_PROVIDER,
    'PT': PT_PROVIDER,
    'RO': RO_GHCND_PROVIDER,
    'SE': SE_PROVIDER,
    'SI': SI_GHCND_PROVIDER,
    'SK': SHMU_PROVIDER,
    'US': US_GHCND_PROVIDER,
}

COUNTRY_ALIASES: dict[str, str] = {
    'UK': 'GB',
}


def normalize_country_code(country: str | None = None) -> str:
    if country is None:
        return 'CZ'
    normalized = country.strip().upper()
    if not normalized:
        return 'CZ'
    return COUNTRY_ALIASES.get(normalized, normalized)


def get_provider(country: str | None = None) -> WeatherProvider:
    normalized = normalize_country_code(country)
    provider = PROVIDERS.get(normalized)
    if provider is None:
        supported = ', '.join(sorted(PROVIDERS))
        raise ValueError(f"Unsupported country code: {country}. Supported countries: {supported}")
    return provider


def list_supported_countries() -> list[str]:
    return sorted(PROVIDERS)
