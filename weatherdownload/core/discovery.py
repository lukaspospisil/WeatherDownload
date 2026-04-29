from __future__ import annotations

import pandas as pd

from .elements import element_mapping_for_spec, supported_elements_for_spec
from .queries import normalize_provider_name


def list_providers(country: str = 'CZ') -> list[str]:
    """List provider tokens for a country."""

    from ..providers import get_provider

    weather_provider = get_provider(country)
    return sorted({spec.provider for spec in weather_provider.list_dataset_specs()})


def list_resolutions(provider: str | None = None, country: str = 'CZ') -> list[str]:
    """List supported resolutions for a country or provider."""

    from ..providers import get_provider

    weather_provider = get_provider(country)
    specs = weather_provider.list_dataset_specs()
    if provider is None:
        return sorted({spec.resolution for spec in specs})
    normalized_provider = normalize_provider_name(provider)
    resolutions = sorted({spec.resolution for spec in specs if spec.provider == normalized_provider})
    if not resolutions:
        raise ValueError(f'Unsupported provider: {normalized_provider}')
    return resolutions


def list_supported_elements(
    resolution: str | None = None,
    provider: str | None = None,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_mapping: bool = False,
):
    """List supported canonical or raw elements for a provider path."""

    from ..providers import get_provider

    weather_provider = get_provider(country)
    specs = weather_provider.list_dataset_specs()
    normalized_provider = normalize_provider_name(provider) if provider is not None else None
    if normalized_provider is not None and resolution is not None:
        spec = weather_provider.get_dataset_spec(normalized_provider, resolution)
        if include_mapping:
            return element_mapping_for_spec(spec)
        return supported_elements_for_spec(spec, provider_raw=provider_raw)

    if resolution is not None:
        matches = [spec for spec in specs if spec.resolution == resolution.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for resolution='{resolution}'.")
    elif normalized_provider is not None:
        matches = [spec for spec in specs if spec.provider == normalized_provider]
        if not matches:
            raise ValueError(f"No supported elements are defined for provider='{normalized_provider}'.")
    else:
        matches = specs

    if include_mapping:
        frames = [element_mapping_for_spec(spec) for spec in matches]
        if not frames:
            return pd.DataFrame(columns=['element', 'element_raw', 'raw_elements'])
        combined = pd.concat(frames, ignore_index=True)
        return combined.drop_duplicates(subset=['element', 'element_raw']).reset_index(drop=True)

    return sorted({element for spec in matches for element in supported_elements_for_spec(spec, provider_raw=provider_raw)})
