from __future__ import annotations

import pandas as pd

from .elements import element_mapping_for_spec, supported_elements_for_spec
from .queries import normalize_provider_scope


def list_dataset_scopes(country: str = 'CZ') -> list[str]:
    from ..providers import get_provider

    provider = get_provider(country)
    return sorted({spec.dataset_scope for spec in provider.list_dataset_specs()})


def list_providers(country: str = 'CZ') -> list[str]:
    return list_dataset_scopes(country=country)


def list_resolutions(dataset_scope: str | None = None, country: str = 'CZ', provider: str | None = None) -> list[str]:
    from ..providers import get_provider

    weather_provider = get_provider(country)
    specs = weather_provider.list_dataset_specs()
    if dataset_scope is None and provider is None:
        return sorted({spec.resolution for spec in specs})
    normalized_scope = normalize_provider_scope(dataset_scope=dataset_scope, provider=provider)
    resolutions = sorted({spec.resolution for spec in specs if spec.dataset_scope == normalized_scope})
    if not resolutions:
        raise ValueError(f'Unsupported provider: {normalized_scope}')
    return resolutions


def list_supported_elements(
    resolution: str | None = None,
    dataset_scope: str | None = None,
    provider: str | None = None,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_mapping: bool = False,
):
    from ..providers import get_provider

    weather_provider = get_provider(country)
    specs = weather_provider.list_dataset_specs()
    normalized_scope = None
    if dataset_scope is not None or provider is not None:
        normalized_scope = normalize_provider_scope(dataset_scope=dataset_scope, provider=provider)
    if normalized_scope is not None and resolution is not None:
        spec = weather_provider.get_dataset_spec(normalized_scope, resolution)
        if include_mapping:
            return element_mapping_for_spec(spec)
        return supported_elements_for_spec(spec, provider_raw=provider_raw)

    if resolution is not None:
        matches = [spec for spec in specs if spec.resolution == resolution.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for resolution='{resolution}'.")
    elif normalized_scope is not None:
        matches = [spec for spec in specs if spec.dataset_scope == normalized_scope]
        if not matches:
            raise ValueError(f"No supported elements are defined for provider='{normalized_scope}'.")
    else:
        matches = specs

    if include_mapping:
        frames = [element_mapping_for_spec(spec) for spec in matches]
        if not frames:
            return pd.DataFrame(columns=['element', 'element_raw', 'raw_elements'])
        combined = pd.concat(frames, ignore_index=True)
        return combined.drop_duplicates(subset=['element', 'element_raw']).reset_index(drop=True)

    return sorted({element for spec in matches for element in supported_elements_for_spec(spec, provider_raw=provider_raw)})


