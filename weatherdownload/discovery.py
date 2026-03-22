from __future__ import annotations

from .elements import supported_elements_for_spec


def list_dataset_scopes(country: str = 'CZ') -> list[str]:
    from .providers import get_provider

    provider = get_provider(country)
    return sorted({spec.dataset_scope for spec in provider.list_dataset_specs()})


def list_resolutions(dataset_scope: str | None = None, country: str = 'CZ') -> list[str]:
    from .providers import get_provider

    provider = get_provider(country)
    specs = provider.list_dataset_specs()
    if dataset_scope is None:
        return sorted({spec.resolution for spec in specs})
    normalized_scope = dataset_scope.strip()
    resolutions = sorted({spec.resolution for spec in specs if spec.dataset_scope == normalized_scope})
    if not resolutions:
        raise ValueError(f'Unsupported dataset_scope: {dataset_scope}')
    return resolutions


def list_supported_elements(
    resolution: str | None = None,
    dataset_scope: str | None = None,
    country: str = 'CZ',
    provider_raw: bool = False,
) -> list[str]:
    from .providers import get_provider

    provider = get_provider(country)
    specs = provider.list_dataset_specs()
    if dataset_scope is not None and resolution is not None:
        spec = provider.get_dataset_spec(dataset_scope, resolution)
        return supported_elements_for_spec(spec, provider_raw=provider_raw)
    if resolution is not None:
        matches = [spec for spec in specs if spec.resolution == resolution.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for resolution='{resolution}'.")
        return sorted({element for spec in matches for element in supported_elements_for_spec(spec, provider_raw=provider_raw)})
    if dataset_scope is not None:
        matches = [spec for spec in specs if spec.dataset_scope == dataset_scope.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for dataset_scope='{dataset_scope}'.")
        return sorted({element for spec in matches for element in supported_elements_for_spec(spec, provider_raw=provider_raw)})
    return sorted({element for spec in specs for element in supported_elements_for_spec(spec, provider_raw=provider_raw)})
