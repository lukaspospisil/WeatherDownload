from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GeosphereDatasetSpec:
    dataset_scope: str
    resolution: str
    source_id: str
    label: str
    metadata_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


_GEOSPHERE_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('tl_mittel',),
    'tas_max': ('tlmax',),
    'tas_min': ('tlmin',),
    'precipitation': ('rr',),
    'sunshine_duration': ('so_h',),
    'wind_speed': ('vv_mittel',),
    'pressure': ('p_mittel',),
    'relative_humidity': ('rf_mittel',),
}

_GEOSPHERE_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('tl',),
    'precipitation': ('rr',),
    'wind_speed': ('ff',),
    'relative_humidity': ('rf',),
    'pressure': ('p',),
    'sunshine_duration': ('so',),
}

_GEOSPHERE_TENMIN_CANONICAL_ELEMENTS = {
    'tas_mean': ('tl',),
    'precipitation': ('rr',),
    'wind_speed': ('ff',),
    'relative_humidity': ('rf',),
    'pressure': ('p',),
    'sunshine_duration': ('so',),
}

_GEOSPHERE_DATASET_SPECS = [
    GeosphereDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        source_id='klima-v2-1d',
        label='GeoSphere Austria station historical daily climate observations v2',
        metadata_url='https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d/metadata',
        supported_elements=('tl_mittel', 'tlmax', 'tlmin', 'rr', 'so_h', 'vv_mittel', 'p_mittel', 'rf_mittel'),
        canonical_elements=_GEOSPHERE_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    GeosphereDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='klima-v2-1h',
        label='GeoSphere Austria station historical hourly climate observations v2',
        metadata_url='https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h/metadata',
        supported_elements=('tl', 'rr', 'ff', 'rf', 'p', 'so'),
        canonical_elements=_GEOSPHERE_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    GeosphereDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='klima-v2-10min',
        label='GeoSphere Austria station historical 10-minute climate observations v2',
        metadata_url='https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min/metadata',
        supported_elements=('tl', 'rr', 'ff', 'rf', 'p', 'so'),
        canonical_elements=_GEOSPHERE_TENMIN_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[GeosphereDatasetSpec]:
    return list(_GEOSPHERE_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[GeosphereDatasetSpec]:
    return [spec for spec in _GEOSPHERE_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> GeosphereDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _GEOSPHERE_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported GeoSphere Austria dataset combination: {dataset_scope}/{resolution}')

