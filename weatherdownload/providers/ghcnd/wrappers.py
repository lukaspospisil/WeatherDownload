from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable

import pandas as pd

from ...errors import UnsupportedQueryError
from ...queries import ObservationQuery
from ..base import WeatherProvider
from .registry import (
    GhcndDatasetSpec,
    build_country_dataset_specs,
    get_country_dataset_spec,
    list_country_dataset_specs,
    list_country_implemented_dataset_specs,
)
from .metadata import (
    read_station_metadata_ghcnd as read_station_metadata_ghcnd_shared,
    read_station_observation_metadata_ghcnd as read_station_observation_metadata_ghcnd_shared,
)
from .observations import (
    build_station_dly_url as build_station_dly_url_shared,
    download_daily_observations_ghcnd as download_daily_observations_ghcnd_shared,
)


@dataclass(frozen=True, slots=True)
class GhcndCountryWrapperBundle:
    country_code: str
    ghcn_prefix: str
    supported_canonical_elements: tuple[str, ...]
    supported_raw_elements: tuple[str, ...]
    canonical_elements: dict[str, tuple[str, ...]]
    list_dataset_specs: Callable[[], list[GhcndDatasetSpec]]
    list_implemented_dataset_specs: Callable[[], list[GhcndDatasetSpec]]
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec]
    read_station_metadata: Callable[[str | None, int], pd.DataFrame]
    read_station_observation_metadata: Callable[[str | None, int], pd.DataFrame]
    download_daily_observations: Callable[[ObservationQuery, int, pd.DataFrame | None], pd.DataFrame]
    build_station_dly_url: Callable[[str], str]
    provider: WeatherProvider


def assert_supported_ghcnd_query(query: ObservationQuery | None, *, country_code: str) -> None:
    if query is None:
        raise UnsupportedQueryError('NOAA GHCN-Daily provider requires an ObservationQuery.')
    if getattr(query, 'country', '').strip().upper() != country_code:
        raise UnsupportedQueryError(f"NOAA GHCN-Daily provider supports only country='{country_code}'.")
    if getattr(query, 'dataset_scope', None) != 'ghcnd':
        raise UnsupportedQueryError(
            "NOAA GHCN-Daily provider supports provider='ghcnd' (dataset_scope='ghcnd')."
        )
    if getattr(query, 'resolution', None) != 'daily':
        raise UnsupportedQueryError("NOAA GHCN-Daily provider supports only resolution='daily'.")


def build_country_provider(
    *,
    country_code: str,
    read_station_metadata: Callable[..., pd.DataFrame],
    read_station_observation_metadata: Callable[..., pd.DataFrame],
    list_dataset_specs: Callable[[], list[GhcndDatasetSpec]],
    list_implemented_dataset_specs: Callable[[], list[GhcndDatasetSpec]],
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec],
    download_daily_observations: Callable[..., pd.DataFrame],
    supported_canonical_elements: tuple[str, ...],
) -> WeatherProvider:
    def _download_observations(*args, **kwargs):
        query = args[0] if args else kwargs.get('query')
        assert_supported_ghcnd_query(query, country_code=country_code)
        return download_daily_observations(*args, **kwargs)

    return WeatherProvider(
        country_code=country_code,
        name='NOAA NCEI GHCN-Daily',
        read_station_metadata=read_station_metadata,
        read_station_observation_metadata=read_station_observation_metadata,
        list_dataset_specs=list_dataset_specs,
        list_implemented_dataset_specs=list_implemented_dataset_specs,
        get_dataset_spec=get_dataset_spec,
        download_observations=_download_observations,
        supported_country_codes=(country_code,),
        supported_dataset_scopes=('ghcnd',),
        supported_resolutions=('daily',),
        supported_canonical_elements=supported_canonical_elements,
    )


def build_station_metadata_reader(
    *,
    country_prefix: str,
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec],
) -> Callable[[str | None, int], pd.DataFrame]:
    def _read_station_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
        spec = get_dataset_spec('ghcnd', 'daily')
        return read_station_metadata_ghcnd_shared(source_url, timeout, spec=spec, country_prefix=country_prefix)

    return _read_station_metadata


def build_station_observation_metadata_reader(
    *,
    country_prefix: str,
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec],
) -> Callable[[str | None, int], pd.DataFrame]:
    def _read_station_observation_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
        spec = get_dataset_spec('ghcnd', 'daily')
        return read_station_observation_metadata_ghcnd_shared(source_url, timeout, spec=spec, country_prefix=country_prefix)

    return _read_station_observation_metadata


def build_daily_observation_downloader(
    *,
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec],
) -> Callable[[ObservationQuery, int, pd.DataFrame | None], pd.DataFrame]:
    def _download_daily_observations(
        query: ObservationQuery,
        timeout: int = 60,
        station_metadata: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        spec = get_dataset_spec('ghcnd', 'daily')
        return download_daily_observations_ghcnd_shared(query, timeout, station_metadata, spec=spec)

    return _download_daily_observations


def build_station_dly_url_builder(
    *,
    get_dataset_spec: Callable[[str, str], GhcndDatasetSpec],
) -> Callable[[str], str]:
    def _build_station_dly_url(station_id: str, *, spec: GhcndDatasetSpec | None = None) -> str:
        effective_spec = spec or get_dataset_spec('ghcnd', 'daily')
        return build_station_dly_url_shared(station_id, spec=effective_spec)

    return _build_station_dly_url


def build_country_wrapper_bundle(
    *,
    country_code: str,
    canonical_elements: dict[str, tuple[str, ...]],
    ghcn_prefix: str | None = None,
    label: str = 'NOAA NCEI GHCN-Daily station observations',
    source_id: str = 'ncei_ghcnd_daily',
) -> GhcndCountryWrapperBundle:
    normalized_country = country_code.strip().upper()
    normalized_prefix = (ghcn_prefix or normalized_country).strip().upper()
    supported_canonical_elements = tuple(canonical_elements.keys())
    supported_raw_elements = tuple(
        raw_element
        for raw_elements in canonical_elements.values()
        for raw_element in raw_elements
    )
    specs = build_country_dataset_specs(
        supported_elements=supported_raw_elements,
        canonical_elements=canonical_elements,
        label=label,
        source_id=source_id,
    )

    def _list_dataset_specs() -> list[GhcndDatasetSpec]:
        return list_country_dataset_specs(specs)

    def _list_implemented_dataset_specs() -> list[GhcndDatasetSpec]:
        return list_country_implemented_dataset_specs(specs)

    def _get_dataset_spec(dataset_scope: str, resolution: str) -> GhcndDatasetSpec:
        return get_country_dataset_spec(specs, dataset_scope, resolution)

    read_station_metadata = build_station_metadata_reader(
        country_prefix=normalized_prefix,
        get_dataset_spec=_get_dataset_spec,
    )
    read_station_observation_metadata = build_station_observation_metadata_reader(
        country_prefix=normalized_prefix,
        get_dataset_spec=_get_dataset_spec,
    )
    download_daily_observations = build_daily_observation_downloader(
        get_dataset_spec=_get_dataset_spec,
    )
    build_station_dly_url = build_station_dly_url_builder(get_dataset_spec=_get_dataset_spec)
    provider = build_country_provider(
        country_code=normalized_country,
        read_station_metadata=read_station_metadata,
        read_station_observation_metadata=read_station_observation_metadata,
        list_dataset_specs=_list_dataset_specs,
        list_implemented_dataset_specs=_list_implemented_dataset_specs,
        get_dataset_spec=_get_dataset_spec,
        download_daily_observations=download_daily_observations,
        supported_canonical_elements=supported_canonical_elements,
    )
    return GhcndCountryWrapperBundle(
        country_code=normalized_country,
        ghcn_prefix=normalized_prefix,
        supported_canonical_elements=supported_canonical_elements,
        supported_raw_elements=supported_raw_elements,
        canonical_elements=canonical_elements,
        list_dataset_specs=_list_dataset_specs,
        list_implemented_dataset_specs=_list_implemented_dataset_specs,
        get_dataset_spec=_get_dataset_spec,
        read_station_metadata=read_station_metadata,
        read_station_observation_metadata=read_station_observation_metadata,
        download_daily_observations=download_daily_observations,
        build_station_dly_url=build_station_dly_url,
        provider=provider,
    )
