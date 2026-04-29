from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def build_mixed_observation_downloader(
    *,
    download_national_observations: Callable[..., pd.DataFrame],
    download_ghcnd_observations: Callable[..., pd.DataFrame],
) -> Callable[..., pd.DataFrame]:
    def _download_observations(*args, **kwargs):
        query = args[0] if args else kwargs.get('query')
        if getattr(query, 'provider', None) == 'ghcnd':
            return download_ghcnd_observations(*args, **kwargs)
        return download_national_observations(*args, **kwargs)

    return _download_observations


def is_ghcnd_metadata_source(source_url: str | None) -> bool:
    if source_url is None:
        return False
    normalized = source_url.replace('\\', '/').lower()
    filename = normalized.rsplit('/', 1)[-1]
    return 'ghcnd' in normalized or filename.endswith('stations.txt') or filename.endswith('inventory.txt')


def build_mixed_station_metadata_reader(
    *,
    read_national_station_metadata: Callable[[str | None, int], pd.DataFrame],
    read_ghcnd_station_metadata: Callable[[str | None, int], pd.DataFrame],
    list_implemented_dataset_specs: Callable[[], list[object]],
) -> Callable[[str | None, int], pd.DataFrame]:
    def _read_station_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
        if is_ghcnd_metadata_source(source_url):
            ghcnd_table = read_ghcnd_station_metadata(source_url, timeout)
            return _attach_station_elements_attrs(
                pd.DataFrame(columns=ghcnd_table.columns),
                ghcnd_table,
                list_implemented_dataset_specs=list_implemented_dataset_specs,
            )
        if source_url is not None:
            national_table = read_national_station_metadata(source_url, timeout)
            return _attach_station_elements_attrs(
                national_table,
                pd.DataFrame(columns=national_table.columns),
                list_implemented_dataset_specs=list_implemented_dataset_specs,
            )
        national_table = read_national_station_metadata(source_url, timeout)
        ghcnd_table = read_ghcnd_station_metadata(None, timeout)
        return _combine_station_metadata(
            national_table,
            ghcnd_table,
            list_implemented_dataset_specs=list_implemented_dataset_specs,
        )

    return _read_station_metadata


def build_mixed_station_observation_metadata_reader(
    *,
    read_national_station_observation_metadata: Callable[[str | None, int], pd.DataFrame],
    read_ghcnd_station_observation_metadata: Callable[[str | None, int], pd.DataFrame],
) -> Callable[[str | None, int], pd.DataFrame]:
    def _read_station_observation_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
        if is_ghcnd_metadata_source(source_url):
            return read_ghcnd_station_observation_metadata(source_url, timeout)
        if source_url is not None:
            return read_national_station_observation_metadata(source_url, timeout)
        national_table = read_national_station_observation_metadata(source_url, timeout)
        ghcnd_table = read_ghcnd_station_observation_metadata(None, timeout)
        combined = pd.concat([national_table, ghcnd_table], ignore_index=True)
        return combined.sort_values(['station_id', 'element', 'obs_type'], kind='stable').reset_index(drop=True)

    return _read_station_observation_metadata


def _build_station_elements_attrs(
    national_table: pd.DataFrame,
    ghcnd_table: pd.DataFrame,
    *,
    list_implemented_dataset_specs: Callable[[], list[object]],
) -> dict[tuple[str, str], dict[str, list[str]]]:
    attrs: dict[tuple[str, str], dict[str, list[str]]] = {
        (spec.provider, spec.resolution): {}
        for spec in list_implemented_dataset_specs()
    }
    implemented_national_specs = [
        spec for spec in list_implemented_dataset_specs()
        if spec.provider != 'ghcnd'
    ]
    national_station_ids = [str(value) for value in national_table.get('station_id', pd.Series(dtype='object')).tolist()]
    for spec in implemented_national_specs:
        attrs[(spec.provider, spec.resolution)] = {
            station_id: list(spec.supported_elements)
            for station_id in national_station_ids
        }
    ghcnd_attrs = ghcnd_table.attrs.get('station_provider_raw_elements_by_path', {})
    for path_key, station_map in ghcnd_attrs.items():
        attrs.setdefault(path_key, {})
        attrs[path_key].update(station_map)
    return attrs


def _attach_station_elements_attrs(
    national_table: pd.DataFrame,
    ghcnd_table: pd.DataFrame,
    *,
    list_implemented_dataset_specs: Callable[[], list[object]],
) -> pd.DataFrame:
    if national_table.empty and ghcnd_table.empty:
        return national_table.copy()
    table = national_table.copy() if not national_table.empty else ghcnd_table.copy()
    table.attrs['station_provider_raw_elements_by_path'] = _build_station_elements_attrs(
        national_table,
        ghcnd_table,
        list_implemented_dataset_specs=list_implemented_dataset_specs,
    )
    return table


def _combine_station_metadata(
    national_table: pd.DataFrame,
    ghcnd_table: pd.DataFrame,
    *,
    list_implemented_dataset_specs: Callable[[], list[object]],
) -> pd.DataFrame:
    if national_table.empty:
        combined = ghcnd_table.copy()
    elif ghcnd_table.empty:
        combined = national_table.copy()
    else:
        combined = pd.concat([national_table, ghcnd_table], ignore_index=True)
        combined = combined.sort_values('station_id', kind='stable').reset_index(drop=True)
    combined.attrs['station_provider_raw_elements_by_path'] = _build_station_elements_attrs(
        national_table,
        ghcnd_table,
        list_implemented_dataset_specs=list_implemented_dataset_specs,
    )
    return combined
