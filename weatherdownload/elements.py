from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd


CanonicalElementMap = Mapping[str, tuple[str, ...]]
ELEMENT_MAPPING_COLUMNS = ['element', 'element_raw', 'raw_elements']


def canonical_element_map_for_spec(spec: Any) -> dict[str, tuple[str, ...]]:
    mapping = getattr(spec, 'canonical_elements', None) or {}
    return {str(key): tuple(value) for key, value in mapping.items()}


def raw_to_canonical_map_for_spec(spec: Any) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for canonical_name, raw_codes in canonical_element_map_for_spec(spec).items():
        for raw_code in raw_codes:
            lookup[str(raw_code).casefold()] = canonical_name
    return lookup


def supported_elements_for_spec(spec: Any, provider_raw: bool = False) -> list[str]:
    if provider_raw:
        return list(getattr(spec, 'supported_elements', ()))
    canonical_map = canonical_element_map_for_spec(spec)
    if canonical_map:
        return list(canonical_map.keys())
    return list(getattr(spec, 'supported_elements', ()))


def element_mapping_for_spec(spec: Any) -> pd.DataFrame:
    canonical_map = canonical_element_map_for_spec(spec)
    if canonical_map:
        rows = [
            {
                'element': canonical_name,
                'element_raw': raw_codes[0],
                'raw_elements': list(raw_codes),
            }
            for canonical_name, raw_codes in canonical_map.items()
        ]
        return pd.DataFrame.from_records(rows, columns=ELEMENT_MAPPING_COLUMNS)
    raw_elements = list(getattr(spec, 'supported_elements', ()))
    rows = [
        {
            'element': raw_code,
            'element_raw': raw_code,
            'raw_elements': [raw_code],
        }
        for raw_code in raw_elements
    ]
    return pd.DataFrame.from_records(rows, columns=ELEMENT_MAPPING_COLUMNS)


def element_mapping_dict_for_spec(spec: Any) -> dict[str, list[str]]:
    mapping_frame = element_mapping_for_spec(spec)
    if mapping_frame.empty:
        return {}
    return {
        str(row.element): list(row.raw_elements)
        for row in mapping_frame.itertuples(index=False)
    }


def normalize_requested_elements(elements: Sequence[str], spec: Any) -> list[str]:
    canonical_map = canonical_element_map_for_spec(spec)
    supported_raw_lookup = {
        str(element).casefold(): str(element)
        for element in getattr(spec, 'supported_elements', ())
    }
    normalized: list[str] = []
    seen: set[str] = set()

    for item in elements:
        cleaned = item.strip()
        if not cleaned:
            continue
        canonical_name = cleaned.lower()
        if canonical_name in canonical_map:
            preferred_raw = canonical_map[canonical_name][0]
            if preferred_raw not in seen:
                seen.add(preferred_raw)
                normalized.append(preferred_raw)
            continue
        raw_code = supported_raw_lookup.get(cleaned.casefold())
        if raw_code is not None and raw_code not in seen:
            seen.add(raw_code)
            normalized.append(raw_code)
            continue
        if canonical_name not in canonical_map and cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    return normalized


def unsupported_requested_elements(elements: Sequence[str], spec: Any) -> list[str]:
    canonical_map = canonical_element_map_for_spec(spec)
    supported_raw_lookup = {
        str(element).casefold(): str(element)
        for element in getattr(spec, 'supported_elements', ())
    }
    unsupported: list[str] = []

    for item in elements:
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.lower() in canonical_map:
            continue
        if cleaned.casefold() in supported_raw_lookup:
            continue
        unsupported.append(cleaned)
    return unsupported


def canonicalize_element_series(raw_series: pd.Series, query: Any) -> pd.DataFrame:
    from .providers import get_provider

    provider = get_provider(query.country)
    spec = provider.get_dataset_spec(query.dataset_scope, query.resolution)
    raw_to_canonical = raw_to_canonical_map_for_spec(spec)
    element_raw = raw_series.astype('string').str.strip()
    element = element_raw.map(lambda raw: raw_to_canonical.get(str(raw).casefold(), str(raw)))
    return pd.DataFrame({'element': element, 'element_raw': element_raw})
