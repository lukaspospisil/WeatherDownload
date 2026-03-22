from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


CanonicalElementMap = Mapping[str, tuple[str, ...]]


def canonical_element_map_for_spec(spec: Any) -> dict[str, tuple[str, ...]]:
    mapping = getattr(spec, 'canonical_elements', None) or {}
    return {str(key): tuple(value) for key, value in mapping.items()}


def supported_elements_for_spec(spec: Any, provider_raw: bool = False) -> list[str]:
    if provider_raw:
        return list(getattr(spec, 'supported_elements', ()))
    canonical_map = canonical_element_map_for_spec(spec)
    if canonical_map:
        return list(canonical_map.keys())
    return list(getattr(spec, 'supported_elements', ()))


def normalize_requested_elements(elements: Sequence[str], spec: Any) -> list[str]:
    canonical_map = canonical_element_map_for_spec(spec)
    supported_raw = {str(element).upper() for element in getattr(spec, 'supported_elements', ())}
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
        raw_code = cleaned.upper()
        if raw_code in supported_raw and raw_code not in seen:
            seen.add(raw_code)
            normalized.append(raw_code)
            continue
        if canonical_name not in canonical_map and raw_code not in supported_raw:
            normalized.append(raw_code)
    return normalized


def unsupported_requested_elements(elements: Sequence[str], spec: Any) -> list[str]:
    canonical_map = canonical_element_map_for_spec(spec)
    supported_raw = {str(element).upper() for element in getattr(spec, 'supported_elements', ())}
    unsupported: list[str] = []

    for item in elements:
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.lower() in canonical_map:
            continue
        if cleaned.upper() in supported_raw:
            continue
        unsupported.append(cleaned)
    return unsupported
