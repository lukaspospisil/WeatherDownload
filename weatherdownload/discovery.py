from __future__ import annotations

DATASET_SCOPES: tuple[str, ...] = ("now", "recent", "historical", "historical_csv")

RESOLUTIONS_BY_SCOPE: dict[str, tuple[str, ...]] = {
    "now": ("10min", "1hour", "daily", "phenomena"),
    "recent": ("10min", "1hour", "daily", "monthly", "phenomena"),
    "historical": ("10min", "1hour", "daily", "monthly", "yearly", "phenomena"),
    "historical_csv": ("10min", "1hour", "daily", "monthly", "pentadic", "yearly", "phenomena"),
}

SUPPORTED_ELEMENTS_BY_QUERY: dict[tuple[str, str], tuple[str, ...]] = {
    ("historical_csv", "daily"): (
        "HS", "P", "RH", "SRA", "SSV", "T", "TMA", "TMI", "WDIR", "WSPD",
    ),
}


def list_dataset_scopes() -> list[str]:
    return list(DATASET_SCOPES)


def list_resolutions(dataset_scope: str | None = None) -> list[str]:
    if dataset_scope is None:
        return sorted({resolution for values in RESOLUTIONS_BY_SCOPE.values() for resolution in values})
    normalized_scope = dataset_scope.strip()
    if normalized_scope not in RESOLUTIONS_BY_SCOPE:
        raise ValueError(f"Unsupported dataset_scope: {dataset_scope}")
    return list(RESOLUTIONS_BY_SCOPE[normalized_scope])


def list_supported_elements(resolution: str | None = None, dataset_scope: str | None = None) -> list[str]:
    if dataset_scope is not None and resolution is not None:
        key = (dataset_scope.strip(), resolution.strip())
        if key in SUPPORTED_ELEMENTS_BY_QUERY:
            return list(SUPPORTED_ELEMENTS_BY_QUERY[key])
        raise ValueError(
            f"No supported elements are defined for dataset_scope='{dataset_scope}' and resolution='{resolution}'."
        )
    if resolution is not None and dataset_scope is None:
        matches = [elements for (_, candidate_resolution), elements in SUPPORTED_ELEMENTS_BY_QUERY.items() if candidate_resolution == resolution.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for resolution='{resolution}'.")
        return sorted({element for elements in matches for element in elements})
    if dataset_scope is not None and resolution is None:
        matches = [elements for (candidate_scope, _), elements in SUPPORTED_ELEMENTS_BY_QUERY.items() if candidate_scope == dataset_scope.strip()]
        if not matches:
            raise ValueError(f"No supported elements are defined for dataset_scope='{dataset_scope}'.")
        return sorted({element for elements in matches for element in elements})
    return sorted({element for elements in SUPPORTED_ELEMENTS_BY_QUERY.values() for element in elements})
