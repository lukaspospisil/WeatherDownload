from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

from .elements import normalize_requested_elements, unsupported_requested_elements


class QueryValidationError(ValueError):
    """Raised when ObservationQuery contains invalid provider-specific query dimensions."""


@dataclass(slots=True)
class ObservationQuery:
    """Public observation query.

    `provider` is the preferred public selector for the country-specific source.
    `dataset_scope` remains accepted as a backward-compatible alias and is
    normalized internally to the same canonical value.
    """

    dataset_scope: str = ''
    resolution: str = ''
    station_ids: list[str] = field(default_factory=list)
    start: datetime | str | None = None
    end: datetime | str | None = None
    start_date: date | str | None = None
    end_date: date | str | None = None
    all_history: bool = False
    elements: list[str] | None = None
    country: str = 'CZ'
    provider: str | None = None

    def __post_init__(self) -> None:
        validate_observation_query(self)


def normalize_provider_scope(
    dataset_scope: str | None = None,
    provider: str | None = None,
) -> str:
    """Resolve `provider` and `dataset_scope` into one canonical provider token."""

    normalized_dataset_scope = _normalize_optional_scope(dataset_scope, field_name='dataset_scope')
    normalized_provider = _normalize_optional_scope(provider, field_name='provider')

    if normalized_dataset_scope is None and normalized_provider is None:
        raise QueryValidationError('Either provider or dataset_scope is required.')
    if normalized_dataset_scope is not None and normalized_provider is not None and normalized_dataset_scope != normalized_provider:
        raise QueryValidationError(
            f"Conflicting provider selectors: provider='{normalized_provider}' does not match dataset_scope='{normalized_dataset_scope}'."
        )
    return normalized_provider or normalized_dataset_scope or ''


def validate_observation_query(query: ObservationQuery) -> ObservationQuery:
    """Validate and normalize an observation query in place."""

    from ..providers import get_provider, normalize_country_code

    query.dataset_scope = normalize_provider_scope(
        getattr(query, 'dataset_scope', None),
        getattr(query, 'provider', None),
    )
    query.provider = query.dataset_scope

    if not query.dataset_scope:
        raise QueryValidationError('provider is required.')
    if not query.resolution:
        raise QueryValidationError('resolution is required.')

    query.country = normalize_country_code(query.country)
    provider = get_provider(query.country)
    query.resolution = _normalize_scalar(query.resolution, field_name='resolution')

    supported_scopes = sorted({spec.dataset_scope for spec in provider.list_dataset_specs()})
    if query.dataset_scope not in supported_scopes:
        raise QueryValidationError(
            f"Unsupported provider '{query.dataset_scope}' for country '{query.country}'. "
            'dataset_scope remains accepted as a backward-compatible alias.'
        )

    supported_resolutions = sorted({
        spec.resolution for spec in provider.list_dataset_specs() if spec.dataset_scope == query.dataset_scope
    })
    if query.resolution not in supported_resolutions:
        raise QueryValidationError(
            f"Unsupported resolution '{query.resolution}' for provider '{query.dataset_scope}'. "
            'dataset_scope remains accepted as a backward-compatible alias.'
        )

    dataset_spec = provider.get_dataset_spec(query.dataset_scope, query.resolution)

    query.station_ids = _normalize_string_sequence(query.station_ids, 'station_ids', uppercase=True, required=True)

    if query.elements is not None:
        normalized_input_elements = _normalize_elements_input(query.elements)
        unsupported_elements = unsupported_requested_elements(normalized_input_elements, dataset_spec)
        if unsupported_elements:
            raise QueryValidationError(
                f"Unsupported elements for provider '{query.dataset_scope}' and resolution '{query.resolution}': "
                f'{unsupported_elements}. dataset_scope remains accepted as a backward-compatible alias.'
            )
        query.elements = normalize_requested_elements(normalized_input_elements, dataset_spec)

    has_datetime_range = query.start is not None or query.end is not None
    has_date_range = query.start_date is not None or query.end_date is not None
    if not isinstance(query.all_history, bool):
        raise QueryValidationError('all_history must be a boolean.')

    if has_datetime_range and has_date_range:
        raise QueryValidationError('Use either start/end or start_date/end_date, but not both.')

    if query.all_history and (has_datetime_range or has_date_range):
        raise QueryValidationError('all_history cannot be used together with start/end or start_date/end_date.')

    if has_datetime_range:
        if query.start is None or query.end is None:
            raise QueryValidationError('start and end must be provided together.')
        start_dt = _coerce_datetime(query.start, 'start')
        end_dt = _coerce_datetime(query.end, 'end')
        if start_dt > end_dt:
            raise QueryValidationError('start must be earlier than or equal to end.')
        query.start = start_dt
        query.end = end_dt

    if has_date_range:
        if query.start_date is None or query.end_date is None:
            raise QueryValidationError('start_date and end_date must be provided together.')
        start_date = _coerce_date(query.start_date, 'start_date')
        end_date = _coerce_date(query.end_date, 'end_date')
        if start_date > end_date:
            raise QueryValidationError('start_date must be earlier than or equal to end_date.')
        query.start_date = start_date
        query.end_date = end_date

    if dataset_spec.time_semantics == 'date' and has_datetime_range:
        raise QueryValidationError(
            'For daily data, use start_date/end_date. Datetime precision is not supported.'
        )

    if dataset_spec.time_semantics == 'datetime' and has_date_range:
        raise QueryValidationError(
            'For hourly data, use start/end. Date-only precision is not supported.'
        )

    if not query.all_history:
        if dataset_spec.time_semantics == 'date' and not has_date_range:
            raise QueryValidationError(
                'Daily data require start_date/end_date unless all_history=True is set explicitly.'
            )
        if dataset_spec.time_semantics == 'datetime' and not has_datetime_range:
            raise QueryValidationError(
                'Timestamp-based data require start/end unless all_history=True is set explicitly.'
            )

    return query


def _normalize_scalar(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise QueryValidationError(f'{field_name} must be a string.')
    normalized = value.strip()
    if not normalized:
        raise QueryValidationError(f'{field_name} must not be empty.')
    return normalized


def _normalize_optional_scope(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _normalize_scalar(value, field_name=field_name)


def _normalize_string_sequence(value: Sequence[str] | None, field_name: str, uppercase: bool, required: bool) -> list[str]:
    if value is None:
        if required:
            raise QueryValidationError(f'{field_name} is required.')
        return []
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise QueryValidationError(f'{field_name} must be a sequence of strings.')
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise QueryValidationError(f'{field_name} must be a sequence of strings.')
        cleaned = item.strip()
        if not cleaned:
            continue
        if uppercase:
            cleaned = cleaned.upper()
        if cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    if required and not normalized:
        raise QueryValidationError(f'{field_name} must not be empty.')
    return normalized


def _normalize_elements_input(value: Sequence[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise QueryValidationError('elements must be a sequence of strings.')
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise QueryValidationError('elements must be a sequence of strings.')
        cleaned = item.strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        key = lowered if any(character.islower() for character in cleaned) else cleaned.upper()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)
    return normalized


def _coerce_datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip().replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise QueryValidationError(f'Invalid datetime for {field_name}: {value}') from exc
    raise QueryValidationError(f'{field_name} must be a datetime or ISO datetime string.')


def _coerce_date(value: date | str, field_name: str) -> date:
    if isinstance(value, datetime):
        raise QueryValidationError(f'{field_name} must be a date without time.')
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        try:
            return date.fromisoformat(normalized)
        except ValueError as exc:
            raise QueryValidationError(f'Invalid date for {field_name}: {value}') from exc
    raise QueryValidationError(f'{field_name} must be a date or ISO date string.')


