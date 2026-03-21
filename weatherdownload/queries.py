from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

from .discovery import DATASET_SCOPES, RESOLUTIONS_BY_SCOPE


class QueryValidationError(ValueError):
    """Raised when ObservationQuery contains invalid CHMI query dimensions."""


@dataclass(slots=True)
class ObservationQuery:
    dataset_scope: str
    resolution: str
    station_ids: list[str] = field(default_factory=list)
    start: datetime | str | None = None
    end: datetime | str | None = None
    start_date: date | str | None = None
    end_date: date | str | None = None
    elements: list[str] | None = None

    def __post_init__(self) -> None:
        validate_observation_query(self)


def validate_observation_query(query: ObservationQuery) -> ObservationQuery:
    if not query.dataset_scope:
        raise QueryValidationError("dataset_scope is required.")
    if not query.resolution:
        raise QueryValidationError("resolution is required.")

    query.dataset_scope = _normalize_scalar(query.dataset_scope, field_name="dataset_scope")
    query.resolution = _normalize_scalar(query.resolution, field_name="resolution")

    if query.dataset_scope not in DATASET_SCOPES:
        raise QueryValidationError(f"Unsupported dataset_scope: {query.dataset_scope}")

    if query.resolution not in RESOLUTIONS_BY_SCOPE[query.dataset_scope]:
        raise QueryValidationError(
            f"Unsupported resolution '{query.resolution}' for dataset_scope '{query.dataset_scope}'."
        )

    query.station_ids = _normalize_string_sequence(query.station_ids, "station_ids", uppercase=True, required=True)

    if query.elements is not None:
        query.elements = _normalize_string_sequence(query.elements, "elements", uppercase=True, required=False)

    has_datetime_range = query.start is not None or query.end is not None
    has_date_range = query.start_date is not None or query.end_date is not None

    if has_datetime_range and has_date_range:
        raise QueryValidationError("Use either start/end or start_date/end_date, but not both.")

    if has_datetime_range:
        if query.start is None or query.end is None:
            raise QueryValidationError("start and end must be provided together.")
        start_dt = _coerce_datetime(query.start, "start")
        end_dt = _coerce_datetime(query.end, "end")
        if start_dt > end_dt:
            raise QueryValidationError("start must be earlier than or equal to end.")
        query.start = start_dt
        query.end = end_dt

    if has_date_range:
        if query.start_date is None or query.end_date is None:
            raise QueryValidationError("start_date and end_date must be provided together.")
        start_date = _coerce_date(query.start_date, "start_date")
        end_date = _coerce_date(query.end_date, "end_date")
        if start_date > end_date:
            raise QueryValidationError("start_date must be earlier than or equal to end_date.")
        query.start_date = start_date
        query.end_date = end_date

    if query.resolution == "daily" and has_datetime_range:
        raise QueryValidationError(
            "For daily data, use start_date/end_date. Datetime precision is not supported."
        )

    return query


def _normalize_scalar(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise QueryValidationError(f"{field_name} must be a string.")
    normalized = value.strip()
    if not normalized:
        raise QueryValidationError(f"{field_name} must not be empty.")
    return normalized


def _normalize_string_sequence(value: Sequence[str] | None, field_name: str, uppercase: bool, required: bool) -> list[str]:
    if value is None:
        if required:
            raise QueryValidationError(f"{field_name} is required.")
        return []
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise QueryValidationError(f"{field_name} must be a sequence of strings.")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise QueryValidationError(f"{field_name} must be a sequence of strings.")
        cleaned = item.strip()
        if not cleaned:
            continue
        if uppercase:
            cleaned = cleaned.upper()
        if cleaned not in seen:
            seen.add(cleaned)
            normalized.append(cleaned)
    if required and not normalized:
        raise QueryValidationError(f"{field_name} must not be empty.")
    return normalized


def _coerce_datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise QueryValidationError(f"Invalid datetime for {field_name}: {value}") from exc
    raise QueryValidationError(f"{field_name} must be a datetime or ISO datetime string.")


def _coerce_date(value: date | str, field_name: str) -> date:
    if isinstance(value, datetime):
        raise QueryValidationError(f"{field_name} must be a date without time.")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        try:
            return date.fromisoformat(normalized)
        except ValueError as exc:
            raise QueryValidationError(f"Invalid date for {field_name}: {value}") from exc
    raise QueryValidationError(f"{field_name} must be a date or ISO date string.")
