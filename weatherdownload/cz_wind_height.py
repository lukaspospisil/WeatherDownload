from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .fao import wind_speed_2m


CZ_DAILY_WIND_OBS_TYPE = 'DLY'
CZ_DAILY_WIND_RAW_ELEMENT = 'F'
FALLBACK_HEIGHT_SOURCE = 'fallback_10m'
METADATA_HEIGHT_SOURCE = 'metadata'


@dataclass(frozen=True, slots=True)
class WindHeightAuditSummary:
    num_observations: int
    num_metadata_rows: int
    num_fallback_rows: int
    num_no_metadata_rows: int
    num_overlap_rows: int
    num_zero_height_rows: int


def build_cz_daily_wind_height_lookup(meta2: pd.DataFrame) -> pd.DataFrame:
    required_columns = {'obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'}
    missing_columns = sorted(required_columns - set(meta2.columns))
    if missing_columns:
        raise KeyError(f'CZ wind-height lookup requires meta2 columns: {missing_columns}')

    lookup = meta2[
        meta2['obs_type'].astype('string').str.upper().eq(CZ_DAILY_WIND_OBS_TYPE)
        & meta2['element'].astype('string').str.upper().eq(CZ_DAILY_WIND_RAW_ELEMENT)
    ].copy()
    if lookup.empty:
        return pd.DataFrame(
            columns=[
                'station_id',
                'begin_date',
                'end_date',
                'begin_day',
                'end_day',
                'element',
                'schedule',
                'name',
                'description',
                'height',
            ]
        )

    lookup['begin_ts'] = pd.to_datetime(lookup['begin_date'], utc=True, errors='coerce')
    lookup['end_ts'] = pd.to_datetime(lookup['end_date'], utc=True, errors='coerce')
    lookup['begin_day'] = lookup['begin_ts'].dt.date
    lookup['end_day'] = lookup['end_ts'].dt.date
    lookup['height'] = pd.to_numeric(lookup['height'], errors='coerce')
    lookup = lookup.sort_values(['station_id', 'begin_ts', 'end_ts'], kind='stable').reset_index(drop=True)
    return lookup[
        [
            'station_id',
            'begin_date',
            'end_date',
            'begin_day',
            'end_day',
            'element',
            'schedule',
            'name',
            'description',
            'height',
        ]
    ].copy()


def audit_and_standardize_cz_daily_wind(
    observations: pd.DataFrame,
    lookup: pd.DataFrame,
    *,
    fallback_height_m: float = 10.0,
) -> pd.DataFrame:
    required_columns = {'station_id', 'date', 'wind_speed'}
    missing_columns = sorted(required_columns - set(observations.columns))
    if missing_columns:
        raise KeyError(f'CZ wind-height audit requires observation columns: {missing_columns}')

    prepared = observations.copy()
    prepared['date'] = pd.to_datetime(prepared['date'], errors='coerce').dt.date
    prepared['wind_speed'] = pd.to_numeric(prepared['wind_speed'], errors='coerce')
    prepared = prepared.sort_values(['station_id', 'date'], kind='stable').reset_index(drop=True)

    result_parts: list[pd.DataFrame] = []
    lookup_by_station = {
        station_id: group.reset_index(drop=True)
        for station_id, group in lookup.groupby('station_id', sort=False)
    }
    for station_id, station_rows in prepared.groupby('station_id', sort=False, dropna=False):
        station_lookup = lookup_by_station.get(str(station_id), pd.DataFrame(columns=lookup.columns))
        result_parts.append(_audit_station_daily_wind(station_rows.reset_index(drop=True), station_lookup, fallback_height_m=fallback_height_m))

    if not result_parts:
        return _empty_audit_frame()
    combined = pd.concat(result_parts, ignore_index=True, sort=False)
    combined['u2_fixed10_m_s'] = pd.Series(
        wind_speed_2m(pd.to_numeric(combined['wind_speed'], errors='coerce'), measurement_height_m=10.0),
        index=combined.index,
        dtype='float64',
    )
    combined['u2_m_s'] = pd.Series(
        wind_speed_2m(
            pd.to_numeric(combined['wind_speed'], errors='coerce'),
            measurement_height_m=pd.to_numeric(combined['wind_measurement_height_m'], errors='coerce'),
        ),
        index=combined.index,
        dtype='float64',
    )
    combined['u2_abs_diff_m_s'] = (combined['u2_m_s'] - combined['u2_fixed10_m_s']).abs()
    denominator = combined['u2_fixed10_m_s'].abs()
    combined['u2_rel_diff'] = pd.Series(
        np.where(
            denominator.gt(0.0),
            combined['u2_abs_diff_m_s'] / denominator,
            np.where(combined['u2_abs_diff_m_s'].fillna(0.0).eq(0.0), 0.0, np.nan),
        ),
        index=combined.index,
        dtype='float64',
    )
    return combined


def summarize_cz_daily_wind_audit(audit: pd.DataFrame) -> WindHeightAuditSummary:
    if audit.empty:
        return WindHeightAuditSummary(0, 0, 0, 0, 0, 0)
    return WindHeightAuditSummary(
        num_observations=int(len(audit)),
        num_metadata_rows=int(audit['wind_height_source'].eq(METADATA_HEIGHT_SOURCE).sum()),
        num_fallback_rows=int(audit['wind_height_source'].eq(FALLBACK_HEIGHT_SOURCE).sum()),
        num_no_metadata_rows=int(audit['wind_height_issue'].eq('no_metadata').sum()),
        num_overlap_rows=int(audit['matched_metadata_rows'].gt(1).sum()),
        num_zero_height_rows=int(audit['matched_zero_height'].sum()),
    )


def _audit_station_daily_wind(
    station_rows: pd.DataFrame,
    station_lookup: pd.DataFrame,
    *,
    fallback_height_m: float,
) -> pd.DataFrame:
    dates = pd.to_datetime(station_rows['date'], errors='coerce').dt.date.to_numpy()
    matched_metadata_rows = np.zeros(len(station_rows), dtype=np.int64)
    matched_zero_height = np.zeros(len(station_rows), dtype=bool)
    matched_missing_height = np.zeros(len(station_rows), dtype=bool)
    resolved_height = np.full(len(station_rows), np.nan, dtype='float64')
    conflicting_height = np.zeros(len(station_rows), dtype=bool)

    lookup_rows = list(station_lookup.itertuples(index=False))
    for lookup_row in lookup_rows:
        begin_day = getattr(lookup_row, 'begin_day', None)
        end_day = getattr(lookup_row, 'end_day', None)
        if begin_day is None or end_day is None:
            continue
        mask = (dates >= begin_day) & (dates <= end_day)
        if not np.any(mask):
            continue
        matched_metadata_rows[mask] += 1
        height_value = getattr(lookup_row, 'height', np.nan)
        if pd.isna(height_value):
            matched_missing_height[mask] = True
            continue
        if float(height_value) == 0.0:
            matched_zero_height[mask] = True
            continue
        unresolved = np.isnan(resolved_height[mask])
        if np.any(unresolved):
            target_indexes = np.flatnonzero(mask)[unresolved]
            resolved_height[target_indexes] = float(height_value)
        existing_height = resolved_height[mask]
        conflict_mask = (~np.isnan(existing_height)) & (~np.isclose(existing_height, float(height_value), atol=0.0, rtol=0.0))
        if np.any(conflict_mask):
            target_indexes = np.flatnonzero(mask)[conflict_mask]
            conflicting_height[target_indexes] = True

    result = station_rows.copy()
    result['matched_metadata_rows'] = matched_metadata_rows
    result['matched_zero_height'] = matched_zero_height
    result['matched_missing_height'] = matched_missing_height
    result['matched_height_m'] = pd.Series(resolved_height, index=result.index, dtype='float64')
    result['matched_height_conflict'] = conflicting_height

    issue = np.full(len(result), '', dtype=object)
    issue[matched_metadata_rows == 0] = 'no_metadata'
    issue[(matched_metadata_rows > 0) & matched_zero_height & np.isnan(resolved_height)] = 'height_zero_only'
    issue[(matched_metadata_rows > 0) & matched_missing_height & np.isnan(resolved_height)] = 'height_missing_only'
    issue[(matched_metadata_rows > 1) & conflicting_height] = 'overlap_conflict'
    issue[(matched_metadata_rows > 1) & (issue == '')] = 'overlap_same_height'

    source = np.full(len(result), METADATA_HEIGHT_SOURCE, dtype=object)
    use_fallback = (issue != '') & (issue != 'overlap_same_height')
    source[use_fallback] = FALLBACK_HEIGHT_SOURCE

    height_used = pd.Series(resolved_height, index=result.index, dtype='float64')
    height_used.loc[use_fallback] = float(fallback_height_m)

    result['wind_height_issue'] = pd.Series(issue, index=result.index, dtype='string')
    result['wind_height_source'] = pd.Series(source, index=result.index, dtype='string')
    result['wind_measurement_height_m'] = pd.Series(height_used, index=result.index, dtype='float64')
    result['fallback_height_m'] = np.where(use_fallback, float(fallback_height_m), np.nan)
    return result[
        [
            *station_rows.columns.tolist(),
            'matched_metadata_rows',
            'matched_zero_height',
            'matched_missing_height',
            'matched_height_m',
            'matched_height_conflict',
            'wind_measurement_height_m',
            'wind_height_source',
            'wind_height_issue',
            'fallback_height_m',
        ]
    ].copy()


def _empty_audit_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            'station_id',
            'date',
            'wind_speed',
            'matched_metadata_rows',
            'matched_zero_height',
            'matched_missing_height',
            'matched_height_m',
            'matched_height_conflict',
            'wind_measurement_height_m',
            'wind_height_source',
            'wind_height_issue',
            'fallback_height_m',
            'u2_fixed10_m_s',
            'u2_m_s',
            'u2_abs_diff_m_s',
            'u2_rel_diff',
        ]
    )


def collapse_issue_intervals(audit: pd.DataFrame, *, issue_mask: pd.Series) -> pd.DataFrame:
    if audit.empty or not bool(issue_mask.any()):
        return pd.DataFrame(columns=['station_id', 'issue', 'begin_date', 'end_date', 'n_days'])

    rows = audit.loc[issue_mask, ['station_id', 'date', 'wind_height_issue']].copy()
    rows['date'] = pd.to_datetime(rows['date'], errors='coerce').dt.date
    rows = rows.sort_values(['station_id', 'wind_height_issue', 'date'], kind='stable').reset_index(drop=True)

    collapsed: list[dict[str, object]] = []
    for (station_id, issue_name), group in rows.groupby(['station_id', 'wind_height_issue'], sort=False, dropna=False):
        current_begin: date | None = None
        current_end: date | None = None
        count = 0
        for current_date in group['date'].tolist():
            if current_begin is None:
                current_begin = current_date
                current_end = current_date
                count = 1
                continue
            assert current_end is not None
            if (current_date - current_end).days == 1:
                current_end = current_date
                count += 1
                continue
            collapsed.append(
                {
                    'station_id': station_id,
                    'issue': issue_name,
                    'begin_date': current_begin.isoformat(),
                    'end_date': current_end.isoformat(),
                    'n_days': count,
                }
            )
            current_begin = current_date
            current_end = current_date
            count = 1
        if current_begin is not None and current_end is not None:
            collapsed.append(
                {
                    'station_id': station_id,
                    'issue': issue_name,
                    'begin_date': current_begin.isoformat(),
                    'end_date': current_end.isoformat(),
                    'n_days': count,
                }
            )
    return pd.DataFrame.from_records(collapsed, columns=['station_id', 'issue', 'begin_date', 'end_date', 'n_days'])
