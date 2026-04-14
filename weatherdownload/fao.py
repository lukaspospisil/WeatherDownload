from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


FINAL_SERIES_COLUMNS = [
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'vapour_pressure',
    'sunshine_duration',
]
DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION = (
    'Derived vapour_pressure from observed daily tas_mean and relative_humidity '
    'using the Magnus saturation-vapour-pressure formula in hPa.'
)
PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS = 18
PL_HOURLY_WIND_SPEED_RULE_DESCRIPTION = (
    'Filled daily wind_speed from official IMGW historical/1hour wind_speed by arithmetic mean over the UTC calendar day '
    f'when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations were available.'
)
PL_HOURLY_VAPOUR_PRESSURE_RULE_DESCRIPTION = (
    'Filled daily vapour_pressure from official IMGW historical/1hour vapour_pressure by arithmetic mean over the UTC calendar day '
    f'when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations were available.'
)


@dataclass(frozen=True)
class FieldFillSummary:
    field: str
    status: str
    rule: str
    observed_count: int
    aggregated_count: int
    derived_count: int
    missing_count: int


def fill_policy_uses_derived(fill_missing: str) -> bool:
    return fill_missing == 'allow-derived'


def fill_policy_uses_hourly_aggregate(fill_missing: str) -> bool:
    return fill_missing == 'allow-hourly-aggregate'


def build_provider_element_mapping(config: Any) -> dict[str, dict[str, Any]]:
    return dict(config.provider_element_mapping)


def build_data_info(config: Any, station_rows: list[dict[str, Any]], *, min_complete_days: int, fill_missing: str = 'none') -> dict[str, Any]:
    data_info = {
        'created_at': pd.Timestamp.now('UTC').isoformat(),
        'dataset_type': config.dataset_type,
        'source': config.source,
        'country': config.country,
        'elements': FINAL_SERIES_COLUMNS,
        'provider_element_mapping': build_provider_element_mapping(config),
        'min_complete_days': int(min_complete_days),
        'num_stations': int(len(station_rows)),
        'fill_policy': {'selected': fill_missing},
    }
    if config.assumptions:
        data_info['assumptions'] = dict(config.assumptions)
    if fill_missing == 'allow-hourly-aggregate' and config.country == 'PL':
        data_info['fill_policy'].update(
            {
                'hourly_aggregation_day_boundary': 'UTC calendar day based on normalized historical/1hour timestamps',
                'hourly_aggregation_min_observations': int(PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS),
                'hourly_aggregation_fields': ['wind_speed', 'vapour_pressure'],
            }
        )
    return data_info


def summarize_field_fill_status(
    provenance_tables: list[pd.DataFrame],
    *,
    fill_missing: str,
    applied_rules_by_field: dict[str, set[str]],
) -> list[FieldFillSummary]:
    summaries: list[FieldFillSummary] = []
    for field_name in FINAL_SERIES_COLUMNS:
        observed_count = 0
        aggregated_count = 0
        derived_count = 0
        missing_count = 0
        for provenance in provenance_tables:
            if field_name not in provenance.columns:
                continue
            counts = provenance[field_name].astype('string').value_counts(dropna=False)
            observed_count += int(counts.get('observed_daily', 0))
            aggregated_count += int(counts.get('aggregated_hourly_opt_in', 0))
            derived_count += int(counts.get('derived_opt_in', 0))
            missing_count += int(counts.get('missing', 0))
        if aggregated_count > 0 and (observed_count > 0 or derived_count > 0):
            status = 'partially hourly-aggregated'
        elif aggregated_count > 0:
            status = 'hourly-aggregated opt-in'
        elif derived_count > 0 and observed_count > 0:
            status = 'partially derived'
        elif derived_count > 0:
            status = 'fully derived'
        elif observed_count > 0:
            status = 'observed-only'
        else:
            status = 'still missing'
        if applied_rules_by_field.get(field_name):
            rule = '; '.join(sorted(applied_rules_by_field[field_name]))
        elif fill_missing == 'allow-derived' and field_name == 'vapour_pressure':
            rule = 'No fill rule applied.'
        elif fill_missing == 'allow-hourly-aggregate' and field_name in {'wind_speed', 'vapour_pressure'}:
            rule = 'No hourly aggregation rule applied.'
        elif observed_count > 0:
            rule = 'Observed daily source values only.'
        else:
            rule = 'No fill rule applied.'
        summaries.append(
            FieldFillSummary(
                field=field_name,
                status=status,
                rule=rule,
                observed_count=observed_count,
                aggregated_count=aggregated_count,
                derived_count=derived_count,
                missing_count=missing_count,
            )
        )
    return summaries


def prepare_complete_station_series(daily_table: pd.DataFrame, *, config: Any, fill_missing: str = 'none') -> pd.DataFrame:
    complete, _, _ = prepare_complete_station_series_with_provenance(
        daily_table,
        hourly_table=None,
        config=config,
        fill_missing=fill_missing,
    )
    return complete


def prepare_complete_station_series_with_provenance(
    daily_table: pd.DataFrame,
    *,
    hourly_table: pd.DataFrame | None = None,
    config: Any,
    fill_missing: str = 'none',
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str | None]]:
    selected_tables: dict[str, pd.DataFrame] = {}
    selected_names = set(config.required_complete_elements) | set(FINAL_SERIES_COLUMNS)
    if fill_policy_uses_derived(fill_missing):
        selected_names.add('relative_humidity')
    for canonical_name in selected_names:
        selected = select_daily_variable_rows(daily_table, canonical_name=canonical_name, config=config)
        if canonical_name in config.required_complete_elements and selected.empty:
            empty = pd.DataFrame(columns=['date', *FINAL_SERIES_COLUMNS])
            empty_provenance = pd.DataFrame(columns=['date', *FINAL_SERIES_COLUMNS])
            return empty, empty_provenance, {field: None for field in FINAL_SERIES_COLUMNS}
        if not selected.empty:
            selected_tables[canonical_name] = selected

    merged = selected_tables[config.required_complete_elements[0]]
    for canonical_name in config.required_complete_elements[1:]:
        merged = merged.merge(selected_tables[canonical_name], on='date', how='inner')

    for canonical_name in (set(FINAL_SERIES_COLUMNS) | {'relative_humidity'}) - set(config.required_complete_elements):
        table = selected_tables.get(canonical_name)
        if table is not None:
            merged = merged.merge(table, on='date', how='left')

    for canonical_name in set(FINAL_SERIES_COLUMNS) | {'relative_humidity'}:
        if canonical_name not in merged.columns:
            merged[canonical_name] = pd.NA

    complete = merged.dropna(subset=list(config.required_complete_elements)).sort_values('date').reset_index(drop=True)
    provenance = pd.DataFrame({'date': complete['date']})
    for canonical_name in FINAL_SERIES_COLUMNS:
        provenance[canonical_name] = pd.Series(
            np.where(complete[canonical_name].notna(), 'observed_daily', 'missing'),
            dtype='string',
        )

    applied_rules = {field: None for field in FINAL_SERIES_COLUMNS}
    if fill_policy_uses_hourly_aggregate(fill_missing) and config.country == 'PL':
        hourly_fills = build_hourly_daily_fill_tables(hourly_table, fields=('wind_speed', 'vapour_pressure'))
        for field_name, rule_text in (
            ('wind_speed', PL_HOURLY_WIND_SPEED_RULE_DESCRIPTION),
            ('vapour_pressure', PL_HOURLY_VAPOUR_PRESSURE_RULE_DESCRIPTION),
        ):
            fill_table = hourly_fills.get(field_name)
            if fill_table is None or fill_table.empty:
                continue
            complete = complete.merge(fill_table, on='date', how='left')
            fill_column = f'{field_name}__hourly_fill'
            fill_mask = complete[field_name].isna() & complete[fill_column].notna()
            if fill_mask.any():
                complete.loc[fill_mask, field_name] = complete.loc[fill_mask, fill_column]
                provenance.loc[fill_mask, field_name] = 'aggregated_hourly_opt_in'
                applied_rules[field_name] = rule_text
            complete = complete.drop(columns=[fill_column])

    if fill_policy_uses_derived(fill_missing):
        vapour_pressure_mask = (
            complete['vapour_pressure'].isna()
            & complete['tas_mean'].notna()
            & complete['relative_humidity'].notna()
        )
        if vapour_pressure_mask.any():
            relative_humidity = pd.to_numeric(complete.loc[vapour_pressure_mask, 'relative_humidity'], errors='coerce')
            valid_mask = relative_humidity.between(0, 100, inclusive='both')
            if valid_mask.any():
                target_index = relative_humidity[valid_mask].index
                tas_mean = pd.to_numeric(complete.loc[target_index, 'tas_mean'], errors='coerce')
                complete.loc[target_index, 'vapour_pressure'] = 6.108 * np.exp((17.27 * tas_mean) / (tas_mean + 237.3)) * (relative_humidity.loc[target_index] / 100.0)
                provenance.loc[target_index, 'vapour_pressure'] = 'derived_opt_in'
                applied_rules['vapour_pressure'] = DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION

    return complete.loc[:, ['date', *FINAL_SERIES_COLUMNS]], provenance.loc[:, ['date', *FINAL_SERIES_COLUMNS]], applied_rules


def build_hourly_daily_fill_tables(hourly_table: pd.DataFrame | None, *, fields: tuple[str, ...]) -> dict[str, pd.DataFrame]:
    if hourly_table is None or hourly_table.empty:
        return {}
    tables: dict[str, pd.DataFrame] = {}
    for field_name in fields:
        aggregated = aggregate_hourly_field_to_daily(hourly_table, field_name)
        if aggregated is not None and not aggregated.empty:
            tables[field_name] = aggregated
    return tables


def aggregate_hourly_field_to_daily(hourly_table: pd.DataFrame, canonical_name: str) -> pd.DataFrame | None:
    filtered = hourly_table[hourly_table['element'].astype(str) == canonical_name].copy()
    if filtered.empty:
        return None
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
    if filtered.empty:
        return None
    filtered['date'] = filtered['timestamp'].dt.date
    filtered['numeric_value'] = pd.to_numeric(filtered['value'], errors='coerce')
    filtered = filtered.dropna(subset=['numeric_value'])
    if filtered.empty:
        return None
    aggregated = (
        filtered.groupby('date', as_index=False)
        .agg(hourly_count=('numeric_value', 'count'), hourly_mean=('numeric_value', 'mean'))
    )
    aggregated = aggregated[aggregated['hourly_count'] >= PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS].copy()
    if aggregated.empty:
        return None
    return aggregated.rename(columns={'hourly_mean': f'{canonical_name}__hourly_fill'})[['date', f'{canonical_name}__hourly_fill']]


def select_daily_variable_rows(daily_table: pd.DataFrame, *, canonical_name: str, config: Any) -> pd.DataFrame:
    filtered = daily_table[daily_table['element'].astype(str) == canonical_name].copy()
    required_time_function = config.time_function_by_canonical.get(canonical_name)
    if required_time_function is not None:
        filtered = filtered[filtered['time_function'].astype(str).str.strip() == required_time_function]
    if filtered.empty:
        return pd.DataFrame(columns=['date', canonical_name])
    filtered['date'] = pd.to_datetime(filtered['observation_date']).dt.date
    filtered[canonical_name] = pd.to_numeric(filtered['value'], errors='coerce')
    filtered = filtered[['date', canonical_name]].dropna(subset=[canonical_name]).drop_duplicates(subset=['date'], keep='last')
    return filtered.reset_index(drop=True)


def build_series_record(complete: pd.DataFrame, *, station_id: str, full_name: str, latitude: float | None, longitude: float | None, elevation: float | None) -> dict[str, Any]:
    return {
        'station_id': station_id,
        'full_name': full_name,
        'latitude': latitude,
        'longitude': longitude,
        'elevation_m': elevation,
        'date': [value.isoformat() for value in complete['date'].tolist()],
        'tas_mean': pd.to_numeric(complete['tas_mean'], errors='coerce').tolist(),
        'tas_max': pd.to_numeric(complete['tas_max'], errors='coerce').tolist(),
        'tas_min': pd.to_numeric(complete['tas_min'], errors='coerce').tolist(),
        'wind_speed': pd.to_numeric(complete['wind_speed'], errors='coerce').tolist(),
        'vapour_pressure': pd.to_numeric(complete['vapour_pressure'], errors='coerce').tolist(),
        'sunshine_duration': pd.to_numeric(complete['sunshine_duration'], errors='coerce').tolist(),
    }


def build_station_table(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'num_complete_days', 'first_complete_date', 'last_complete_date']
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(rows, columns=columns)


def build_series_table(series: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'date', *FINAL_SERIES_COLUMNS]
    records: list[dict[str, Any]] = []
    for item in series:
        dates = item['date']
        num_rows = len(dates)
        for index in range(num_rows):
            records.append(
                {
                    'station_id': item['station_id'],
                    'full_name': item['full_name'],
                    'latitude': item['latitude'],
                    'longitude': item['longitude'],
                    'elevation_m': item['elevation_m'],
                    'date': dates[index],
                    'tas_mean': item['tas_mean'][index],
                    'tas_max': item['tas_max'][index],
                    'tas_min': item['tas_min'][index],
                    'wind_speed': item['wind_speed'][index],
                    'vapour_pressure': item['vapour_pressure'][index],
                    'sunshine_duration': item['sunshine_duration'][index],
                }
            )
    if not records:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(records, columns=columns)
