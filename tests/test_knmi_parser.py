import unittest
from pathlib import Path

import pandas as pd

from weatherdownload.providers.nl.parser import (
    KNMI_NORMALIZED_DAILY_COLUMNS,
    normalize_knmi_daily_rows,
    normalize_knmi_observation_metadata,
    normalize_knmi_station_metadata,
    parse_knmi_daily_text,
)
from weatherdownload.providers.nl.registry import get_dataset_spec

SAMPLE_TEXT = Path('tests/data/sample_knmi_daily_public.txt').read_text(encoding='utf-8')


class KnmiParserTests(unittest.TestCase):
    def test_parse_knmi_daily_text_extracts_station_metadata_and_table(self) -> None:
        parsed = parse_knmi_daily_text(SAMPLE_TEXT)
        stations = normalize_knmi_station_metadata(parsed)

        self.assertEqual(stations['station_id'].tolist(), ['260', '310'])
        self.assertEqual(stations['full_name'].tolist(), ['De Bilt', 'Vlissingen'])
        self.assertAlmostEqual(float(stations.iloc[0]['longitude']), 5.18)
        self.assertAlmostEqual(float(stations.iloc[1]['latitude']), 51.442)
        self.assertEqual(parsed.table.columns.tolist(), ['STN', 'YYYYMMDD', 'TG', 'TX', 'TN', 'RH', 'FG', 'UG', 'PG', 'SQ', 'Q'])

    def test_normalize_knmi_observation_metadata_lists_supported_daily_elements(self) -> None:
        parsed = parse_knmi_daily_text(SAMPLE_TEXT)
        metadata = normalize_knmi_observation_metadata(parsed, get_dataset_spec('knmi', 'daily'))

        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['FG', 'PG', 'Q', 'RH', 'SQ', 'TG', 'TN', 'TX', 'UG'])
        self.assertTrue(metadata['schedule'].eq('P1D KNMI daggegevens public daily CSV').all())

    def test_normalize_knmi_daily_rows_applies_exact_unit_and_trace_conversions(self) -> None:
        parsed = parse_knmi_daily_text(SAMPLE_TEXT)
        query_start = pd.Timestamp('2024-01-01').date()
        query_end = pd.Timestamp('2024-01-02').date()

        frames = {
            raw_code: normalize_knmi_daily_rows(
                parsed,
                raw_code=raw_code,
                provider='knmi',
                resolution='daily',
                station_ids={'260'},
                start_date=query_start,
                end_date=query_end,
            )
            for raw_code in ['TG', 'TX', 'TN', 'RH', 'FG', 'UG', 'PG', 'SQ', 'Q']
        }

        self.assertEqual(list(frames['TG'].columns), KNMI_NORMALIZED_DAILY_COLUMNS)
        self.assertAlmostEqual(float(frames['TG'].iloc[0]['value']), 7.4)
        self.assertAlmostEqual(float(frames['TX'].iloc[0]['value']), 9.1)
        self.assertAlmostEqual(float(frames['TN'].iloc[0]['value']), 6.4)
        self.assertAlmostEqual(float(frames['RH'].iloc[0]['value']), 0.0)
        self.assertAlmostEqual(float(frames['RH'].iloc[1]['value']), 24.0)
        self.assertAlmostEqual(float(frames['FG'].iloc[0]['value']), 5.5)
        self.assertAlmostEqual(float(frames['UG'].iloc[0]['value']), 87.0)
        self.assertAlmostEqual(float(frames['PG'].iloc[0]['value']), 1001.3)
        self.assertAlmostEqual(float(frames['SQ'].iloc[0]['value']), 0.0)
        self.assertAlmostEqual(float(frames['SQ'].iloc[1]['value']), 0.0)
        self.assertAlmostEqual(float(frames['Q'].iloc[0]['value']), 1.42)

    def test_normalize_knmi_daily_rows_keeps_missing_values_as_na(self) -> None:
        parsed = parse_knmi_daily_text(SAMPLE_TEXT)
        frame = normalize_knmi_daily_rows(
            parsed,
            raw_code='TG',
            provider='knmi',
            resolution='daily',
            station_ids={'310'},
            start_date=pd.Timestamp('2024-01-01').date(),
            end_date=pd.Timestamp('2024-01-02').date(),
        )

        self.assertAlmostEqual(float(frame.iloc[0]['value']), 8.5)
        self.assertTrue(pd.isna(frame.iloc[1]['value']))


if __name__ == '__main__':
    unittest.main()
