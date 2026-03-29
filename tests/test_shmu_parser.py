import json
import unittest

import pandas as pd

from weatherdownload.providers.sk.parser import (
    extract_recent_daily_station_date_ranges,
    extract_recent_daily_begin_end_dates,
    extract_recent_daily_station_ids,
    normalize_recent_daily_long_table,
    normalize_shmu_numeric_series,
    parse_recent_daily_payload_json,
    parse_shmu_metadata_json,
)


class ShmuParserTests(unittest.TestCase):
    def test_parse_recent_daily_payload_json_rejects_invalid_json(self) -> None:
        with self.assertRaisesRegex(ValueError, r'SHMU recent daily observations is not valid JSON\.'):
            parse_recent_daily_payload_json('{not-json}')

    def test_parse_recent_daily_payload_json_rejects_missing_data_list(self) -> None:
        with self.assertRaisesRegex(ValueError, r'SHMU recent daily observations is missing a "data" list\.'):
            parse_recent_daily_payload_json(json.dumps({'id': 'kli_inter'}))

    def test_parse_recent_daily_payload_json_rejects_missing_required_columns(self) -> None:
        payload = {
            'id': 'kli_inter',
            'data': [
                {'ind_kli': '11800', 't_max': '5.2'},
            ],
        }
        with self.assertRaisesRegex(ValueError, r"SHMU recent daily observations is missing required columns: \['datum'\]"):
            parse_recent_daily_payload_json(json.dumps(payload))

    def test_parse_recent_daily_payload_json_accepts_reordered_columns(self) -> None:
        payload = {
            'id': 'kli_inter',
            'data': [
                {'t_max': '5.2', 'datum': '2025-01-01', 'ind_kli': '11800'},
                {'datum': '2025-01-02', 'ind_kli': '11800', 't_min': '-1.0'},
            ],
        }
        metadata, table = parse_recent_daily_payload_json(json.dumps(payload))
        self.assertEqual(metadata['id'], 'kli_inter')
        self.assertEqual(table.loc[0, 'ind_kli'], '11800')
        self.assertEqual(table.loc[0, 'datum'], '2025-01-01')

    def test_extract_recent_daily_station_ids_is_order_independent(self) -> None:
        table = pd.DataFrame([
            {'datum': '2025-01-02', 'ind_kli': '11999'},
            {'datum': '2025-01-01', 'ind_kli': '11800'},
            {'datum': '2025-01-01', 'ind_kli': '11800'},
        ])
        self.assertEqual(extract_recent_daily_station_ids(table), ['11800', '11999'])

    def test_extract_recent_daily_station_date_ranges_is_per_station(self) -> None:
        table = pd.DataFrame([
            {'ind_kli': '11800', 'datum': '2025-01-01'},
            {'ind_kli': '11800', 'datum': '2025-01-03'},
            {'ind_kli': '11999', 'datum': '2025-01-02'},
            {'ind_kli': '11999', 'datum': '2025-01-03'},
        ])
        ranges = extract_recent_daily_station_date_ranges(table).set_index('station_id')
        self.assertEqual(ranges.loc['11800', 'begin_date'], '2025-01-01T00:00Z')
        self.assertEqual(ranges.loc['11800', 'end_date'], '2025-01-03T00:00Z')
        self.assertEqual(ranges.loc['11999', 'begin_date'], '2025-01-02T00:00Z')
        self.assertEqual(ranges.loc['11999', 'end_date'], '2025-01-03T00:00Z')

    def test_extract_recent_daily_station_date_ranges_is_deterministic_and_ignores_duplicate_station_dates(self) -> None:
        table = pd.DataFrame([
            {'ind_kli': '11999', 'datum': '2025-01-03'},
            {'ind_kli': '11800', 'datum': '2025-01-02'},
            {'ind_kli': '11800', 'datum': '2025-01-01'},
            {'ind_kli': '11999', 'datum': '2025-01-02'},
            {'ind_kli': '11800', 'datum': '2025-01-01'},
        ])
        ranges = extract_recent_daily_station_date_ranges(table)
        self.assertEqual(ranges['station_id'].tolist(), ['11800', '11999'])
        self.assertEqual(ranges['begin_date'].tolist(), ['2025-01-01T00:00Z', '2025-01-02T00:00Z'])
        self.assertEqual(ranges['end_date'].tolist(), ['2025-01-02T00:00Z', '2025-01-03T00:00Z'])
        self.assertTrue(all(isinstance(value, str) for value in ranges['begin_date']))
        self.assertTrue(all(isinstance(value, str) for value in ranges['end_date']))

    def test_extract_recent_daily_begin_end_dates_uses_datum_column(self) -> None:
        table = pd.DataFrame([
            {'datum': '2025-01-02', 'ind_kli': '11999'},
            {'datum': '2025-01-01', 'ind_kli': '11800'},
        ])
        self.assertEqual(extract_recent_daily_begin_end_dates(table), ('2025-01-01T00:00Z', '2025-01-02T00:00Z'))

    def test_normalize_recent_daily_long_table_converts_blank_strings_to_missing(self) -> None:
        table = pd.DataFrame([
            {'ind_kli': '11800', 'datum': '2025-01-01', 't_max': '5.2', 'sln_svit': ''},
            {'ind_kli': '11800', 'datum': '2025-01-02', 't_max': '6.3', 'sln_svit': '1.5'},
        ])
        normalized = normalize_recent_daily_long_table(table, ['t_max', 'sln_svit'])
        self.assertEqual(list(normalized.columns), ['station_id', 'observation_date', 'element_raw', 'value'])
        lookup = normalized.set_index(['element_raw', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('t_max', pd.Timestamp('2025-01-01').date())]), 5.2)
        self.assertTrue(pd.isna(lookup[('sln_svit', pd.Timestamp('2025-01-01').date())]))

    def test_normalize_recent_daily_long_table_rejects_missing_station_column(self) -> None:
        table = pd.DataFrame([
            {'datum': '2025-01-01', 't_max': '5.2'},
        ])
        with self.assertRaisesRegex(ValueError, r"SHMU recent daily table is missing required columns: \['ind_kli'\]"):
            normalize_recent_daily_long_table(table, ['t_max'])

    def test_normalize_recent_daily_long_table_ignores_unsupported_requested_raw_codes(self) -> None:
        table = pd.DataFrame([
            {'ind_kli': '11800', 'datum': '2025-01-01', 't_max': '5.2'},
        ])
        normalized = normalize_recent_daily_long_table(table, ['missing_raw_code'])
        self.assertTrue(normalized.empty)
        self.assertEqual(list(normalized.columns), ['station_id', 'observation_date', 'element_raw', 'value'])

    def test_normalize_shmu_numeric_series_is_explicit_about_blank_missing_values(self) -> None:
        series = pd.Series(['1.5', '', ' ', None, '0.0'])
        normalized = normalize_shmu_numeric_series(series)
        self.assertEqual(normalized.tolist()[0], 1.5)
        self.assertTrue(pd.isna(normalized.tolist()[1]))
        self.assertTrue(pd.isna(normalized.tolist()[2]))
        self.assertTrue(pd.isna(normalized.tolist()[3]))
        self.assertEqual(normalized.tolist()[4], 0.0)

    def test_parse_shmu_metadata_json_rejects_missing_data_list(self) -> None:
        with self.assertRaisesRegex(ValueError, r'SHMU metadata JSON is missing a "data" list\.'):
            parse_shmu_metadata_json(json.dumps({'id': 'metadata'}))


if __name__ == '__main__':
    unittest.main()

