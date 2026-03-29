import json
import unittest

import pandas as pd

from weatherdownload.providers.at.registry import get_dataset_spec
from weatherdownload.providers.at.parser import (
    normalize_geosphere_metadata_datetime,
    normalize_geosphere_observation_metadata,
    normalize_geosphere_station_metadata,
    parse_geosphere_daily_csv,
    parse_geosphere_hourly_csv,
    parse_geosphere_metadata_json,
    parse_geosphere_tenmin_csv,
)


class GeosphereParserTests(unittest.TestCase):
    def test_parse_geosphere_metadata_json_rejects_invalid_json(self) -> None:
        with self.assertRaisesRegex(ValueError, r'GeoSphere metadata response is not valid JSON\.'):
            parse_geosphere_metadata_json('{not-json}')

    def test_parse_geosphere_metadata_json_rejects_missing_stations(self) -> None:
        with self.assertRaisesRegex(ValueError, r'GeoSphere metadata response is missing a stations list\.'):
            parse_geosphere_metadata_json(json.dumps({'parameters': []}))

    def test_parse_geosphere_metadata_json_rejects_missing_parameters(self) -> None:
        with self.assertRaisesRegex(ValueError, r'GeoSphere metadata response is missing a parameters list\.'):
            parse_geosphere_metadata_json(json.dumps({'stations': []}))

    def test_normalize_geosphere_station_metadata_is_sorted_and_uses_project_date_strings(self) -> None:
        payload = {
            'stations': [
                {
                    'id': 2,
                    'name': 'Second',
                    'lat': 47.5,
                    'lon': 14.1,
                    'altitude': 641.0,
                    'valid_from': '1939-03-01T00:00:00+00:00',
                    'valid_to': '2100-12-31T00:00:00+00:00',
                },
                {
                    'id': 1,
                    'name': 'First',
                    'lat': 47.5,
                    'lon': 15.2,
                    'altitude': 783.2,
                    'valid_from': '1983-05-01T00:00:00+00:00',
                    'valid_to': '2100-12-31T00:00:00+00:00',
                },
            ],
            'parameters': [],
        }
        stations = normalize_geosphere_station_metadata(payload)
        self.assertEqual(stations['station_id'].tolist(), ['1', '2'])
        self.assertEqual(stations.iloc[0]['begin_date'], '1983-05-01T00:00Z')
        self.assertTrue(all(isinstance(value, str) for value in stations['begin_date']))
        self.assertTrue(all(isinstance(value, str) for value in stations['end_date']))

    def test_normalize_geosphere_observation_metadata_builds_daily_station_parameter_rows(self) -> None:
        payload = {
            'stations': [
                {
                    'id': 1,
                    'name': 'Aflenz',
                    'lat': 47.5,
                    'lon': 15.2,
                    'altitude': 783.2,
                    'valid_from': '1983-05-01T00:00:00+00:00',
                    'valid_to': '2100-12-31T00:00:00+00:00',
                },
            ],
            'parameters': [
                {
                    'name': 'tl_mittel',
                    'long_name': 'Air temperature 2m mean',
                    'description': 'Daily mean air temperature',
                    'unit': 'degC',
                },
            ],
        }
        metadata = normalize_geosphere_observation_metadata(payload, get_dataset_spec('historical', 'daily'))
        self.assertEqual(metadata.iloc[0]['station_id'], '1')
        self.assertEqual(metadata.iloc[0]['element'], 'tl_mittel')
        self.assertEqual(metadata.iloc[0]['obs_type'], 'HISTORICAL_DAILY')
        self.assertIn('Daily mean air temperature', metadata.iloc[0]['description'])

    def test_normalize_geosphere_observation_metadata_builds_hourly_station_parameter_rows(self) -> None:
        payload = {
            'stations': [
                {
                    'id': 1,
                    'name': 'Aflenz',
                    'lat': 47.5,
                    'lon': 15.2,
                    'altitude': 783.2,
                    'valid_from': '1983-05-01T00:00:00+00:00',
                    'valid_to': '2100-12-31T00:00:00+00:00',
                },
            ],
            'parameters': [
                {
                    'name': 'tl',
                    'long_name': 'Air temperature 2m',
                    'description': 'Hourly air temperature',
                    'unit': 'degC',
                },
            ],
        }
        metadata = normalize_geosphere_observation_metadata(payload, get_dataset_spec('historical', '1hour'))
        self.assertEqual(metadata.iloc[0]['element'], 'tl')
        self.assertEqual(metadata.iloc[0]['obs_type'], 'HISTORICAL_HOURLY')
        self.assertEqual(metadata.iloc[0]['schedule'], 'PT1H GeoSphere station API')

    def test_normalize_geosphere_observation_metadata_builds_tenmin_station_parameter_rows(self) -> None:
        payload = {
            'stations': [
                {
                    'id': 1,
                    'name': 'Aflenz',
                    'lat': 47.5,
                    'lon': 15.2,
                    'altitude': 783.2,
                    'valid_from': '1983-05-01T00:00:00+00:00',
                    'valid_to': '2100-12-31T00:00:00+00:00',
                },
            ],
            'parameters': [
                {
                    'name': 'tl',
                    'long_name': 'Air temperature 2m',
                    'description': '10-minute air temperature',
                    'unit': 'degC',
                },
            ],
        }
        metadata = normalize_geosphere_observation_metadata(payload, get_dataset_spec('historical', '10min'))
        self.assertEqual(metadata.iloc[0]['element'], 'tl')
        self.assertEqual(metadata.iloc[0]['obs_type'], 'HISTORICAL_10MIN')
        self.assertEqual(metadata.iloc[0]['schedule'], 'PT10M GeoSphere station API')

    def test_parse_geosphere_daily_csv_rejects_missing_required_columns(self) -> None:
        with self.assertRaisesRegex(ValueError, r"GeoSphere daily CSV is missing required columns: \['station'\]"):
            parse_geosphere_daily_csv('time,tl_mittel\n2024-01-01T00:00+00:00,2.2\n')

    def test_parse_geosphere_daily_csv_accepts_documented_shape(self) -> None:
        table = parse_geosphere_daily_csv('time,station,tl_mittel,tl_mittel_flag\n2024-01-01T00:00+00:00,1,2.2,20\n')
        self.assertEqual(list(table.columns), ['time', 'station', 'tl_mittel', 'tl_mittel_flag'])
        self.assertEqual(table.iloc[0]['station'], '1')

    def test_parse_geosphere_hourly_csv_accepts_documented_shape(self) -> None:
        table = parse_geosphere_hourly_csv('time,station,tl,tl_flag\n2024-01-01T01:00:00+00:00,1,2.4,21\n')
        self.assertEqual(list(table.columns), ['time', 'station', 'tl', 'tl_flag'])
        self.assertEqual(table.iloc[0]['tl'], '2.4')

    def test_parse_geosphere_tenmin_csv_accepts_documented_shape(self) -> None:
        table = parse_geosphere_tenmin_csv('time,station,tl,tl_flag\n2024-01-01T00:10:00+00:00,1,0.1,12\n')
        self.assertEqual(list(table.columns), ['time', 'station', 'tl', 'tl_flag'])
        self.assertEqual(table.iloc[0]['tl'], '0.1')

    def test_normalize_geosphere_metadata_datetime_converts_to_project_format(self) -> None:
        self.assertEqual(normalize_geosphere_metadata_datetime('2024-01-01T00:00:00+00:00'), '2024-01-01T00:00Z')


if __name__ == '__main__':
    unittest.main()

