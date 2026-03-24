import unittest

import pandas as pd

from weatherdownload.be_parser import (
    normalize_be_metadata_datetime,
    normalize_be_observation_metadata,
    normalize_be_station_metadata,
    parse_be_feature_collection_json,
)
from weatherdownload.be_registry import BE_DAILY_PARAMETER_METADATA, BE_TENMIN_PARAMETER_METADATA, get_dataset_spec


class BelgiumParserTests(unittest.TestCase):
    def test_parse_be_feature_collection_json_rejects_invalid_json(self) -> None:
        with self.assertRaisesRegex(ValueError, r'RMI/KMI AWS response is not valid JSON\.'):
            parse_be_feature_collection_json('{not-json}')

    def test_parse_be_feature_collection_json_rejects_missing_features(self) -> None:
        with self.assertRaisesRegex(ValueError, r'RMI/KMI AWS response is missing a features list\.'):
            parse_be_feature_collection_json('{"type":"FeatureCollection"}')

    def test_normalize_be_station_metadata_extracts_station_core_fields(self) -> None:
        payload = {
            'features': [
                {
                    'geometry': {'type': 'Point', 'coordinates': [4.364, 51.325]},
                    'properties': {
                        'code': 6438,
                        'name': 'STABROEK',
                        'date_begin': '2012-08-05T00:00:00Z',
                        'date_end': None,
                        'altitude': 4.0,
                    },
                },
                {
                    'geometry': {'type': 'Point', 'coordinates': [3.122, 50.904]},
                    'properties': {
                        'code': 6414,
                        'name': 'BEITEM',
                        'date_begin': '2003-07-26T00:10:00Z',
                        'date_end': None,
                        'altitude': 24.8,
                    },
                },
            ]
        }
        stations = normalize_be_station_metadata(payload)
        self.assertEqual(stations['station_id'].tolist(), ['6414', '6438'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(stations.iloc[0]['begin_date'], '2003-07-26T00:10Z')
        self.assertEqual(float(stations.iloc[0]['longitude']), 3.122)

    def test_normalize_be_station_metadata_does_not_infer_missing_geometry(self) -> None:
        payload = {
            'features': [
                {
                    'geometry': None,
                    'properties': {
                        'code': 7001,
                        'name': 'NO_GEOMETRY',
                        'date_begin': '2020-01-01T00:00:00Z',
                        'date_end': None,
                        'altitude': 12.0,
                    },
                },
            ]
        }
        stations = normalize_be_station_metadata(payload)
        self.assertTrue(pd.isna(stations.iloc[0]['longitude']))
        self.assertTrue(pd.isna(stations.iloc[0]['latitude']))
        self.assertAlmostEqual(float(stations.iloc[0]['elevation_m']), 12.0)

    def test_normalize_be_observation_metadata_builds_daily_rows_for_supported_elements(self) -> None:
        stations = pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': pd.NA,
                'begin_date': '2003-07-26T00:10Z',
                'end_date': '',
                'full_name': 'BEITEM',
                'longitude': 3.122,
                'latitude': 50.904,
                'elevation_m': 24.8,
            }
        ])
        metadata = normalize_be_observation_metadata(stations, get_dataset_spec('historical', 'daily'), BE_DAILY_PARAMETER_METADATA)
        self.assertEqual(metadata.iloc[0]['station_id'], '6414')
        self.assertIn('temp_avg', metadata['element'].tolist())
        self.assertIn('P1D', metadata['schedule'].iloc[0])
        self.assertIn('precipitation', ' '.join(metadata['description'].fillna('').tolist()).lower())

    def test_normalize_be_observation_metadata_builds_tenmin_rows_for_supported_elements(self) -> None:
        stations = pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': pd.NA,
                'begin_date': '2003-07-26T00:10Z',
                'end_date': '',
                'full_name': 'BEITEM',
                'longitude': 3.122,
                'latitude': 50.904,
                'elevation_m': 24.8,
            }
        ])
        metadata = normalize_be_observation_metadata(stations, get_dataset_spec('historical', '10min'), BE_TENMIN_PARAMETER_METADATA)
        self.assertEqual(metadata.iloc[0]['station_id'], '6414')
        self.assertIn('temp_dry_shelter_avg', metadata['element'].tolist())
        self.assertIn('PT10M', metadata['schedule'].iloc[0])
        self.assertIn('last-minute average', ' '.join(metadata['description'].fillna('').tolist()).lower())

    def test_normalize_be_metadata_datetime_converts_to_project_format(self) -> None:
        self.assertEqual(normalize_be_metadata_datetime('2024-01-01T00:00:00+00:00'), '2024-01-01T00:00Z')


if __name__ == '__main__':
    unittest.main()
