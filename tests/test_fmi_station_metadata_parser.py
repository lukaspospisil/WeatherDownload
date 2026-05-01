import unittest
from pathlib import Path

import pandas as pd

from weatherdownload.providers.fi.fmi_station_metadata_parser import parse_fmi_station_feature_collection


SAMPLE_STATIONS_XML = Path('tests/data/sample_fmi_stations.xml')


class FmiStationMetadataParserTests(unittest.TestCase):
    def test_parse_station_fixture_extracts_core_fields(self) -> None:
        stations = parse_fmi_station_feature_collection(SAMPLE_STATIONS_XML.read_text(encoding='utf-8'))
        self.assertFalse(stations.empty)
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertTrue(stations['station_id'].astype(str).str.isnumeric().all())
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertTrue(stations['full_name'].notna().all())
        self.assertTrue(stations['latitude'].notna().all())
        self.assertTrue(stations['longitude'].notna().all())
        # Begin dates are present in the fixture, end_date may be empty for "now".
        self.assertTrue(stations['begin_date'].astype(str).str.endswith('Z').all())


if __name__ == '__main__':
    unittest.main()

