import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path('utils/find_cz_fao_plus_evaporation.py')
SPEC = importlib.util.spec_from_file_location('find_cz_fao_plus_evaporation', MODULE_PATH)
script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = script
assert SPEC.loader is not None
SPEC.loader.exec_module(script)


class FindCzFaoPlusEvaporationTests(unittest.TestCase):
    def test_discover_matching_stations_requires_full_element_intersection(self) -> None:
        stations = pd.DataFrame(
            [
                {
                    'station_id': 'A',
                    'full_name': 'Alpha',
                    'latitude': 50.1,
                    'longitude': 14.4,
                    'elevation_m': 250.0,
                },
                {
                    'station_id': 'B',
                    'full_name': 'Beta',
                    'latitude': 49.9,
                    'longitude': 15.1,
                    'elevation_m': 310.0,
                },
                {
                    'station_id': 'C',
                    'full_name': 'Gamma',
                    'latitude': 49.1,
                    'longitude': 16.6,
                    'elevation_m': 220.0,
                },
            ]
        )
        meta2 = pd.DataFrame(
            [
                *[
                    {
                        'obs_type': 'DLY',
                        'station_id': 'A',
                        'element': raw_code,
                        'begin_date': '2000-01-01T00:00Z',
                        'end_date': '2020-12-31T00:00Z',
                    }
                    for raw_code in ['T', 'TMA', 'TMI', 'F', 'E', 'SSV', 'VY']
                ],
                *[
                    {
                        'obs_type': 'DLY',
                        'station_id': 'B',
                        'element': raw_code,
                        'begin_date': '2000-01-01T00:00Z',
                        'end_date': '2020-12-31T00:00Z',
                    }
                    for raw_code in ['T', 'TMA', 'TMI', 'F', 'E', 'SSV']
                ],
                {
                    'obs_type': 'DLY',
                    'station_id': 'C',
                    'element': 'VY',
                    'begin_date': '2000-01-01T00:00Z',
                    'end_date': '2020-12-31T00:00Z',
                },
            ]
        )

        candidates, inspected_count = script.discover_matching_stations(
            stations,
            meta2,
        )

        self.assertEqual(inspected_count, 3)
        self.assertEqual(candidates['station_id'].tolist(), ['A'])
        self.assertEqual(candidates.iloc[0]['available_required_elements'], ','.join(script.REQUIRED_ELEMENTS))
        self.assertEqual(candidates.iloc[0]['first_required_date'], '2000-01-01')
        self.assertEqual(candidates.iloc[0]['last_required_date'], '2020-12-31')


if __name__ == '__main__':
    unittest.main()
