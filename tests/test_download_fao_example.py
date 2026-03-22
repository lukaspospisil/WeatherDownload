import importlib.util
import unittest
from pathlib import Path

import pandas as pd


MODULE_PATH = Path('examples/download_fao.py')
SPEC = importlib.util.spec_from_file_location('download_fao_example', MODULE_PATH)
download_fao = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(download_fao)


class DownloadFaoExampleTests(unittest.TestCase):
    def test_prepare_complete_station_series_applies_timefunc_rules_and_keeps_complete_days(self) -> None:
        raw_tables = {
            'T': pd.DataFrame([
                {'ELEMENT': 'T', 'TIMEFUNC': 'AVG', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '1.0'},
                {'ELEMENT': 'T', 'TIMEFUNC': 'AVG', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '2.0'},
            ]),
            'TMA': pd.DataFrame([
                {'ELEMENT': 'TMA', 'TIMEFUNC': '20:00', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '5.0'},
                {'ELEMENT': 'TMA', 'TIMEFUNC': '21:00', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '6.0'},
            ]),
            'TMI': pd.DataFrame([
                {'ELEMENT': 'TMI', 'TIMEFUNC': '20:00', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '-1.0'},
                {'ELEMENT': 'TMI', 'TIMEFUNC': '20:00', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '0.0'},
            ]),
            'F': pd.DataFrame([
                {'ELEMENT': 'F', 'TIMEFUNC': 'AVG', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '3.0'},
                {'ELEMENT': 'F', 'TIMEFUNC': 'AVG', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '4.0'},
            ]),
            'E': pd.DataFrame([
                {'ELEMENT': 'E', 'TIMEFUNC': 'AVG', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '7.0'},
                {'ELEMENT': 'E', 'TIMEFUNC': 'AVG', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '8.0'},
            ]),
            'SSV': pd.DataFrame([
                {'ELEMENT': 'SSV', 'TIMEFUNC': '00:00', 'DT': '2024-01-01T00:00:00Z', 'VALUE': '0.5'},
                {'ELEMENT': 'SSV', 'TIMEFUNC': '00:00', 'DT': '2024-01-02T00:00:00Z', 'VALUE': '0.8'},
            ]),
        }

        complete = download_fao.prepare_complete_station_series(raw_tables)

        self.assertEqual(len(complete), 1)
        self.assertEqual(complete['Date'].tolist(), [pd.Timestamp('2024-01-01').date()])
        self.assertEqual(list(complete.columns), ['Date', 'T', 'TMA', 'TMI', 'F', 'E', 'SSV'])


if __name__ == '__main__':
    unittest.main()
