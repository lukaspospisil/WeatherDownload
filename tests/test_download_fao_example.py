import importlib.util
import json
import tempfile
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

    def test_export_parquet_bundle_writes_portable_bundle_files(self) -> None:
        data_info = {
            'CreatedAt': '2026-03-22T10:00:00+00:00',
            'DatasetType': 'test bundle',
            'Source': 'test',
            'Elements': ['T', 'TMA', 'TMI', 'F', 'E', 'SSV'],
            'MinCompleteDays': 3650,
            'NumStations': 1,
        }
        stations = [
            {
                'WSI': '0-20000-0-11406',
                'FULL_NAME': 'TEST STATION',
                'Latitude': 50.1,
                'Longitude': 14.4,
                'Elevation': 250.0,
                'NumCompleteDays_E': 2,
                'FirstCompleteDate_E': '2024-01-01',
                'LastCompleteDate_E': '2024-01-02',
            }
        ]
        series = [
            {
                'WSI': '0-20000-0-11406',
                'FULL_NAME': 'TEST STATION',
                'Latitude': 50.1,
                'Longitude': 14.4,
                'Elevation': 250.0,
                'Date': ['2024-01-01', '2024-01-02'],
                'T': [1.0, 2.0],
                'TMA': [3.0, 4.0],
                'TMI': [-1.0, 0.0],
                'F': [2.5, 3.5],
                'E': [7.0, 8.0],
                'SSV': [0.5, 0.8],
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / 'fao_bundle'
            download_fao.export_parquet_bundle(output_dir, data_info=data_info, stations=stations, series=series)

            data_info_path = output_dir / 'data_info.json'
            stations_path = output_dir / 'stations.parquet'
            series_path = output_dir / 'series.parquet'

            self.assertTrue(data_info_path.exists())
            self.assertTrue(stations_path.exists())
            self.assertTrue(series_path.exists())

            written_info = json.loads(data_info_path.read_text(encoding='utf-8'))
            written_stations = pd.read_parquet(stations_path)
            written_series = pd.read_parquet(series_path)

            self.assertEqual(written_info['NumStations'], 1)
            self.assertEqual(list(written_stations['WSI']), ['0-20000-0-11406'])
            self.assertEqual(list(written_series['Date']), ['2024-01-01', '2024-01-02'])
            self.assertEqual(list(written_series['E']), [7.0, 8.0])
            self.assertFalse(written_series[['T', 'TMA', 'TMI', 'F', 'E', 'SSV']].isna().any().any())

    def test_screen_candidate_stations_deduplicates_meta1_by_station_id(self) -> None:
        meta1 = pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'full_name': 'Cheb primary',
                'latitude': 50.08,
                'longitude': 12.37,
                'elevation_m': 471.0,
            },
            {
                'station_id': '0-20000-0-11406',
                'full_name': 'Cheb duplicate',
                'latitude': 50.09,
                'longitude': 12.38,
                'elevation_m': 472.0,
            },
            {
                'station_id': '0-20000-0-99999',
                'full_name': 'Other station',
                'latitude': 49.0,
                'longitude': 15.0,
                'elevation_m': 300.0,
            },
        ])
        meta2_rows = []
        for element in download_fao.REQUIRED_ELEMENTS:
            meta2_rows.append(
                {
                    'obs_type': 'DLY',
                    'station_id': '0-20000-0-11406',
                    'element': element,
                    'begin_date': '2000-01-01',
                    'end_date': '2015-12-31',
                }
            )
        meta2 = pd.DataFrame(meta2_rows)

        candidates = download_fao.screen_candidate_stations(meta1, meta2, min_complete_days=3650)

        self.assertEqual(list(candidates['station_id']), ['0-20000-0-11406'])
        self.assertEqual(list(candidates['full_name']), ['Cheb primary'])
        self.assertEqual(len(candidates), 1)

    def test_load_station_metadata_with_cache_build_mode_requires_cached_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(download_fao.CacheMissingError):
                download_fao.load_station_metadata_with_cache(Path(tmpdir), mode='build', timeout=60)

    def test_fetch_required_daily_tables_build_mode_reads_cached_files(self) -> None:
        station_id = '0-20000-0-11406'
        raw_template = 'STATION,ELEMENT,TIMEFUNC,DT,VALUE,FLAG,QUALITY\n{station},{element},{timefunc},2024-01-01T00:00:00Z,1.0,,0\n'
        timefunc_map = download_fao.TIMEFUNC_BY_ELEMENT

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            for element in download_fao.REQUIRED_ELEMENTS:
                cache_path = download_fao.cached_daily_csv_path(cache_dir, station_id, element)
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    raw_template.format(station=station_id, element=element, timefunc=timefunc_map[element]),
                    encoding='utf-8',
                )

            original_download_daily_csv = download_fao.download_daily_csv
            try:
                def fail_download(*args, **kwargs):
                    raise AssertionError('build mode should not download daily CSV files')

                download_fao.download_daily_csv = fail_download
                tables = download_fao.fetch_required_daily_tables(
                    station_id,
                    cache_dir=cache_dir,
                    mode='build',
                    timeout=60,
                )
            finally:
                download_fao.download_daily_csv = original_download_daily_csv

            self.assertIsNotNone(tables)
            self.assertEqual(set(tables.keys()), set(download_fao.REQUIRED_ELEMENTS))
            self.assertEqual(tables['T'].iloc[0]['ELEMENT'], 'T')


if __name__ == '__main__':
    unittest.main()
