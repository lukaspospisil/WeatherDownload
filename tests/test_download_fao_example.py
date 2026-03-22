import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd


MODULE_PATH = Path('examples/download_fao.py')
SPEC = importlib.util.spec_from_file_location('download_fao_example', MODULE_PATH)
download_fao = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = download_fao
assert SPEC.loader is not None
SPEC.loader.exec_module(download_fao)


class DownloadFaoExampleTests(unittest.TestCase):
    def test_build_parser_defaults_country_to_cz(self) -> None:
        parser = download_fao.build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.country, 'CZ')

    def test_build_parser_accepts_explicit_country_cz(self) -> None:
        parser = download_fao.build_parser()
        args = parser.parse_args(['--country', 'CZ'])
        self.assertEqual(args.country, 'CZ')

    def test_get_fao_country_config_returns_cz_mapping(self) -> None:
        config = download_fao.get_fao_country_config('CZ')
        self.assertEqual(config.country, 'CZ')
        self.assertEqual(config.dataset_scope, 'historical_csv')
        self.assertEqual(config.canonical_to_raw['tas_mean'], ('T',))
        self.assertEqual(config.time_function_by_canonical['tas_max'], '20:00')
        self.assertEqual(config.obs_types, ('DLY',))

    def test_get_fao_country_config_returns_de_mapping(self) -> None:
        config = download_fao.get_fao_country_config('DE')
        self.assertEqual(config.country, 'DE')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.canonical_to_raw['vapour_pressure'], ('VPM',))
        self.assertEqual(config.raw_to_canonical['TMK'], 'tas_mean')
        self.assertEqual(config.raw_to_canonical['SDK'], 'sunshine_duration')
        self.assertEqual(config.time_function_by_canonical, {})
        self.assertEqual(config.obs_types, ('DAILY',))

    def test_main_reports_unsupported_country_clearly(self) -> None:
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exit_code = download_fao.main(['--country', 'FR'])
        self.assertEqual(exit_code, 1)
        self.assertIn('FAO preparation example is not implemented for country FR', stderr_buffer.getvalue())

    def test_prepare_complete_station_series_applies_timefunc_rules_for_cz(self) -> None:
        config = download_fao.get_fao_country_config('CZ')
        daily_table = pd.DataFrame([
            {'station_id': '0-20000-0-11406', 'element': 'tas_mean', 'element_raw': 'T', 'observation_date': '2024-01-01', 'time_function': 'AVG', 'value': '1.0'},
            {'station_id': '0-20000-0-11406', 'element': 'tas_mean', 'element_raw': 'T', 'observation_date': '2024-01-02', 'time_function': 'AVG', 'value': '2.0'},
            {'station_id': '0-20000-0-11406', 'element': 'tas_max', 'element_raw': 'TMA', 'observation_date': '2024-01-01', 'time_function': '20:00', 'value': '5.0'},
            {'station_id': '0-20000-0-11406', 'element': 'tas_max', 'element_raw': 'TMA', 'observation_date': '2024-01-02', 'time_function': '21:00', 'value': '6.0'},
            {'station_id': '0-20000-0-11406', 'element': 'tas_min', 'element_raw': 'TMI', 'observation_date': '2024-01-01', 'time_function': '20:00', 'value': '-1.0'},
            {'station_id': '0-20000-0-11406', 'element': 'tas_min', 'element_raw': 'TMI', 'observation_date': '2024-01-02', 'time_function': '20:00', 'value': '0.0'},
            {'station_id': '0-20000-0-11406', 'element': 'wind_speed', 'element_raw': 'F', 'observation_date': '2024-01-01', 'time_function': 'AVG', 'value': '3.0'},
            {'station_id': '0-20000-0-11406', 'element': 'wind_speed', 'element_raw': 'F', 'observation_date': '2024-01-02', 'time_function': 'AVG', 'value': '4.0'},
            {'station_id': '0-20000-0-11406', 'element': 'vapour_pressure', 'element_raw': 'E', 'observation_date': '2024-01-01', 'time_function': 'AVG', 'value': '7.0'},
            {'station_id': '0-20000-0-11406', 'element': 'vapour_pressure', 'element_raw': 'E', 'observation_date': '2024-01-02', 'time_function': 'AVG', 'value': '8.0'},
            {'station_id': '0-20000-0-11406', 'element': 'sunshine_duration', 'element_raw': 'SSV', 'observation_date': '2024-01-01', 'time_function': '00:00', 'value': '0.5'},
            {'station_id': '0-20000-0-11406', 'element': 'sunshine_duration', 'element_raw': 'SSV', 'observation_date': '2024-01-02', 'time_function': '00:00', 'value': '0.8'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(len(complete), 1)
        self.assertEqual(complete['Date'].tolist(), [pd.Timestamp('2024-01-01').date()])
        self.assertEqual(list(complete.columns), ['Date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])

    def test_prepare_complete_station_series_handles_de_without_timefunc_rules(self) -> None:
        config = download_fao.get_fao_country_config('DE')
        daily_table = pd.DataFrame([
            {'station_id': '00044', 'element': 'tas_mean', 'element_raw': 'TMK', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.0'},
            {'station_id': '00044', 'element': 'tas_max', 'element_raw': 'TXK', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '5.0'},
            {'station_id': '00044', 'element': 'tas_min', 'element_raw': 'TNK', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '-1.0'},
            {'station_id': '00044', 'element': 'wind_speed', 'element_raw': 'FM', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.0'},
            {'station_id': '00044', 'element': 'vapour_pressure', 'element_raw': 'VPM', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': '00044', 'element': 'sunshine_duration', 'element_raw': 'SDK', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(len(complete), 1)
        self.assertEqual(list(complete.columns), ['Date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(float(complete.loc[0, 'vapour_pressure']), 7.0)
        self.assertEqual(float(complete.loc[0, 'sunshine_duration']), 0.5)

    def test_export_parquet_bundle_writes_portable_bundle_files(self) -> None:
        data_info = {
            'CreatedAt': '2026-03-22T10:00:00+00:00',
            'DatasetType': 'test bundle',
            'Source': 'test',
            'Elements': ['tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'],
            'ProviderElementMapping': {
                'tas_mean': {'raw_codes': ['T'], 'selection_rule': 'AVG'},
            },
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
                'tas_mean': [1.0, 2.0],
                'tas_max': [3.0, 4.0],
                'tas_min': [-1.0, 0.0],
                'wind_speed': [2.5, 3.5],
                'vapour_pressure': [7.0, 8.0],
                'sunshine_duration': [0.5, 0.8],
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
            self.assertEqual(written_info['ProviderElementMapping']['tas_mean']['raw_codes'], ['T'])
            self.assertEqual(list(written_stations['WSI']), ['0-20000-0-11406'])
            self.assertEqual(list(written_series['Date']), ['2024-01-01', '2024-01-02'])
            self.assertEqual(list(written_series['vapour_pressure']), [7.0, 8.0])
            self.assertFalse(written_series[download_fao.FINAL_SERIES_COLUMNS].isna().any().any())

    def test_screen_candidate_stations_deduplicates_meta1_by_station_id(self) -> None:
        config = download_fao.get_fao_country_config('CZ')
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
        ])
        meta2_rows = []
        for raw_element in ['T', 'TMA', 'TMI', 'F', 'E', 'SSV']:
            meta2_rows.append(
                {
                    'obs_type': 'DLY',
                    'station_id': '0-20000-0-11406',
                    'element': raw_element,
                    'begin_date': '2000-01-01',
                    'end_date': '2015-12-31',
                }
            )
        meta2 = pd.DataFrame(meta2_rows)

        candidates = download_fao.screen_candidate_stations(meta1, meta2, config=config, min_complete_days=3650)

        self.assertEqual(list(candidates['station_id']), ['0-20000-0-11406'])
        self.assertEqual(list(candidates['full_name']), ['Cheb primary'])
        self.assertEqual(len(candidates), 1)

    def test_load_station_metadata_with_cache_build_mode_requires_cached_file(self) -> None:
        reporter = download_fao.ProgressReporter(silent=True)
        stats = download_fao.CacheStats()
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(download_fao.CacheMissingError):
                download_fao.load_station_metadata_with_cache(
                    Path(tmpdir),
                    country='CZ',
                    mode='build',
                    timeout=60,
                    reporter=reporter,
                    stats=stats,
                )

    def test_read_cached_daily_observations_reads_cached_file(self) -> None:
        station_id = '0-20000-0-11406'
        table = pd.DataFrame([
            {
                'station_id': station_id,
                'gh_id': 'L3CHEB01',
                'element': 'tas_mean',
                'element_raw': 'T',
                'observation_date': '2024-01-01',
                'time_function': 'AVG',
                'value': 1.0,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': 'daily',
            }
        ])

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_path = download_fao.cached_daily_observations_path(cache_dir, station_id)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            table.to_csv(cache_path, index=False)

            loaded = download_fao.read_cached_daily_observations(station_id, cache_dir=cache_dir)

            self.assertEqual(loaded.iloc[0]['element_raw'], 'T')
            self.assertEqual(str(loaded.iloc[0]['observation_date']), '2024-01-01')

    def test_build_series_record_uses_canonical_export_names(self) -> None:
        complete = pd.DataFrame([
            {
                'Date': pd.Timestamp('2024-01-01').date(),
                'tas_mean': 1.0,
                'tas_max': 3.0,
                'tas_min': -1.0,
                'wind_speed': 2.5,
                'vapour_pressure': 7.0,
                'sunshine_duration': 0.5,
            }
        ])

        series = download_fao.build_series_record(
            complete,
            station_id='00044',
            full_name='TEST',
            latitude=54.3,
            longitude=11.0,
            elevation=10.0,
        )

        self.assertIn('tas_mean', series)
        self.assertIn('sunshine_duration', series)
        self.assertNotIn('T', series)
        self.assertNotIn('SSV', series)

    def test_ensure_daily_observations_cached_counts_cache_hits_without_downloading(self) -> None:
        station_id = '0-20000-0-11406'
        config = download_fao.get_fao_country_config('CZ')
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_path = download_fao.cached_daily_observations_path(cache_dir, station_id)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text('cached', encoding='utf-8')

            stats = download_fao.CacheStats()
            with patch.object(download_fao, 'download_observations', side_effect=AssertionError('cache hit should skip download')):
                result = download_fao.ensure_daily_observations_cached(
                    station_id,
                    cache_dir=cache_dir,
                    config=config,
                    mode='full',
                    timeout=60,
                    stats=stats,
                )

            self.assertTrue(result.available)
            self.assertEqual(result.reused, 1)
            self.assertEqual(stats.reused, 1)
            self.assertEqual(stats.downloaded, 0)

    def test_silent_mode_suppresses_nonessential_progress(self) -> None:
        reporter = download_fao.ProgressReporter(silent=True)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            reporter.info('hidden')
            reporter.essential('shown')
        self.assertEqual(buffer.getvalue().strip(), 'shown')


if __name__ == '__main__':
    unittest.main()
