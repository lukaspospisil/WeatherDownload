import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class ObservationCliTests(unittest.TestCase):
    def _sample_tenmin_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'tas_mean',
                'element_raw': 'T',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': -1.2,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': '10min',
            },
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'soil_temperature_10cm',
                'element_raw': 'T10',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': -0.5,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': '10min',
            },
        ])

    def _sample_hourly_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'vapour_pressure',
                'element_raw': 'E',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': 82.0,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': '1hour',
            },
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'pressure',
                'element_raw': 'P',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': 1012.5,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': '1hour',
            },
        ])

    def _sample_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'tas_mean',
                'element_raw': 'T',
                'observation_date': '1865-06-01',
                'time_function': 'AVG',
                'value': 18.5,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': 'daily',
            },
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'tas_max',
                'element_raw': 'TMA',
                'observation_date': '1865-06-01',
                'time_function': '20:00',
                'value': 21.0,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': 'daily',
            },
        ])

    def _sample_de_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'TMK',
                'observation_date': '2024-01-01',
                'time_function': None,
                'value': 3.4,
                'flag': None,
                'quality': 1,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])

    def _sample_de_hourly_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'TT_TU',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': 3.1,
                'flag': None,
                'quality': 1,
                'dataset_scope': 'historical',
                'resolution': '1hour',
            }
        ])

    def _sample_de_tenmin_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'TT_10',
                'timestamp': '2024-01-01T00:00:00Z',
                'value': 2.8,
                'flag': None,
                'quality': 2,
                'dataset_scope': 'historical',
                'resolution': '10min',
            }
        ])

    def _sample_be_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'temp_avg',
                'observation_date': '2024-01-01',
                'time_function': None,
                'value': 4.2,
                'flag': '{"validated":{"TEMP_AVG":true}}',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
    def _sample_nl_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-06260',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'TG',
                'observation_date': '2024-01-01',
                'time_function': None,
                'value': 3.4,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])

    def _sample_nl_hourly_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-06260',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'T',
                'timestamp': '2024-01-01T01:00:00Z',
                'value': 3.1,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '1hour',
            }
        ])

    def _sample_nl_tenmin_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-06260',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'ta',
                'timestamp': '2024-01-01T09:10:00Z',
                'value': 3.1,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '10min',
            }
        ])

    def _sample_us_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': 'USC00000001',
                'gh_id': None,
                'element': 'open_water_evaporation',
                'element_raw': 'EVAP',
                'observation_date': '2020-05-01',
                'time_function': None,
                'value': 1.2,
                'flag': '{"source_flag":"7"}',
                'quality': None,
                'dataset_scope': 'ghcnd',
                'resolution': 'daily',
            }
        ])

    def test_tenmin_cli_screen_output_defaults_to_wide_layout(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_tenmin_table()):
            with redirect_stdout(buffer):
                exit_code = main(['observations', '10min', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--element', 'soil_temperature_10cm', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T00:20:00Z'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('timestamp', output)
        self.assertIn('tas_mean', output)
        self.assertIn('soil_temperature_10cm', output)
        self.assertNotIn('element_raw', output)

    def test_tenmin_cli_uses_default_country_cz(self) -> None:
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_tenmin_table()) as download_mock:
            exit_code = main(['observations', '10min', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T00:20:00Z'])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'CZ')
        self.assertEqual(query.dataset_scope, 'historical_csv')
        self.assertEqual(query.elements, ['T'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'CZ')

    def test_tenmin_cli_all_history_sets_query_mode(self) -> None:
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_tenmin_table()) as download_mock:
            exit_code = main(['observations', '10min', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--all-history'])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertTrue(query.all_history)
        self.assertIsNone(query.start)
        self.assertIsNone(query.end)

    def test_tenmin_cli_explicit_country_de_uses_de_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_de_tenmin_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', '10min', '--country', 'DE', '--station-id', '00044', '--element', 'tas_mean', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T00:20:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.elements, ['TT_10'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'DE')
        self.assertIn('00044', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_tenmin_cli_explicit_country_nl_uses_nl_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_nl_tenmin_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', '10min', '--country', 'NL', '--station-id', '0-20000-0-06260', '--element', 'tas_mean', '--start', '2024-01-01T09:10:00Z', '--end', '2024-01-01T09:20:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'NL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.elements, ['ta'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'NL')
        self.assertIn('0-20000-0-06260', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_tenmin_cli_csv_export_defaults_to_wide_layout(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_tenmin_table()):
                    with redirect_stdout(buffer):
                        exit_code = main(['observations', '10min', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--element', 'soil_temperature_10cm', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T00:20:00Z', '--format', 'csv', '--output', 'tenmin.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/tenmin.csv')
                self.assertTrue(output_path.exists())
                content = output_path.read_text(encoding='utf-8')
                self.assertIn('timestamp', content)
                self.assertIn('tas_mean', content)
                self.assertIn('soil_temperature_10cm', content)
                self.assertNotIn('element_raw', content)
                self.assertIn('Exported 10min observations to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)

    def test_hourly_cli_screen_output_defaults_to_wide_layout(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_hourly_table()):
            with redirect_stdout(buffer):
                exit_code = main(['observations', 'hourly', '--station-id', '0-20000-0-11406', '--element', 'vapour_pressure', '--element', 'pressure', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T02:00:00Z'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('timestamp', output)
        self.assertIn('vapour_pressure', output)
        self.assertIn('pressure', output)
        self.assertNotIn('element_raw', output)

    def test_hourly_cli_all_history_sets_query_mode(self) -> None:
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_hourly_table()) as download_mock:
            exit_code = main(['observations', 'hourly', '--station-id', '0-20000-0-11406', '--element', 'vapour_pressure', '--all-history'])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertTrue(query.all_history)
        self.assertIsNone(query.start)
        self.assertIsNone(query.end)

    def test_hourly_cli_explicit_country_de_uses_de_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_de_hourly_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'hourly', '--country', 'DE', '--station-id', '00044', '--element', 'tas_mean', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T02:00:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.elements, ['TT_TU'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'DE')
        self.assertIn('00044', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_hourly_cli_explicit_country_nl_uses_nl_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_nl_hourly_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'hourly', '--country', 'NL', '--station-id', '0-20000-0-06260', '--element', 'tas_mean', '--start', '2024-01-01T01:00:00Z', '--end', '2024-01-01T02:00:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'NL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.elements, ['T'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'NL')
        self.assertIn('0-20000-0-06260', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_hourly_cli_rejects_mixed_all_history_and_explicit_range(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            exit_code = main([
                'observations', 'hourly', '--station-id', '0-20000-0-11406', '--element', 'vapour_pressure', '--all-history', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T02:00:00Z'
            ])
        self.assertEqual(exit_code, 1)
        self.assertIn('--all-history cannot be used together with --start or --end', stderr.getvalue())

    def test_hourly_cli_layout_long_preserves_normalized_output(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_hourly_table()):
            with redirect_stdout(buffer):
                exit_code = main(['observations', 'hourly', '--station-id', '0-20000-0-11406', '--element', 'vapour_pressure', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T02:00:00Z', '--layout', 'long'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('element_raw', output)
        self.assertIn('vapour_pressure', output)

    def test_hourly_cli_csv_export_uses_outputs_for_bare_filename(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_hourly_table()):
                    with redirect_stdout(buffer):
                        exit_code = main(['observations', 'hourly', '--station-id', '0-20000-0-11406', '--element', 'vapour_pressure', '--element', 'pressure', '--start', '2024-01-01T00:00:00Z', '--end', '2024-01-01T02:00:00Z', '--format', 'csv', '--output', 'hourly.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/hourly.csv')
                self.assertTrue(output_path.exists())
                content = output_path.read_text(encoding='utf-8')
                self.assertIn('timestamp', content)
                self.assertIn('vapour_pressure', content)
                self.assertIn('pressure', content)
                self.assertNotIn('element_raw', content)
                self.assertIn('Exported hourly observations to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)

    def test_daily_cli_screen_output_defaults_to_wide_layout(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
            with redirect_stdout(buffer):
                exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--element', 'tas_max', '--start-date', '1865-06-01', '--end-date', '1865-06-03'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('observation_date', output)
        self.assertIn('tas_mean', output)
        self.assertIn('tas_max', output)
        self.assertNotIn('element_raw', output)

    def test_daily_cli_all_history_sets_query_mode(self) -> None:
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()) as download_mock:
            exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'tas_max', '--all-history'])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertTrue(query.all_history)
        self.assertIsNone(query.start_date)
        self.assertIsNone(query.end_date)

    def test_daily_cli_explicit_country_de_uses_de_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_de_daily_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'daily', '--country', 'DE', '--station-id', '00044', '--element', 'tas_mean', '--start-date', '2024-01-01', '--end-date', '2024-01-03'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['TMK'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'DE')
        self.assertIn('00044', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_daily_cli_explicit_country_be_uses_be_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_be_daily_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'daily', '--country', 'BE', '--station-id', '6414', '--element', 'tas_mean', '--start-date', '2024-01-01', '--end-date', '2024-01-03'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'BE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['temp_avg'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'BE')
        self.assertIn('6414', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())
    def test_daily_cli_explicit_country_nl_uses_nl_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_nl_daily_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'daily', '--country', 'NL', '--station-id', '0-20000-0-06260', '--element', 'tas_mean', '--start-date', '2024-01-01', '--end-date', '2024-01-03'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'NL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['TG'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'NL')
        self.assertIn('0-20000-0-06260', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())

    def test_daily_cli_explicit_country_us_uses_ghcnd_query_shape(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_us_daily_table()) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'daily', '--country', 'US', '--station-id', 'USC00000001', '--element', 'open_water_evaporation', '--start-date', '2020-05-01', '--end-date', '2020-05-03'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'US')
        self.assertEqual(query.dataset_scope, 'ghcnd')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['EVAP'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'US')
        self.assertIn('USC00000001', buffer.getvalue())
        self.assertIn('open_water_evaporation', buffer.getvalue())

    def test_daily_cli_csv_export_defaults_to_wide_layout(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
                    with redirect_stdout(buffer):
                        exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'tas_mean', '--element', 'tas_max', '--start-date', '1865-06-01', '--end-date', '1865-06-03', '--format', 'csv', '--output', 'daily.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/daily.csv')
                self.assertTrue(output_path.exists())
                content = output_path.read_text(encoding='utf-8')
                self.assertIn('observation_date', content)
                self.assertIn('tas_mean', content)
                self.assertIn('tas_max', content)
                self.assertNotIn('element_raw', content)
                self.assertIn('Exported daily observations to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)

    def test_daily_cli_csv_export_keeps_explicit_relative_path(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                output_path = Path('reports/daily.csv')
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
                    exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'tas_max', '--start-date', '1865-06-01', '--end-date', '1865-06-03', '--format', 'csv', '--output', str(output_path)])
                self.assertEqual(exit_code, 0)
                self.assertTrue(output_path.exists())
            finally:
                os.chdir(original_cwd)


class StationAvailabilityCliTests(unittest.TestCase):
    def _sample_availability_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'dataset_scope': 'historical_csv',
                'resolution': '10min',
                'implemented': True,
                'supported_elements': ['tas_mean'],
            }
        ])

    def _sample_paths_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'dataset_scope': 'historical_csv',
                'resolution': '10min',
                'implemented': True,
            }
        ])

    def _sample_de_metadata_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': None,
                'full_name': 'Grossenbrode',
                'latitude': 54.36,
                'longitude': 11.09,
                'elevation_m': 10.0,
                'begin_date': '1980-01-01T00:00Z',
                'end_date': '2025-12-31T00:00Z',
            }
        ])

    def _sample_de_paths_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
                'implemented': True,
            }
        ])

    def test_station_metadata_cli_uses_default_country_cz(self) -> None:
        with patch('weatherdownload.cli.read_station_metadata', return_value=self._sample_de_metadata_table()) as read_mock:
            exit_code = main(['stations', 'metadata'])
        self.assertEqual(exit_code, 0)
        self.assertEqual(read_mock.call_args.kwargs['country'], 'CZ')

    def test_station_metadata_cli_explicit_country_de(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=self._sample_de_metadata_table()) as read_mock:
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'metadata', '--country', 'DE'])
        self.assertEqual(exit_code, 0)
        self.assertEqual(read_mock.call_args.kwargs['country'], 'DE')
        self.assertIn('00044', buffer.getvalue())

    def test_station_availability_cli_screen_output(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
            with patch('weatherdownload.cli.list_station_paths', return_value=self._sample_paths_table()):
                with redirect_stdout(buffer):
                    exit_code = main(['stations', 'availability', '--station-id', '0-20000-0-11406'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('historical_csv', output)
        self.assertIn('10min', output)

    def test_station_availability_cli_explicit_country_de(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=self._sample_de_metadata_table()):
            with patch('weatherdownload.cli.list_station_paths', return_value=self._sample_de_paths_table()) as paths_mock:
                with redirect_stdout(buffer):
                    exit_code = main(['stations', 'availability', '--country', 'DE', '--station-id', '00044'])
        self.assertEqual(exit_code, 0)
        self.assertEqual(paths_mock.call_args.kwargs['country'], 'DE')
        self.assertIn('historical', buffer.getvalue())
        self.assertIn('00044', buffer.getvalue())

    def test_station_availability_cli_csv_export(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
                    with patch('weatherdownload.cli.station_availability', return_value=self._sample_availability_table()):
                        with redirect_stdout(buffer):
                            exit_code = main(['stations', 'availability', '--station-id', '0-20000-0-11406', '--format', 'csv', '--output', 'availability.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/availability.csv')
                self.assertTrue(output_path.exists())
                self.assertIn('historical_csv', output_path.read_text(encoding='utf-8'))
                self.assertIn('Exported station availability to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)

    def test_station_supports_cli_screen_output(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
            with patch('weatherdownload.cli.station_supports', return_value=True):
                with redirect_stdout(buffer):
                    exit_code = main(['stations', 'supports', '--station-id', '0-20000-0-11406', '--dataset-scope', 'historical_csv', '--resolution', '10min'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('0-20000-0-11406', output)
        self.assertIn('True', output)

    def test_station_elements_cli_screen_output(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
            with patch('weatherdownload.cli.list_station_elements', return_value=['tas_mean', 'open_water_evaporation']):
                with redirect_stdout(buffer):
                    exit_code = main(['stations', 'elements', '--station-id', '0-20000-0-11406', '--dataset-scope', 'historical_csv', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('tas_mean', output)
        self.assertIn('open_water_evaporation', output)

    def test_station_elements_cli_explicit_country_us(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
            with patch('weatherdownload.cli.list_station_elements', return_value=['open_water_evaporation']) as elements_mock:
                with redirect_stdout(buffer):
                    exit_code = main(['stations', 'elements', '--country', 'US', '--station-id', 'USC00000001', '--dataset-scope', 'ghcnd', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        self.assertEqual(elements_mock.call_args.kwargs['country'], 'US')
        self.assertIn('open_water_evaporation', buffer.getvalue())

    def test_station_elements_cli_csv_export(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.read_station_metadata', return_value=pd.DataFrame()):
                    with patch('weatherdownload.cli.list_station_elements', return_value=['tas_mean']):
                        with redirect_stdout(buffer):
                            exit_code = main(['stations', 'elements', '--station-id', '0-20000-0-11406', '--dataset-scope', 'historical_csv', '--resolution', '10min', '--format', 'csv', '--output', 'elements.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/elements.csv')
                self.assertTrue(output_path.exists())
                content = output_path.read_text(encoding='utf-8')
                self.assertIn('tas_mean', content)
                self.assertIn('Exported station elements to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)


if __name__ == '__main__':
    unittest.main()







