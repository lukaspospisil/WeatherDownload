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
from weatherdownload.providers.at.daily import normalize_daily_observations_geosphere
from weatherdownload.providers.at.parser import parse_geosphere_daily_csv
from weatherdownload.queries import ObservationQuery


MODULE_PATH = Path('examples/workflows/download_fao.py')
GEOSPHERE_SAMPLE_CSV_PATH = Path('tests/data/sample_geosphere_klima_v2_1d.csv')
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

    def test_build_parser_defaults_fill_missing_to_none(self) -> None:
        parser = download_fao.build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.fill_missing, 'none')

    def test_build_parser_accepts_explicit_fill_missing_policy(self) -> None:
        parser = download_fao.build_parser()
        args = parser.parse_args(['--fill-missing', 'allow-derived'])
        self.assertEqual(args.fill_missing, 'allow-derived')
    def test_default_mat_output_path_is_country_aware(self) -> None:
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='CZ'),
            Path('outputs/fao_daily.cz.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='DE'),
            Path('outputs/fao_daily.de.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='AT'),
            Path('outputs/fao_daily.at.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='CH'),
            Path('outputs/fao_daily.ch.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='DK'),
            Path('outputs/fao_daily.dk.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='HU'),
            Path('outputs/fao_daily.hu.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='PL'),
            Path('outputs/fao_daily.pl.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='NL'),
            Path('outputs/fao_daily.nl.mat'),
        )
        self.assertEqual(
            download_fao.resolve_mat_output_path(None, country='SE'),
            Path('outputs/fao_daily.se.mat'),
        )

    def test_default_parquet_output_dir_is_country_aware(self) -> None:
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='CZ'),
            Path('outputs/fao_daily.cz'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='DE'),
            Path('outputs/fao_daily.de'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='AT'),
            Path('outputs/fao_daily.at'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='CH'),
            Path('outputs/fao_daily.ch'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='DK'),
            Path('outputs/fao_daily.dk'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='HU'),
            Path('outputs/fao_daily.hu'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='PL'),
            Path('outputs/fao_daily.pl'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='NL'),
            Path('outputs/fao_daily.nl'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(None, country='SE'),
            Path('outputs/fao_daily.se'),
        )

    def test_explicit_output_paths_override_country_defaults(self) -> None:
        self.assertEqual(
            download_fao.resolve_mat_output_path(Path('custom.mat'), country='CZ'),
            Path('custom.mat'),
        )
        self.assertEqual(
            download_fao.resolve_parquet_output_dir(Path('custom_bundle'), country='DE'),
            Path('custom_bundle'),
        )

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

    def test_get_fao_country_config_returns_ch_mapping(self) -> None:
        config = download_fao.get_fao_country_config('CH')
        self.assertEqual(config.country, 'CH')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, download_fao.FAO_CANONICAL_ELEMENTS)
        self.assertEqual(config.canonical_to_raw['tas_mean'], ('tre200d0',))
        self.assertEqual(config.canonical_to_raw['vapour_pressure'], ('pva200d0',))
        self.assertEqual(config.raw_to_canonical['PVA200D0'], 'vapour_pressure')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'observed')

    def test_get_fao_country_config_returns_pl_mapping(self) -> None:
        config = download_fao.get_fao_country_config('PL')
        self.assertEqual(config.country, 'PL')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['STD'], 'tas_mean')
        self.assertEqual(config.raw_to_canonical['USL'], 'sunshine_duration')
        self.assertEqual(config.provider_element_mapping['wind_speed']['status'], 'unavailable')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')

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
        self.assertEqual(complete['date'].tolist(), [pd.Timestamp('2024-01-01').date()])
        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])

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
        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(float(complete.loc[0, 'vapour_pressure']), 7.0)
        self.assertEqual(float(complete.loc[0, 'sunshine_duration']), 0.5)

    def test_export_parquet_bundle_writes_portable_bundle_files(self) -> None:
        data_info = {
            'created_at': '2026-03-22T10:00:00+00:00',
            'dataset_type': 'test bundle',
            'source': 'test',
            'elements': ['tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'],
            'provider_element_mapping': {
                'tas_mean': {'raw_codes': ['T'], 'selection_rule': 'AVG'},
            },
            'min_complete_days': 3650,
            'num_stations': 1,
        }
        stations = [
            {
                'station_id': '0-20000-0-11406',
                'full_name': 'TEST STATION',
                'latitude': 50.1,
                'longitude': 14.4,
                'elevation_m': 250.0,
                'num_complete_days': 2,
                'first_complete_date': '2024-01-01',
                'last_complete_date': '2024-01-02',
            }
        ]
        series = [
            {
                'station_id': '0-20000-0-11406',
                'full_name': 'TEST STATION',
                'latitude': 50.1,
                'longitude': 14.4,
                'elevation_m': 250.0,
                'date': ['2024-01-01', '2024-01-02'],
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

            self.assertEqual(written_info['num_stations'], 1)
            self.assertEqual(written_info['provider_element_mapping']['tas_mean']['raw_codes'], ['T'])
            self.assertEqual(list(written_stations['station_id']), ['0-20000-0-11406'])
            self.assertEqual(list(written_stations['num_complete_days']), [2])
            self.assertEqual(list(written_series['date']), ['2024-01-01', '2024-01-02'])
            self.assertEqual(list(written_series['station_id']), ['0-20000-0-11406', '0-20000-0-11406'])
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
                'date': pd.Timestamp('2024-01-01').date(),
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

        self.assertIn('station_id', series)
        self.assertIn('full_name', series)
        self.assertIn('elevation_m', series)
        self.assertIn('tas_mean', series)
        self.assertIn('sunshine_duration', series)
        self.assertNotIn('WSI', series)
        self.assertNotIn('FULL_NAME', series)
        self.assertNotIn('Elevation', series)
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
    def test_get_fao_country_config_returns_nl_mapping(self) -> None:
        config = download_fao.get_fao_country_config('NL')
        self.assertEqual(config.country, 'NL')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['TG'], 'tas_mean')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')

    def test_get_fao_country_config_allow_derived_adds_relative_humidity_helper_for_nl(self) -> None:
        config = download_fao.get_fao_country_config('NL', fill_missing='allow-derived')
        self.assertEqual(
            config.query_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration', 'relative_humidity'),
        )
    def test_get_fao_country_config_returns_at_mapping(self) -> None:
        config = download_fao.get_fao_country_config('AT')
        self.assertEqual(config.country, 'AT')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['VV_MITTEL'], 'wind_speed')

    def test_get_fao_country_config_returns_be_mapping(self) -> None:
        config = download_fao.get_fao_country_config('BE')
        self.assertEqual(config.country, 'BE')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['TEMP_AVG'], 'tas_mean')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')

    def test_get_fao_country_config_returns_dk_mapping(self) -> None:
        config = download_fao.get_fao_country_config('DK')
        self.assertEqual(config.country, 'DK')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['MEAN_TEMP'], 'tas_mean')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')

    def test_get_fao_country_config_returns_hu_mapping(self) -> None:
        config = download_fao.get_fao_country_config('HU')
        self.assertEqual(config.country, 'HU')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'))
        self.assertEqual(config.raw_to_canonical['T'], 'tas_mean')
        self.assertEqual(config.raw_to_canonical['F'], 'sunshine_duration')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')

    def test_get_fao_country_config_allow_derived_adds_relative_humidity_helper_for_hu(self) -> None:
        config = download_fao.get_fao_country_config('HU', fill_missing='allow-derived')
        self.assertEqual(
            config.query_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration', 'relative_humidity'),
        )

    def test_prepare_complete_station_series_handles_at_without_deriving_vapour_pressure(self) -> None:
        csv_text = GEOSPHERE_SAMPLE_CSV_PATH.read_text(encoding='utf-8')
        query = ObservationQuery(
            country='AT',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['1'],
            start_date='2024-01-01',
            end_date='2024-01-03',
            elements=['tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'sunshine_duration'],
        )
        parsed = parse_geosphere_daily_csv(csv_text)
        normalized = normalize_daily_observations_geosphere(parsed, query)

        complete = download_fao.prepare_complete_station_series(normalized, config=download_fao.get_fao_country_config('AT'))

        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01', '2024-01-03'])
        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertTrue(complete['vapour_pressure'].isna().all())
    def test_prepare_complete_station_series_handles_nl_without_deriving_vapour_pressure(self) -> None:
        config = download_fao.get_fao_country_config('NL')
        daily_table = pd.DataFrame([
            {'station_id': '0-20000-0-06260', 'element': 'tas_mean', 'element_raw': 'TG', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.0'},
            {'station_id': '0-20000-0-06260', 'element': 'tas_max', 'element_raw': 'TX', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '6.0'},
            {'station_id': '0-20000-0-06260', 'element': 'tas_min', 'element_raw': 'TN', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.0'},
            {'station_id': '0-20000-0-06260', 'element': 'wind_speed', 'element_raw': 'FG', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '2.5'},
            {'station_id': '0-20000-0-06260', 'element': 'sunshine_duration', 'element_raw': 'SQ', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
            {'station_id': '0-20000-0-06260', 'element': 'relative_humidity', 'element_raw': 'RH', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '81.0'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertTrue(complete['vapour_pressure'].isna().all())

    def test_prepare_complete_station_series_can_derive_vapour_pressure_when_enabled(self) -> None:
        config = download_fao.get_fao_country_config('NL', fill_missing='allow-derived')
        daily_table = pd.DataFrame([
            {'station_id': '0-20000-0-06260', 'element': 'tas_mean', 'element_raw': 'TG', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '10.0'},
            {'station_id': '0-20000-0-06260', 'element': 'tas_max', 'element_raw': 'TX', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '15.0'},
            {'station_id': '0-20000-0-06260', 'element': 'tas_min', 'element_raw': 'TN', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': '0-20000-0-06260', 'element': 'wind_speed', 'element_raw': 'FG', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '2.5'},
            {'station_id': '0-20000-0-06260', 'element': 'sunshine_duration', 'element_raw': 'SQ', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
            {'station_id': '0-20000-0-06260', 'element': 'relative_humidity', 'element_raw': 'RH', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '80.0'},
        ])

        complete, provenance, applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='allow-derived',
        )

        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertGreater(float(complete.loc[0, 'vapour_pressure']), 0.0)
        self.assertEqual(provenance.loc[0, 'vapour_pressure'], 'derived')
        self.assertEqual(
            applied_rules['vapour_pressure'],
            download_fao.DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION,
        )
    def test_prepare_complete_station_series_handles_be_without_deriving_vapour_pressure(self) -> None:
        config = download_fao.get_fao_country_config('BE')
        daily_table = pd.DataFrame([
            {'station_id': '6414', 'element': 'tas_mean', 'element_raw': 'temp_avg', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '4.2'},
            {'station_id': '6414', 'element': 'tas_max', 'element_raw': 'temp_max', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.5'},
            {'station_id': '6414', 'element': 'tas_min', 'element_raw': 'temp_min', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.0'},
            {'station_id': '6414', 'element': 'wind_speed', 'element_raw': 'wind_speed_10m', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.47'},
            {'station_id': '6414', 'element': 'sunshine_duration', 'element_raw': 'sun_duration', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '380.92'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertTrue(complete['vapour_pressure'].isna().all())

    def test_prepare_complete_station_series_handles_dk_without_deriving_vapour_pressure(self) -> None:
        config = download_fao.get_fao_country_config('DK')
        daily_table = pd.DataFrame([
            {'station_id': '06180', 'element': 'tas_mean', 'element_raw': 'mean_temp', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.5'},
            {'station_id': '06180', 'element': 'tas_max', 'element_raw': 'mean_daily_max_temp', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '6.0'},
            {'station_id': '06180', 'element': 'tas_min', 'element_raw': 'mean_daily_min_temp', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.2'},
            {'station_id': '06180', 'element': 'wind_speed', 'element_raw': 'mean_wind_speed', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '4.1'},
            {'station_id': '06180', 'element': 'sunshine_duration', 'element_raw': 'bright_sunshine', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.3'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertTrue(complete['vapour_pressure'].isna().all())

    def test_prepare_complete_station_series_handles_hu_without_deriving_vapour_pressure(self) -> None:
        config = download_fao.get_fao_country_config('HU')
        daily_table = pd.DataFrame([
            {'station_id': '13704', 'element': 'tas_mean', 'element_raw': 't', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.5'},
            {'station_id': '13704', 'element': 'tas_max', 'element_raw': 'tx', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '6.0'},
            {'station_id': '13704', 'element': 'tas_min', 'element_raw': 'tn', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.2'},
            {'station_id': '13704', 'element': 'wind_speed', 'element_raw': 'fs', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '4.1'},
            {'station_id': '13704', 'element': 'sunshine_duration', 'element_raw': 'f', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.3'},
            {'station_id': '13704', 'element': 'relative_humidity', 'element_raw': 'u', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '78.0'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertTrue(complete['vapour_pressure'].isna().all())

    def test_prepare_complete_station_series_can_derive_vapour_pressure_for_hu_when_enabled(self) -> None:
        config = download_fao.get_fao_country_config('HU', fill_missing='allow-derived')
        daily_table = pd.DataFrame([
            {'station_id': '13704', 'element': 'tas_mean', 'element_raw': 't', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '10.0'},
            {'station_id': '13704', 'element': 'tas_max', 'element_raw': 'tx', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '15.0'},
            {'station_id': '13704', 'element': 'tas_min', 'element_raw': 'tn', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': '13704', 'element': 'wind_speed', 'element_raw': 'fs', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '2.5'},
            {'station_id': '13704', 'element': 'sunshine_duration', 'element_raw': 'f', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
            {'station_id': '13704', 'element': 'relative_humidity', 'element_raw': 'u', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '80.0'},
        ])

        complete, provenance, applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='allow-derived',
        )

        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertGreater(float(complete.loc[0, 'vapour_pressure']), 0.0)
        self.assertEqual(provenance.loc[0, 'vapour_pressure'], 'derived')
        self.assertEqual(
            applied_rules['vapour_pressure'],
            download_fao.DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION,
        )

    def test_prepare_complete_station_series_handles_pl_without_deriving_missing_fields(self) -> None:
        config = download_fao.get_fao_country_config('PL')
        daily_table = pd.DataFrame([
            {'station_id': '00375', 'element': 'tas_mean', 'element_raw': 'STD', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '3.5'},
            {'station_id': '00375', 'element': 'tas_max', 'element_raw': 'TMAX', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '6.0'},
            {'station_id': '00375', 'element': 'tas_min', 'element_raw': 'TMIN', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '1.2'},
            {'station_id': '00375', 'element': 'sunshine_duration', 'element_raw': 'USL', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '1.3'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2025-01-01'])
        self.assertEqual(float(complete.loc[0, 'sunshine_duration']), 1.3)
        self.assertTrue(complete['wind_speed'].isna().all())
        self.assertTrue(complete['vapour_pressure'].isna().all())

    def test_prepare_complete_station_series_pl_allow_derived_keeps_vapour_pressure_missing_without_helper_inputs(self) -> None:
        config = download_fao.get_fao_country_config('PL', fill_missing='allow-derived')
        daily_table = pd.DataFrame([
            {'station_id': '00375', 'element': 'tas_mean', 'element_raw': 'STD', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '10.0'},
            {'station_id': '00375', 'element': 'tas_max', 'element_raw': 'TMAX', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '15.0'},
            {'station_id': '00375', 'element': 'tas_min', 'element_raw': 'TMIN', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': '00375', 'element': 'sunshine_duration', 'element_raw': 'USL', 'observation_date': '2025-01-01', 'time_function': pd.NA, 'value': '0.5'},
        ])

        complete, provenance, applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='allow-derived',
        )

        self.assertEqual(list(complete['date'].astype(str)), ['2025-01-01'])
        self.assertTrue(complete['vapour_pressure'].isna().all())
        self.assertEqual(provenance.loc[0, 'vapour_pressure'], 'missing')
        self.assertIsNone(applied_rules['vapour_pressure'])

    def test_prepare_complete_station_series_handles_ch_with_observed_vapour_pressure(self) -> None:
        config = download_fao.get_fao_country_config('CH')
        daily_table = pd.DataFrame([
            {'station_id': 'AIG', 'element': 'tas_mean', 'element_raw': 'tre200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '3.5'},
            {'station_id': 'AIG', 'element': 'tas_max', 'element_raw': 'tre200dx', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '6.0'},
            {'station_id': 'AIG', 'element': 'tas_min', 'element_raw': 'tre200dn', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.2'},
            {'station_id': 'AIG', 'element': 'wind_speed', 'element_raw': 'fkl010d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '4.1'},
            {'station_id': 'AIG', 'element': 'vapour_pressure', 'element_raw': 'pva200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.4'},
            {'station_id': 'AIG', 'element': 'sunshine_duration', 'element_raw': 'sre000d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '1.3'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['2024-01-01'])
        self.assertEqual(float(complete.loc[0, 'vapour_pressure']), 7.4)

    def test_prepare_complete_station_series_keeps_ch_observed_vapour_pressure_in_allow_derived_mode(self) -> None:
        config = download_fao.get_fao_country_config('CH', fill_missing='allow-derived')
        daily_table = pd.DataFrame([
            {'station_id': 'AIG', 'element': 'tas_mean', 'element_raw': 'tre200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '10.0'},
            {'station_id': 'AIG', 'element': 'tas_max', 'element_raw': 'tre200dx', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '15.0'},
            {'station_id': 'AIG', 'element': 'tas_min', 'element_raw': 'tre200dn', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': 'AIG', 'element': 'wind_speed', 'element_raw': 'fkl010d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '2.5'},
            {'station_id': 'AIG', 'element': 'vapour_pressure', 'element_raw': 'pva200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '8.0'},
            {'station_id': 'AIG', 'element': 'sunshine_duration', 'element_raw': 'sre000d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
            {'station_id': 'AIG', 'element': 'relative_humidity', 'element_raw': 'ure200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '80.0'},
        ])

        complete, provenance, applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='allow-derived',
        )

        self.assertEqual(float(complete.loc[0, 'vapour_pressure']), 8.0)
        self.assertEqual(provenance.loc[0, 'vapour_pressure'], 'observed')
        self.assertIsNone(applied_rules['vapour_pressure'])

    def test_prepare_complete_station_series_can_derive_vapour_pressure_for_ch_when_observed_value_is_missing(self) -> None:
        config = download_fao.get_fao_country_config('CH', fill_missing='allow-derived')
        daily_table = pd.DataFrame([
            {'station_id': 'AIG', 'element': 'tas_mean', 'element_raw': 'tre200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '10.0'},
            {'station_id': 'AIG', 'element': 'tas_max', 'element_raw': 'tre200dx', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '15.0'},
            {'station_id': 'AIG', 'element': 'tas_min', 'element_raw': 'tre200dn', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '7.0'},
            {'station_id': 'AIG', 'element': 'wind_speed', 'element_raw': 'fkl010d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '2.5'},
            {'station_id': 'AIG', 'element': 'sunshine_duration', 'element_raw': 'sre000d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '0.5'},
            {'station_id': 'AIG', 'element': 'relative_humidity', 'element_raw': 'ure200d0', 'observation_date': '2024-01-01', 'time_function': pd.NA, 'value': '80.0'},
        ])

        complete, provenance, applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='allow-derived',
        )

        self.assertGreater(float(complete.loc[0, 'vapour_pressure']), 0.0)
        self.assertEqual(provenance.loc[0, 'vapour_pressure'], 'derived')
        self.assertEqual(applied_rules['vapour_pressure'], download_fao.DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION)

    def test_build_data_info_includes_nl_limitations(self) -> None:
        config = download_fao.get_fao_country_config('NL')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '0-20000-0-06260'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'NL')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('vapour_pressure_availability', info['assumptions'])

    def test_build_data_info_includes_be_limitations(self) -> None:
        config = download_fao.get_fao_country_config('BE')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '6414'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'BE')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('provider_daily_aggregation', info['assumptions'])

    def test_build_data_info_includes_dk_limitations(self) -> None:
        config = download_fao.get_fao_country_config('DK')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '06180'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'DK')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('denmark_only_scope', info['assumptions'])
        self.assertIn('vapour_pressure_availability', info['assumptions'])

    def test_build_data_info_includes_hu_limitations(self) -> None:
        config = download_fao.get_fao_country_config('HU')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '13704'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'HU')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('vapour_pressure_availability', info['assumptions'])
        self.assertIn('relative_humidity_helper', info['assumptions'])

    def test_build_data_info_includes_pl_limitations(self) -> None:
        config = download_fao.get_fao_country_config('PL')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '00375'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'PL')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['wind_speed']['status'], 'unavailable')
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('station_metadata_limits', info['assumptions'])

    def test_build_data_info_includes_ch_assumptions(self) -> None:
        config = download_fao.get_fao_country_config('CH')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': 'AIG'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'CH')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'observed')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('vapour_pressure_availability', info['assumptions'])
        self.assertIn('fallback_policy', info['assumptions'])

    def test_build_data_info_includes_at_assumptions(self) -> None:
        config = download_fao.get_fao_country_config('AT')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '1'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'AT')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertIn('pressure_usage', info['assumptions'])
    def test_get_fao_country_config_returns_se_mapping(self) -> None:
        config = download_fao.get_fao_country_config('SE')
        self.assertEqual(config.country, 'SE')
        self.assertEqual(config.dataset_scope, 'historical')
        self.assertEqual(config.query_elements, ('tas_mean', 'tas_max', 'tas_min'))
        self.assertEqual(config.raw_to_canonical['2'], 'tas_mean')
        self.assertEqual(config.provider_element_mapping['wind_speed']['status'], 'unavailable')
        self.assertEqual(config.provider_element_mapping['vapour_pressure']['status'], 'unavailable')
        self.assertEqual(config.provider_element_mapping['sunshine_duration']['status'], 'unavailable')

    def test_prepare_complete_station_series_handles_se_without_deriving_missing_fields(self) -> None:
        config = download_fao.get_fao_country_config('SE')
        daily_table = pd.DataFrame([
            {'station_id': '98230', 'element': 'tas_mean', 'element_raw': '2', 'observation_date': '1996-10-01', 'time_function': pd.NA, 'value': '11.1'},
            {'station_id': '98230', 'element': 'tas_max', 'element_raw': '20', 'observation_date': '1996-10-01', 'time_function': pd.NA, 'value': '14.3'},
            {'station_id': '98230', 'element': 'tas_min', 'element_raw': '19', 'observation_date': '1996-10-01', 'time_function': pd.NA, 'value': '8.8'},
        ])

        complete = download_fao.prepare_complete_station_series(daily_table, config=config)

        self.assertEqual(list(complete.columns), ['date', 'tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration'])
        self.assertEqual(list(complete['date'].astype(str)), ['1996-10-01'])
        self.assertTrue(complete['wind_speed'].isna().all())
        self.assertTrue(complete['vapour_pressure'].isna().all())
        self.assertTrue(complete['sunshine_duration'].isna().all())

    def test_build_data_info_includes_se_limitations(self) -> None:
        config = download_fao.get_fao_country_config('SE')
        info = download_fao.build_data_info(config, station_rows=[{'station_id': '98230'}], min_complete_days=3650)

        self.assertEqual(info['country'], 'SE')
        self.assertIn('assumptions', info)
        self.assertEqual(info['provider_element_mapping']['wind_speed']['status'], 'unavailable')
        self.assertEqual(info['provider_element_mapping']['vapour_pressure']['status'], 'unavailable')
        self.assertEqual(info['provider_element_mapping']['sunshine_duration']['status'], 'unavailable')
        self.assertIn('observed_inputs_only', info['assumptions'])
        self.assertIn('corrected_archive_limit', info['assumptions'])
        self.assertIn('wind_speed_availability', info['assumptions'])

if __name__ == '__main__':
    unittest.main()







