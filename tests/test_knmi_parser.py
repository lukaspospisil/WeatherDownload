import unittest
import warnings

import pandas as pd

from weatherdownload.knmi_parser import (
    normalize_knmi_metadata_datetime,
    normalize_knmi_observation_metadata,
    parse_knmi_api_listing_json,
    parse_knmi_daily_netcdf_bytes,
    parse_knmi_hourly_netcdf_bytes,
    parse_knmi_station_metadata_csv,
    parse_knmi_tenmin_netcdf_bytes,
)
from weatherdownload.knmi_registry import KNMI_PARAMETER_METADATA, get_dataset_spec


def _import_netcdf4_for_knmi_tests() -> tuple[object, object]:
    # netCDF4 currently emits this import-time RuntimeWarning under pytest warning
    # capture in otherwise clean environments; keep the filter local to the exact
    # upstream warning so unrelated warnings still fail loudly in tests.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message=r'numpy\.ndarray size changed, may indicate binary incompatibility\. Expected 16 from C header, got 96 from PyObject',
            category=RuntimeWarning,
        )
        import netCDF4
        import numpy as np

    return netCDF4, np


class KnmiParserTests(unittest.TestCase):
    def test_parse_knmi_api_listing_json_rejects_invalid_json(self) -> None:
        with self.assertRaisesRegex(ValueError, r'KNMI Open Data API response is not valid JSON\.'):
            parse_knmi_api_listing_json('{not-json}')

    def test_parse_knmi_api_listing_json_rejects_missing_files(self) -> None:
        with self.assertRaisesRegex(ValueError, r'KNMI Open Data API response is missing a files list\.'):
            parse_knmi_api_listing_json('{"nextPageToken":"abc"}')

    def test_parse_knmi_station_metadata_csv_normalizes_core_columns(self) -> None:
        csv_text = (
            'WSI,NAME,LAT,LON,ELEVATION,VALID_FROM,VALID_TO\n'
            '0-20000-0-06310,Vlissingen,51.442,3.596,8,1951-01-01T00:00:00Z,9999-12-31T00:00:00Z\n'
            '0-20000-0-06260,De Bilt,52.100,5.180,4,1901-01-01T00:00:00Z,9999-12-31T00:00:00Z\n'
        )
        stations = parse_knmi_station_metadata_csv(csv_text)
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06260', '0-20000-0-06310'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(stations.iloc[0]['begin_date'], '1901-01-01T00:00Z')

    def test_normalize_knmi_daily_observation_metadata_builds_rows_for_supported_elements(self) -> None:
        stations = self._sample_stations()
        metadata = normalize_knmi_observation_metadata(stations, get_dataset_spec('historical', 'daily'), KNMI_PARAMETER_METADATA)
        self.assertEqual(metadata.iloc[0]['station_id'], '0-20000-0-06260')
        self.assertIn('TG', metadata['element'].tolist())
        self.assertIn('Daily mean air temperature', metadata.iloc[0]['name'])
        self.assertTrue(metadata['obs_type'].eq('HISTORICAL_DAILY').all())
        self.assertTrue(metadata['schedule'].eq('P1D KNMI Open Data API').all())

    def test_normalize_knmi_hourly_observation_metadata_builds_rows_for_supported_elements(self) -> None:
        stations = self._sample_stations()
        metadata = normalize_knmi_observation_metadata(stations, get_dataset_spec('historical', '1hour'), KNMI_PARAMETER_METADATA)
        self.assertIn('T', metadata['element'].tolist())
        self.assertIn('Hourly air temperature', metadata.iloc[0]['name'])
        self.assertTrue(metadata['obs_type'].eq('HISTORICAL_HOURLY').all())
        self.assertTrue(metadata['schedule'].eq('PT1H KNMI Open Data API').all())

    def test_normalize_knmi_tenmin_observation_metadata_builds_rows_for_supported_elements(self) -> None:
        stations = self._sample_stations()
        metadata = normalize_knmi_observation_metadata(stations, get_dataset_spec('historical', '10min'), KNMI_PARAMETER_METADATA)
        self.assertIn('ta', metadata['element'].tolist())
        self.assertIn('10-minute air temperature', metadata.iloc[0]['name'])
        self.assertTrue(metadata['obs_type'].eq('HISTORICAL_10MIN').all())
        self.assertTrue(metadata['schedule'].eq('PT10M KNMI Open Data API').all())

    def test_normalize_knmi_metadata_datetime_converts_to_project_format(self) -> None:
        self.assertEqual(normalize_knmi_metadata_datetime('2024-01-01T00:00:00+00:00'), '2024-01-01T00:00Z')

    def test_parse_knmi_daily_netcdf_bytes_reads_documented_daily_shape(self) -> None:
        try:
            netCDF4, np = _import_netcdf4_for_knmi_tests()
        except ImportError as exc:
            self.skipTest(f'netCDF4 not installed: {exc}')

        payload = self._build_netcdf_payload(
            netCDF4,
            np,
            time_units='hours since 2024-01-03 00:00:00',
            variables={'TG': [3.4, 5.6], 'RH': [1.2, 0.0]},
            parser=parse_knmi_daily_netcdf_bytes,
        )

        self.assertEqual(str(payload['observation_date']), '2024-01-02')
        stations = payload['stations']
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06260', '0-20000-0-06310'])
        self.assertAlmostEqual(float(payload['variables']['TG'].iloc[0]), 3.4, places=6)
        self.assertAlmostEqual(float(payload['variables']['RH'].iloc[1]), 0.0, places=6)

    def test_parse_knmi_hourly_netcdf_bytes_reads_documented_hourly_shape(self) -> None:
        try:
            netCDF4, np = _import_netcdf4_for_knmi_tests()
        except ImportError as exc:
            self.skipTest(f'netCDF4 not installed: {exc}')

        payload = self._build_netcdf_payload(
            netCDF4,
            np,
            time_units='hours since 2024-01-01 01:00:00',
            variables={'T': [3.1, 4.2], 'P': [1012.1, 1011.2]},
            parser=parse_knmi_hourly_netcdf_bytes,
        )

        self.assertEqual(payload['timestamp'].isoformat(), '2024-01-01T01:00:00+00:00')
        stations = payload['stations']
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06260', '0-20000-0-06310'])
        self.assertAlmostEqual(float(payload['variables']['T'].iloc[0]), 3.1, places=6)
        self.assertAlmostEqual(float(payload['variables']['P'].iloc[1]), 1011.2, places=4)

    def test_parse_knmi_tenmin_netcdf_bytes_reads_documented_tenmin_shape(self) -> None:
        try:
            netCDF4, np = _import_netcdf4_for_knmi_tests()
        except ImportError as exc:
            self.skipTest(f'netCDF4 not installed: {exc}')

        payload = self._build_netcdf_payload(
            netCDF4,
            np,
            time_units='minutes since 2024-01-01 09:00:00',
            variables={'ta': [3.1, 4.2], 'pp': [1012.1, 1011.2], 'ss': [0.0, 1.0]},
            parser=parse_knmi_tenmin_netcdf_bytes,
        )

        self.assertEqual(payload['timestamp'].isoformat(), '2024-01-01T09:00:00+00:00')
        stations = payload['stations']
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06260', '0-20000-0-06310'])
        self.assertAlmostEqual(float(payload['variables']['ta'].iloc[0]), 3.1, places=6)
        self.assertAlmostEqual(float(payload['variables']['pp'].iloc[1]), 1011.2, places=4)

    @staticmethod
    def _sample_stations() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-06260',
                'gh_id': pd.NA,
                'begin_date': '1901-01-01T00:00Z',
                'end_date': '9999-12-31T00:00Z',
                'full_name': 'De Bilt',
                'longitude': 5.18,
                'latitude': 52.1,
                'elevation_m': 4.0,
            }
        ])

    @staticmethod
    def _build_netcdf_payload(netcdf4: object, np: object, *, time_units: str, variables: dict[str, list[float]], parser):
        import tempfile
        from pathlib import Path

        def _char_matrix(values: list[str], width: int) -> object:
            matrix = np.full((len(values), width), b' ', dtype='S1')
            for row_index, value in enumerate(values):
                encoded = value.encode('utf-8')[:width]
                for column_index, byte in enumerate(encoded):
                    matrix[row_index, column_index] = bytes([byte])
            return matrix

        dataset = None
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as handle:
            temp_path = Path(handle.name)
        try:
            dataset = netcdf4.Dataset(temp_path, mode='w')
            dataset.createDimension('time', 1)
            dataset.createDimension('station', 2)
            dataset.createDimension('strlen', 16)
            time = dataset.createVariable('time', 'i4', ('time',))
            time.units = time_units
            time[:] = [0]
            station = dataset.createVariable('station', 'S1', ('station', 'strlen'))
            station[:] = _char_matrix(['0-20000-0-06260', '0-20000-0-06310'], 16)
            station_name = dataset.createVariable('station_name', 'S1', ('station', 'strlen'))
            station_name[:] = _char_matrix(['De Bilt', 'Vlissingen'], 16)
            latitude = dataset.createVariable('lat', 'f4', ('station',))
            latitude[:] = [52.1, 51.442]
            longitude = dataset.createVariable('lon', 'f4', ('station',))
            longitude[:] = [5.18, 3.596]
            height = dataset.createVariable('height', 'f4', ('station',))
            height[:] = [4.0, 8.0]
            for variable_name, values in variables.items():
                variable = dataset.createVariable(variable_name, 'f4', ('time', 'station'))
                variable[:] = [values]
            dataset.close()
            dataset = None

            return parser(temp_path.read_bytes())
        finally:
            if dataset is not None:
                dataset.close()
            temp_path.unlink(missing_ok=True)


if __name__ == '__main__':
    unittest.main()
