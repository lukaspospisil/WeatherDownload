import unittest
from pathlib import Path

import pandas as pd

from weatherdownload.providers.fi.fmi_parser import (
    FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS,
    normalize_fmi_timevaluepair_hourly_observations,
)


SAMPLE_SYNTHETIC_FIXTURE_PATH = Path('tests/data/sample_fmi_timevaluepair.xml')
SAMPLE_REAL_FIXTURE_PATH = Path('tests/data/sample_fmi_timevaluepair_real.xml')
SAMPLE_REAL_FMISID_FIXTURE_PATH = Path('tests/data/sample_fmi_timevaluepair_real_fmisid.xml')


class FmiParserTests(unittest.TestCase):
    def test_synthetic_fixture_parses_to_normalized_schema(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(list(frame.columns), FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        self.assertFalse(frame.empty)
        self.assertTrue(frame['provider'].eq('fmi').all())
        self.assertTrue(frame['resolution'].eq('1hour').all())

    def test_synthetic_extracts_station_id(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(frame.attrs.get('station_id_candidates'), ['100971'])

    def test_synthetic_maps_raw_elements_and_preserves_element_raw(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        # One t2m value is missing and is dropped by the normalizer.
        # Normalizer sorts by station_id/timestamp/element, so row order is not "by parameter".
        self.assertEqual(sorted(frame['element_raw'].tolist()), sorted(['t2m', 'ws_10min', 'ws_10min', 'rh', 'p_sea', 'r_1h']))
        self.assertEqual(
            sorted(frame['element'].tolist()),
            sorted(['tas_mean', 'wind_speed', 'wind_speed', 'relative_humidity', 'pressure', 'precipitation']),
        )

    def test_synthetic_parses_timestamps_as_utc(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        timestamps = sorted(frame['timestamp'].unique().tolist())
        self.assertEqual(
            [ts.isoformat() for ts in timestamps],
            [
                '2026-04-30T00:00:00+00:00',
                '2026-04-30T01:00:00+00:00',
            ],
        )

    def test_synthetic_preserves_units_when_present(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(
            frame.attrs.get('units_by_element_raw'),
            {'t2m': 'degC', 'ws_10min': 'm/s', 'rh': '%', 'p_sea': 'hPa', 'r_1h': 'mm'},
        )

    def test_synthetic_missing_values_are_dropped(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_SYNTHETIC_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame['element_raw'].eq('t2m').sum(), 1)
        self.assertFalse(pd.isna(frame['value']).any())

    def test_real_fixture_parses_observed_property_param_encoding(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_REAL_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(sorted(frame['element_raw'].unique().tolist()), sorted(['t2m', 'ws_10min', 'rh', 'p_sea', 'r_1h']))
        self.assertEqual(
            sorted(frame['element'].unique().tolist()),
            sorted(['tas_mean', 'wind_speed', 'relative_humidity', 'pressure', 'precipitation']),
        )

    def test_real_fmisid_request_fixture_matches(self) -> None:
        frame_place = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_REAL_FIXTURE_PATH.read_text(encoding='utf-8'))
        frame_id = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_REAL_FMISID_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame_place['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(frame_id['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(frame_place[['element_raw', 'timestamp', 'value']].to_dict('records'), frame_id[['element_raw', 'timestamp', 'value']].to_dict('records'))

    def test_real_fixture_units_may_be_absent(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_REAL_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame.attrs.get('units_by_element_raw'), {})


if __name__ == '__main__':
    unittest.main()
