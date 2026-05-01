import unittest
from pathlib import Path

import pandas as pd

from weatherdownload.providers.fi.fmi_parser import (
    FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS,
    normalize_fmi_timevaluepair_hourly_observations,
)


SAMPLE_FIXTURE_PATH = Path('tests/data/sample_fmi_timevaluepair.xml')


class FmiParserTests(unittest.TestCase):
    def test_fixture_parses_to_normalized_schema(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(list(frame.columns), FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        self.assertFalse(frame.empty)
        self.assertTrue(frame['provider'].eq('fmi').all())
        self.assertTrue(frame['resolution'].eq('1hour').all())

    def test_extracts_station_id(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(frame.attrs.get('station_id_candidates'), ['100971'])

    def test_maps_raw_elements_and_preserves_element_raw(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        # One t2m value is missing and is dropped by the normalizer.
        self.assertEqual(frame['element_raw'].tolist(), ['t2m', 'ws_10min', 'ws_10min'])
        self.assertEqual(frame['element'].tolist(), ['tas_mean', 'wind_speed', 'wind_speed'])

    def test_parses_timestamps_as_utc(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        timestamps = frame['timestamp'].tolist()
        self.assertEqual(
            [ts.isoformat() for ts in timestamps],
            [
                '2026-04-30T00:00:00+00:00',
                '2026-04-30T00:00:00+00:00',
                '2026-04-30T01:00:00+00:00',
            ],
        )

    def test_preserves_units_when_present(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame.attrs.get('units_by_element_raw'), {'t2m': 'degC', 'ws_10min': 'm/s'})

    def test_missing_values_are_dropped(self) -> None:
        frame = normalize_fmi_timevaluepair_hourly_observations(SAMPLE_FIXTURE_PATH.read_text(encoding='utf-8'))
        self.assertEqual(frame['element_raw'].eq('t2m').sum(), 1)
        self.assertFalse(pd.isna(frame['value']).any())


if __name__ == '__main__':
    unittest.main()

