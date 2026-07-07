from __future__ import annotations

import unittest

import pandas as pd

from examples.workflows.download_cz_chmi_tusimice_pm_evaporation import (
    TARGET_LOCAL_ID,
    TARGET_WSI,
    build_availability_report,
    build_station_element_metadata,
    prepare_output_tables,
    resolve_tusimice_station,
)


class TusimiceWorkflowTests(unittest.TestCase):
    def test_resolve_tusimice_station_prefers_exact_metadata_ids(self) -> None:
        stations = pd.DataFrame.from_records(
            [
                {
                    "station_id": TARGET_WSI,
                    "gh_id": TARGET_LOCAL_ID,
                    "begin_date": "1961-01-01T00:00+00:00Z",
                    "end_date": "3999-12-31T23:59+00:00Z",
                    "full_name": "Tušimice",
                    "longitude": 13.33,
                    "latitude": 50.38,
                    "elevation_m": 322.0,
                },
                {
                    "station_id": "0-20000-0-99999",
                    "gh_id": "U1KATU99",
                    "begin_date": "1961-01-01T00:00+00:00Z",
                    "end_date": "3999-12-31T23:59+00:00Z",
                    "full_name": "Tušimice Nearby",
                    "longitude": 13.34,
                    "latitude": 50.39,
                    "elevation_m": 323.0,
                },
            ]
        )

        station = resolve_tusimice_station(stations)

        self.assertEqual(station.station_id, TARGET_WSI)
        self.assertEqual(station.gh_id, TARGET_LOCAL_ID)
        self.assertEqual(station.full_name, "Tušimice")

    def test_prepare_output_tables_preserves_evaporation_and_reports_missing(self) -> None:
        observations = pd.DataFrame.from_records(
            [
                {
                    "station_id": TARGET_WSI,
                    "gh_id": TARGET_LOCAL_ID,
                    "element": "tas_mean",
                    "element_raw": "T",
                    "observation_date": pd.Timestamp("2024-01-01").date(),
                    "time_function": "AVG",
                    "value": 1.0,
                    "flag": pd.NA,
                    "quality": pd.NA,
                    "provider": "historical_csv",
                    "resolution": "daily",
                },
                {
                    "station_id": TARGET_WSI,
                    "gh_id": TARGET_LOCAL_ID,
                    "element": "precipitation",
                    "element_raw": "SRA",
                    "observation_date": pd.Timestamp("2024-01-01").date(),
                    "time_function": "00:00",
                    "value": 2.0,
                    "flag": pd.NA,
                    "quality": pd.NA,
                    "provider": "historical_csv",
                    "resolution": "daily",
                },
                {
                    "station_id": TARGET_WSI,
                    "gh_id": TARGET_LOCAL_ID,
                    "element": "open_water_evaporation",
                    "element_raw": "VY",
                    "observation_date": pd.Timestamp("2024-01-01").date(),
                    "time_function": "06:00",
                    "value": 0.4,
                    "flag": pd.NA,
                    "quality": pd.NA,
                    "provider": "historical_csv",
                    "resolution": "daily",
                },
                {
                    "station_id": TARGET_WSI,
                    "gh_id": TARGET_LOCAL_ID,
                    "element": "open_water_evaporation",
                    "element_raw": "VY",
                    "observation_date": pd.Timestamp("2024-01-02").date(),
                    "time_function": "06:00",
                    "value": pd.NA,
                    "flag": pd.NA,
                    "quality": pd.NA,
                    "provider": "historical_csv",
                    "resolution": "daily",
                },
            ]
        )
        station = resolve_tusimice_station(
            pd.DataFrame.from_records(
                [
                    {
                        "station_id": TARGET_WSI,
                        "gh_id": TARGET_LOCAL_ID,
                        "begin_date": "1961-01-01T00:00Z",
                        "end_date": "3999-12-31T23:59Z",
                        "full_name": "Tušimice",
                        "longitude": 13.33,
                        "latitude": 50.38,
                        "elevation_m": 322.0,
                    }
                ]
            )
        )
        station_element_metadata = build_station_element_metadata(
            pd.DataFrame.from_records(
                [
                    {
                        "obs_type": "DLY",
                        "station_id": TARGET_WSI,
                        "begin_date": "1961-01-01T00:00Z",
                        "end_date": "3999-12-31T23:59Z",
                        "element": "VY",
                        "schedule": "06:00",
                        "name": "Vypar",
                        "description": "mm",
                        "height": 0.0,
                    }
                ]
            ),
            station_id=TARGET_WSI,
        )

        _filtered, wide, availability, provenance = prepare_output_tables(
            observations,
            station,
            station_element_metadata,
        )

        self.assertIn("open_water_evaporation", wide.columns)
        open_water = availability.loc[availability["element"].eq("open_water_evaporation")].iloc[0]
        pressure = availability.loc[availability["element"].eq("pressure")].iloc[0]
        self.assertTrue(bool(open_water["downloaded"]))
        self.assertEqual(int(open_water["n_valid_values"]), 1)
        self.assertEqual(float(open_water["missing_percent"]), 50.0)
        self.assertFalse(bool(pressure["downloaded"]))
        self.assertEqual(int(pressure["n_valid_values"]), 0)
        provenance_evap = provenance.loc[provenance["element"].eq("open_water_evaporation")].iloc[0]
        self.assertEqual(provenance_evap["preferred_raw_code"], "VY")
        self.assertEqual(provenance_evap["source_raw_codes_seen"], "VY")
        self.assertEqual(provenance_evap["metadata_schedule"], "06:00")
        self.assertEqual(provenance_evap["metadata_description"], "mm")

    def test_build_availability_report_marks_unavailable_when_column_missing(self) -> None:
        wide = pd.DataFrame.from_records(
            [
                {"station_id": TARGET_WSI, "date": "2024-01-01", "tas_mean": 1.0},
                {"station_id": TARGET_WSI, "date": "2024-01-02", "tas_mean": pd.NA},
            ]
        )

        availability = build_availability_report(wide)
        vapour_pressure = availability.loc[availability["element"].eq("vapour_pressure")].iloc[0]

        self.assertFalse(bool(vapour_pressure["downloaded"]))
        self.assertEqual(int(vapour_pressure["n_valid_values"]), 0)


if __name__ == "__main__":
    unittest.main()
