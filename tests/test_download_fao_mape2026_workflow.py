import importlib.util
import tempfile
import unittest
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "examples" / "workflows" / "download_fao_mape2026.py"
SPEC = importlib.util.spec_from_file_location("download_fao_mape2026", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
WORKFLOW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKFLOW)


class FilterCzDailyTimeFunctionsTests(unittest.TestCase):
    def test_build_parser_accepts_debug_duplicates_flag(self) -> None:
        parser = WORKFLOW.build_parser()
        args = parser.parse_args(["--debug-duplicates"])

        self.assertTrue(args.debug_duplicates)

    def test_tas_mean_prefers_avg_over_hourly_rows(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "time_function": "AVG",
                    "value": 1.5,
                },
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "time_function": "07:00",
                    "value": 0.5,
                },
            ]
        )

        filtered = WORKFLOW.filter_cz_daily_time_functions(observations)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["time_function"], "AVG")
        self.assertEqual(filtered.iloc[0]["value"], 1.5)

    def test_avg_preference_filters_pressure_wind_and_vapour_pressure(self) -> None:
        observations = pd.DataFrame(
            [
                {"station_id": "A", "observation_date": "2023-01-01", "element": "pressure", "time_function": "AVG", "value": 1000.0},
                {"station_id": "A", "observation_date": "2023-01-01", "element": "pressure", "time_function": "07:00", "value": 1001.0},
                {"station_id": "A", "observation_date": "2023-01-01", "element": "wind_speed", "time_function": "AVG", "value": 3.0},
                {"station_id": "A", "observation_date": "2023-01-01", "element": "wind_speed", "time_function": "14:00", "value": 4.0},
                {"station_id": "A", "observation_date": "2023-01-01", "element": "vapour_pressure", "time_function": "AVG", "value": 8.0},
                {"station_id": "A", "observation_date": "2023-01-01", "element": "vapour_pressure", "time_function": "21:00", "value": 8.5},
            ]
        )

        filtered = WORKFLOW.filter_cz_daily_time_functions(observations)

        self.assertEqual(len(filtered), 3)
        self.assertEqual(list(filtered["time_function"]), ["AVG", "AVG", "AVG"])
        self.assertEqual(list(filtered["element"]), ["pressure", "wind_speed", "vapour_pressure"])

    def test_element_without_configured_preference_is_preserved_unchanged(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "sunshine_duration",
                    "time_function": "00:00",
                    "value": 3.2,
                },
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "sunshine_duration",
                    "time_function": "12:00",
                    "value": 0.0,
                }
            ]
        )

        filtered = WORKFLOW.filter_cz_daily_time_functions(observations)

        self.assertEqual(filtered.to_dict(orient="records"), observations.to_dict(orient="records"))

    def test_missing_preferred_time_function_is_left_for_post_filter_duplicate_check(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "time_function": "07:00",
                    "value": 0.5,
                },
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "time_function": "14:00",
                    "value": 1.5,
                },
            ]
        )

        filtered = WORKFLOW.filter_cz_daily_time_functions(observations)

        self.assertTrue(filtered.empty)

    def test_post_filter_duplicate_check_raises_for_remaining_duplicates(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "value": 1.5,
                },
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "value": 1.7,
                },
            ]
        )

        with self.assertRaisesRegex(ValueError, "remain after CZ time_function filtering"):
            WORKFLOW.ensure_no_duplicate_observation_keys(observations)

    def test_filtered_cache_path_uses_filtered_marker(self) -> None:
        cache_path = WORKFLOW.resolve_station_cache_path(
            cache_dir=Path("cache"),
            station_id="0-20000-0-11406",
            start_date="2023-01-01",
            end_date="2023-01-31",
            elements=WORKFLOW.DAILY_ELEMENTS,
            stage="filtered",
        )

        self.assertIn("filtered", cache_path.name)

    def test_process_station_observations_prefers_filtered_cache(self) -> None:
        filtered_rows = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "tas_mean",
                    "time_function": "AVG",
                    "value": 1.5,
                }
            ]
        )
        station_metadata = pd.DataFrame([{"station_id": "0-20000-0-11406"}])

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            filtered_cache_path = WORKFLOW.resolve_station_cache_path(
                cache_dir=cache_dir,
                station_id="0-20000-0-11406",
                start_date="2023-01-01",
                end_date="2023-01-31",
                elements=WORKFLOW.DAILY_ELEMENTS,
                stage="filtered",
            )
            WORKFLOW.write_cached_station_observations(filtered_rows, filtered_cache_path)

            result = WORKFLOW.process_station_observations(
                station_id="0-20000-0-11406",
                station_metadata=station_metadata,
                start_date="2023-01-01",
                end_date="2023-01-31",
                cache_dir=cache_dir,
                force_refresh=False,
                debug_duplicates=False,
            )

            self.assertEqual(result.to_dict(orient="records"), filtered_rows.to_dict(orient="records"))


if __name__ == "__main__":
    unittest.main()
