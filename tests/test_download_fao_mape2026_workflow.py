import importlib.util
import unittest
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "examples" / "workflows" / "download_fao_mape2026.py"
SPEC = importlib.util.spec_from_file_location("download_fao_mape2026", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
WORKFLOW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKFLOW)


class FilterCzDailyTimeFunctionsTests(unittest.TestCase):
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

    def test_unique_group_is_preserved_without_filtering(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "station_id": "0-20000-0-11406",
                    "observation_date": "2023-01-01",
                    "element": "sunshine_duration",
                    "time_function": "00:00",
                    "value": 3.2,
                }
            ]
        )

        filtered = WORKFLOW.filter_cz_daily_time_functions(observations)

        self.assertEqual(filtered.to_dict(orient="records"), observations.to_dict(orient="records"))

    def test_ambiguous_duplicate_without_preferred_match_raises(self) -> None:
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

        with self.assertRaisesRegex(ValueError, "preferred time_function 'AVG' is missing"):
            WORKFLOW.filter_cz_daily_time_functions(observations)


if __name__ == "__main__":
    unittest.main()
