from datetime import date, datetime, timezone

import pandas as pd

from weatherdownload.fao import build_data_info, summarize_field_fill_status
from weatherdownload.fao_config import FILL_MISSING_CHOICES, get_fao_country_config, list_supported_fao_countries
from weatherdownload.gui.services.commands import build_cli_command, build_cli_command_info
from weatherdownload.gui.services.download import build_observation_query, observation_query_payload, summarize_observations, to_wide_preview
from weatherdownload.gui.services.export import export_table_bytes, mime_type_for_export, resolve_export_filename
from weatherdownload.gui.services.fao import _end_of_day, _start_of_day, fao_fill_choices, supported_fao_countries
from weatherdownload.gui.services.stations import filter_station_table, normalize_station_ids
from weatherdownload import ObservationQuery


def test_filter_station_table_matches_partial_station_id_and_name() -> None:
    stations = pd.DataFrame(
        [
            {"station_id": "00001", "full_name": "Alpha", "gh_id": None, "begin_date": None, "end_date": None, "longitude": 1.0, "latitude": 2.0, "elevation_m": 3.0},
            {"station_id": "00002", "full_name": "Beta Hill", "gh_id": None, "begin_date": None, "end_date": None, "longitude": 1.0, "latitude": 2.0, "elevation_m": 3.0},
        ]
    )

    filtered = filter_station_table(stations, station_id_filter="2", station_name_filter="beta")

    assert filtered["station_id"].tolist() == ["00002"]


def test_to_wide_preview_pivots_long_observations() -> None:
    table = pd.DataFrame(
        [
            {"station_id": "A", "observation_date": "2024-01-01", "element": "tas_mean", "value": 1.5},
            {"station_id": "A", "observation_date": "2024-01-01", "element": "precipitation", "value": 0.2},
        ]
    )

    wide = to_wide_preview(table)

    assert list(wide.columns) == ["station_id", "observation_date", "precipitation", "tas_mean"]
    assert wide.iloc[0]["tas_mean"] == 1.5


def test_summarize_observations_uses_timestamp_range() -> None:
    table = pd.DataFrame(
        [
            {"timestamp": "2024-01-01T00:00:00Z", "value": 1},
            {"timestamp": "2024-01-01T01:00:00Z", "value": 2},
        ]
    )

    summary = summarize_observations(table)

    assert summary.rows == 2
    assert summary.columns == 2
    assert summary.date_column == "timestamp"
    assert summary.min_date == "2024-01-01T00:00:00+00:00"
    assert summary.max_date == "2024-01-01T01:00:00+00:00"


def test_build_cli_command_returns_none_for_non_default_dataset_scope() -> None:
    query = ObservationQuery(
        country="PL",
        dataset_scope="historical_klimat",
        resolution="daily",
        station_ids=["00375"],
        start_date="2025-01-01",
        end_date="2025-01-02",
        elements=["tas_mean"],
    )

    assert build_cli_command(query) is None


def test_build_cli_command_info_explains_fallback_reason() -> None:
    query = ObservationQuery(
        country="PL",
        dataset_scope="historical_klimat",
        resolution="daily",
        station_ids=["00375"],
        start_date="2025-01-01",
        end_date="2025-01-02",
        elements=["tas_mean"],
    )

    info = build_cli_command_info(query)

    assert info.supported is False
    assert info.command is None
    assert "default observation dataset path" in info.reason


def test_resolve_export_filename_replaces_wrong_suffix() -> None:
    assert resolve_export_filename("report.tmp", format_name="csv") == "report.csv"
    assert resolve_export_filename("report", format_name="xlsx") == "report.xlsx"


def test_mime_type_for_export_uses_stable_types() -> None:
    assert mime_type_for_export("csv") == "text/csv"
    assert mime_type_for_export("parquet") == "application/x-parquet"


def test_export_table_bytes_adds_expected_extension() -> None:
    table = pd.DataFrame([{"a": 1}])

    payload, filename = export_table_bytes(table, format_name="csv", filename="sample")

    assert filename == "sample.csv"
    assert payload.startswith(b"a")


def test_shared_fao_build_data_info_keeps_fill_policy_metadata() -> None:
    class Config:
        country = "PL"
        dataset_type = "test"
        source = "test-source"
        provider_element_mapping = {"tas_mean": {"status": "observed"}}
        assumptions = {"note": "kept"}

    info = build_data_info(Config(), station_rows=[{"station_id": "00375"}], min_complete_days=10, fill_missing="allow-hourly-aggregate")

    assert info["country"] == "PL"
    assert info["fill_policy"]["selected"] == "allow-hourly-aggregate"
    assert info["fill_policy"]["hourly_aggregation_fields"] == ["wind_speed", "vapour_pressure"]
    assert info["assumptions"]["note"] == "kept"


def test_shared_fao_summarize_field_fill_status_marks_missing_and_observed() -> None:
    provenance = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "tas_mean": ["observed_daily"],
            "tas_max": ["missing"],
            "tas_min": ["missing"],
            "wind_speed": ["missing"],
            "vapour_pressure": ["missing"],
            "sunshine_duration": ["missing"],
        }
    )

    summaries = summarize_field_fill_status(
        [provenance],
        fill_missing="none",
        applied_rules_by_field={field: set() for field in ["tas_mean", "tas_max", "tas_min", "wind_speed", "vapour_pressure", "sunshine_duration"]},
    )

    by_field = {summary.field: summary for summary in summaries}
    assert by_field["tas_mean"].status == "observed-only"
    assert by_field["tas_max"].status == "still missing"


def test_package_import_sanity_for_gui_services() -> None:
    from weatherdownload import fao_config
    from weatherdownload.gui.services import commands, download, export, fao, stations

    assert fao_config is not None
    assert commands is not None
    assert download is not None
    assert export is not None
    assert fao is not None
    assert stations is not None


def test_fao_date_helpers_cover_full_utc_day() -> None:
    target = date(2024, 1, 1)

    assert _start_of_day(target) == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert _end_of_day(target) == datetime(2024, 1, 1, 23, 0, tzinfo=timezone.utc)


def test_shared_fao_config_exposes_supported_countries_and_fill_choices() -> None:
    assert "CZ" in list_supported_fao_countries()
    assert tuple(FILL_MISSING_CHOICES) == fao_fill_choices()
    assert supported_fao_countries() == list_supported_fao_countries()


def test_shared_fao_config_returns_poland_hourly_aggregate_config() -> None:
    config = get_fao_country_config("PL", fill_missing="allow-hourly-aggregate")

    assert config.country == "PL"
    assert config.hourly_dataset_scope == "historical"
    assert config.hourly_resolution == "1hour"
    assert config.hourly_query_elements == ("wind_speed", "vapour_pressure")


def test_normalize_station_ids_accepts_display_labels_and_canonical_ids() -> None:
    stations = pd.DataFrame(
        [
            {"station_id": "LUG", "full_name": "Lugano"},
            {"station_id": "ZUR", "full_name": "Zurich"},
        ]
    )

    assert normalize_station_ids(["LUG - Lugano", "zur"], stations) == ["LUG", "ZUR"]


def test_normalize_station_ids_supports_multi_station_label_selection() -> None:
    stations = pd.DataFrame(
        [
            {"station_id": "LUG", "full_name": "Lugano"},
            {"station_id": "GEN", "full_name": "Geneva"},
            {"station_id": "ZUR", "full_name": "Zurich"},
        ]
    )

    assert normalize_station_ids(["GEN - Geneva", "LUG - Lugano", "gen"], stations) == ["GEN", "LUG"]


def test_build_observation_query_normalizes_ch_daily_station_selection() -> None:
    stations = pd.DataFrame([{"station_id": "LUG", "full_name": "Lugano"}])

    query = build_observation_query(
        country="CH",
        dataset_scope="historical",
        resolution="daily",
        station_ids=["LUG - Lugano"],
        elements=["tas_mean", "tas_max", "tas_min"],
        all_history=False,
        station_metadata=stations,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 10),
    )

    assert query.country == "CH"
    assert query.dataset_scope == "historical"
    assert query.resolution == "daily"
    assert query.station_ids == ["LUG"]
    assert query.elements == ["tre200d0", "tre200dx", "tre200dn"]
    assert query.start_date.isoformat() == "2024-01-01"
    assert query.end_date.isoformat() == "2024-01-10"


def test_build_observation_query_normalizes_cz_daily_selection_and_payload() -> None:
    stations = pd.DataFrame([{"station_id": "0-20000-0-11406", "full_name": "Praha-Libus"}])

    query = build_observation_query(
        country="CZ",
        dataset_scope="historical_csv",
        resolution="daily",
        station_ids=["0-20000-0-11406 - Praha-Libus"],
        elements=["tas_mean", "tas_max", "tas_min"],
        all_history=False,
        station_metadata=stations,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 10),
    )
    payload = observation_query_payload(query)

    assert query.country == "CZ"
    assert query.dataset_scope == "historical_csv"
    assert query.resolution == "daily"
    assert query.station_ids == ["0-20000-0-11406"]
    assert query.elements == ["T", "TMA", "TMI"]
    assert query.start_date.isoformat() == "2024-01-01"
    assert query.end_date.isoformat() == "2024-01-10"
    assert payload == {
        "country": "CZ",
        "dataset_scope": "historical_csv",
        "resolution": "daily",
        "station_ids": ["0-20000-0-11406"],
        "elements": ["T", "TMA", "TMI"],
        "all_history": False,
        "start_date": "2024-01-01",
        "end_date": "2024-01-10",
    }


def test_build_observation_query_subdaily_uses_timestamp_fields_and_payload() -> None:
    stations = pd.DataFrame(
        [
            {"station_id": "AIG", "full_name": "Aigle"},
            {"station_id": "LUG", "full_name": "Lugano"},
        ]
    )
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 23, 0, tzinfo=timezone.utc)

    query = build_observation_query(
        country="CH",
        dataset_scope="historical",
        resolution="1hour",
        station_ids=["AIG - Aigle", "LUG"],
        elements=["tas_mean", "pressure"],
        all_history=False,
        station_metadata=stations,
        start_datetime=start,
        end_datetime=end,
    )
    payload = observation_query_payload(query)

    assert query.station_ids == ["AIG", "LUG"]
    assert query.elements == ["tre200h0", "prestah0"]
    assert query.start == start
    assert query.end == end
    assert query.start_date is None
    assert query.end_date is None
    assert payload == {
        "country": "CH",
        "dataset_scope": "historical",
        "resolution": "1hour",
        "station_ids": ["AIG", "LUG"],
        "elements": ["tre200h0", "prestah0"],
        "all_history": False,
        "start": "2024-01-01T00:00:00+00:00",
        "end": "2024-01-01T23:00:00+00:00",
    }


def test_build_observation_query_all_history_drops_explicit_daily_dates() -> None:
    stations = pd.DataFrame([{"station_id": "LUG", "full_name": "Lugano"}])

    query = build_observation_query(
        country="CH",
        dataset_scope="historical",
        resolution="daily",
        station_ids=["LUG - Lugano"],
        elements=["tas_mean"],
        all_history=True,
        station_metadata=stations,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 10),
    )
    payload = observation_query_payload(query)

    assert query.all_history is True
    assert query.start_date is None
    assert query.end_date is None
    assert payload["station_ids"] == ["LUG"]
    assert payload["all_history"] is True
    assert payload["start_date"] is None
    assert payload["end_date"] is None


def test_build_cli_command_info_mentions_cli_compatible_scope_in_fallback_message() -> None:
    query = ObservationQuery(
        country="PL",
        dataset_scope="historical_klimat",
        resolution="daily",
        station_ids=["00375"],
        start_date="2025-01-01",
        end_date="2025-01-10",
        elements=["tas_mean"],
    )

    info = build_cli_command_info(query)

    assert info.supported is False
    assert info.command is None
    assert "dataset_scope 'historical_klimat'" in info.reason
    assert "CLI-compatible path for PL is 'historical'" in info.reason
