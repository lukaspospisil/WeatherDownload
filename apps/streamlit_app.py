from __future__ import annotations

from datetime import date, datetime, time, timezone
import json

import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError as exc:
    raise SystemExit("Streamlit is not installed. Install it with `pip install .[gui]` or `pip install .[gui,full]`.") from exc

GUI_IMPORT_ERROR: Exception | None = None
try:
    from weatherdownload.gui.services.commands import build_cli_command_info, build_python_snippet
    from weatherdownload.gui.services.download import (
        build_observation_query,
        dataset_elements,
        dataset_resolutions,
        dataset_scopes,
        observation_query_payload,
        preview_table,
        run_observation_query,
        summarize_observations,
    )
    from weatherdownload.gui.services.export import export_table_bytes, mime_type_for_export
    from weatherdownload.gui.services.fao import fao_fill_choices, prepare_fao_preview, supported_fao_countries
    from weatherdownload.gui.services.stations import filter_station_table, load_station_metadata, station_detail, station_option_labels, supported_countries
except Exception as exc:
    GUI_IMPORT_ERROR = exc


def main() -> None:
    st.set_page_config(page_title="WeatherDownload GUI MVP", layout="wide")
    if GUI_IMPORT_ERROR is not None:
        st.title("WeatherDownload GUI MVP")
        st.error(
            "Failed to import the GUI support modules. "
            "Install the project in editable mode with `pip install -e .[gui]` or `pip install -e .[gui,full]` before launching Streamlit."
        )
        st.code(str(GUI_IMPORT_ERROR), language="text")
        st.stop()
    _init_session_state()

    st.title("WeatherDownload GUI MVP")
    st.caption("Thin local Streamlit layer over the existing WeatherDownload public API.")

    with st.sidebar:
        st.header("Session")
        timeout = st.number_input("Request timeout (s)", min_value=5, max_value=600, value=int(st.session_state.timeout), step=5)
        st.session_state.timeout = int(timeout)
        default_export_name = st.text_input("Export basename", value=st.session_state.export_basename)
        st.session_state.export_basename = default_export_name.strip() or "weatherdownload_export"
        if st.button("Clear session log"):
            st.session_state.session_log = []
            _log("Cleared session log.")

    stations_tab, download_tab, fao_tab, log_tab = st.tabs(["Stations", "Download", "FAO bundle", "Log / Command"])

    with stations_tab:
        render_stations_tab()
    with download_tab:
        render_download_tab()
    with fao_tab:
        render_fao_tab()
    with log_tab:
        render_log_tab()


@st.cache_data(show_spinner=False)
def _cached_station_metadata(country: str, timeout: int) -> pd.DataFrame:
    return load_station_metadata(country, timeout=timeout)


def render_stations_tab() -> None:
    countries = supported_countries()
    country = st.selectbox("Country", options=countries, index=_safe_index(countries, st.session_state.station_country), key="station_country")
    stations = _load_stations_or_stop(country)
    if stations is None:
        return

    col1, col2 = st.columns(2)
    station_id_filter = col1.text_input("Filter by station_id", value=st.session_state.station_id_filter, key="station_id_filter")
    station_name_filter = col2.text_input("Filter by station name", value=st.session_state.station_name_filter, key="station_name_filter")

    filtered = filter_station_table(stations, station_id_filter=station_id_filter, station_name_filter=station_name_filter)
    st.write(f"{len(filtered)} station(s) shown")
    st.dataframe(filtered, use_container_width=True, hide_index=True)

    if filtered.empty:
        st.info("No stations match the current filters.")
        return

    labels = station_option_labels(filtered)
    selected_station = st.selectbox(
        "Selected station",
        options=list(labels),
        format_func=lambda station_id: labels[station_id],
        key="selected_station_id",
    )
    detail = station_detail(filtered, selected_station)
    if detail is not None:
        st.subheader("Station detail")
        st.json(_json_ready(detail))


def render_download_tab() -> None:
    countries = supported_countries()
    country = st.selectbox("Country", options=countries, index=_safe_index(countries, st.session_state.download_country), key="download_country")
    stations = _load_stations_or_stop(country)
    if stations is None:
        return
    try:
        scopes = dataset_scopes(country)
    except Exception as exc:
        _show_operation_error("Failed to load dataset scopes", exc)
        return
    default_scope = st.session_state.download_dataset_scope if st.session_state.download_dataset_scope in scopes else scopes[0]
    dataset_scope = st.selectbox("Dataset scope", options=scopes, index=scopes.index(default_scope), key="download_dataset_scope")
    try:
        resolutions = dataset_resolutions(country, dataset_scope)
    except Exception as exc:
        _show_operation_error(f"Failed to load resolutions for {dataset_scope}", exc)
        return
    default_resolution = st.session_state.download_resolution if st.session_state.download_resolution in resolutions else resolutions[0]
    resolution = st.selectbox("Resolution", options=resolutions, index=resolutions.index(default_resolution), key="download_resolution")
    try:
        elements = dataset_elements(country, dataset_scope, resolution)
    except Exception as exc:
        _show_operation_error(f"Failed to load supported elements for {dataset_scope}/{resolution}", exc)
        return

    search = st.text_input("Find stations for selection", value=st.session_state.download_station_filter, key="download_station_filter")
    narrowed = filter_station_table(stations, station_id_filter=search, station_name_filter=search)
    labels = station_option_labels(narrowed.head(500))
    selected_station_ids = st.multiselect("Stations", options=list(labels), format_func=lambda station_id: labels[station_id], key="download_station_ids")
    selected_elements = st.multiselect("Elements", options=elements, default=_element_defaults(elements, st.session_state.download_elements), key="download_elements")
    all_history = st.checkbox("Use all available history", value=st.session_state.download_all_history, key="download_all_history")

    if resolution == "daily":
        date_col1, date_col2 = st.columns(2)
        start_date = date_col1.date_input("Start date", value=st.session_state.download_start_date, key="download_start_date", disabled=all_history)
        end_date = date_col2.date_input("End date", value=st.session_state.download_end_date, key="download_end_date", disabled=all_history)
        start_datetime = None
        end_datetime = None
    else:
        dt_col1, dt_col2 = st.columns(2)
        start_date = None
        end_date = None
        start_datetime = _combine_date(dt_col1.date_input("Start day", value=st.session_state.download_start_day, key="download_start_day", disabled=all_history), time(0, 0))
        end_datetime = _combine_date(dt_col2.date_input("End day", value=st.session_state.download_end_day, key="download_end_day", disabled=all_history), time(23, 0))
        st.caption("Sub-daily MVP uses full UTC-day boundaries in the picker. The generated Python snippet shows the exact datetimes used.")

    build_col, download_col = st.columns(2)
    if build_col.button("Build query", use_container_width=True):
        _handle_build_query(
            country=country,
            dataset_scope=dataset_scope,
            resolution=resolution,
            station_ids=selected_station_ids,
            elements=selected_elements,
            all_history=all_history,
            station_metadata=stations,
            start_date=start_date,
            end_date=end_date,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    if download_col.button("Download data", use_container_width=True):
        _handle_download(
            country=country,
            dataset_scope=dataset_scope,
            resolution=resolution,
            station_ids=selected_station_ids,
            elements=selected_elements,
            all_history=all_history,
            start_date=start_date,
            end_date=end_date,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            station_metadata=stations,
        )

    if st.session_state.current_query_payload:
        st.subheader("Normalized query payload")
        st.json(st.session_state.current_query_payload)

    result = st.session_state.download_result
    if result is not None:
        summary = summarize_observations(result)
        metric1, metric2, metric3 = st.columns(3)
        metric1.metric("Rows", summary.rows)
        metric2.metric("Columns", summary.columns)
        metric3.metric("Date range", _format_range(summary.min_date, summary.max_date))
        st.subheader("Preview")
        st.dataframe(preview_table(result), use_container_width=True, hide_index=True)

        st.subheader("Export")
        export_format = st.selectbox("Export format", options=["csv", "parquet", "xlsx", "mat"], key="download_export_format")
        try:
            export_bytes, export_name = export_table_bytes(
                result,
                format_name=export_format,
                filename=f"{st.session_state.export_basename}_{country}_{dataset_scope}_{resolution}",
            )
            st.download_button(
                label=f"Download {export_format.upper()}",
                data=export_bytes,
                file_name=export_name,
                mime=mime_type_for_export(export_format),
                use_container_width=True,
            )
        except Exception as exc:
            _show_operation_error("Export failed", exc)


def render_fao_tab() -> None:
    st.info("This tab prepares and exports FAO-oriented input data only. It does not compute FAO ET0.")
    try:
        countries = supported_fao_countries()
    except Exception as exc:
        _show_operation_error("Failed to load FAO country support", exc)
        return
    country = st.selectbox("FAO country", options=countries, key="fao_country")
    stations = _load_stations_or_stop(country)
    if stations is None:
        return
    search = st.text_input("Find FAO stations", value=st.session_state.fao_station_filter, key="fao_station_filter")
    narrowed = filter_station_table(stations, station_id_filter=search, station_name_filter=search)
    labels = station_option_labels(narrowed.head(300))
    selected_station_ids = st.multiselect("Stations", options=list(labels), format_func=lambda station_id: labels[station_id], key="fao_station_ids")
    fill_missing = st.selectbox("Fill policy", options=list(fao_fill_choices()), key="fao_fill_missing")
    min_complete_days = st.number_input("Minimum complete days", min_value=1, max_value=36500, value=int(st.session_state.fao_min_complete_days), step=1, key="fao_min_complete_days")
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Start date", value=st.session_state.fao_start_date, key="fao_start_date")
    end_date = col2.date_input("End date", value=st.session_state.fao_end_date, key="fao_end_date")

    if st.button("Prepare FAO input preview", use_container_width=True):
        try:
            preview = prepare_fao_preview(
                country=country,
                station_ids=selected_station_ids,
                start_date=start_date,
                end_date=end_date,
                fill_missing=fill_missing,
                min_complete_days=min_complete_days,
                timeout=st.session_state.timeout,
            )
            st.session_state.fao_preview = preview
            _log(f"Prepared FAO input preview for {country} with {len(preview.stations)} retained station(s).")
        except Exception as exc:
            st.session_state.fao_preview = None
            _show_operation_error("FAO preview failed", exc)
            _log(f"FAO preview failed: {exc}")

    preview = st.session_state.fao_preview
    if preview is None:
        st.caption("TODO: future iterations can expose the full cache/build workflow from `examples/workflows/download_fao.py`.")
        return

    st.subheader("Data info")
    st.json(preview.data_info)
    st.subheader("Stations")
    st.dataframe(preview.stations, use_container_width=True, hide_index=True)
    st.subheader("Series preview")
    st.dataframe(preview.series.head(50), use_container_width=True, hide_index=True)
    st.subheader("Field fill summary")
    st.dataframe(preview.field_summary, use_container_width=True, hide_index=True)

    info_bytes = json.dumps(preview.data_info, indent=2).encode("utf-8")
    st.download_button("Download data_info.json", data=info_bytes, file_name=f"{st.session_state.export_basename}_{country}_fao_data_info.json", mime="application/json")

    for table_name, table in [("stations", preview.stations), ("series", preview.series)]:
        fmt_key = f"fao_{table_name}_format"
        export_format = st.selectbox(f"{table_name.title()} export format", options=["csv", "parquet", "xlsx", "mat"], key=fmt_key)
        try:
            payload, filename = export_table_bytes(
                table,
                format_name=export_format,
                filename=f"{st.session_state.export_basename}_{country}_fao_{table_name}",
            )
            st.download_button(
                f"Download {table_name} ({export_format.upper()})",
                data=payload,
                file_name=filename,
                mime=mime_type_for_export(export_format),
            )
        except Exception as exc:
            _show_operation_error(f"{table_name.title()} export failed", exc)


def render_log_tab() -> None:
    st.subheader("Session log")
    if st.session_state.session_log:
        st.code("\n".join(st.session_state.session_log), language="text")
    else:
        st.caption("No log messages yet.")

    st.subheader("Python snippet")
    if st.session_state.current_python_snippet:
        st.code(st.session_state.current_python_snippet, language="python")
    else:
        st.caption("Build a download query to generate a reproducible snippet.")

    st.subheader("CLI equivalent")
    if st.session_state.current_cli_info and st.session_state.current_cli_info["supported"]:
        st.code(st.session_state.current_cli_info["command"], language="powershell")
    elif st.session_state.current_cli_info and st.session_state.current_cli_info["reason"]:
        st.caption(st.session_state.current_cli_info["reason"])
    else:
        st.caption("Build a download query to evaluate whether the current query shape maps to the existing CLI.")

    st.subheader("Normalized query payload")
    if st.session_state.current_query_payload:
        st.json(st.session_state.current_query_payload)
    else:
        st.caption("Build a download query to inspect the normalized payload sent to the library.")


def _handle_build_query(**kwargs: object) -> None:
    try:
        query = build_observation_query(**kwargs)
    except Exception as exc:
        st.session_state.current_query = None
        st.session_state.current_python_snippet = ""
        st.session_state.current_cli_info = None
        st.session_state.current_query_payload = None
        st.error(str(exc))
        _log(f"Query build failed: {exc}")
        return
    st.session_state.current_query = query
    st.session_state.current_query_payload = observation_query_payload(query)
    st.session_state.current_python_snippet = build_python_snippet(query)
    cli_info = build_cli_command_info(query)
    st.session_state.current_cli_info = {"supported": cli_info.supported, "command": cli_info.command, "reason": cli_info.reason}
    _log(f"Built query for {query.country} {query.dataset_scope}/{query.resolution} with {len(query.station_ids)} station(s).")
    st.success("Query built successfully.")


def _handle_download(*, station_metadata: pd.DataFrame, **kwargs: object) -> None:
    try:
        query = build_observation_query(**kwargs)
        st.session_state.current_query = query
        st.session_state.current_query_payload = observation_query_payload(query)
        st.session_state.current_python_snippet = build_python_snippet(query)
        cli_info = build_cli_command_info(query)
        st.session_state.current_cli_info = {"supported": cli_info.supported, "command": cli_info.command, "reason": cli_info.reason}
        result = run_observation_query(query, timeout=st.session_state.timeout, station_metadata=station_metadata)
        st.session_state.download_result = result
        _log(f"Downloaded {len(result)} observation row(s) for {query.country} {query.dataset_scope}/{query.resolution}.")
        st.success("Download completed.")
    except Exception as exc:
        st.session_state.download_result = None
        if st.session_state.current_query is not None:
            _log(f"Normalized query payload: {json.dumps(st.session_state.current_query_payload or {}, ensure_ascii=True)}")
        _show_operation_error("Download failed", exc)
        _log(f"Download failed: {exc}")


def _init_session_state() -> None:
    defaults = {
        "timeout": 60,
        "export_basename": "weatherdownload_export",
        "session_log": [],
        "station_country": "CZ",
        "station_id_filter": "",
        "station_name_filter": "",
        "selected_station_id": None,
        "download_country": "CZ",
        "download_dataset_scope": None,
        "download_resolution": None,
        "download_station_filter": "",
        "download_station_ids": [],
        "download_elements": [],
        "download_all_history": False,
        "download_start_date": date(2024, 1, 1),
        "download_end_date": date(2024, 1, 10),
        "download_start_day": date(2024, 1, 1),
        "download_end_day": date(2024, 1, 1),
        "download_export_format": "csv",
        "download_result": None,
        "current_query": None,
        "current_query_payload": None,
        "current_python_snippet": "",
        "current_cli_info": None,
        "fao_country": "CZ",
        "fao_station_filter": "",
        "fao_station_ids": [],
        "fao_fill_missing": "none",
        "fao_min_complete_days": 1,
        "fao_start_date": date(2024, 1, 1),
        "fao_end_date": date(2024, 12, 31),
        "fao_preview": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    st.session_state.session_log.append(f"[{timestamp}] {message}")


def _safe_index(options: list[str], value: str) -> int:
    try:
        return options.index(value)
    except ValueError:
        return 0


def _combine_date(value: date, value_time: time) -> datetime:
    return datetime.combine(value, value_time, tzinfo=timezone.utc)


def _element_defaults(all_elements: list[str], current: list[str]) -> list[str]:
    if current:
        return [element for element in current if element in all_elements]
    return all_elements[: min(3, len(all_elements))]


def _format_range(start: str | None, end: str | None) -> str:
    if start and end:
        return f"{start} to {end}"
    return "n/a"


def _json_ready(mapping: dict[str, object]) -> dict[str, object]:
    ready: dict[str, object] = {}
    for key, value in mapping.items():
        if isinstance(value, (pd.Timestamp, datetime, date)):
            ready[key] = value.isoformat()
        elif pd.isna(value):
            ready[key] = None
        else:
            ready[key] = value
    return ready


def _load_stations_or_stop(country: str) -> pd.DataFrame | None:
    try:
        return _cached_station_metadata(country, st.session_state.timeout)
    except Exception as exc:
        _show_operation_error(f"Failed to load station metadata for {country}", exc)
        return None


def _show_operation_error(prefix: str, exc: Exception) -> None:
    st.error(f"{prefix}: {exc}")


if __name__ == "__main__":
    main()
