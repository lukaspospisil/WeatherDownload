from __future__ import annotations

import pandas as pd


def observations_to_wide(
    observations: pd.DataFrame,
    *,
    station_col: str = "station_id",
    date_col: str | None = None,
    element_col: str = "element",
    value_col: str = "value",
    rename_elements: dict[str, str] | None = None,
    sort: bool = True,
) -> pd.DataFrame:
    """Pivot normalized long observations into a station/date wide table."""

    resolved_date_col = _resolve_date_column(observations, date_col=date_col)
    required_columns = [station_col, resolved_date_col, element_col, value_col]
    missing_columns = [column for column in required_columns if column not in observations.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"observations_to_wide requires columns: missing {missing}.")

    duplicate_mask = observations.duplicated(
        subset=[station_col, resolved_date_col, element_col],
        keep=False,
    )
    if duplicate_mask.any():
        duplicate_rows = observations.loc[
            duplicate_mask,
            [station_col, resolved_date_col, element_col],
        ].drop_duplicates()
        examples = duplicate_rows.head(5).to_dict("records")
        raise ValueError(
            "observations_to_wide found duplicated station/date/element rows; "
            f"examples: {examples}"
        )

    prepared = observations.loc[:, required_columns].copy()
    wide = prepared.pivot(
        index=[station_col, resolved_date_col],
        columns=element_col,
        values=value_col,
    ).reset_index()
    wide = wide.rename(columns={resolved_date_col: "date"})
    wide.columns.name = None

    if rename_elements:
        wide = wide.rename(columns=rename_elements)

    if sort:
        wide = wide.sort_values([station_col, "date"]).reset_index(drop=True)

    return wide


def _resolve_date_column(observations: pd.DataFrame, *, date_col: str | None) -> str:
    if date_col is not None:
        return date_col
    if "date" in observations.columns:
        return "date"
    if "observation_date" in observations.columns:
        return "observation_date"
    raise ValueError(
        "observations_to_wide could not infer a date column. "
        "Provide date_col or include 'date' or 'observation_date'."
    )
