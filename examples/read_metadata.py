from weatherdownload import filter_stations, read_station_metadata

stations = read_station_metadata()
active = filter_stations(stations, active_on='2024-01-01')
selected = active.loc[:, ['station_id', 'gh_id', 'full_name', 'latitude', 'longitude']]

print(selected.head(10).to_string(index=False))
