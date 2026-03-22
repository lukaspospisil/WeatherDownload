from weatherdownload import filter_stations, read_station_metadata

# Load the normalized station metadata table for the default provider.
stations = read_station_metadata()

# Apply a simple lifecycle filter and keep a compact preview column set.
active = filter_stations(stations, active_on='2024-01-01')
selected = active.loc[:, ['station_id', 'gh_id', 'full_name', 'latitude', 'longitude']]

# Print a small terminal-friendly preview.
print(selected.head(10).to_string(index=False))
