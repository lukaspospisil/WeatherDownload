from weatherdownload import (
    list_station_elements,
    list_station_paths,
    read_station_metadata,
    station_supports,
)

# Load station metadata once, then inspect one station's implemented paths.
stations = read_station_metadata()
station_id = '0-20000-0-11406'

# Show implemented paths, then print a couple of narrower helper lookups.
print(list_station_paths(stations, station_id, include_elements=True).to_string(index=False))
print()
print('10min elements:', list_station_elements(stations, station_id, 'historical_csv', '10min'))
print('Supports hourly:', station_supports(stations, station_id, 'historical_csv', '1hour'))
