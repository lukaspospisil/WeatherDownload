from weatherdownload import (
    list_station_elements,
    list_station_paths,
    read_station_metadata,
    station_supports,
)

stations = read_station_metadata()
station_id = '0-20000-0-11406'

print(list_station_paths(stations, station_id, include_elements=True).to_string(index=False))
print()
print('10min elements:', list_station_elements(stations, station_id, 'historical_csv', '10min'))
print('Supports hourly:', station_supports(stations, station_id, 'historical_csv', '1hour'))
