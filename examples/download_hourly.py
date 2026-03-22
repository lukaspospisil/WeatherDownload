from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    dataset_scope='historical_csv',
    resolution='1hour',
    station_ids=['0-20000-0-11406'],
    start='2024-01-01T00:00:00Z',
    end='2024-01-01T02:00:00Z',
    elements=['E', 'P'],
)

hourly = download_observations(query)
print(hourly.head(10).to_string(index=False))
