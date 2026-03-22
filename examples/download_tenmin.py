from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    dataset_scope='historical_csv',
    resolution='10min',
    station_ids=['0-20000-0-11406'],
    start='2024-01-01T00:00:00Z',
    end='2024-01-01T00:20:00Z',
    elements=['T', 'T10'],
)

tenmin = download_observations(query)
print(tenmin.head(10).to_string(index=False))
