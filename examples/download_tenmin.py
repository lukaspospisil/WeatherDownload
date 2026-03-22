from weatherdownload import ObservationQuery, download_observations

# Build a narrow 10-minute query with explicit timestamp bounds.
query = ObservationQuery(
    dataset_scope='historical_csv',
    resolution='10min',
    station_ids=['0-20000-0-11406'],
    start='2024-01-01T00:00:00Z',
    end='2024-01-01T00:20:00Z',
    elements=['T', 'T10'],
)

# Download normalized 10-minute observations and print a preview.
tenmin = download_observations(query)
print(tenmin.head(10).to_string(index=False))
