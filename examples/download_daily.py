from weatherdownload import ObservationQuery, download_observations

# Build a minimal daily query using the public API.
query = ObservationQuery(
    dataset_scope='historical_csv',
    resolution='daily',
    station_ids=['0-20000-0-11406'],
    start_date='1865-06-01',
    end_date='1865-06-10',
    elements=['TMA'],
)

# Download normalized observations and print a small preview.
daily = download_observations(query)
print(daily.head(10).to_string(index=False))
