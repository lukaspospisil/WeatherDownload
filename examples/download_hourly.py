import argparse

from weatherdownload import ObservationQuery, download_observations


COUNTRY_DEFAULTS = {
    'BE': {
        'dataset_scope': 'historical',
        'station_ids': ['6414'],
        'start': '2024-01-01T01:00:00Z',
        'end': '2024-01-01T02:00:00Z',
        'elements': ['tas_mean', 'pressure'],
    },
    'CZ': {
        'dataset_scope': 'historical_csv',
        'station_ids': ['0-20000-0-11406'],
        'start': '2024-01-01T00:00:00Z',
        'end': '2024-01-01T02:00:00Z',
        'elements': ['E', 'P'],
    },
    'DE': {
        'dataset_scope': 'historical',
        'station_ids': ['00044'],
        'start': '2024-01-01T00:00:00Z',
        'end': '2024-01-01T02:00:00Z',
        'elements': ['tas_mean', 'wind_speed'],
    },
    'DK': {
        'dataset_scope': 'historical',
        'station_ids': ['06180'],
        'start': '2024-01-01T01:00:00Z',
        'end': '2024-01-01T02:00:00Z',
        'elements': ['tas_mean', 'pressure'],
    },
    'SE': {
        'dataset_scope': 'historical',
        'station_ids': ['98230'],
        'start': '2012-11-29T11:00:00Z',
        'end': '2012-11-29T13:00:00Z',
        'elements': ['tas_mean', 'pressure'],
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Download a small hourly slice through the unified public API.')
    parser.add_argument('--country', default='CZ', choices=sorted(COUNTRY_DEFAULTS), help='Country code for the example query.')
    return parser


def main() -> None:
    args = build_parser().parse_args()
    defaults = COUNTRY_DEFAULTS[args.country]

    # Build a minimal hourly query using the same public API shape for each country.
    query = ObservationQuery(
        country=args.country,
        dataset_scope=defaults['dataset_scope'],
        resolution='1hour',
        station_ids=defaults['station_ids'],
        start=defaults['start'],
        end=defaults['end'],
        elements=defaults['elements'],
    )

    # Download normalized hourly observations and print a small preview.
    hourly = download_observations(query)
    print(hourly.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
