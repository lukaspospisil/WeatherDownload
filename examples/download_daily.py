import argparse

from weatherdownload import ObservationQuery, download_observations


COUNTRY_DEFAULTS = {
    'AT': {
        'dataset_scope': 'historical',
        'station_ids': ['1'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-03',
        'elements': ['tas_mean', 'precipitation', 'sunshine_duration'],
    },
    'BE': {
        'dataset_scope': 'historical',
        'station_ids': ['6414'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-03',
        'elements': ['tas_mean', 'precipitation', 'sunshine_duration'],
    },
    'CZ': {
        'dataset_scope': 'historical_csv',
        'station_ids': ['0-20000-0-11406'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-10',
        'elements': ['tas_mean', 'tas_max', 'tas_min'],
    },
    'DE': {
        'dataset_scope': 'historical',
        'station_ids': ['00044'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-03',
        'elements': ['tas_mean', 'precipitation'],
    },
    'DK': {
        'dataset_scope': 'historical',
        'station_ids': ['06180'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-03',
        'elements': ['tas_mean', 'precipitation', 'sunshine_duration'],
    },
    'NL': {
        'dataset_scope': 'historical',
        'station_ids': ['0-20000-0-06260'],
        'start_date': '2024-01-01',
        'end_date': '2024-01-03',
        'elements': ['tas_mean', 'precipitation'],
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Download a small daily slice through the unified public API.')
    parser.add_argument('--country', default='CZ', choices=sorted(COUNTRY_DEFAULTS), help='Country code for the example query.')
    return parser


def main() -> None:
    args = build_parser().parse_args()
    defaults = COUNTRY_DEFAULTS[args.country]

    # Build a minimal daily query using the same public API shape for each country.
    query = ObservationQuery(
        country=args.country,
        dataset_scope=defaults['dataset_scope'],
        resolution='daily',
        station_ids=defaults['station_ids'],
        start_date=defaults['start_date'],
        end_date=defaults['end_date'],
        elements=defaults['elements'],
    )

    # Download normalized observations and print a small preview.
    daily = download_observations(query)
    print(daily.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
