import argparse

from weatherdownload import ObservationQuery, download_observations


COUNTRY_DEFAULTS = {
    'BE': {
        'dataset_scope': 'historical',
        'station_ids': ['6414'],
        'start': '2024-01-01T00:10:00Z',
        'end': '2024-01-01T00:20:00Z',
        'elements': ['tas_mean', 'pressure'],
    },
    'CZ': {
        'dataset_scope': 'historical_csv',
        'station_ids': ['0-20000-0-11406'],
        'start': '2024-01-01T00:00:00Z',
        'end': '2024-01-01T00:20:00Z',
        'elements': ['T', 'T10'],
    },
    'DE': {
        'dataset_scope': 'historical',
        'station_ids': ['00044'],
        'start': '2024-01-01T00:00:00Z',
        'end': '2024-01-01T00:20:00Z',
        'elements': ['tas_mean', 'relative_humidity'],
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Download a small 10-minute slice through the unified public API.')
    parser.add_argument('--country', default='CZ', choices=sorted(COUNTRY_DEFAULTS), help='Country code for the example query.')
    return parser


def main() -> None:
    args = build_parser().parse_args()
    defaults = COUNTRY_DEFAULTS[args.country]

    # Build a minimal 10-minute query using the same public API shape for each country.
    query = ObservationQuery(
        country=args.country,
        dataset_scope=defaults['dataset_scope'],
        resolution='10min',
        station_ids=defaults['station_ids'],
        start=defaults['start'],
        end=defaults['end'],
        elements=defaults['elements'],
    )

    # Download normalized 10-minute observations and print a small preview.
    tenmin = download_observations(query)
    print(tenmin.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
