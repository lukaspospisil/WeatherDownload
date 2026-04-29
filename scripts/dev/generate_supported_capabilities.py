from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from weatherdownload import list_providers, list_resolutions, list_supported_countries, list_supported_elements
from weatherdownload.providers import get_provider


OUTPUT_PATH = Path('docs/supported_capabilities.md')


@dataclass(frozen=True)
class CapabilityRow:
    country: str
    provider: str
    resolution: str
    elements: tuple[str, ...]
    source_description: str
    notes: str


SOURCE_DESCRIPTIONS: dict[tuple[str, str, str], str] = {
    ('AT', 'historical', 'daily'): 'GeoSphere Austria historical daily station observations',
    ('AT', 'historical', '1hour'): 'GeoSphere Austria historical hourly station observations',
    ('AT', 'historical', '10min'): 'GeoSphere Austria historical 10-minute station observations',
    ('AT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('BE', 'historical', 'daily'): 'RMI/KMI AWS daily observations',
    ('BE', 'historical', '1hour'): 'RMI/KMI AWS hourly observations',
    ('BE', 'historical', '10min'): 'RMI/KMI AWS 10-minute observations',
    ('CA', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('CH', 'historical', 'daily'): 'MeteoSwiss A1 historical daily station observations',
    ('CH', 'historical', '1hour'): 'MeteoSwiss A1 historical hourly station observations',
    ('CH', 'historical', '10min'): 'MeteoSwiss A1 historical 10-minute station observations',
    ('CH', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('CZ', 'historical_csv', 'daily'): 'CHMI historical CSV daily observations',
    ('CZ', 'historical_csv', '1hour'): 'CHMI historical CSV hourly observations',
    ('CZ', 'historical_csv', '10min'): 'CHMI historical CSV 10-minute observations',
    ('CZ', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('DE', 'historical', 'daily'): 'DWD historical daily station observations',
    ('DE', 'historical', '1hour'): 'DWD historical hourly station observations',
    ('DE', 'historical', '10min'): 'DWD historical 10-minute station observations',
    ('DE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('DK', 'historical', 'daily'): 'DMI historical daily station observations',
    ('DK', 'historical', '1hour'): 'DMI historical hourly station observations',
    ('DK', 'historical', '10min'): 'DMI historical 10-minute station observations',
    ('DK', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('FI', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('FR', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('HU', 'historical', 'daily'): 'HungaroMet historical daily station observations',
    ('HU', 'historical', '1hour'): 'HungaroMet historical hourly station observations',
    ('HU', 'historical', '10min'): 'HungaroMet historical 10-minute station observations',
    ('HU', 'historical_wind', '10min'): 'HungaroMet historical 10-minute wind station observations',
    ('IT', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('MX', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('NL', 'historical', 'daily'): 'KNMI validated daily in-situ observations',
    ('NL', 'historical', '1hour'): 'KNMI validated hourly in-situ observations',
    ('NL', 'historical', '10min'): 'KNMI near-real-time 10-minute in-situ observations',
    ('NO', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('NZ', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('PL', 'historical', 'daily'): 'IMGW-PIB historical daily synop station observations',
    ('PL', 'historical', '1hour'): 'IMGW-PIB historical hourly synop station observations',
    ('PL', 'historical_klimat', 'daily'): 'IMGW-PIB historical daily klimat station observations',
    ('SE', 'historical', 'daily'): 'SMHI corrected-archive daily observations',
    ('SE', 'historical', '1hour'): 'SMHI corrected-archive hourly observations',
    ('SE', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('SK', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
    ('SK', 'recent', 'daily'): 'SHMU recent daily climatological stations',
    ('US', 'ghcnd', 'daily'): 'NOAA GHCN-Daily',
}

NOTES: dict[tuple[str, str, str], str] = {
    ('AT', 'historical', 'daily'): 'National GeoSphere daily path; `ghcnd` is an additional daily provider.',
    ('AT', 'historical', '1hour'): 'Single public hourly provider for AT.',
    ('AT', 'historical', '10min'): 'Single public 10-minute provider for AT.',
    ('AT', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix AU; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('BE', 'historical', 'daily'): 'Provider-side daily aggregates; raw QC stays in flag.',
    ('BE', 'historical', '1hour'): 'Provider-side hourly aggregates; raw QC stays in flag.',
    ('BE', 'historical', '10min'): 'Raw 10-minute path; no derived daily/hourly recomputation.',
    ('CA', 'ghcnd', 'daily'): 'Raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('CH', 'historical', 'daily'): 'MeteoSwiss daily path; FAO reference evaporation exists but is intentionally not mapped to open_water_evaporation.',
    ('CH', 'historical', '1hour'): 'FAO reference evaporation is not mapped to open_water_evaporation.',
    ('CH', 'historical', '10min'): 'FAO reference evaporation is not mapped to open_water_evaporation.',
    ('CH', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SZ; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('CZ', 'historical_csv', 'daily'): 'Measured open-water evaporation supported via raw VY.',
    ('CZ', 'historical_csv', '1hour'): 'Implemented CHMI historical CSV hourly path.',
    ('CZ', 'historical_csv', '10min'): 'Implemented CHMI historical CSV 10-minute path.',
    ('CZ', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix EZ; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('DE', 'historical', 'daily'): 'National DWD daily path; `ghcnd` is an additional daily provider.',
    ('DE', 'historical', '1hour'): 'Single public hourly provider for DE.',
    ('DE', 'historical', '10min'): 'Single public 10-minute provider for DE.',
    ('DE', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix GM; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('DK', 'historical', 'daily'): 'National DMI daily path; `ghcnd` is an additional daily provider.',
    ('DK', 'historical', '1hour'): 'Single public hourly provider for DK.',
    ('DK', 'historical', '10min'): 'Meteorological Observation API path for 10-minute data.',
    ('DK', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix DA; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('FI', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('FR', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('HU', 'historical', 'daily'): 'Generic HungaroMet archive path; open_water_evaporation not implemented.',
    ('HU', 'historical', '1hour'): 'Generic HungaroMet archive path; open_water_evaporation not implemented.',
    ('HU', 'historical', '10min'): 'Generic HungaroMet archive path; separate wind-only product exists.',
    ('HU', 'historical_wind', '10min'): 'Separate HungaroMet wind-only 10-minute product.',
    ('IT', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('MX', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('NL', 'historical', 'daily'): 'Requires KNMI API key.',
    ('NL', 'historical', '1hour'): 'Requires KNMI API key.',
    ('NL', 'historical', '10min'): 'Requires KNMI API key; near-real-time rather than validated historical product.',
    ('NO', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('NZ', 'ghcnd', 'daily'): 'Thin shared GHCN wrapper; raw GHCN station ids; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('PL', 'historical', 'daily'): 'IMGW synop daily path; open_water_evaporation not implemented.',
    ('PL', 'historical', '1hour'): 'IMGW synop hourly path; open_water_evaporation not implemented.',
    ('PL', 'historical_klimat', 'daily'): 'Separate IMGW klimat daily path.',
    ('SE', 'historical', 'daily'): 'Corrected-archive excludes latest three months; `ghcnd` is an additional daily provider.',
    ('SE', 'historical', '1hour'): 'Corrected-archive excludes latest three months.',
    ('SE', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix SW; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('SK', 'ghcnd', 'daily'): 'Mapped-prefix GHCN wrapper using raw GHCN station ids with prefix LO; inventory-driven station elements; no EVAP/open_water_evaporation.',
    ('SK', 'recent', 'daily'): 'Experimental provider; measured water-surface evaporation supported via raw voda_vypar.',
    ('US', 'ghcnd', 'daily'): 'Raw GHCN station ids; inventory-driven station elements; measured open_water_evaporation supported via raw EVAP.',
}


def _collect_rows() -> list[CapabilityRow]:
    rows: list[CapabilityRow] = []
    for country in list_supported_countries():
        weather_provider = get_provider(country)
        for spec in weather_provider.list_implemented_dataset_specs():
            provider = spec.provider
            resolution = spec.resolution
            elements = tuple(list_supported_elements(country=country, provider=provider, resolution=resolution))
            key = (country, provider, resolution)
            rows.append(
                CapabilityRow(
                    country=country,
                    provider=provider,
                    resolution=resolution,
                    elements=elements,
                    source_description=SOURCE_DESCRIPTIONS.get(key, getattr(spec, 'label', weather_provider.name)),
                    notes=NOTES.get(key, ''),
                )
            )
    resolution_order = {'daily': 0, '1hour': 1, '10min': 2}
    return sorted(rows, key=lambda row: (row.country, row.provider, resolution_order.get(row.resolution, 99), row.resolution))


def render_supported_capabilities_markdown() -> str:
    rows = _collect_rows()
    lines = [
        '# Supported Capabilities',
        '',
        '<p align="right">',
        '  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">',
        '</p>',
        '',
        'This page is generated from the current provider registry and discovery APIs, then checked in tests so it stays aligned with the code.',
        '',
        'Navigation:',
        '',
        '- conceptual provider model: [Provider Model](providers.md)',
        '- provider-specific source notes: [Provider Notes](provider_notes/README.md)',
        '- canonical element semantics: [Canonical Elements](canonical_elements.md)',
        '',
        'Conceptual model:',
        '',
        '- `country` selects the country',
        '- `provider` selects the concrete data source or product within that country',
        '- `resolution` selects the temporal resolution',
        '- `element` selects the canonical meteorological variable',
        '- provider values are provider-specific and are not globally standardized',
        '',
        'Programmatic discovery:',
        '',
        '```python',
        'from weatherdownload import list_providers, list_resolutions, list_supported_elements',
        '',
        'list_providers(country="CZ")',
        'list_resolutions(country="US", provider="ghcnd")',
        'list_supported_elements(country="US", provider="ghcnd", resolution="daily")',
        '```',
        '',
        'CLI note:',
        '',
        '- the current CLI does not expose a country-wide capability-listing command',
        '- `weatherdownload stations elements ...` is station-level inspection and requires `--station-id`',
        '- for country-wide provider/resolution/element discovery, use the Python discovery functions above',
        '',
        'Representative station-level CLI examples:',
        '',
        '```powershell',
        'weatherdownload stations elements --country CZ --station-id 0-20000-0-11406 --provider historical_csv --resolution daily',
        'weatherdownload stations elements --country CZ --station-id EZM00011406 --provider ghcnd --resolution daily',
        'weatherdownload stations elements --country US --station-id USC00000001 --provider ghcnd --resolution daily',
        '```',
        '',
        '## Capability Table',
        '',
        '| Country | Provider | Resolution | Supported canonical elements | Provider/source description | Important notes |',
        '| --- | --- | --- | --- | --- | --- |',
    ]
    for row in rows:
        elements = ', '.join(f'`{element}`' for element in row.elements)
        lines.append(
            f"| `{row.country}` | `{row.provider}` | `{row.resolution}` | {elements} | {row.source_description} | {row.notes} |"
        )

    lines.extend([
        '',
        '## Open-Water Evaporation',
        '',
        'Measured `open_water_evaporation` is currently supported only for:',
        '',
        '- `CZ / historical_csv / daily` via CHMI raw `VY`',
        '- `SK / recent / daily` via SHMU raw `voda_vypar`',
        '- `US / ghcnd / daily` via NOAA GHCN-Daily raw `EVAP`',
        '',
        'It is intentionally not supported for:',
        '',
        '- `AT`, `CA`, `CH`, `CZ`, `DE`, `DK`, `FI`, `FR`, `IT`, `MX`, `NO`, `NZ`, `SE`, and `SK` on the current `ghcnd / daily` wrappers',
        '- `CH` MeteoSwiss FAO reference evaporation fields, because they are not measured open-water or pan evaporation',
        '- `HU`, `PL`, and other providers unless a measured open-water, pan, or evaporimeter variable is explicitly implemented',
        '',
        '## Coverage Scope',
        '',
        '- this table lists currently implemented provider/resolution paths',
        '- it is derived from `list_providers()`, `list_resolutions()`, `list_supported_elements()`, and provider registry metadata',
        '- station-level availability can still be narrower on inventory-driven providers such as GHCN-Daily',
    ])
    return '\n'.join(lines) + '\n'


def write_supported_capabilities_doc(output_path: Path = OUTPUT_PATH) -> Path:
    output_path.write_text(render_supported_capabilities_markdown(), encoding='utf-8')
    return output_path


if __name__ == '__main__':
    destination = write_supported_capabilities_doc()
    print(f'Wrote {destination}')
