from __future__ import annotations

from pathlib import Path

from examples.workflows import build_cz_fao_wind_height_candidate as workflow


def test_run_cz_fao_wind_height_candidate_from_repo_cache() -> None:
    output_dir = Path('outputs/fao_daily.cz.wind_height_candidate')
    mat_output = Path('outputs/fao_daily.cz.wind_height_candidate.mat')
    audit_csv = Path('outputs/fao_daily.cz.wind_height_audit.csv')
    summary_json = Path('outputs/fao_daily.cz.wind_height_audit_summary.json')

    exit_code = workflow.main(
        [
            '--cache-dir',
            'outputs/fao_cache',
            '--output-dir',
            str(output_dir),
            '--mat-output',
            str(mat_output),
            '--audit-csv',
            str(audit_csv),
            '--summary-json',
            str(summary_json),
        ]
    )

    assert exit_code == 0
    assert (output_dir / 'stations.parquet').exists()
    assert (output_dir / 'series.parquet').exists()
    assert (output_dir / 'data_info.json').exists()
    assert mat_output.exists()
    assert audit_csv.exists()
    assert summary_json.exists()
