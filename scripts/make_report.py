"""
make_report.py
===============

Generate a simple Markdown report summarising the results of the composite
alert analysis.  The report includes a summary table of event counts and
a detailed listing of each event with its reason.  It writes the report
to ``assets/report_composite.md``.

Usage::

    python scripts/make_report.py

The script assumes that ``build_composite_alerts.py`` has already been
executed and that the alerts file exists at ``data/processed/alerts_composite.csv``.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import os


def main() -> Path:
    # Ensure working directory is project root
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)

    alerts_path = Path('data/processed/alerts_composite.csv')
    if not alerts_path.exists():
        raise FileNotFoundError(f'Alerts file not found: {alerts_path}')
    alerts = pd.read_csv(alerts_path, parse_dates=['date'])

    # Summarise event counts
    summary = alerts['event_type'].value_counts().rename_axis('event_type').to_frame('count')

    # Build report content
    report_lines = []
    report_lines.append('# Composite Alert Report')
    report_lines.append('')
    report_lines.append('## Event Summary')
    report_lines.append('')
    report_lines.append(summary.to_markdown())
    report_lines.append('')
    report_lines.append('## Event Details')
    report_lines.append('')
    # Sort events by date for readability
    alerts_sorted = alerts.sort_values('date')
    # Limit columns to essential fields
    details = alerts_sorted[['date', 'event_type', 'reason']].copy()
    report_lines.append(details.to_markdown(index=False))

    # Write to file
    out_path = Path('assets') / 'report_composite.md'
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f'[OK] Report written to {out_path}')
    return out_path


if __name__ == '__main__':
    main()