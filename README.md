# splash

**Spl**unk + D**ash**board = **Splash**. Yeah, that's it. That's how it got its name.

Splash is a CLI tool that takes CSV exports of BIRT report execution data and turns them into a self-contained HTML dashboard you can open in any browser. No servers, no logins, no dependencies at runtime — just one HTML file with interactive charts and tables.

## What It Does

Point it at one or more CSV files from your Splunk/PostgreSQL report history and get a dashboard covering:

- **Report Inventory** — what reports exist, how often they run, their hyperfinds and timeframes
- **Timing & Scheduling** — duration distribution, hourly/weekly patterns, overlapping runs
- **Error Analysis** — failure rates by report/engine/hour, concurrent load at failure time, error message grouping, full failure detail log
- **Engine Routing** — load distribution across engine sizes (SMALL/MEDIUM/LARGE/HCA/ADHOC), expected vs actual mismatches
- **Performance** — slowest reports, queue wait times, file size and object count stats

## Quick Start

Requires Python >= 3.14 and [uv](https://docs.astral.sh/uv/).

```bash
# Install
uv sync

# Generate a dashboard
uv run splash report_history.csv -o dashboard.html --open

# Multiple files, custom title
uv run splash jan.csv feb.csv mar.csv --title "Q1 Report Analysis" -o q1.html
```

## Usage

```
splash <CSV_FILE>... [OPTIONS]

Options:
  -o, --output PATH     Output HTML path (default: splash_report.html)
  --title TEXT           Dashboard title (default: "Splash Report")
  --open                Open in browser after generation
  -q, --quiet           Suppress info output
  --version             Show version
```

## Required CSV Columns

Only two columns are mandatory:

| Column | Description |
|---|---|
| `report_name` | Name of the report |
| `report_execution_status_id` | Execution status (1=Running, 2=Completed, 3=Failed, 5=Suspended) |

All other columns are optional — the dashboard adapts to what's available:

`report_type`, `start_datetime`, `end_datetime`, `birt_report_starttime`, `birt_report_endtime`, `actual_engine`, `expected_engine`, `requested_engine`, `route_to_node`, `error_code`, `error_message`, `error_stack`, `output_file_size`, `report_object_count`, `parameters`

## Tech Stack

- **Python** with Click (CLI) and Jinja2 (templating)
- **Tailwind CSS** and **Chart.js** loaded via CDN in the output HTML
- No pandas, no heavyweight dependencies — just stdlib `csv` for parsing
