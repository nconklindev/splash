# splash

<span style="color: green"><b>Spl</b></span>unk + D<span style="color: cyan"><b>ash</b></span>board = **Splash**. Yeah,
that's
it. That's how it got its name.

Splash is a CLI tool that takes CSV exports of BIRT report execution data and turns them into a self-contained HTML
dashboard you can open in any browser. No servers, no logins, no dependencies at runtime — just one HTML file with
interactive charts and tables.

## What It Does

Point it at one or more CSV files containing report history and get a dashboard covering:

- **Report Inventory** — what reports exist, how often they run, their hyperfinds and timeframes, parameter variations
- **Timing & Scheduling** — duration distribution, hourly/weekly patterns, overlapping runs
- **Error Analysis** — failure rates by report/engine/hour, concurrent load at failure time, error message grouping,
  full failure detail log
- **Engine Routing** — load distribution across engine sizes (SMALL/MEDIUM/LARGE/HCA/ADHOC), expected vs actual
  mismatches
- **Performance** — slowest reports, queue wait times, file size and object count stats, duration vs file size scatter

When your CSV includes a `schema_name` column, the dashboard gains a **multi-tenant drill-down**:

- A tenant selector filters all views to a single tenant
- A report selector within a tenant opens a **per-report detail view**

### Per-Report Detail View

Selecting a specific report shows a dedicated analysis panel with everything you need to diagnose what happened:

| Section                  | What you get                                                                                                                                                                                         |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **KPIs**                 | Total runs, failures, failure rate, avg/median/P90/max duration, avg/max queue time                                                                                                                  |
| **Status distribution**  | Donut chart — COMPLETED vs FAILED vs SUSPENDED at a glance                                                                                                                                           |
| **Engine distribution**  | Which engines this report actually ran on                                                                                                                                                            |
| **Error codes**          | Breakdown of error codes across failed runs (when failures exist)                                                                                                                                    |
| **Duration histogram**   | Spread of run times — reveals bimodal distributions                                                                                                                                                  |
| **Hourly run pattern**   | Which hours of the day this report executes                                                                                                                                                          |
| **Duration over time**   | Line chart with **failure runs highlighted as red dots**                                                                                                                                             |
| **Queue time over time** | Was the report waiting longer in queue over time?                                                                                                                                                    |
| **Execution log**        | Every run with start time, duration, queue time, status, engine, expected engine (when mismatches exist), node, file size, object count, error code, and error message. Failure rows are tinted red. |

## Installation

### Option A — uv + git (recommended)

Requires [uv](https://docs.astral.sh/uv/) and [git](https://git-scm.com/downloads). If you don't have uv, follow the
install instructions on [astral.sh](https://docs.astral.sh/uv/getting-started/installation/) — it's a single command.

```bash
uv tool install git+https://github.com/nconklindev/splash
```

That's it. `splash` will be available globally without needing to activate a virtual environment.

To update to the latest version later:

```bash
uv tool upgrade splash
```

### Option B — Python + pip (no uv required)

Requires Python >= 3.14. If you need to install Python, download it from [python.org](https://www.python.org/downloads/).

Clone or download this repository using git, then from the repo root:

```bash
pip install .
```

`splash` will be available as a command in your active Python environment.

## Quick Start

```bash
# Generate a dashboard
splash report_history.csv -o dashboard.html --open

# Multiple files, custom title
splash jan.csv feb.csv mar.csv --title "Q1 Report Analysis" -o q1.html
```

## Usage

```
splash <CSV_FILE>... [OPTIONS]

Options:
  -o, --output PATH     Output HTML path (default: splash_report.html)
  --title TEXT           Dashboard title (default: "Splash Report")
  --start-date DATE      Start date for report inventory (format: YYYY-MM-DD)
  --end-date DATE        End date for report inventory (format: YYYY-MM-DD)
  --open                Open in browser after generation
  -q, --quiet           Suppress info output
  --version             Show version
```

> [!TIP]
> Use the `--start-date` and `--end-date` options to limit the timeframe of the dashboard. This can be useful when
> you don't want to include all the data from the entire SQL query.

## Required CSV Columns

Only two columns are mandatory:

| Column                       | Description                                                      |
|------------------------------|------------------------------------------------------------------|
| `report_name`                | Name of the report                                               |
| `report_execution_status_id` | Execution status (1=Running, 2=Completed, 3=Failed, 5=Suspended) |

All other columns are optional — the dashboard adapts to what's available:

`schema_name`, `id`, `report_type`, `start_datetime`, `end_datetime`, `birt_report_starttime`, `birt_report_endtime`,
`actual_engine`, `expected_engine`, `requested_engine`, `route_to_node`, `error_code`, `error_message`, `error_stack`,
`output_file_size`, `report_object_count`, `parameters`

> [!TIP] 
> Including `schema_name` unlocks the multi-tenant drill-down. Including `parameters` enables
> hyperfind/timeframe extraction and parameter variation analysis.

Example: `WHERE start_datetime >= NOW() - INTERVAL '48 HOUR'` OR
`WHERE DATE(start_datetime) > (CURRENT_DATE - INTERVAL '1 month')`

> [!WARNING]
> Passing multiple CSV files to splash without an `id` field may result in duplicated data. We do not make any attempts
> to de-duplicate without the presence of this field. If you are passing multiple CSVs to the CLI, be sure `id` is
> included in your SQL query and the CSV. A single CSV file without the `id` field will be OK.

