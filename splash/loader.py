from __future__ import annotations

import csv
import json
import sys

# Some CSVs have large fields (e.g. error_stack traces) that exceed the default 128 KB limit.
# Try sys.maxsize first; fall back to 2^31-1 on Windows where csv uses a C long (32-bit).
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path


class Column(StrEnum):
    # Core
    REPORT_NAME = "report_name"
    REPORT_TYPE = "report_type"
    PARAMETERS = "parameters"
    SCHEMA_NAME = "schema_name"
    # Timing
    START_DATETIME = "start_datetime"
    END_DATETIME = "end_datetime"
    BIRT_REPORT_STARTTIME = "birt_report_starttime"
    BIRT_REPORT_ENDTIME = "birt_report_endtime"
    # Status & errors
    REPORT_EXECUTION_STATUS_ID = "report_execution_status_id"
    ERROR_CODE = "error_code"
    ERROR_MESSAGE = "error_message"
    ERROR_STACK = "error_stack"
    # Engine routing (numeric IDs)
    ACTUAL_ENGINE = "actual_engine"
    EXPECTED_ENGINE = "expected_engine"
    REQUESTED_ENGINE = "requested_engine"
    ROUTE_TO_NODE = "route_to_node"
    # Output metrics
    OUTPUT_FILE_SIZE = "output_file_size"
    REPORT_OBJECT_COUNT = "report_object_count"


# Engine ID -> human-readable label
ENGINE_LABELS: dict[int, str] = {
    1: "ADHOC",
    2: "MEDIUM",
    3: "LARGE",
    4: "SMALL",
    6: "HCA",
}

# Status ID -> human-readable state
STATUS_LABELS: dict[int, str] = {
    1: "RUNNING",
    2: "COMPLETED",
    3: "FAILED",
    5: "SUSPENDED",
}

_FAILURE_STATUSES = {3, 5}


_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %I:%M:%S %p",
]

_DATETIME_COLUMNS = {
    Column.START_DATETIME,
    Column.END_DATETIME,
    Column.BIRT_REPORT_STARTTIME,
    Column.BIRT_REPORT_ENDTIME,
}
_INT_COLUMNS = {
    Column.OUTPUT_FILE_SIZE,
    Column.REPORT_OBJECT_COUNT,
    Column.REPORT_EXECUTION_STATUS_ID,
    Column.ACTUAL_ENGINE,
    Column.EXPECTED_ENGINE,
    Column.REQUESTED_ENGINE,
}


def _parse_datetime(value: str) -> datetime | str:
    v = value.strip()
    if not v:
        return v
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    return v


def _parse_int(value: str) -> int | str:
    v = value.strip()
    if not v:
        return v
    try:
        return int(float(v))
    except ValueError, OverflowError:
        return v


def _parse_json(value: str) -> dict | str:
    v = value.strip()
    if not v:
        return v
    try:
        return json.loads(v)
    except json.JSONDecodeError, ValueError:
        return v


def _normalize_value(col_name: str, value: str) -> object:
    lower = col_name.lower()
    if lower in {c.value for c in _DATETIME_COLUMNS}:
        return _parse_datetime(value)
    if col_name in _INT_COLUMNS or lower in {c.value for c in _INT_COLUMNS}:
        return _parse_int(value)
    if lower == Column.PARAMETERS:
        return _parse_json(value)
    return value.strip()


# Keys extracted from the parameters JSON into top-level row fields
PARAM_EXTRACTIONS = {
    "HyperFindSelector_name": "hyperfind_name",
    "WorkUnitHyperFind_Title": "work_unit_hyperfind_name",
    "TimeFrame_startDate": "report_timeframe_start_date",
    "TimeFrame_endDate": "report_timeframe_end_date",
}


def _extract_parameter_fields(row: dict) -> None:
    params = row.get(Column.PARAMETERS)
    if not isinstance(params, dict):
        return
    for json_key, row_key in PARAM_EXTRACTIONS.items():
        val = params.get(json_key, "")
        if val:
            row[row_key] = str(val)


@dataclass
class Dataset:
    rows: list[dict] = field(default_factory=list)
    available_columns: set[Column] = field(default_factory=set)
    all_headers: list[str] = field(default_factory=list)

    def has(self, *columns: Column) -> bool:
        return all(c in self.available_columns for c in columns)


def _detect_columns(headers: list[str]) -> set[Column]:
    known = {c.value for c in Column}
    return {Column(h) for h in headers if h in known}


def load_csvs(paths: list[Path]) -> Dataset:
    all_rows: list[dict] = []
    all_headers_set: set[str] = set()

    for path in paths:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            cleaned_fields = [h.strip().lower() for h in reader.fieldnames]
            all_headers_set.update(cleaned_fields)
            for raw_row in reader:
                row = {}
                for orig_key, value in raw_row.items():
                    if orig_key is None:
                        continue
                    clean_key = orig_key.strip().lower()
                    row[clean_key] = _normalize_value(clean_key, value or "")
                _extract_parameter_fields(row)
                if not row.get(Column.REPORT_NAME):
                    continue
                all_rows.append(row)

    all_headers = sorted(all_headers_set)
    available = _detect_columns(all_headers)

    required = {Column.REPORT_NAME, Column.REPORT_EXECUTION_STATUS_ID}
    missing = required - available
    if missing:
        raise SystemExit(
            f"Error: CSV data must contain columns: {', '.join(sorted(c.value for c in missing))}"
        )

    return Dataset(rows=all_rows, available_columns=available, all_headers=all_headers)
