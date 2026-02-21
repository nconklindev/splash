from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TimingData:
    duration_buckets: dict[str, int]  # bucket label -> count
    hourly_distribution: dict[int, int]  # hour (0-23) -> count
    weekly_distribution: dict[str, int]  # day name -> count
    overlapping_runs: list[dict]  # rows where runs overlap


@dataclass
class InventoryData:
    unique_report_count: int
    reports_by_type: dict[str, int] | None  # type -> count (None if no report_type col)
    top_reports_by_frequency: list[tuple[str, int]]  # (report_name, count)
    parameter_variation_counts: list[tuple[str, int]] | None  # (report_name, distinct param count)
    report_overview: list[dict] | None  # per-report summary with extracted param fields


@dataclass
class ErrorData:
    total_executions: int
    failure_count: int
    failure_rate: float
    error_code_distribution: dict[str, int] | None  # code -> count
    failures_per_day: list[tuple[str, int]]  # (date str, count)
    most_failing_reports: list[tuple[str, int]]  # (report_name, failure count)
    # Extended failure analysis
    failure_detail: list[dict]  # full log of each failed execution
    failure_rate_by_report: list[dict]  # [{name, total, failures, rate}, ...] sorted by rate desc
    failures_by_engine: dict[str, dict] | None  # engine label -> {total, failures, rate}
    failures_by_hour: dict[int, int]  # hour (0-23) -> failure count
    concurrent_load_at_failure: list[dict]  # [{report_name, start, concurrent_count, ...}]
    error_message_groups: dict[str, int] | None  # error message -> count


@dataclass
class EngineData:
    load_per_engine: dict[str, int] | None  # engine -> count
    load_per_node: dict[str, int] | None  # node -> count
    mismatch_rate: float | None
    mismatch_samples: list[dict] | None  # sample rows with mismatches


@dataclass
class PerformanceData:
    slowest_reports: list[dict]  # rows sorted by duration desc
    duration_vs_size: list[dict] | None  # [{name, duration_s, size}, ...]
    object_count_stats: dict[str, float] | None  # min, max, mean, median
    file_size_stats: dict[str, float] | None  # min, max, mean, median
    queue_time_stats: dict[str, float] | None  # min, max, mean, median (seconds)


@dataclass
class DashboardData:
    title: str
    generated_at: datetime
    total_rows: int
    available_columns: list[str]
    csv_files: list[str]
    timing: TimingData | None = None
    inventory: InventoryData | None = None
    errors: ErrorData | None = None
    engine: EngineData | None = None
    performance: PerformanceData | None = None
