from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from statistics import mean, median

from .loader import ENGINE_LABELS, STATUS_LABELS, Column, Dataset, _FAILURE_STATUSES
from .models import (
    DashboardData,
    EngineData,
    ErrorData,
    InventoryData,
    PerformanceData,
    TenantSummary,
    TimingData,
)

_DURATION_BUCKETS = [
    ("<1s", 0, 1),
    ("1-10s", 1, 10),
    ("10s-1m", 10, 60),
    ("1-5m", 60, 300),
    ("5-30m", 300, 1800),
    ("30m+", 1800, float("inf")),
]

_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def _get_duration_seconds(row: dict) -> float | None:
    start = row.get(Column.START_DATETIME) or row.get(Column.BIRT_REPORT_STARTTIME)
    end = row.get(Column.END_DATETIME) or row.get(Column.BIRT_REPORT_ENDTIME)
    if isinstance(start, datetime) and isinstance(end, datetime):
        return (end - start).total_seconds()
    return None


def _get_start(row: dict) -> datetime | None:
    v = row.get(Column.START_DATETIME) or row.get(Column.BIRT_REPORT_STARTTIME)
    return v if isinstance(v, datetime) else None


def _get_end(row: dict) -> datetime | None:
    v = row.get(Column.END_DATETIME) or row.get(Column.BIRT_REPORT_ENDTIME)
    return v if isinstance(v, datetime) else None


def _engine_label(engine_id) -> str:
    if isinstance(engine_id, int):
        return ENGINE_LABELS.get(engine_id, f"UNKNOWN ({engine_id})")
    return str(engine_id).strip() if engine_id else "UNKNOWN"


def _get_queue_time_seconds(row: dict) -> float | None:
    """Queue time = birt_report_starttime - start_datetime (time waiting before execution)."""
    start = row.get(Column.START_DATETIME)
    birt_start = row.get(Column.BIRT_REPORT_STARTTIME)
    if isinstance(start, datetime) and isinstance(birt_start, datetime):
        delta = (birt_start - start).total_seconds()
        return delta if delta >= 0 else None
    return None


def analyze_timing(dataset: Dataset) -> TimingData | None:
    has_start = dataset.has(Column.START_DATETIME) or dataset.has(
        Column.BIRT_REPORT_STARTTIME
    )
    has_end = dataset.has(Column.END_DATETIME) or dataset.has(
        Column.BIRT_REPORT_ENDTIME
    )
    if not (has_start and has_end):
        return None

    buckets: dict[str, int] = {label: 0 for label, *_ in _DURATION_BUCKETS}
    hourly: Counter[int] = Counter()
    weekly: Counter[str] = Counter()
    intervals: list[tuple[datetime, datetime, dict]] = []

    for row in dataset.rows:
        start = _get_start(row)
        end = _get_end(row)
        dur = _get_duration_seconds(row)

        if start:
            hourly[start.hour] += 1
            weekly[_DAY_NAMES[start.weekday()]] += 1

        if dur is not None and dur >= 0:
            for label, lo, hi in _DURATION_BUCKETS:
                if lo <= dur < hi:
                    buckets[label] += 1
                    break

        if isinstance(start, datetime) and isinstance(end, datetime):
            intervals.append((start, end, row))

    # Find overlapping runs — sort by start, check adjacent
    intervals.sort(key=lambda x: x[0])
    overlapping: list[dict] = []
    for i in range(len(intervals) - 1):
        _, end_a, row_a = intervals[i]
        start_b, _, row_b = intervals[i + 1]
        if start_b < end_a:
            if not overlapping or overlapping[-1] is not row_a:
                overlapping.append(row_a)
            overlapping.append(row_b)
    # Deduplicate while preserving order
    seen = set()
    deduped: list[dict] = []
    for r in overlapping:
        rid = id(r)
        if rid not in seen:
            seen.add(rid)
            deduped.append(r)

    hourly_dist = {h: hourly.get(h, 0) for h in range(24)}
    weekly_dist = {d: weekly.get(d, 0) for d in _DAY_NAMES}

    return TimingData(
        duration_buckets=buckets,
        hourly_distribution=hourly_dist,
        weekly_distribution=weekly_dist,
        overlapping_runs=deduped[:50],  # cap for display
    )


def analyze_inventory(dataset: Dataset) -> InventoryData:
    name_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter() if dataset.has(Column.REPORT_TYPE) else None
    param_sets: dict[str, set[str]] = {}

    # Track per-report info for overview table
    report_info: dict[str, dict] = {}

    for row in dataset.rows:
        name = row.get(Column.REPORT_NAME, "")
        name_counts[name] += 1

        if type_counts is not None:
            rt = row.get(Column.REPORT_TYPE, "")
            if rt:
                type_counts[rt] += 1

        if dataset.has(Column.PARAMETERS):
            params = row.get(Column.PARAMETERS, "")
            key = (
                json.dumps(params, sort_keys=True)
                if isinstance(params, dict)
                else str(params)
            )
            param_sets.setdefault(name, set()).add(key)

        # Build overview: keep first-seen values for each report
        if name not in report_info:
            info: dict = {"report_name": name}
            if dataset.has(Column.REPORT_TYPE):
                info["report_type"] = row.get(Column.REPORT_TYPE, "")
            info["hyperfind_name"] = row.get("hyperfind_name", "")
            info["work_unit_hyperfind_name"] = row.get("work_unit_hyperfind_name", "")
            info["timeframe_start"] = row.get("report_timeframe_start_date", "")
            info["timeframe_end"] = row.get("report_timeframe_end_date", "")
            report_info[name] = info

    param_variations = None
    if dataset.has(Column.PARAMETERS):
        param_variations = sorted(
            ((name, len(variants)) for name, variants in param_sets.items()),
            key=lambda x: x[1],
            reverse=True,
        )[:20]

    # Build overview table sorted by execution count desc
    report_overview = None
    if dataset.has(Column.PARAMETERS):
        report_overview = []
        for name, count in name_counts.most_common():
            entry = {**report_info[name], "executions": count}
            report_overview.append(entry)

    return InventoryData(
        unique_report_count=len(name_counts),
        reports_by_type=dict(type_counts.most_common()) if type_counts else None,
        top_reports_by_frequency=name_counts.most_common(20),
        parameter_variation_counts=param_variations,
        report_overview=report_overview,
    )


def _is_failure(row: dict) -> bool:
    # 1=RUNNING, 2=COMPLETED, 3=FAILED, 5=SUSPENDED
    status_id = row.get(Column.REPORT_EXECUTION_STATUS_ID)
    return isinstance(status_id, int) and status_id in _FAILURE_STATUSES


def analyze_errors(dataset: Dataset) -> ErrorData:
    from .loader import STATUS_LABELS

    total = len(dataset.rows)
    failures: list[dict] = []
    error_codes: Counter[str] = Counter()
    error_messages: Counter[str] = Counter()
    daily_failures: Counter[str] = Counter()
    hourly_failures: Counter[int] = Counter()
    report_failures: Counter[str] = Counter()
    report_totals: Counter[str] = Counter()
    engine_totals: Counter[str] = Counter()
    engine_failures: Counter[str] = Counter()

    # Build interval list for concurrent load calculation
    all_intervals: list[tuple[datetime, datetime, dict]] = []

    for row in dataset.rows:
        name = row.get(Column.REPORT_NAME, "unknown")
        report_totals[name] += 1

        # Track engine totals
        ae = row.get(Column.ACTUAL_ENGINE)
        if ae is not None and ae != "":
            engine_totals[_engine_label(ae)] += 1

        # Build intervals for concurrency
        start = _get_start(row)
        end = _get_end(row)
        if isinstance(start, datetime) and isinstance(end, datetime):
            all_intervals.append((start, end, row))

        if not _is_failure(row):
            continue

        failures.append(row)
        report_failures[name] += 1

        if ae is not None and ae != "":
            engine_failures[_engine_label(ae)] += 1

        if dataset.has(Column.ERROR_CODE):
            ec = row.get(Column.ERROR_CODE, "")
            if ec and str(ec).strip():
                error_codes[str(ec).strip()] += 1

        if dataset.has(Column.ERROR_MESSAGE):
            em = row.get(Column.ERROR_MESSAGE, "")
            if em and str(em).strip():
                error_messages[str(em).strip()] += 1

        if isinstance(start, datetime):
            daily_failures[start.strftime("%Y-%m-%d")] += 1
            hourly_failures[start.hour] += 1

    failure_count = len(failures)
    failure_rate = (failure_count / total * 100) if total else 0.0
    failures_per_day = sorted(daily_failures.items()) if daily_failures else []

    # 1. Failure detail table
    failure_detail = []
    for row in failures:
        start = _get_start(row)
        dur = _get_duration_seconds(row)
        qt = _get_queue_time_seconds(row)
        ae = row.get(Column.ACTUAL_ENGINE)
        status_id = row.get(Column.REPORT_EXECUTION_STATUS_ID)
        entry = {
            "report_name": row.get(Column.REPORT_NAME, ""),
            "status": STATUS_LABELS.get(status_id, f"UNKNOWN ({status_id})")
            if isinstance(status_id, int)
            else str(status_id),
            "start": start.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(start, datetime)
            else "",
            "duration_s": round(dur, 2) if dur is not None else None,
            "queue_s": round(qt, 2) if qt is not None else None,
            "engine": _engine_label(ae) if ae is not None and ae != "" else "",
            "error_code": str(row.get(Column.ERROR_CODE, "")).strip(),
            "error_message": str(row.get(Column.ERROR_MESSAGE, "")).strip(),
        }
        failure_detail.append(entry)
    # Sort by start time desc
    failure_detail.sort(key=lambda x: x["start"], reverse=True)

    # 2. Failure rate by report
    failure_rate_by_report = []
    for name in report_totals:
        t = report_totals[name]
        f = report_failures.get(name, 0)
        if t > 0:
            failure_rate_by_report.append(
                {
                    "name": name,
                    "total": t,
                    "failures": f,
                    "rate": round(f / t * 100, 1),
                }
            )
    failure_rate_by_report.sort(key=lambda x: x["rate"], reverse=True)

    # 3. Failures by engine
    failures_by_engine = None
    if dataset.has(Column.ACTUAL_ENGINE) and engine_totals:
        failures_by_engine = {}
        for eng in engine_totals:
            t = engine_totals[eng]
            f = engine_failures.get(eng, 0)
            failures_by_engine[eng] = {
                "total": t,
                "failures": f,
                "rate": round(f / t * 100, 1) if t else 0.0,
            }

    # 4. Failures by hour
    failures_by_hour = {h: hourly_failures.get(h, 0) for h in range(24)}

    # 5. Concurrent load at failure time
    all_intervals.sort(key=lambda x: x[0])
    concurrent_load = []
    for row in failures[:50]:  # cap for display
        f_start = _get_start(row)
        f_end = _get_end(row)
        if not isinstance(f_start, datetime) or not isinstance(f_end, datetime):
            continue
        count = 0
        for i_start, i_end, i_row in all_intervals:
            if i_row is row:
                continue
            # Overlaps if i_start < f_end and i_end > f_start
            if i_start < f_end and i_end > f_start:
                count += 1
        concurrent_load.append(
            {
                "report_name": row.get(Column.REPORT_NAME, ""),
                "start": f_start.strftime("%Y-%m-%d %H:%M:%S"),
                "concurrent_count": count,
                "error_code": str(row.get(Column.ERROR_CODE, "")).strip(),
            }
        )
    concurrent_load.sort(key=lambda x: x["concurrent_count"], reverse=True)

    # 6. Error message groups
    error_msg_groups = dict(error_messages.most_common()) if error_messages else None

    return ErrorData(
        total_executions=total,
        failure_count=failure_count,
        failure_rate=round(failure_rate, 2),
        error_code_distribution=dict(error_codes.most_common())
        if error_codes
        else None,
        failures_per_day=failures_per_day,
        most_failing_reports=report_failures.most_common(20),
        failure_detail=failure_detail,
        failure_rate_by_report=failure_rate_by_report,
        failures_by_engine=failures_by_engine,
        failures_by_hour=failures_by_hour,
        concurrent_load_at_failure=concurrent_load,
        error_message_groups=error_msg_groups,
    )


def analyze_engine(dataset: Dataset) -> EngineData | None:
    has_engine = dataset.has(Column.ACTUAL_ENGINE)
    has_node = dataset.has(Column.ROUTE_TO_NODE)
    if not (has_engine or has_node):
        return None

    engine_counts: Counter[str] = Counter()
    node_counts: Counter[str] = Counter()
    mismatches: list[dict] = []

    for row in dataset.rows:
        if has_engine:
            ae = row.get(Column.ACTUAL_ENGINE)
            if ae != "" and ae is not None:
                engine_counts[_engine_label(ae)] += 1
        if has_node:
            rn = row.get(Column.ROUTE_TO_NODE, "")
            if rn:
                node_counts[str(rn)] += 1

        if dataset.has(Column.ACTUAL_ENGINE) and dataset.has(Column.EXPECTED_ENGINE):
            actual = row.get(Column.ACTUAL_ENGINE)
            expected = row.get(Column.EXPECTED_ENGINE)
            if (
                actual != ""
                and expected != ""
                and actual is not None
                and expected is not None
                and actual != expected
            ):
                mismatches.append(row)

    total_with_both = (
        sum(
            1
            for r in dataset.rows
            if r.get(Column.ACTUAL_ENGINE) not in ("", None)
            and r.get(Column.EXPECTED_ENGINE) not in ("", None)
        )
        if dataset.has(Column.ACTUAL_ENGINE) and dataset.has(Column.EXPECTED_ENGINE)
        else None
    )

    mismatch_rate = None
    if total_with_both:
        mismatch_rate = round(len(mismatches) / total_with_both * 100, 2)

    # Build labeled mismatch samples for display
    mismatch_samples = None
    if mismatches:
        mismatch_samples = [
            {
                "report_name": r.get(Column.REPORT_NAME, ""),
                "expected": _engine_label(r.get(Column.EXPECTED_ENGINE)),
                "actual": _engine_label(r.get(Column.ACTUAL_ENGINE)),
            }
            for r in mismatches[:20]
        ]

    return EngineData(
        load_per_engine=dict(engine_counts.most_common()) if engine_counts else None,
        load_per_node=dict(node_counts.most_common()) if node_counts else None,
        mismatch_rate=mismatch_rate,
        mismatch_samples=mismatch_samples,
    )


def analyze_performance(dataset: Dataset) -> PerformanceData | None:
    has_start = dataset.has(Column.START_DATETIME) or dataset.has(
        Column.BIRT_REPORT_STARTTIME
    )
    has_end = dataset.has(Column.END_DATETIME) or dataset.has(
        Column.BIRT_REPORT_ENDTIME
    )
    if not (has_start and has_end):
        return None

    timed_rows: list[tuple[float, dict]] = []
    for row in dataset.rows:
        dur = _get_duration_seconds(row)
        if dur is not None and dur >= 0:
            timed_rows.append((dur, row))

    if not timed_rows:
        return None

    timed_rows.sort(key=lambda x: x[0], reverse=True)
    slowest = []
    for dur, row in timed_rows[:20]:
        entry = {
            "report_name": row.get(Column.REPORT_NAME, ""),
            "duration_s": round(dur, 2),
        }
        start = _get_start(row)
        if isinstance(start, datetime):
            entry["start"] = start.strftime("%Y-%m-%d %H:%M:%S")
        ae = row.get(Column.ACTUAL_ENGINE)
        if ae != "" and ae is not None:
            entry["engine"] = _engine_label(ae)
        qt = _get_queue_time_seconds(row)
        if qt is not None:
            entry["queue_s"] = round(qt, 2)
        slowest.append(entry)

    # Duration vs file size scatter
    dur_vs_size = None
    if dataset.has(Column.OUTPUT_FILE_SIZE):
        points = []
        for dur, row in timed_rows:
            size = row.get(Column.OUTPUT_FILE_SIZE)
            if isinstance(size, int):
                points.append(
                    {
                        "name": row.get(Column.REPORT_NAME, ""),
                        "duration_s": round(dur, 2),
                        "size": size,
                    }
                )
        if points:
            dur_vs_size = points[:500]  # cap for chart performance

    # Object count stats
    obj_stats = None
    if dataset.has(Column.REPORT_OBJECT_COUNT):
        counts = [
            row.get(Column.REPORT_OBJECT_COUNT)
            for row in dataset.rows
            if isinstance(row.get(Column.REPORT_OBJECT_COUNT), int)
        ]
        if counts:
            obj_stats = {
                "min": min(counts),
                "max": max(counts),
                "mean": round(mean(counts), 2),
                "median": round(median(counts), 2),
            }

    # File size stats
    size_stats = None
    if dataset.has(Column.OUTPUT_FILE_SIZE):
        sizes = [
            row.get(Column.OUTPUT_FILE_SIZE)
            for row in dataset.rows
            if isinstance(row.get(Column.OUTPUT_FILE_SIZE), int)
        ]
        if sizes:
            size_stats = {
                "min": min(sizes),
                "max": max(sizes),
                "mean": round(mean(sizes), 2),
                "median": round(median(sizes), 2),
            }

    # Queue time stats (birt_report_starttime - start_datetime)
    queue_stats = None
    if dataset.has(Column.START_DATETIME) and dataset.has(Column.BIRT_REPORT_STARTTIME):
        queue_times = [
            qt
            for row in dataset.rows
            if (qt := _get_queue_time_seconds(row)) is not None and qt >= 0
        ]
        if queue_times:
            queue_stats = {
                "min": round(min(queue_times), 2),
                "max": round(max(queue_times), 2),
                "mean": round(mean(queue_times), 2),
                "median": round(median(queue_times), 2),
            }

    return PerformanceData(
        slowest_reports=slowest,
        duration_vs_size=dur_vs_size,
        object_count_stats=obj_stats,
        file_size_stats=size_stats,
        queue_time_stats=queue_stats,
    )


def _build_tenant_json(tenant_rows: list[dict], dataset: Dataset) -> dict:
    """Build a JSON-serializable dict for a single tenant, running all five analyses."""
    sub_ds = Dataset(
        rows=tenant_rows,
        available_columns=dataset.available_columns,
        all_headers=dataset.all_headers,
    )

    inv = analyze_inventory(sub_ds)
    timing = analyze_timing(sub_ds)
    errors = analyze_errors(sub_ds)
    engine = analyze_engine(sub_ds)
    perf = analyze_performance(sub_ds)

    # Overlapping runs — raw rows still contain datetime objects; stringify them
    overlapping_runs = []
    if timing:
        for row in timing.overlapping_runs[:15]:
            start = row.get(Column.START_DATETIME) or row.get(
                Column.BIRT_REPORT_STARTTIME
            )
            end = row.get(Column.END_DATETIME) or row.get(Column.BIRT_REPORT_ENDTIME)
            overlapping_runs.append(
                {
                    "report_name": row.get(Column.REPORT_NAME, ""),
                    "start_datetime": start.strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(start, datetime)
                    else str(start or ""),
                    "end_datetime": end.strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(end, datetime)
                    else str(end or ""),
                }
            )

    # Per-report drill-down data
    report_buckets: dict[str, dict] = {}
    for row in tenant_rows:
        name = row.get(Column.REPORT_NAME, "")
        if not name:
            continue
        if name not in report_buckets:
            report_buckets[name] = {
                "total": 0,
                "failures": 0,
                "durations": [],
                "queue_times": [],
                "executions": [],
            }
        rb = report_buckets[name]
        rb["total"] += 1
        if _is_failure(row):
            rb["failures"] += 1
        dur = _get_duration_seconds(row)
        if dur is not None and dur >= 0:
            rb["durations"].append(dur)
        qt = _get_queue_time_seconds(row)
        if qt is not None and qt >= 0:
            rb["queue_times"].append(qt)
        start = _get_start(row)
        ae = row.get(Column.ACTUAL_ENGINE)
        ee = row.get(Column.EXPECTED_ENGINE)
        status_id = row.get(Column.REPORT_EXECUTION_STATUS_ID)
        fs = row.get(Column.OUTPUT_FILE_SIZE)
        oc = row.get(Column.REPORT_OBJECT_COUNT)
        rn = row.get(Column.ROUTE_TO_NODE)
        rb["executions"].append(
            {
                "start": start.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(start, datetime)
                else "",
                "duration_s": round(dur, 2) if dur is not None else None,
                "queue_s": round(qt, 2) if qt is not None else None,
                "status": STATUS_LABELS.get(status_id, f"UNKNOWN ({status_id})")
                if isinstance(status_id, int)
                else str(status_id or ""),
                "engine": _engine_label(ae) if ae is not None and ae != "" else "",
                "expected_engine": _engine_label(ee)
                if ee is not None and ee != ""
                else "",
                "node": str(rn).strip() if rn else "",
                "file_size": fs if isinstance(fs, int) else None,
                "object_count": oc if isinstance(oc, int) else None,
                "error_code": str(row.get(Column.ERROR_CODE, "")).strip(),
                "error_message": str(row.get(Column.ERROR_MESSAGE, "")).strip(),
            }
        )

    reports: dict[str, dict] = {}
    for name, rb in report_buckets.items():
        rb["executions"].sort(key=lambda x: x["start"])
        total = rb["total"]
        failures = rb["failures"]
        durations = rb["durations"]
        queue_times = rb["queue_times"]
        sorted_dur = sorted(durations)
        p90_dur = (
            round(sorted_dur[int(len(sorted_dur) * 0.9)], 2)
            if len(sorted_dur) >= 10
            else None
        )
        reports[name] = {
            "total": total,
            "failures": failures,
            "failure_rate": round(failures / total * 100, 2) if total else 0.0,
            "avg_duration_s": round(mean(durations), 2) if durations else None,
            "median_duration_s": round(median(durations), 2) if durations else None,
            "p90_duration_s": p90_dur,
            "max_duration_s": round(max(durations), 2) if durations else None,
            "min_duration_s": round(min(durations), 2) if durations else None,
            "avg_queue_s": round(mean(queue_times), 2) if queue_times else None,
            "max_queue_s": round(max(queue_times), 2) if queue_times else None,
            "executions": rb["executions"][:500],
        }

    return {
        # Inventory
        "unique_report_count": inv.unique_report_count,
        "top_reports_by_frequency": [
            [n, c] for n, c in inv.top_reports_by_frequency[:15]
        ],
        "reports_by_type": inv.reports_by_type,
        "report_overview": inv.report_overview,
        "parameter_variation_counts": [
            [n, c] for n, c in inv.parameter_variation_counts
        ]
        if inv.parameter_variation_counts
        else None,
        # Timing
        "duration_buckets": dict(timing.duration_buckets) if timing else {},
        "hourly_distribution": {
            str(k): v for k, v in timing.hourly_distribution.items()
        }
        if timing
        else {},
        "weekly_distribution": dict(timing.weekly_distribution) if timing else {},
        "overlapping_runs": overlapping_runs,
        # Errors
        "total_executions": errors.total_executions,
        "failure_count": errors.failure_count,
        "failure_rate": errors.failure_rate,
        "error_code_distribution": errors.error_code_distribution,
        "failures_per_day": [[d, c] for d, c in errors.failures_per_day],
        "failure_rate_by_report": errors.failure_rate_by_report,
        "failures_by_hour": {str(k): v for k, v in errors.failures_by_hour.items()},
        "failures_by_engine": errors.failures_by_engine,
        "concurrent_load_at_failure": errors.concurrent_load_at_failure[:20],
        "error_message_groups": errors.error_message_groups,
        "failure_detail": errors.failure_detail[:200],
        # Engine
        "load_per_engine": engine.load_per_engine if engine else None,
        "load_per_node": engine.load_per_node if engine else None,
        "mismatch_rate": engine.mismatch_rate if engine else None,
        "mismatch_samples": engine.mismatch_samples if engine else None,
        # Performance
        "slowest_reports": perf.slowest_reports if perf else [],
        "duration_vs_size": perf.duration_vs_size if perf else None,
        "file_size_stats": perf.file_size_stats if perf else None,
        "object_count_stats": perf.object_count_stats if perf else None,
        "queue_time_stats": perf.queue_time_stats if perf else None,
        # Per-report drill-down
        "reports": reports,
    }


def run_all_analyses(
    dataset: Dataset,
    *,
    title: str,
    csv_files: list[str],
) -> DashboardData:
    dd = DashboardData(
        title=title,
        generated_at=datetime.now(),
        total_rows=len(dataset.rows),
        available_columns=sorted(c.value for c in dataset.available_columns),
        csv_files=csv_files,
        timing=analyze_timing(dataset),
        inventory=analyze_inventory(dataset),
        errors=analyze_errors(dataset),
        engine=analyze_engine(dataset),
        performance=analyze_performance(dataset),
    )

    if dataset.has(Column.SCHEMA_NAME):
        tenant_groups: dict[str, list[dict]] = {}
        for row in dataset.rows:
            sn = row.get(Column.SCHEMA_NAME, "")
            if not sn:
                continue
            tenant_groups.setdefault(str(sn), []).append(row)

        if tenant_groups:
            summaries = []
            for sn, rows in tenant_groups.items():
                failure_count = sum(1 for r in rows if _is_failure(r))
                unique_reports = len({r.get(Column.REPORT_NAME, "") for r in rows})
                total = len(rows)
                summaries.append(
                    TenantSummary(
                        schema_name=sn,
                        total_executions=total,
                        failure_count=failure_count,
                        failure_rate=round(failure_count / total * 100, 2)
                        if total
                        else 0.0,
                        unique_reports=unique_reports,
                    )
                )
            summaries.sort(key=lambda x: x.total_executions, reverse=True)
            dd.tenant_summaries = summaries
            dd.per_tenant_json = {
                sn: _build_tenant_json(rows, dataset)
                for sn, rows in tenant_groups.items()
            }

    return dd
