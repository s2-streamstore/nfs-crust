#!/usr/bin/env python3
"""Generate an offline, theme-aware HTML fragment from one benchmark run."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable


KIB = 1024
MIB = 1024 * KIB
SERIES_CLASSES = ["series-1", "series-2", "series-3", "series-4", "series-5", "series-6"]
QUICK_OPEN_LOOP_MAX_OUTSTANDING = 32
STANDARD_OPEN_LOOP_MAX_OUTSTANDING = 1024


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def median(values: Iterable[float]) -> float:
    materialized = list(values)
    return statistics.median(materialized) if materialized else 0.0


@lru_cache(maxsize=None)
def bootstrap_resample_indices(count: int) -> tuple[tuple[int, ...], ...]:
    rng = random.Random(0x4E46534352555354 + count)
    return tuple(
        tuple(rng.randrange(count) for _ in range(count)) for _ in range(10_000)
    )


def bootstrap_median_interval(values: list[float]) -> tuple[float, float]:
    if len(values) <= 1:
        value = values[0] if values else 0.0
        return value, value
    medians = sorted(
        statistics.median(values[index] for index in indices)
        for indices in bootstrap_resample_indices(len(values))
    )
    return quantile(medians, 0.025), quantile(medians, 0.975)


def quantile(values: list[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[min(rank - 1, len(ordered) - 1)]


def fmt_size(value: float | int) -> str:
    value = float(value)
    if value >= MIB:
        return f"{value / MIB:g} MiB"
    if value >= KIB:
        return f"{value / KIB:g} KiB"
    return f"{value:g} B"


def fmt_latency(value_us: float) -> str:
    if value_us >= 1_000_000:
        return f"{value_us / 1_000_000:.2f} s"
    if value_us >= 1_000:
        return f"{value_us / 1_000:.2f} ms"
    if value_us >= 10:
        return f"{value_us:.1f} µs"
    return f"{value_us:.2f} µs"


def fmt_rate(value: float) -> str:
    if value >= 10_000:
        return f"{value / 1_000:.1f}k ops/s"
    if value >= 100:
        return f"{value:,.0f} ops/s"
    return f"{value:,.1f} ops/s"


def fmt_mibps(value: float) -> str:
    return f"{value:,.1f} MiB/s"


def fmt_cpu_ms(value: float) -> str:
    return f"{value:.3f} CPU-ms/op"


def fmt_bytes(value: float) -> str:
    return fmt_size(value) + "/op"


def fmt_percent(value: float) -> str:
    return f"{value * 100:.3g}%"


def fmt_timestamp(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def display_backend(value: str) -> str:
    return {
        "nfs-crust": "nfs-crust",
        "linux-remote": "Linux reading from EFS",
        "linux-remote-trusted-size": "Linux reading from EFS, size supplied",
        "linux-cached": "Linux reading from its memory cache",
        "linux-page-cache": "Linux reading from its memory cache",
    }.get(value, value)


def display_operation(value: str) -> str:
    return {
        "get": "read",
        "get-known-size": "known-size read",
        "get-range4k": "4 KiB range read",
        "get-range-4k": "4 KiB range read",
        "put-create-new": "create-new write",
        "put-overwrite": "overwrite write",
        "entry-info": "file-metadata lookup",
        "delete": "delete",
        "list100": "list 100 entries",
        "list-100": "list 100 entries",
    }.get(value, value)


def display_family(value: str) -> str:
    return {
        "latency-profile": "Response time by object size",
        "concurrency-scaling": "Throughput by concurrent worker count",
        "connection-pool": "NFS connection count",
        "directory-sharding": "Directory count",
        "read-granularity": "Read request size",
        "linux-page-cache-context": "Linux memory-cache context",
        "known-size-reference": "Known-size Linux comparison",
        "read-working-set": "Files used by each worker",
        "open-loop-equal-demand-60pct": "Scheduled ramp: same request rate at 60% of the slower client's peak",
        "open-loop-equal-demand-85pct": "Scheduled load: same request rate at 85% of the slower client's peak",
        "open-loop-equal-utilization-60pct": "Scheduled ramp: each client at 60% of its own peak",
        "open-loop-equal-utilization-85pct": "Scheduled load: each client at 85% of its own peak",
    }.get(value, value.replace("-", " ").title())


PLAN_FIELDS = (
    "family",
    "backend",
    "operation",
    "mode",
    "object_size_bytes",
    "concurrency",
    "client_connections",
    "read_granularity_bytes",
    "directory_shards",
    "read_files_per_worker",
    "repetition",
    "warmup_ms",
    "duration_ms",
    "fixed_operations_per_worker",
    "offered_rate_ops_per_sec",
    "max_outstanding",
    "open_loop_load_basis",
)

AGGREGATE_FIELDS = tuple(field for field in PLAN_FIELDS if field != "repetition")


def plan_key(spec: dict[str, Any]) -> tuple[Any, ...]:
    values = [spec.get(field) for field in PLAN_FIELDS]
    rate_index = PLAN_FIELDS.index("offered_rate_ops_per_sec")
    if values[rate_index] is not None:
        values[rate_index] = round(float(values[rate_index]), 6)
    return tuple(values)


def expected_closed_loop_plan(
    quick: bool, repetitions: int, diagnostics: bool
) -> list[dict[str, Any]]:
    repetitions = 1 if quick else repetitions
    duration = 350 if quick else 3_000
    scale_duration = 450 if quick else 4_000
    warmup = 100 if quick else 750
    specs: list[dict[str, Any]] = []

    def add(
        family: str,
        backend: str,
        operation: str,
        size: int,
        concurrency: int = 1,
        clients: int = 1,
        granularity: int = 128 * KIB,
        shards: int = 64,
        read_files: int = 2,
        repetition: int = 1,
        scenario_warmup: int | None = None,
        scenario_duration: int | None = None,
        fixed: int | None = None,
    ) -> None:
        specs.append(
            {
                "family": family,
                "backend": backend,
                "operation": operation,
                "mode": "closed-loop",
                "object_size_bytes": size,
                "concurrency": concurrency,
                "client_connections": clients,
                "read_granularity_bytes": granularity,
                "directory_shards": shards,
                "read_files_per_worker": read_files,
                "repetition": repetition,
                "warmup_ms": warmup if scenario_warmup is None else scenario_warmup,
                "duration_ms": duration if scenario_duration is None else scenario_duration,
                "fixed_operations_per_worker": fixed,
                "offered_rate_ops_per_sec": None,
                "max_outstanding": None,
                "open_loop_load_basis": None,
            }
        )

    for repetition in range(1, repetitions + 1):
        for size in [32 * KIB, 128 * KIB, 512 * KIB, 2 * MIB]:
            for operation in ["get", "get-known-size", "put-create-new", "put-overwrite"]:
                for backend in ["nfs-crust", "linux-remote"]:
                    add("latency-profile", backend, operation, size, repetition=repetition)
        for size in [128 * KIB, MIB]:
            for concurrency in [1, 8, 32]:
                for operation in ["get-known-size", "put-create-new"]:
                    for backend in ["nfs-crust", "linux-remote"]:
                        add("concurrency-scaling", backend, operation, size, concurrency, repetition=repetition, scenario_duration=scale_duration)
        if not diagnostics:
            continue
        for clients in [1, 4]:
            for operation in ["get-known-size", "put-create-new"]:
                add("connection-pool", "nfs-crust", operation, MIB, 32, clients, repetition=repetition, scenario_duration=scale_duration)
        for shards in [1, 64]:
            add("directory-sharding", "nfs-crust", "put-create-new", 128 * KIB, 32, shards=shards, repetition=repetition, scenario_duration=scale_duration)
        for granularity in [128 * KIB, MIB]:
            add("read-granularity", "nfs-crust", "get", 4 * KIB, granularity=granularity, repetition=repetition)
            for concurrency in [1, 32]:
                add("read-granularity", "nfs-crust", "get-known-size", MIB, concurrency, granularity=granularity, repetition=repetition, scenario_duration=scale_duration)
        for size in [4 * KIB, MIB]:
            for concurrency in [1, 32]:
                add("linux-page-cache-context", "linux-cached", "get-known-size", size, concurrency, repetition=repetition, scenario_duration=300 if quick else 2_000)
        for size in [128 * KIB, MIB]:
            for concurrency in [1, 8, 32]:
                add("known-size-reference", "linux-remote-trusted-size", "get-known-size", size, concurrency, repetition=repetition, scenario_duration=scale_duration)
        for backend in ["nfs-crust", "linux-remote"]:
            for read_files in [2, 64]:
                add("read-working-set", backend, "get-known-size", 128 * KIB, 32, read_files=read_files, repetition=repetition, scenario_duration=scale_duration)
        for backend in ["nfs-crust", "linux-remote"]:
            add("operation-detail", backend, "get-range4k", MIB, repetition=repetition)
            add("operation-detail", backend, "entry-info", 128 * KIB, shards=1, repetition=repetition, scenario_duration=0, fixed=20 if quick else 1_000)
            add("operation-detail", backend, "delete", 4 * KIB, repetition=repetition, scenario_warmup=0, scenario_duration=0, fixed=10 if quick else 250)
            add("operation-detail", backend, "list100", 4 * KIB, shards=1, repetition=repetition, scenario_duration=0, fixed=10 if quick else 100)
    return specs


def expected_open_loop_plan(
    summary: dict[str, Any], quick: bool, repetitions: int
) -> tuple[list[dict[str, Any]], str | None]:
    bases = summary.get("open_loop_rate_basis")
    if not isinstance(bases, list) or len(bases) != 4:
        return [], "open-loop rate basis must contain exactly four operation/load combinations"
    by_key: dict[tuple[str, float], dict[str, Any]] = {}
    try:
        for basis in bases:
            operation = basis["operation"]
            fraction = float(basis["load_fraction"])
            key = (operation, round(fraction, 2))
            if key in by_key:
                return [], f"duplicate open-loop rate basis: {key}"
            if operation not in {"get-known-size", "put-create-new"} or key[1] not in {0.60, 0.85}:
                return [], f"unexpected open-loop rate basis: {key}"
            nfs_peak = float(basis["nfs_crust_peak_ops_per_sec"])
            linux_peak = float(basis["linux_remote_peak_ops_per_sec"])
            equal_demand = float(basis["equal_demand_ops_per_sec"])
            nfs_relative = float(basis["nfs_crust_equal_utilization_ops_per_sec"])
            linux_relative = float(basis["linux_remote_equal_utilization_ops_per_sec"])
            if (
                basis["object_size_bytes"] != 128 * KIB
                or nfs_peak <= 0
                or linux_peak <= 0
                or not math.isclose(equal_demand, max(min(nfs_peak, linux_peak) * fraction, 1.0), rel_tol=1e-9)
                or not math.isclose(nfs_relative, max(nfs_peak * fraction, 1.0), rel_tol=1e-9)
                or not math.isclose(linux_relative, max(linux_peak * fraction, 1.0), rel_tol=1e-9)
            ):
                return [], f"invalid open-loop rate basis: {key}"
            by_key[key] = basis
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        return [], f"invalid open-loop rate basis: {error}"

    expected_keys = {
        (operation, fraction)
        for operation in ["get-known-size", "put-create-new"]
        for fraction in [0.60, 0.85]
    }
    if set(by_key) != expected_keys:
        return [], "open-loop rate basis is incomplete"
    specs = []
    repetitions = 1 if quick else repetitions

    def duration_ms(rate: float) -> int:
        return 1_000 if quick else math.ceil(min(max(5_000.0 / rate, 5.0), 30.0) * 1_000.0)

    for operation in ["get-known-size", "put-create-new"]:
        for fraction in [0.60, 0.85]:
            basis = by_key[(operation, fraction)]
            for repetition in range(1, repetitions + 1):
                backends = ["linux-remote", "nfs-crust"] if repetition % 2 == 0 else ["nfs-crust", "linux-remote"]
                for backend in backends:
                    maximum = (
                        QUICK_OPEN_LOOP_MAX_OUTSTANDING
                        if quick
                        else STANDARD_OPEN_LOOP_MAX_OUTSTANDING
                    )
                    for load_basis in ["equal-demand", "equal-utilization"]:
                        if load_basis == "equal-demand":
                            rate = basis["equal_demand_ops_per_sec"]
                        elif backend == "nfs-crust":
                            rate = basis["nfs_crust_equal_utilization_ops_per_sec"]
                        else:
                            rate = basis["linux_remote_equal_utilization_ops_per_sec"]
                        specs.append({
                            "family": f"open-loop-{load_basis}-{round(fraction * 100)}pct",
                            "backend": backend,
                            "operation": operation,
                            "mode": "open-loop",
                            "object_size_bytes": 128 * KIB,
                            "concurrency": maximum,
                            "client_connections": 1,
                            "read_granularity_bytes": 128 * KIB,
                            "directory_shards": 64,
                            "read_files_per_worker": 2,
                            "repetition": repetition,
                            "warmup_ms": 200 if quick else 750,
                            "duration_ms": duration_ms(rate),
                            "fixed_operations_per_worker": None,
                            "offered_rate_ops_per_sec": rate,
                            "max_outstanding": maximum,
                            "open_loop_load_basis": load_basis,
                        })
    return specs, None


def validate_scenario_plan(summary: dict[str, Any]) -> tuple[bool, str]:
    if summary.get("schema_version") != 4:
        return False, "unsupported benchmark summary schema"
    quick = summary.get("metadata", {}).get("quick")
    diagnostics = summary.get("metadata", {}).get("diagnostics")
    repetitions = summary.get("metadata", {}).get("repetitions")
    scenarios = summary.get("scenarios")
    if (
        not isinstance(quick, bool)
        or not isinstance(diagnostics, bool)
        or not isinstance(repetitions, int)
        or isinstance(repetitions, bool)
        or not 1 <= repetitions <= 20
        or (quick and repetitions != 1)
        or not isinstance(scenarios, list)
    ):
        return False, "benchmark summary has invalid plan metadata"
    ids = [scenario.get("spec", {}).get("id") for scenario in scenarios]
    if any(not isinstance(identifier, str) or not identifier for identifier in ids) or len(set(ids)) != len(ids):
        return False, "scenario IDs are missing or duplicated"
    if [scenario.get("order") for scenario in scenarios] != list(range(1, len(scenarios) + 1)):
        return False, "scenario orders are not unique and contiguous"
    expected_open, error = expected_open_loop_plan(summary, quick, repetitions)
    if error:
        return False, error
    expected = expected_closed_loop_plan(quick, repetitions, diagnostics) + expected_open
    actual_specs = [scenario["spec"] for scenario in scenarios]
    missing = Counter(map(plan_key, expected)) - Counter(map(plan_key, actual_specs))
    unexpected = Counter(map(plan_key, actual_specs)) - Counter(map(plan_key, expected))
    if missing or unexpected:
        return False, f"scenario matrix differs from the benchmark plan ({sum(missing.values())} missing, {sum(unexpected.values())} unexpected)"
    actual_open = [plan_key(spec) for spec in actual_specs if spec.get("mode") == "open-loop"]
    if actual_open != [plan_key(spec) for spec in expected_open]:
        return False, "open-loop backend order is not counterbalanced as planned"
    return True, f"exact {len(expected)}-scenario plan verified"


def aggregate_scenarios(scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for scenario in scenarios:
        spec = scenario["spec"]
        values = [spec.get(field) for field in AGGREGATE_FIELDS]
        rate_index = AGGREGATE_FIELDS.index("offered_rate_ops_per_sec")
        if values[rate_index] is not None:
            values[rate_index] = round(float(values[rate_index]), 6)
        key = tuple(values)
        grouped[key].append(scenario)

    aggregates = []
    metric_paths = {
        "ops": ("achieved_ops_per_sec",),
        "mibps": ("achieved_mib_per_sec",),
        "service_p50": ("service_latency", "p50_us"),
        "service_p99": ("service_latency", "p99_us"),
        "total_p50": ("total_response_latency", "p50_us"),
        "total_p90": ("total_response_latency", "p90_us"),
        "total_p95": ("total_response_latency", "p95_us"),
        "total_p99": ("total_response_latency", "p99_us"),
        "total_p999": ("total_response_latency", "p999_us"),
        "total_max": ("total_response_latency", "max_us"),
        "queue_p99": ("queue_latency", "p99_us"),
    }
    for key, runs in grouped.items():
        spec = dict(runs[0]["spec"])
        spec.pop("id", None)
        spec.pop("repetition", None)
        metrics: dict[str, dict[str, float]] = {}
        for name, path in metric_paths.items():
            values = []
            for run in runs:
                value: Any = run
                for component in path:
                    value = value[component]
                values.append(float(value))
            ci_low, ci_high = bootstrap_median_interval(values)
            metrics[name] = {
                "median": median(values),
                "min": min(values),
                "max": max(values),
                "ci_low": ci_low,
                "ci_high": ci_high,
            }
        cpu_values = [
            (run["resource_delta"]["user_cpu_ms"] + run["resource_delta"]["system_cpu_ms"])
            / max(run["successes"], 1)
            for run in runs
        ]
        network_values = [
            (run["resource_delta"]["host_rx_bytes"] + run["resource_delta"]["host_tx_bytes"])
            / max(run["successes"], 1)
            for run in runs
            if run["resource_delta"].get("host_rx_bytes") is not None
            and run["resource_delta"].get("host_tx_bytes") is not None
        ]
        for name, values in [("cpu_ms_per_op", cpu_values), ("host_bytes_per_op", network_values)]:
            if values:
                ci_low, ci_high = bootstrap_median_interval(values)
                metrics[name] = {
                    "median": median(values),
                    "min": min(values),
                    "max": max(values),
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                }
        measurement_error_values = [run.get("measurement_errors") for run in runs]
        measurement_errors = (
            sum(measurement_error_values)
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in measurement_error_values
            )
            else None
        )
        aggregates.append(
            {
                "spec": spec,
                "repetitions": len(runs),
                "metrics": metrics,
                "successes": sum(run["successes"] for run in runs),
                "errors": sum(run["errors"] for run in runs),
                "measurement_errors": measurement_errors,
                "drops": sum(run["overload_drops"] for run in runs),
                "reconciled_outcomes": sum(run["reconciled_outcomes"] for run in runs),
                "all_p99_sample_sufficient": all(run["p99_sample_sufficient"] for run in runs),
                "all_p999_sample_sufficient": all(run["p999_sample_sufficient"] for run in runs),
                "orders": [run["order"] for run in runs],
            }
        )
    aggregates.sort(
        key=lambda item: (
            item["spec"]["family"],
            item["spec"]["operation"],
            item["spec"]["object_size_bytes"],
            item["spec"]["concurrency"],
            item["spec"]["backend"],
        )
    )
    return aggregates


def select_group(groups: list[dict[str, Any]], **criteria: Any) -> dict[str, Any] | None:
    for group in groups:
        if all(group["spec"].get(key) == value for key, value in criteria.items()):
            return group
    return None


def metric(group: dict[str, Any] | None, name: str) -> float:
    if group is None:
        return 0.0
    return float(group["metrics"][name]["median"])


def ratio(numerator: float, denominator: float) -> str:
    if denominator <= 0:
        return "n/a"
    value = numerator / denominator
    return f"{value:.2f}×"


def read_raw_samples(
    path: Path,
    scenario_specs: dict[str, dict[str, Any]],
) -> tuple[
    dict[str, list[float]],
    dict[str, dict[int, list[float]]],
    dict[str, dict[str, int]],
    dict[str, int],
    dict[str, dict[str, list[int]]],
    bool,
    bool,
]:
    tails: dict[str, list[float]] = defaultdict(list)
    seconds: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    reconciled_counts: dict[str, int] = defaultdict(int)
    timings: dict[str, dict[str, list[int]]] = defaultdict(
        lambda: {"service_ns": [], "queue_ns": [], "total_response_ns": []}
    )
    samples_well_formed = True
    reconciliations_well_formed = True
    opener: Callable[..., Any] = gzip.open if path.suffix == ".gz" else open
    if not path.exists():
        return tails, seconds, counts, reconciled_counts, timings, False, False
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            try:
                sample = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                samples_well_formed = False
                continue
            if not isinstance(sample, dict):
                samples_well_formed = False
                continue
            scenario_id = sample.get("scenario_id")
            status = sample.get("status")
            if (
                not isinstance(scenario_id, str)
                or not scenario_id
                or status not in {"ok", "error", "dropped"}
            ):
                samples_well_formed = False
                continue
            counts[scenario_id][status] += 1
            reconciled = sample.get("reconciled_outcome_unknown", False)
            if not isinstance(reconciled, bool):
                reconciliations_well_formed = False
            elif reconciled:
                reconciled_counts[scenario_id] += 1
                spec = scenario_specs.get(scenario_id, {})
                if (
                    status != "ok"
                    or spec.get("backend") != "nfs-crust"
                    or spec.get("operation") != "put-create-new"
                ):
                    reconciliations_well_formed = False

            integer_fields = [
                "worker",
                "sequence",
                "completion_offset_ns",
                "queue_ns",
                "service_ns",
                "total_response_ns",
            ]
            if any(
                not isinstance(sample.get(field), int)
                or isinstance(sample.get(field), bool)
                or sample[field] < 0
                for field in integer_fields
            ):
                samples_well_formed = False
                continue
            error = sample.get("error")
            if (status == "ok" and error is not None) or (
                status != "ok" and (not isinstance(error, str) or not error)
            ):
                samples_well_formed = False
            spec = scenario_specs.get(scenario_id)
            scheduled = sample.get("scheduled_offset_ns")
            scheduled_is_ns = (
                isinstance(scheduled, int)
                and not isinstance(scheduled, bool)
                and scheduled >= 0
            )
            if "scheduled_offset_ns" not in sample or spec is None or (
                (spec.get("mode") == "open-loop" and not scheduled_is_ns)
                or (spec.get("mode") == "closed-loop" and scheduled is not None)
            ):
                samples_well_formed = False
            if status != "ok":
                continue
            for field in ["service_ns", "queue_ns", "total_response_ns"]:
                timings[scenario_id][field].append(sample[field])
            if spec is None or spec.get("mode") != "open-loop":
                continue
            latency_us = sample["total_response_ns"] / 1000.0
            tails[scenario_id].append(latency_us)
            second = int(sample["completion_offset_ns"] // 1_000_000_000)
            seconds[scenario_id][second].append(latency_us)
    return (
        tails,
        seconds,
        counts,
        reconciled_counts,
        timings,
        samples_well_formed,
        reconciliations_well_formed,
    )


def distribution_matches_raw(distribution: Any, values_ns: list[int]) -> bool:
    if not isinstance(distribution, dict):
        return False
    ordered = sorted(values_ns)
    count = len(ordered)
    if count:
        mean_ns = sum(float(value) for value in ordered) / count
        variance_ns = sum((float(value) - mean_ns) ** 2 for value in ordered) / count
        expected = {
            "count": count,
            "min_us": ordered[0] / 1_000.0,
            "mean_us": mean_ns / 1_000.0,
            "p50_us": quantile(ordered, 0.50) / 1_000.0,
            "p90_us": quantile(ordered, 0.90) / 1_000.0,
            "p95_us": quantile(ordered, 0.95) / 1_000.0,
            "p99_us": quantile(ordered, 0.99) / 1_000.0,
            "p999_us": quantile(ordered, 0.999) / 1_000.0,
            "max_us": ordered[-1] / 1_000.0,
            "stddev_us": math.sqrt(variance_ns) / 1_000.0,
        }
    else:
        expected = {
            "count": 0,
            "min_us": 0.0,
            "mean_us": 0.0,
            "p50_us": 0.0,
            "p90_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "p999_us": 0.0,
            "max_us": 0.0,
            "stddev_us": 0.0,
        }
    if distribution.get("count") != expected["count"]:
        return False
    for field, expected_value in expected.items():
        if field == "count":
            continue
        actual = distribution.get(field)
        if (
            not isinstance(actual, (int, float))
            or isinstance(actual, bool)
            or not math.isfinite(float(actual))
            or not math.isclose(float(actual), expected_value, rel_tol=1e-9, abs_tol=1e-6)
        ):
            return False
    return True


def verify_checksum_manifest(root: Path) -> tuple[bool, str]:
    manifest = root / "SHA256SUMS"
    if not manifest.exists():
        return False, "remote SHA256SUMS is missing"
    root = root.resolve()
    manifest_entries: set[Path] = set()
    rows = [line for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        return False, "remote SHA256SUMS is empty"
    for line in rows:
        try:
            expected, relative = line.split(maxsplit=1)
        except ValueError:
            return False, "remote SHA256SUMS contains an invalid row"
        if len(expected) != 64 or any(character not in "0123456789abcdefABCDEF" for character in expected):
            return False, "remote SHA256SUMS contains an invalid digest"
        relative = relative.lstrip("*").removeprefix("./")
        candidate = (root / relative).resolve()
        if root not in candidate.parents:
            return False, "remote SHA256SUMS references a path outside the run"
        if candidate in manifest_entries:
            return False, f"remote SHA256SUMS contains a duplicate row: {relative}"
        manifest_entries.add(candidate)
        if not candidate.is_file():
            return False, f"checksummed file is missing: {relative}"
        with candidate.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if digest != expected.lower():
            return False, f"checksum mismatch: {relative}"
    actual_files = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.resolve() != manifest.resolve()
    }
    unlisted = actual_files - manifest_entries
    if unlisted:
        relative = min(path.relative_to(root).as_posix() for path in unlisted)
        return False, f"remote SHA256SUMS omits an artifact: {relative}"
    return True, f"all {len(manifest_entries)} downloaded-file checksums verified"


def ccdf_points(values: list[float]) -> list[tuple[float, float]]:
    probabilities = [
        0.0,
        0.10,
        0.25,
        0.50,
        0.75,
        0.90,
        0.95,
        0.975,
        0.99,
        0.995,
        0.999,
        0.9995,
        0.9999,
    ]
    minimum_tail = 1.0 / max(len(values), 1)
    return [
        (quantile(values, probability), max(1.0 - probability, minimum_tail))
        for probability in probabilities
        if values
    ]


def load_cloudwatch(directory: Path) -> dict[str, list[dict[str, Any]]]:
    metrics = {}
    if not directory.exists():
        return metrics
    for path in sorted(directory.glob("*.json")):
        document = load_json(path, {})
        metrics[path.stem] = sorted(document.get("Datapoints", []), key=lambda point: point["Timestamp"])
    return metrics


def load_proc_net_snmp(path: Path, section: str) -> dict[str, int]:
    if not path.exists():
        return {}
    lines = [line.split() for line in path.read_text(encoding="utf-8").splitlines()]
    prefix = f"{section}:"
    for header, values in zip(lines, lines[1:]):
        if header and values and header[0] == prefix and values[0] == prefix:
            try:
                return {
                    name: int(value)
                    for name, value in zip(header[1:], values[1:])
                }
            except ValueError:
                return {}
    return {}


def counter_delta(before: dict[str, int], after: dict[str, int], name: str) -> int | None:
    if name not in before or name not in after or after[name] < before[name]:
        return None
    return after[name] - before[name]


def cloudwatch_max(metrics: dict[str, list[dict[str, Any]]], stem: str, field: str) -> float | None:
    values = [float(point[field]) for point in metrics.get(stem, []) if field in point]
    return max(values) if values else None


def cloudwatch_series(
    metrics: dict[str, list[dict[str, Any]]],
    stem: str,
    field: str,
    scale: float = 1.0,
) -> list[tuple[float, float]]:
    points = metrics.get(stem, [])
    if not points:
        return []
    timestamps = [datetime.fromisoformat(point["Timestamp"].replace("Z", "+00:00")).timestamp() for point in points]
    origin = min(timestamps)
    return [
        ((timestamp - origin) / 60.0, float(point[field]) / scale)
        for point, timestamp in zip(points, timestamps)
        if field in point
    ]


def svg_line_chart(
    title: str,
    description: str,
    series: list[tuple[str, list[tuple[float, float]], bool, int]],
    x_label: str,
    y_label: str,
    x_formatter: Callable[[float], str],
    y_formatter: Callable[[float], str],
    *,
    log_x: bool = False,
    log_y: bool = False,
    show_caption: bool = True,
    legend: list[tuple[str, bool, int]] | None = None,
    series_roles: dict[str, str] | None = None,
    bands: list[tuple[list[tuple[float, float, float]], int]] | None = None,
    hollow_markers: list[tuple[list[tuple[float, float]], int]] | None = None,
) -> str:
    series_roles = series_roles or {}
    bands = bands or []
    hollow_markers = hollow_markers or []
    series = [
        (
            label,
            [
                (x, y)
                for x, y in points
                if (not log_x or x > 0) and (not log_y or y > 0)
            ],
            dashed,
            color_index,
        )
        for label, points, dashed, color_index in series
    ]
    series = [
        (label, points, dashed, color_index)
        for label, points, dashed, color_index in series
        if points
    ]
    if not series:
        return f'<div class="chart-empty"><strong>{html.escape(title)}</strong><p>No data available.</p></div>'
    legend_items = legend or [
        (label, dashed, color_index)
        for label, _, dashed, color_index in series
    ]
    legend_columns = min(len(legend_items), 2)
    legend_rows = math.ceil(len(legend_items) / legend_columns)
    legend_row_gap = 26
    width, height = 680, 320 + max(legend_rows - 1, 0) * legend_row_gap
    left, right = 120, 60
    top = 62 + max(legend_rows - 1, 0) * legend_row_gap
    bottom = 62
    plot_width = width - left - right
    plot_height = height - top - bottom
    all_x = [point[0] for _, points, _, _ in series for point in points]
    all_y = [point[1] for _, points, _, _ in series for point in points]
    positive_x = [value for value in all_x if value > 0]
    positive_y = [value for value in all_y if value > 0]
    x_min = min(positive_x if log_x else all_x)
    x_max = max(all_x)
    y_min = min(positive_y) if log_y else 0.0
    y_max = max(all_y)
    if math.isclose(x_min, x_max):
        if log_x:
            x_min *= 0.9
            x_max *= 1.1
        else:
            padding = max(abs(x_min) * 0.1, 1.0)
            x_min -= padding
            x_max += padding
    if y_min == y_max:
        y_min = y_min * 0.8 if log_y else 0.0
        y_max = y_max * 1.2 if y_max else 1.0
    if not log_y:
        y_max *= 1.08

    def transform(value: float, minimum: float, maximum: float, logarithmic: bool) -> float:
        if logarithmic:
            value, minimum, maximum = math.log10(value), math.log10(minimum), math.log10(maximum)
        return (value - minimum) / (maximum - minimum)

    def x_position(value: float) -> float:
        return left + transform(value, x_min, x_max, log_x) * plot_width

    def y_position(value: float) -> float:
        return top + (1.0 - transform(value, y_min, y_max, log_y)) * plot_height

    unique_x = sorted(set(all_x))
    if len(unique_x) <= 7:
        x_ticks = unique_x
    else:
        x_ticks = [unique_x[round(index * (len(unique_x) - 1) / 5)] for index in range(6)]
    if log_y:
        y_ticks = [10 ** (math.log10(y_min) + index * (math.log10(y_max) - math.log10(y_min)) / 4) for index in range(5)]
    else:
        y_ticks = [y_max * index / 4 for index in range(5)]

    escaped_title = html.escape(title)
    escaped_description = html.escape(description)
    parts = [
        '<figure class="report-chart">',
        f'<h3 class="mobile-data-chart-title">{escaped_title}</h3>',
        f'<svg class="data-chart" viewBox="0 0 {width} {height}" role="img" '
        f'aria-labelledby="{slug(title)}-title {slug(title)}-desc">',
        f'<title id="{slug(title)}-title">{escaped_title}</title>',
        f'<desc id="{slug(title)}-desc">{escaped_description}</desc>',
        f'<text x="{left}" y="24" class="chart-title">{escaped_title}</text>',
    ]
    for index, (label, dashed, color_index) in enumerate(legend_items):
        column = index % legend_columns
        row = index // legend_columns
        x = left + column * (plot_width / legend_columns)
        y = 48 + row * legend_row_gap
        class_name = SERIES_CLASSES[color_index % len(SERIES_CLASSES)]
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        marker = (
            f'<rect x="{x + 8:.1f}" y="{y - 4:.1f}" width="8" height="8" class="{class_name}-fill"/>'
            if dashed
            else f'<circle cx="{x + 12:.1f}" cy="{y:.1f}" r="4" class="{class_name}-fill"/>'
        )
        parts.extend(
            [
                f'<line x1="{x:.1f}" y1="{y:.1f}" x2="{x + 24:.1f}" '
                f'y2="{y:.1f}" class="chart-line {class_name}-line"{dash}/>',
                marker,
                f'<text x="{x + 32:.1f}" y="{y + 4:.1f}" class="chart-label">{html.escape(label)}</text>',
            ]
        )
    for tick in y_ticks:
        y = y_position(tick)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" class="chart-grid"/>')
        parts.append(f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" class="chart-label">{html.escape(y_formatter(tick))}</text>')
    for tick in x_ticks:
        x = x_position(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{height-bottom}" class="chart-grid chart-grid-x"/>')
        parts.append(f'<text x="{x:.1f}" y="{height-bottom+20}" text-anchor="middle" class="chart-label">{html.escape(x_formatter(tick))}</text>')
    parts.append(f'<text x="{left+plot_width/2:.1f}" y="{height-8}" text-anchor="middle" class="chart-label">{html.escape(x_label)}</text>')
    parts.append(f'<text x="16" y="{top+plot_height/2:.1f}" transform="rotate(-90 16 {top+plot_height/2:.1f})" text-anchor="middle" class="chart-label">{html.escape(y_label)}</text>')
    for band_points, color_index in bands:
        valid_points = sorted(
            (x, low, high)
            for x, low, high in band_points
            if (not log_x or x > 0) and (not log_y or (low > 0 and high > 0))
        )
        if not valid_points:
            continue
        upper = [
            f'{x_position(x):.1f},{y_position(high):.1f}'
            for x, _, high in valid_points
        ]
        lower = [
            f'{x_position(x):.1f},{y_position(low):.1f}'
            for x, low, _ in reversed(valid_points)
        ]
        class_name = SERIES_CLASSES[color_index % len(SERIES_CLASSES)]
        parts.append(
            f'<polygon points="{" ".join(upper + lower)}" '
            f'class="{class_name}-fill-soft percentile-band"/>'
        )
    for label, points, dashed, color_index in series:
        points = sorted(points)
        path = " ".join(
            f'{"M" if point_index == 0 else "L"}{x_position(x):.1f},{y_position(y):.1f}'
            for point_index, (x, y) in enumerate(points)
        )
        class_name = SERIES_CLASSES[color_index % len(SERIES_CLASSES)]
        role = series_roles.get(label, "")
        role_class = f" {role}" if role else ""
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        parts.append(
            f'<path d="{path}" class="chart-line {class_name}-line{role_class}"{dash}/>'
        )
        for x_value, y_value in points:
            x, y = x_position(x_value), y_position(y_value)
            if dashed:
                parts.append(f'<rect x="{x-4:.1f}" y="{y-4:.1f}" width="8" height="8" class="{class_name}-fill{role_class}"/>')
            else:
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" class="{class_name}-fill{role_class}"/>')
    for points, color_index in hollow_markers:
        class_name = SERIES_CLASSES[color_index % len(SERIES_CLASSES)]
        for x_value, y_value in points:
            if (log_x and x_value <= 0) or (log_y and y_value <= 0):
                continue
            parts.append(
                f'<circle cx="{x_position(x_value):.1f}" cy="{y_position(y_value):.1f}" '
                f'r="5" class="{class_name}-line percentile-insufficient"/>'
            )
    parts.append('</svg>')
    if show_caption:
        parts.append(f'<figcaption>{escaped_description}</figcaption>')
    parts.append('</figure>')
    return "\n".join(parts)


def slug(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "-" for character in value).strip("-")


def topology_svg(provenance: dict[str, Any]) -> str:
    az = html.escape(provenance.get("availability_zone", "unknown AZ"))
    region = html.escape(provenance.get("region", "unknown region"))
    instance = html.escape(provenance.get("instance_type", "EC2"))
    return f"""
<figure class="report-chart topology">
<svg viewBox="0 0 680 180" role="img" aria-labelledby="topology-title topology-desc">
<title id="topology-title">Measured AWS topology</title>
<desc id="topology-desc">The benchmark process and Linux NFS client ran on one {instance} instance. Both used TLS to one EFS mount target in {az}. The backing EFS file system was Regional with Elastic throughput.</desc>
<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" class="topology-arrow"/></marker></defs>
<text x="30" y="28" class="chart-title">{region} · exact same-AZ data path</text>
<circle cx="105" cy="112" r="54" class="topology-node series-1-fill-soft"/>
<text x="105" y="102" text-anchor="middle" class="topology-label">EC2 {instance}</text>
<text x="105" y="124" text-anchor="middle" class="chart-label">nfs-crust + Linux</text>
<line x1="164" y1="112" x2="294" y2="112" class="topology-link" marker-end="url(#arrow)"/>
<text x="229" y="94" text-anchor="middle" class="chart-label">TLS NFSv4.1</text>
<circle cx="360" cy="112" r="54" class="topology-node series-2-fill-soft"/>
<text x="360" y="102" text-anchor="middle" class="topology-label">EFS network endpoint</text>
<text x="360" y="124" text-anchor="middle" class="chart-label">{az}</text>
<line x1="419" y1="112" x2="540" y2="112" class="topology-link" marker-end="url(#arrow)"/>
<circle cx="600" cy="112" r="54" class="topology-node series-3-fill-soft"/>
<text x="600" y="96" text-anchor="middle" class="topology-label">Regional EFS</text>
<text x="600" y="118" text-anchor="middle" class="chart-label">General Purpose</text>
<text x="600" y="138" text-anchor="middle" class="chart-label">Elastic throughput</text>
</svg>
</figure>
"""


def stat_card(label: str, value: str, context: str) -> str:
    return f'<div class="card viz-stat"><span class="text-muted">{html.escape(label)}</span><span class="viz-stat-value">{html.escape(value)}</span><span>{html.escape(context)}</span></div>'


def comparison_table(title: str, rows: list[tuple[str, str, str, str]]) -> str:
    body = "".join(
        f'<tr><th scope="row">{html.escape(label)}</th><td>{html.escape(first)}</td><td>{html.escape(second)}</td><td>{html.escape(change)}</td></tr>'
        for label, first, second, change in rows
    )
    return f"""
<section class="investigation">
<h3>{html.escape(title)}</h3>
<table><thead><tr><th>Comparison</th><th>First setting</th><th>Second setting</th><th>Second ÷ first</th></tr></thead><tbody>{body}</tbody></table>
</section>
"""


def range_text(values: dict[str, float], formatter: Callable[[float], str]) -> str:
    center = formatter(values["median"])
    if math.isclose(values["min"], values["max"], rel_tol=1e-9, abs_tol=1e-9):
        return center
    confidence = f'{formatter(values["ci_low"])}–{formatter(values["ci_high"])}'
    observed = f'{formatter(values["min"])}–{formatter(values["max"])}'
    return f"{center} [95% uncertainty interval {confidence}; observed range {observed}]"


def full_results_tables(groups: list[dict[str, Any]]) -> str:
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in groups:
        by_family[group["spec"]["family"]].append(group)
    sections = []
    for family, family_groups in sorted(by_family.items()):
        context_fields = {
            "concurrency-scaling": ("concurrency",),
            "connection-pool": ("concurrency", "client_connections"),
            "directory-sharding": ("concurrency", "directory_shards"),
            "read-granularity": ("concurrency", "read_granularity_bytes"),
            "linux-page-cache-context": ("concurrency",),
            "known-size-reference": ("concurrency",),
            "read-working-set": ("concurrency", "read_files_per_worker"),
            "open-loop-equal-demand-60pct": ("offered_rate_ops_per_sec",),
            "open-loop-equal-demand-85pct": ("offered_rate_ops_per_sec",),
            "open-loop-equal-utilization-60pct": ("offered_rate_ops_per_sec",),
            "open-loop-equal-utilization-85pct": ("offered_rate_ops_per_sec",),
        }.get(family, ())
        rows = []
        for group in family_groups:
            spec, metrics = group["spec"], group["metrics"]
            p99_marker = "" if group["all_p99_sample_sufficient"] else " †"
            p999_marker = "" if group["all_p999_sample_sufficient"] else " ‡"
            context = [fmt_size(spec["object_size_bytes"])]
            for field in context_fields:
                value = spec.get(field)
                if field == "concurrency":
                    context.append(f'{value} concurrent worker' + ("" if value == 1 else "s"))
                elif field == "client_connections":
                    context.append(f'{value} connection' + ("" if value == 1 else "s"))
                elif field == "directory_shards":
                    context.append(f'{value} shard' + ("" if value == 1 else "s"))
                elif field == "read_granularity_bytes":
                    context.append(f"{fmt_size(value)} read request size")
                elif field == "read_files_per_worker":
                    context.append(f"{value} files per worker")
                elif field == "offered_rate_ops_per_sec" and value is not None:
                    context.append(f"scheduled at {fmt_rate(value)}")
            anomalies = ""
            if group["reconciled_outcomes"]:
                anomalies += (
                    f'<div><dt>Needed a follow-up check</dt><dd>{group["reconciled_outcomes"]:,}</dd></div>'
                )
            if group["errors"] or group["drops"]:
                anomalies += (
                    f'<div><dt>Errors / drops</dt><dd>{group["errors"]} / '
                    f'{group["drops"]}</dd></div>'
                )
            network_markup = (
                f'<div><dt>Host network</dt><dd>{html.escape(range_text(metrics["host_bytes_per_op"], fmt_bytes))}</dd></div>'
                if "host_bytes_per_op" in metrics
                else ""
            )
            rows.append(
                '<li class="result-item">'
                f'<h4 class="result-heading">{html.escape(display_backend(spec["backend"]))} · '
                f'{html.escape(display_operation(spec["operation"]))}</h4>'
                f'<p class="result-context text-muted">{html.escape(" · ".join(context))}</p>'
                '<dl class="result-metrics">'
                f'<div><dt>Throughput</dt><dd>{html.escape(range_text(metrics["ops"], fmt_rate))}</dd></div>'
                f'<div><dt>Slowest 1% threshold (p99)</dt><dd>{html.escape(range_text(metrics["total_p99"], fmt_latency))}{p99_marker}</dd></div>'
                f'<div><dt>Typical response time (median, p50)</dt><dd>{html.escape(range_text(metrics["total_p50"], fmt_latency))}</dd></div>'
                f'<div><dt>Slowest 0.1% threshold (p99.9)</dt><dd>{html.escape(range_text(metrics["total_p999"], fmt_latency))}{p999_marker}</dd></div>'
                f'<div><dt>Processor time per operation</dt><dd>{html.escape(range_text(metrics["cpu_ms_per_op"], fmt_cpu_ms))}</dd></div>'
                f'{network_markup}'
                f'<div><dt>Successful samples</dt><dd>{group["successes"]:,}</dd></div>'
                f'{anomalies}'
                '</dl></li>'
            )
        sections.append(
            f"""
<details>
<summary>{html.escape(display_family(family))} · {len(family_groups)} configurations</summary>
<ol class="result-list">{''.join(rows)}</ol>
</details>
"""
        )
    return "\n".join(sections)


STYLES = """
<style>
#nfs-crust-efs-report {
  --report-foreground: var(--foreground, #172033);
  --report-muted: var(--muted-foreground, #5b6475);
  --report-border: var(--border, #d8dee9);
  --report-background: var(--background, #f5f7fb);
  --report-card: var(--card, #ffffff);
  --report-destructive: var(--destructive, #b42318);
  --report-series-1: var(--viz-series-1, #2563eb);
  --report-series-2: var(--viz-series-2, #0f9d76);
  --report-series-3: var(--viz-series-3, #d97706);
  --report-series-4: var(--viz-series-4, #7c3aed);
  --report-series-5: var(--viz-series-5, #db2777);
  --report-series-6: var(--viz-series-6, #0891b2);
  box-sizing: border-box;
  max-width: 90rem;
  margin-inline: auto;
  padding: clamp(1rem, 3vw, 2.5rem);
  color: var(--report-foreground);
  background: var(--report-background);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  line-height: 1.5;
}
#nfs-crust-efs-report *, #nfs-crust-efs-report *::before, #nfs-crust-efs-report *::after { box-sizing: inherit; }
#nfs-crust-efs-report h1, #nfs-crust-efs-report h2, #nfs-crust-efs-report h3 { line-height: 1.2; }
#nfs-crust-efs-report code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
#nfs-crust-efs-report .viz-grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(min(100%,14rem),1fr)); gap: 1rem; }
#nfs-crust-efs-report .card { padding: 1rem; border: 1px solid var(--report-border); border-radius: .75rem; background: var(--report-card); box-shadow: 0 1px 2px rgb(15 23 42 / .06); }
#nfs-crust-efs-report .viz-stat { display: grid; gap: .25rem; }
#nfs-crust-efs-report .viz-stat-value { font-size: clamp(1.25rem, 2.2vw, 1.75rem); font-weight: 650; font-variant-numeric: tabular-nums; }
#nfs-crust-efs-report .text-muted { color: var(--report-muted); }
#nfs-crust-efs-report .text-destructive { color: var(--report-destructive); }
#nfs-crust-efs-report header { margin-bottom: 1.5rem; }
#nfs-crust-efs-report .eyebrow { color: var(--report-muted); letter-spacing: .08em; text-transform: uppercase; }
#nfs-crust-efs-report .report-meta { display: flex; flex-wrap: wrap; gap: .5rem 1rem; color: var(--report-muted); }
#nfs-crust-efs-report .report-section { margin-block: 2rem; }
#nfs-crust-efs-report .report-grid { display: grid; grid-template-columns: minmax(0,1fr); gap: 1rem; }
#nfs-crust-efs-report .report-grid > * { min-width: 0; }
#nfs-crust-efs-report .report-chart { margin: 1rem 0; }
#nfs-crust-efs-report .report-chart svg { display: block; width: 100%; height: auto; }
#nfs-crust-efs-report .report-chart figcaption { color: var(--report-muted); margin-top: .25rem; }
#nfs-crust-efs-report .mobile-chart-hint { display: none; }
#nfs-crust-efs-report .mobile-data-chart-title { display: none; }
#nfs-crust-efs-report .chart-title { fill: var(--report-foreground); font-weight: 500; }
#nfs-crust-efs-report .chart-label { fill: var(--report-muted); }
#nfs-crust-efs-report .chart-grid { stroke: var(--report-border); stroke-width: 1; }
#nfs-crust-efs-report .chart-grid-x { opacity: .5; }
#nfs-crust-efs-report .chart-line { fill: none; stroke-width: 2.2; vector-effect: non-scaling-stroke; }
#nfs-crust-efs-report .chart-line.percentile-p99 { stroke-width: 3.2; }
#nfs-crust-efs-report .percentile-p50 { opacity: .38; }
#nfs-crust-efs-report .chart-line.percentile-p50 { stroke-width: 1.35; }
#nfs-crust-efs-report .percentile-band { opacity: .55; }
#nfs-crust-efs-report .percentile-insufficient { fill: var(--report-card); stroke-width: 2.4; vector-effect: non-scaling-stroke; }
#nfs-crust-efs-report .chart-key { padding: .75rem 1rem; border-left: 3px solid var(--report-series-1); background: var(--report-card); color: var(--report-muted); }
#nfs-crust-efs-report .series-1-line { stroke: var(--report-series-1); } #nfs-crust-efs-report .series-1-fill { fill: var(--report-series-1); }
#nfs-crust-efs-report .series-2-line { stroke: var(--report-series-2); } #nfs-crust-efs-report .series-2-fill { fill: var(--report-series-2); }
#nfs-crust-efs-report .series-3-line { stroke: var(--report-series-3); } #nfs-crust-efs-report .series-3-fill { fill: var(--report-series-3); }
#nfs-crust-efs-report .series-4-line { stroke: var(--report-series-4); } #nfs-crust-efs-report .series-4-fill { fill: var(--report-series-4); }
#nfs-crust-efs-report .series-5-line { stroke: var(--report-series-5); } #nfs-crust-efs-report .series-5-fill { fill: var(--report-series-5); }
#nfs-crust-efs-report .series-6-line { stroke: var(--report-series-6); } #nfs-crust-efs-report .series-6-fill { fill: var(--report-series-6); }
#nfs-crust-efs-report .series-1-fill-soft { fill: color-mix(in srgb,var(--report-series-1) 18%,transparent); }
#nfs-crust-efs-report .series-2-fill-soft { fill: color-mix(in srgb,var(--report-series-2) 18%,transparent); }
#nfs-crust-efs-report .series-3-fill-soft { fill: color-mix(in srgb,var(--report-series-3) 18%,transparent); }
#nfs-crust-efs-report .topology-node { stroke: var(--report-border); stroke-width: 1.5; }
#nfs-crust-efs-report .topology-label { fill: var(--report-foreground); font-weight: 500; }
#nfs-crust-efs-report .topology-link { stroke: var(--report-muted); stroke-width: 2; }
#nfs-crust-efs-report .topology-arrow { fill: var(--report-muted); }
#nfs-crust-efs-report table { width: 100%; table-layout: fixed; border-collapse: collapse; font-variant-numeric: tabular-nums; }
#nfs-crust-efs-report th, #nfs-crust-efs-report td { padding: .5rem .4rem; text-align: left; vertical-align: top; overflow-wrap: anywhere; border-bottom: 1px solid var(--report-border); }
#nfs-crust-efs-report thead th { color: var(--report-muted); font-weight: 500; }
#nfs-crust-efs-report details { margin-block: .75rem; }
#nfs-crust-efs-report summary { cursor: pointer; font-weight: 500; }
#nfs-crust-efs-report .result-list { list-style: none; margin: 0; padding: 0; }
#nfs-crust-efs-report .result-item { padding-block: .75rem; border-bottom: 1px solid var(--report-border); }
#nfs-crust-efs-report .result-heading, #nfs-crust-efs-report .result-context { margin: 0; }
#nfs-crust-efs-report .result-context { margin-block: .15rem .5rem; overflow-wrap: anywhere; }
#nfs-crust-efs-report .result-metrics { display: grid; grid-template-columns: repeat(auto-fit,minmax(min(100%,8rem),1fr)); gap: .5rem 1rem; margin: 0; }
#nfs-crust-efs-report .result-metrics div { min-width: 0; }
#nfs-crust-efs-report .result-metrics dt { color: var(--report-muted); }
#nfs-crust-efs-report .result-metrics dd { margin: 0; overflow-wrap: anywhere; }
#nfs-crust-efs-report .method-list li { margin-block: .45rem; }
#nfs-crust-efs-report .validity { display: inline-flex; align-items: center; gap: .4rem; }
#nfs-crust-efs-report .validity-dot { width: .65rem; height: .65rem; border-radius: 50%; background: var(--report-series-1); }
#nfs-crust-efs-report .validity.invalid .validity-dot { background: var(--report-destructive); }
#nfs-crust-efs-report .chart-empty { padding-block: 1rem; color: var(--report-muted); }
@media (max-width: 700px) {
  #nfs-crust-efs-report .optional { display: none; }
  #nfs-crust-efs-report th, #nfs-crust-efs-report td { padding-inline: .25rem; }
  #nfs-crust-efs-report .report-chart { overflow-x: auto; overscroll-behavior-inline: contain; }
  #nfs-crust-efs-report .report-chart svg { min-width: 42rem; }
  #nfs-crust-efs-report .mobile-chart-hint { display: block; color: var(--report-muted); }
  #nfs-crust-efs-report .mobile-data-chart-title { display: block; margin-bottom: .5rem; }
  #nfs-crust-efs-report .data-chart .chart-title { display: none; }
}
@media print { #nfs-crust-efs-report details { display: block; } #nfs-crust-efs-report details > * { display: block; } }
</style>
"""


def generate_report(run_root: Path) -> tuple[str, list[dict[str, Any]]]:
    summary_path = run_root / "run" / "run" / "summary.json"
    if not summary_path.exists():
        summary_path = run_root / "summary.json"
    summary = load_json(summary_path)
    if not summary:
        raise SystemExit(f"summary not found under {run_root}")
    if summary.get("schema_version") != 4:
        raise SystemExit(
            f'unsupported benchmark summary schema {summary.get("schema_version")!r}; expected 4'
        )
    provenance = load_json(run_root / "provenance.json", {})
    cleanup = load_json(run_root / "cleanup.json", {})
    cloudwatch = load_cloudwatch(run_root / "cloudwatch")
    host_directory = run_root / "run" / "host"
    tcp_before = load_proc_net_snmp(host_directory / "proc-net-snmp-before.txt", "Tcp")
    tcp_after = load_proc_net_snmp(host_directory / "proc-net-snmp-after.txt", "Tcp")
    tcp_established_resets = counter_delta(tcp_before, tcp_after, "EstabResets")
    tcp_retransmitted_segments = counter_delta(tcp_before, tcp_after, "RetransSegs")
    scenarios = summary["scenarios"]
    groups = aggregate_scenarios(scenarios)
    scenario_specs = {scenario["spec"]["id"]: scenario["spec"] for scenario in scenarios}
    raw_path = run_root / "run" / "run" / "raw-samples.jsonl.gz"
    if not raw_path.exists():
        raw_path = run_root / "run" / "run" / "raw-samples.jsonl"
    (
        tails,
        seconds,
        raw_counts,
        raw_reconciled_counts,
        raw_timings,
        raw_samples_well_formed,
        raw_reconciliations_well_formed,
    ) = read_raw_samples(raw_path, scenario_specs)
    checksums_valid, checksum_message = verify_checksum_manifest(run_root / "run")
    remote_run = load_json(run_root / "run" / "remote-run.json", {})
    remote_exit_valid = remote_run.get("benchmark_exit_code") == 0
    required_families = {
        "latency-profile",
        "concurrency-scaling",
        "open-loop-equal-demand-85pct",
        "open-loop-equal-utilization-85pct",
    }
    observed_families = {scenario["spec"]["family"] for scenario in scenarios}
    quick = summary["metadata"].get("quick")
    diagnostics = summary["metadata"].get("diagnostics", False)
    repetitions = summary["metadata"].get("repetitions", 0)
    expected_open, _ = expected_open_loop_plan(summary, quick, repetitions)
    expected_scenario_count = len(
        expected_closed_loop_plan(quick, repetitions, diagnostics)
    ) + len(expected_open)
    scenarios_complete, scenario_plan_message = validate_scenario_plan(summary)
    scenarios_complete = scenarios_complete and required_families <= observed_families
    raw_counts_match = raw_path.exists() and raw_samples_well_formed and set(raw_counts) == {
        scenario["spec"]["id"] for scenario in scenarios
    }
    for scenario in scenarios:
        counts = raw_counts.get(scenario["spec"]["id"], {})
        measurement_errors = scenario.get("measurement_errors")
        raw_counts_match = raw_counts_match and all(
            [
                set(counts) <= {"ok", "error", "dropped"},
                counts.get("ok", 0) == scenario["successes"],
                counts.get("dropped", 0) == scenario["overload_drops"],
                isinstance(measurement_errors, int) and not isinstance(measurement_errors, bool),
                counts.get("error", 0) == measurement_errors,
                scenario.get("p99_sample_sufficient") == (scenario["successes"] >= 1_000),
                scenario.get("p999_sample_sufficient") == (scenario["successes"] >= 10_000),
            ]
        )
    raw_distributions_match = raw_path.exists() and raw_samples_well_formed
    for scenario in scenarios:
        scenario_id = scenario["spec"]["id"]
        timings = raw_timings.get(
            scenario_id,
            {"service_ns": [], "queue_ns": [], "total_response_ns": []},
        )
        raw_distributions_match = raw_distributions_match and all(
            [
                distribution_matches_raw(scenario.get("service_latency"), timings["service_ns"]),
                distribution_matches_raw(scenario.get("queue_latency"), timings["queue_ns"]),
                distribution_matches_raw(
                    scenario.get("total_response_latency"), timings["total_response_ns"]
                ),
            ]
        )
    raw_reconciliations_match = raw_path.exists() and raw_reconciliations_well_formed
    for scenario in scenarios:
        scenario_id = scenario["spec"]["id"]
        expected = scenario["reconciled_outcomes"]
        expected_is_integer = isinstance(expected, int) and not isinstance(expected, bool)
        raw_reconciliations_match = raw_reconciliations_match and expected_is_integer
        if expected_is_integer:
            raw_reconciliations_match = raw_reconciliations_match and all(
                [
                    expected >= 0,
                    expected <= scenario["successes"],
                    raw_reconciled_counts.get(scenario_id, 0) == expected,
                ]
            )

    metadata = summary["metadata"]
    total_samples = sum(scenario["successes"] for scenario in scenarios)
    open_loop_scenarios = [
        scenario for scenario in scenarios if scenario["spec"]["mode"] == "open-loop"
    ]
    open_loop_samples = sum(scenario["successes"] for scenario in open_loop_scenarios)
    open_loop_measurement_errors = sum(
        scenario.get("measurement_errors", scenario.get("errors", 0))
        for scenario in open_loop_scenarios
    )
    open_loop_drops = sum(scenario["overload_drops"] for scenario in open_loop_scenarios)
    total_reconciled = sum(scenario["reconciled_outcomes"] for scenario in scenarios)
    reconciled_noun = "outcome" if total_reconciled == 1 else "outcomes"
    total_errors = sum(scenario["errors"] for scenario in scenarios)
    total_drops = sum(scenario["overload_drops"] for scenario in scenarios)
    failed_validations = [
        scenario for scenario in scenarios if scenario["cross_backend_validation"].startswith("FAILED")
    ]
    efs = provenance.get("efs", {})
    topology_valid = all(
        [
            efs.get("regional"),
            efs.get("encrypted"),
            efs.get("throughput_mode") == "elastic",
            efs.get("same_az_mount_target"),
        ]
    )
    transport = provenance.get("transport", {})
    harness = summary.get("harness", {})
    transport_valid = all(
        [
            harness.get("rpc_transport") == "tls",
            remote_run.get("rpc_transport") == "tls",
            remote_run.get("linux_mount_transport") == "tls",
            transport.get("nfs_crust") == "tls",
            transport.get("linux_reference") == "tls",
            isinstance(metadata.get("tls_server_name"), str),
            metadata.get("tls_server_name") == remote_run.get("tls_server_name"),
            metadata.get("tls_server_name") == transport.get("tls_server_name"),
        ]
    )
    valid = (
        not metadata.get("quick")
        and not metadata.get("source_dirty")
        and total_errors == 0
        and total_drops == 0
        and not failed_validations
        and metadata.get("mount_remote_read_mode") == "o-direct"
        and checksums_valid
        and remote_exit_valid
        and scenarios_complete
        and raw_counts_match
        and raw_distributions_match
        and raw_reconciliations_match
        and topology_valid
        and transport_valid
        and cleanup.get("all_throwaway_resources_removed") is True
    )

    read_nfs = select_group(
        groups,
        family="concurrency-scaling",
        backend="nfs-crust",
        operation="get-known-size",
        object_size_bytes=MIB,
        concurrency=32,
    )
    read_linux = select_group(
        groups,
        family="concurrency-scaling",
        backend="linux-remote",
        operation="get-known-size",
        object_size_bytes=MIB,
        concurrency=32,
    )
    write_nfs = select_group(
        groups,
        family="concurrency-scaling",
        backend="nfs-crust",
        operation="put-create-new",
        object_size_bytes=128 * KIB,
        concurrency=32,
    )
    write_linux = select_group(
        groups,
        family="concurrency-scaling",
        backend="linux-remote",
        operation="put-create-new",
        object_size_bytes=128 * KIB,
        concurrency=32,
    )
    percent_io = cloudwatch_max(cloudwatch, "efs-PercentIOLimit-maximum", "Maximum")
    cpu_max = cloudwatch_max(cloudwatch, "ec2-CPUUtilization-maximum", "Maximum")
    permitted = cloudwatch_max(cloudwatch, "efs-PermittedThroughput-average", "Average")
    tcp_diagnostics = (
        f"{tcp_retransmitted_segments:,} retransmitted segments · {tcp_established_resets:,} connection resets"
        if tcp_retransmitted_segments is not None and tcp_established_resets is not None
        else "unavailable"
    )

    status_class = "validity" if valid else "validity invalid"
    status_text = (
        "Data checks passed"
        if valid
        else "Invalid or incomplete — see data quality"
    )
    cards = "".join(
        [
            stat_card(
                "1 MiB known-size reads · 32 concurrent workers",
                fmt_mibps(metric(read_nfs, "mibps")),
                f'Linux: {fmt_mibps(metric(read_linux, "mibps"))}',
            ),
            stat_card(
                "128 KiB create-new writes · 32 concurrent workers",
                fmt_rate(metric(write_nfs, "ops")),
                f'Linux: {fmt_rate(metric(write_linux, "ops"))} · uncertainty and observed ranges are in All results',
            ),
            stat_card(
                "128 KiB scheduled-load tests",
                f"{open_loop_samples:,} successful operations",
                f'{open_loop_measurement_errors} error{"" if open_loop_measurement_errors == 1 else "s"} · '
                f'{open_loop_drops} drop{"" if open_loop_drops == 1 else "s"} · '
                "known-size reads and create-new writes · tested at 85% of measured peak request rate",
            ),
        ]
    )

    latency_charts = []
    for operation in ["get", "get-known-size", "put-create-new", "put-overwrite"]:
        series = []
        legend = []
        series_roles = {}
        bands = []
        hollow_markers = []
        for color_index, (backend, backend_label) in enumerate(
            [("nfs-crust", "nfs-crust"), ("linux-remote", "Linux")]
        ):
            backend_groups = sorted(
                (
                    group
                    for group in groups
                    if group["spec"]["family"] == "latency-profile"
                    and group["spec"]["operation"] == operation
                    and group["spec"]["backend"] == backend
                ),
                key=lambda group: group["spec"]["object_size_bytes"],
            )
            p50_label = f"{backend_label} typical (p50)"
            p99_label = f"{backend_label} slowest 1% threshold (p99)"
            series.append(
                (
                    p50_label,
                    [
                        (group["spec"]["object_size_bytes"], metric(group, "total_p50"))
                        for group in backend_groups
                    ],
                    False,
                    color_index,
                )
            )
            series.append(
                (
                    p99_label,
                    [
                        (group["spec"]["object_size_bytes"], metric(group, "total_p99"))
                        for group in backend_groups
                    ],
                    False,
                    color_index,
                )
            )
            legend.append((backend_label, False, color_index))
            series_roles[p50_label] = "percentile-p50"
            series_roles[p99_label] = "percentile-p99"
            bands.append(
                (
                    [
                        (
                            group["spec"]["object_size_bytes"],
                            metric(group, "total_p50"),
                            metric(group, "total_p99"),
                        )
                        for group in backend_groups
                    ],
                    color_index,
                )
            )
            hollow_markers.append(
                (
                    [
                        (group["spec"]["object_size_bytes"], metric(group, "total_p99"))
                        for group in backend_groups
                        if not group["all_p99_sample_sufficient"]
                    ],
                    color_index,
                )
            )
        latency_charts.append(
            svg_line_chart(
                f"{display_operation(operation).capitalize()} response time",
                "The lower edge is the typical response time. The upper edge is the threshold below which 99% of operations completed. The shaded area is the gap between them. Hollow upper markers mean at least one run had fewer than 1,000 successful operations.",
                series,
                "object size",
                "response time",
                fmt_size,
                fmt_latency,
                log_x=True,
                log_y=True,
                show_caption=False,
                legend=legend,
                series_roles=series_roles,
                bands=bands,
                hollow_markers=hollow_markers,
            )
        )

    scaling_charts = []
    for operation in ["get-known-size", "put-create-new"]:
        for size in [128 * KIB, MIB]:
            series = []
            for color_index, (backend, label) in enumerate(
                [("nfs-crust", "nfs-crust"), ("linux-remote", "Linux reading from EFS")]
            ):
                points = [
                    (group["spec"]["concurrency"], metric(group, "mibps"))
                    for group in groups
                    if group["spec"]["family"] == "concurrency-scaling"
                    and group["spec"]["operation"] == operation
                    and group["spec"]["object_size_bytes"] == size
                    and group["spec"]["backend"] == backend
                ]
                series.append((label, points, False, color_index))
            scaling_charts.append(
                svg_line_chart(
                    f"{fmt_size(size)} {display_operation(operation)} throughput",
                    "Median payload throughput across repeated runs. All results reports uncertainty intervals and observed ranges.",
                    series,
                    "concurrent workers",
                    "MiB/s",
                    lambda value: f"{value:g}",
                    lambda value: f"{value:.0f}",
                    show_caption=False,
                )
            )

    tail_charts = []
    timeline_charts = []
    for load_basis, basis_label in [
        ("equal-demand", "same rate: 85% of slower peak"),
        ("equal-utilization", "each at 85% of own peak"),
    ]:
        for operation in ["get-known-size", "put-create-new"]:
            chosen = [
                scenario
                for scenario in scenarios
                if scenario["spec"]["family"] == f"open-loop-{load_basis}-85pct"
                and scenario["spec"]["operation"] == operation
            ]
            ccdf_series = []
            timeline_series = []
            for color_index, (backend, backend_label) in enumerate(
                [("nfs-crust", "nfs-crust"), ("linux-remote", "Linux")]
            ):
                backend_runs = sorted(
                    (scenario for scenario in chosen if scenario["spec"]["backend"] == backend),
                    key=lambda scenario: scenario["spec"]["repetition"],
                )
                for scenario in backend_runs:
                    spec = scenario["spec"]
                    repetition = spec["repetition"]
                    label = f"{backend_label} · run {repetition}"
                    dashed = repetition % 2 == 0
                    ccdf_series.append(
                        (label, ccdf_points(tails.get(spec["id"], [])), dashed, color_index)
                    )
                    timeline_series.append(
                        (
                            label,
                            [
                                (second, quantile(values, 0.99))
                                for second, values in sorted(seconds.get(spec["id"], {}).items())
                                if values
                            ],
                            dashed,
                            color_index,
                        )
                    )
            tail_charts.append(
                svg_line_chart(
                    f"128 KiB {display_operation(operation)} tail · {basis_label}",
                    "At each response-time value, the vertical axis shows the share of operations that took longer. Total response time starts when the operation was scheduled and includes time waiting to run. Curves lower and farther left are better.",
                    ccdf_series,
                    "total response time",
                    "share taking longer",
                    fmt_latency,
                    fmt_percent,
                    log_x=True,
                    log_y=True,
                )
            )
            timeline_charts.append(
                svg_line_chart(
                    f"128 KiB {display_operation(operation)} p99 by second · {basis_label}",
                    "Each point is the 99th-percentile response time for one second: 99% of operations in that second completed at or below it.",
                    timeline_series,
                    "seconds from launch",
                    "slowest 1% threshold (p99)",
                    lambda value: f"{value:.0f}",
                    fmt_latency,
                    log_y=True,
                )
            )

    resource_efficiency_table = comparison_table(
        "Processor cost · Linux checking the size first versus nfs-crust",
        [
            (
                "1 MiB known-size read · 32 concurrent workers",
                fmt_cpu_ms(metric(read_linux, "cpu_ms_per_op")),
                fmt_cpu_ms(metric(read_nfs, "cpu_ms_per_op")),
                ratio(metric(read_nfs, "cpu_ms_per_op"), metric(read_linux, "cpu_ms_per_op")),
            ),
            (
                "128 KiB create-new write · 32 concurrent workers",
                fmt_cpu_ms(metric(write_linux, "cpu_ms_per_op")),
                fmt_cpu_ms(metric(write_nfs, "cpu_ms_per_op")),
                ratio(metric(write_nfs, "cpu_ms_per_op"), metric(write_linux, "cpu_ms_per_op")),
            ),
        ],
    )
    resource_charts = [
        svg_line_chart(
            "EFS I/O limit usage",
            "AWS reports this metric once per minute. 100% means the file system reached its allowed I/O rate during that minute.",
            [("maximum", cloudwatch_series(cloudwatch, "efs-PercentIOLimit-maximum", "Maximum"), False, 0)],
            "minutes",
            "percent",
            lambda value: f"{value:.0f}",
            lambda value: f"{value:.0f}%",
        ),
        svg_line_chart(
            "EC2 processor use",
            "Highest processor use reported by AWS in each one-minute period.",
            [("maximum", cloudwatch_series(cloudwatch, "ec2-CPUUtilization-maximum", "Maximum"), False, 0)],
            "minutes",
            "percent",
            lambda value: f"{value:.0f}",
            lambda value: f"{value:.0f}%",
        ),
    ]

    warnings = []
    if metadata.get("quick"):
        warnings.append("This short run only checks that the benchmark program works; it is not a publishable performance measurement.")
    if metadata.get("source_dirty"):
        warnings.append("The measured source tree was dirty.")
    if total_errors:
        warnings.append(
            f'{total_errors} benchmark error{" was" if total_errors == 1 else "s were"} recorded.'
        )
    if total_drops:
        warnings.append(f"{total_drops} scheduled operations were dropped because too many earlier operations were still unfinished.")
    if total_reconciled:
        warnings.append(
            f"{total_reconciled} create-new "
            f"operation{'s' if total_reconciled != 1 else ''} returned "
            "without a definite final outcome and required a follow-up check before being counted as successful."
        )
    if (tcp_retransmitted_segments or 0) > 0 or (tcp_established_resets or 0) > 0:
        warnings.append(
            "Host-wide TCP counters recorded "
            f"{tcp_retransmitted_segments or 0} retransmitted segments and "
            f"{tcp_established_resets or 0} established resets during the run."
        )
    if failed_validations:
        warnings.append(f"{len(failed_validations)} data-correctness checks between the two clients failed.")
    if metadata.get("mount_remote_read_mode") != "o-direct":
        warnings.append("Linux could not bypass its file-data memory cache, so its reads were not comparable with nfs-crust reads from EFS.")
    if not checksums_valid:
        warnings.append(checksum_message)
    if not remote_exit_valid:
        warnings.append("The benchmark program on the EC2 machine did not finish successfully.")
    if not scenarios_complete:
        warnings.append(
            f"The planned test matrix is incomplete: {scenario_plan_message}; found {len(scenarios)} of {expected_scenario_count} expected runs."
        )
    if not raw_counts_match:
        warnings.append(
            "The raw success and error records do not match the totals in the run summary."
        )
    if not raw_distributions_match:
        warnings.append(
            "Response times recalculated from the raw operation records do not match the run summary."
        )
    if not raw_reconciliations_match:
        warnings.append(
            "The raw follow-up-check records do not match the follow-up totals in the run summary."
        )
    if not topology_valid:
        warnings.append("The required Regional EFS, Elastic throughput, and same-Availability-Zone setup was not verified.")
    if not transport_valid:
        warnings.append("Encrypted connections were not verified for both nfs-crust and Linux.")
    if cleanup.get("all_throwaway_resources_removed") is not True:
        warnings.append("Cleanup of temporary AWS resources was not verified.")
    if percent_io is None:
        warnings.append("AWS did not provide the EFS I/O limit-usage metric.")
    low_p99 = sum(not scenario["p99_sample_sufficient"] for scenario in scenarios)
    low_p999 = sum(not scenario["p999_sample_sufficient"] for scenario in scenarios)

    warning_markup = (
        '<ul class="method-list">' + "".join(f'<li class="text-destructive">{html.escape(warning)}</li>' for warning in warnings) + "</ul>"
        if warnings
        else '<p>All planned scenarios completed with zero operation errors, zero overload drops, and successful cross-backend checks.</p>'
    )

    source_revision = metadata.get("source_revision", "unknown")
    repetition_count = metadata["repetitions"]
    repetition_noun = "run" if repetition_count == 1 else "runs"
    fragment = f"""
{STYLES}
<article id="nfs-crust-efs-report">
<header>
<p class="eyebrow">nfs-crust · AWS EFS benchmark</p>
<h1>nfs-crust on AWS EFS</h1>
<p class="{status_class}"><span class="validity-dot" aria-hidden="true"></span><strong>{html.escape(status_text)}</strong></p>
<div class="report-meta"><span>{fmt_timestamp(metadata["started_at_unix_ms"])}</span><span>region {html.escape(provenance.get("region", "unknown"))} · Availability Zone {html.escape(provenance.get("availability_zone", "unknown"))}</span><span>EC2 type {html.escape(provenance.get("instance_type", "unknown"))}</span><span>source revision <code>{html.escape(source_revision[:12])}</code></span></div>
</header>

<section class="report-section" aria-labelledby="summary-heading">
<h2 id="summary-heading">Key results</h2>
<div class="viz-grid">{cards}</div>
<p>Each measurement covers the whole operation as the application experiences it. A create-new write includes creating a temporary file, flushing it to durable storage, checking its size, making the finished file visible in one step, and cleaning up. The Linux comparison follows the same steps. Reads served from Linux's memory cache are labeled separately.</p>
<p>If a create-new write lost its final server reply, it counted as successful only after the benchmark proved that the destination contained the expected data, or proved that it was absent and then retried successfully. The report counts these recovered results separately.</p>
</section>

<section class="report-section" aria-labelledby="guide-heading">
<h2 id="guide-heading">How to read this report</h2>
<ul class="method-list">
<li><strong>Amazon EFS</strong> is a network file system. This report compares two clients for it: nfs-crust and the Linux NFS client.</li>
<li><strong>KiB and MiB</strong> are binary data sizes: 1 KiB is 1,024 bytes and 1 MiB is 1,024 KiB. <strong>ops/s</strong> means completed operations per second; <strong>MiB/s</strong> means payload data completed per second.</li>
<li><strong>µs</strong> means microseconds, or millionths of a second. <strong>ms</strong> means milliseconds, or thousandths of a second. <strong>CPU-ms/op</strong> is the processor time used for each completed operation.</li>
<li><strong>p50</strong> is the median or typical response time: half of operations completed faster and half slower. <strong>p99</strong> is the slowest-1% threshold: 99 of every 100 operations completed at or below it. <strong>p99.9</strong> is the equivalent threshold for 999 of every 1,000 operations.</li>
<li><strong>Concurrent workers</strong> repeatedly start an operation and wait for it to finish before starting the next. More workers allow more operations to be in progress at once.</li>
<li><strong>Scheduled-load tests</strong> start operations at a fixed target rate even when earlier operations are still unfinished. This exposes waiting and queue buildup that the worker tests can hide.</li>
<li><strong>85%</strong> means 85% of a client's measured peak request rate. It does not mean processor use, EFS I/O use, or any other resource-utilization percentage.</li>
<li>For response time and processor cost, lower is better. For throughput, higher is better.</li>
</ul>
</section>

<section class="report-section" aria-labelledby="topology-heading">
<h2 id="topology-heading">Test setup and fair comparison</h2>
<p>nfs-crust and Linux ran on the same EC2 virtual machine and accessed the same EFS file system through the same Availability Zone. Both used encrypted NFSv4.1 connections. The table shows how the two paths were made comparable.</p>
{topology_svg(provenance)}
<table><thead><tr><th>Concern</th><th>nfs-crust</th><th>Linux reference</th></tr></thead><tbody>
<tr><th scope="row">Create a new file</th><td>Write temporary data, flush it durably, verify it, then publish without replacing an existing file.</td><td>Perform the same steps through Linux file-system calls.</td></tr>
<tr><th scope="row">Replace a file</th><td>Write temporary data, flush it durably, verify it, then replace the destination in one step so readers never see a partial file.</td><td>Perform the same steps through Linux file-system calls.</td></tr>
<tr><th scope="row">Read from EFS</th><td>Read directly through NFS without a file-data memory cache.</td><td>Use {html.escape(metadata["mount_remote_read_mode"])} mode to bypass Linux's file-data memory cache; also disable file and directory metadata caches.</td></tr>
<tr><th scope="row">Encrypted connection</th><td>Verify the EFS server's certificate and use encrypted NFS.</td><td>Use Amazon's EFS mount helper with encryption and the same private EFS endpoint.</td></tr>
<tr><th scope="row">Read when size is known</th><td>Use the supplied size and confirm that the file ends exactly there.</td><td>Check the file's size during the timed operation before reading it.</td></tr>
<tr><th scope="row">What the timer includes</th><td>The complete public API call. Writes include making the durable file visible; small writes may schedule final connection cleanup after returning.</td><td>The complete Linux file-system operation, including any time waiting for an available worker thread.</td></tr>
</tbody></table>
</section>

<section class="report-section" aria-labelledby="latency-heading">
<h2 id="latency-heading">Response time by object size</h2>
<p>These tests run one operation at a time. Each chart compares the typical response time (p50) with the slowest-1% threshold (p99) as object size changes. Each plotted value is the median across {repetition_count} separate {repetition_noun}; samples from different runs are not mixed together. Both axes use logarithmic spacing, so each step represents multiplication rather than a fixed addition.</p>
<p class="chart-key"><strong>How to read each band:</strong> bold upper edge = slowest-1% threshold (p99) · faint lower edge = typical response time (p50) · tinted area = gap between them · hollow upper marker = fewer than 1,000 successful operations in at least one run.</p>
<p class="mobile-chart-hint">Scroll charts sideways to see the full plot without shrinking the labels.</p>
<div class="report-grid">{''.join(latency_charts)}</div>
</section>

<section class="report-section" aria-labelledby="scale-heading">
<h2 id="scale-heading">Throughput as concurrent work increases</h2>
<p>These tests use 1, 8, or 32 workers. Each worker starts its next operation only after its previous operation finishes. Throughput is the median across repeated runs and counts payload bytes completed through the end of the operation, not bytes sent over the network.</p>
<div class="report-grid">{''.join(scaling_charts)}</div>
</section>

<section class="report-section" aria-labelledby="tail-heading">
<h2 id="tail-heading">Response time under scheduled load</h2>
<p>These tests schedule 128 KiB known-size reads and create-new writes at a fixed rate, whether or not earlier operations have finished. Response time begins at the scheduled start time, so it includes time waiting for its turn to run as well as time doing the operation.</p>
<p><strong>Same request rate</strong> gives both clients 85% of the slower client's measured peak request rate. <strong>Each client at its own rate</strong> gives each client 85% of its own measured peak. These percentages describe scheduled request rates—not processor use or EFS utilization.</p>
<p>The benchmark uses the demanding 85% load level. Client order was reversed across runs to reduce order bias.</p>
<div class="report-grid">{''.join(tail_charts)}</div>
<div class="report-grid">{''.join(timeline_charts)}</div>
<p class="text-muted">Sample-size check: {low_p99} of {len(scenarios)} runs have fewer than 1,000 successful operations for p99; {low_p999} have fewer than 10,000 for p99.9. Those minimums provide about ten operations beyond each threshold. They do not, by themselves, guarantee statistical confidence.</p>
</section>

<section class="report-section" aria-labelledby="resource-heading">
<h2 id="resource-heading">Resource use</h2>
<div class="viz-grid">
{stat_card("Peak EFS I/O limit usage", f"{percent_io:.1f}%" if percent_io is not None else "unavailable", "100% means EFS reached its allowed I/O rate.")}
{stat_card("Peak EC2 processor use", f"{cpu_max:.1f}%" if cpu_max is not None else "unavailable", "Highest one-minute value reported by AWS.")}
{stat_card("Peak EFS permitted data rate", fmt_mibps(permitted / MIB) if permitted is not None else "unavailable", "The data rate EFS allowed during this test.")}
{stat_card("Host network reliability counters", tcp_diagnostics, "Change during the whole test; may include unrelated traffic.")}
</div>
<div class="report-grid">{''.join(resource_charts)}</div>
<div class="report-grid">{resource_efficiency_table}</div>
<p class="text-muted">Processor cost includes every thread in the benchmark process and is divided by the number of successful operations. Host network counters cover every external network interface and may include unrelated traffic. Per-configuration values are in All results.</p>
</section>

<section class="report-section" aria-labelledby="quality-heading">
<h2 id="quality-heading">Data quality and correctness</h2>
{warning_markup}
<ul class="method-list">
<li>{len(scenarios)} runs across {len(groups)} configurations produced {total_samples:,} successful operation samples. {total_reconciled:,} create-new {reconciled_noun} required a follow-up check after the final server reply was lost or inconclusive.</li>
<li>Data-content checks run outside the timed operation. The benchmark verifies replaced files, a sample of newly created files, and deleted files by reading them through the other client.</li>
<li>An error during untimed preparation, or a measurement with no successful operations, invalidates that test configuration. A follow-up check that is mismatched or inconclusive remains an error.</li>
<li>Downloaded-file integrity: {html.escape(checksum_message)}. Raw success/error counts, follow-up-check flags, and response-time data agree with each run's summary.</li>
</ul>
</section>

<section class="report-section" aria-labelledby="method-heading">
<h2 id="method-heading">Method</h2>
<ul class="method-list">
<li>The test created a fresh, encrypted Regional EFS file system using General Purpose performance mode and Elastic throughput. Its network endpoint was in the same subnet and Availability Zone as the EC2 test machine.</li>
<li>Both clients connected to the same private EFS address from one dedicated {html.escape(provenance.get("instance_type", "EC2"))}. Timed operations did not pass through the developer workstation or AWS Systems Manager.</li>
<li>Both clients used NFSv4.1 over TLS encryption with traditional Unix user/group credentials. nfs-crust verified the EFS server's DNS name against public certificate authorities; Linux used Amazon's EFS mount helper. EFS identity-based (IAM) authorization was off.</li>
<li>nfs-crust requested up to {harness.get("nfs_session_slots", "unknown")} in-flight NFS requests per connection, split reads into {html.escape(fmt_size(harness.get("nfs_read_chunk_bytes", 0)))} chunks, and split writes into {html.escape(fmt_size(harness.get("nfs_write_chunk_bytes", 0)))} EFS-compatible chunks. Individual tests requested 128 KiB or 1 MiB at a time.</li>
<li>Linux reads used {html.escape(metadata["mount_remote_read_mode"])} mode to bypass the file-data memory cache. Linux file-metadata and directory operations used its normal mounted-file-system path. Results explicitly labeled as memory-cache context are never presented as comparable remote EFS reads.</li>
<li>Results apply to this EC2 type, EFS configuration, region and Availability Zone, Linux kernel, and measurement window. Each configuration has {repetition_count} separate {repetition_noun}. The uncertainty intervals describe variation among those runs; they do not capture other machines, times, regions, or production environments.</li>
<li>Read tests rotate among two files per worker. Linux bypassed its memory cache, but both clients still read files that had already been accessed. These are not first-read or production-traffic measurements.</li>
</ul>
</section>

<section class="report-section" aria-labelledby="appendix-heading">
<h2 id="appendix-heading">All results</h2>
<p>Each main value is the median across separate runs. Brackets show a 95% uncertainty interval calculated by resampling those run-level values, followed by the lowest and highest observed run. Operation samples from separate runs are never mixed together. “Reconciled” counts create-new writes that needed a follow-up check after an inconclusive final reply. † means at least one run had fewer than 1,000 successful operations for p99; ‡ means at least one had fewer than 10,000 for p99.9.</p>
{full_results_tables(groups)}
</section>
</article>
"""

    return fragment, groups


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--fragment", type=Path, required=True)
    args = parser.parse_args()

    fragment, _ = generate_report(args.run_root)
    args.fragment.parent.mkdir(parents=True, exist_ok=True)
    args.fragment.write_text(fragment, encoding="utf-8")
    size = args.fragment.stat().st_size
    if size >= 2_000_000:
        raise SystemExit(f"fragment is too large: {size} bytes")
    if '\\"' in fragment or "\\n" in fragment:
        raise SystemExit("fragment contains escaped markup")
    print(args.fragment)


if __name__ == "__main__":
    main()
