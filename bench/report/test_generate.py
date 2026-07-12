import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("generate.py")
SPEC = importlib.util.spec_from_file_location("benchmark_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def rate_basis() -> list[dict[str, float | int | str]]:
    bases = []
    for operation, nfs_peak, linux_peak in [
        ("get-known-size", 4_000.0, 600.0),
        ("put-create-new", 800.0, 300.0),
    ]:
        for fraction in [0.60, 0.85]:
            bases.append(
                {
                    "operation": operation,
                    "object_size_bytes": 128 * REPORT.KIB,
                    "nfs_crust_peak_ops_per_sec": nfs_peak,
                    "linux_remote_peak_ops_per_sec": linux_peak,
                    "load_fraction": fraction,
                    "equal_demand_ops_per_sec": min(nfs_peak, linux_peak) * fraction,
                    "nfs_crust_equal_utilization_ops_per_sec": nfs_peak * fraction,
                    "linux_remote_equal_utilization_ops_per_sec": linux_peak * fraction,
                }
            )
    return bases


def planned_summary(quick: bool, repetitions: int, diagnostics: bool = False) -> dict:
    summary = {
        "schema_version": 4,
        "metadata": {
            "quick": quick,
            "diagnostics": diagnostics,
            "repetitions": repetitions,
        },
        "open_loop_rate_basis": rate_basis(),
    }
    open_specs, error = REPORT.expected_open_loop_plan(summary, quick, repetitions)
    assert error is None
    specs = REPORT.expected_closed_loop_plan(quick, repetitions, diagnostics) + open_specs
    summary["scenarios"] = [
        {"order": order, "spec": {"id": f"scenario-{order}", **spec}}
        for order, spec in enumerate(specs, 1)
    ]
    return summary


class ReportTests(unittest.TestCase):
    def test_scenario_plans(self) -> None:
        publishable = planned_summary(False, 5)
        valid, message = REPORT.validate_scenario_plan(publishable)
        self.assertTrue(valid, message)
        self.assertEqual(len(publishable["scenarios"]), 360)
        bases = {
            scenario["spec"]["open_loop_load_basis"]
            for scenario in publishable["scenarios"]
            if scenario["spec"]["mode"] == "open-loop"
        }
        self.assertEqual(bases, {"equal-demand", "equal-utilization"})

        quick = planned_summary(True, 1)
        valid, message = REPORT.validate_scenario_plan(quick)
        self.assertTrue(valid, message)
        self.assertEqual(len(quick["scenarios"]), 72)

        diagnostic = planned_summary(False, 5, diagnostics=True)
        valid, message = REPORT.validate_scenario_plan(diagnostic)
        self.assertTrue(valid, message)
        self.assertEqual(len(diagnostic["scenarios"]), 530)

        quick["schema_version"] = 3
        valid, _ = REPORT.validate_scenario_plan(quick)
        self.assertFalse(valid)

    def test_aggregation_reports_intervals_and_resource_efficiency(self) -> None:
        scenarios = []
        for repetition, ops in enumerate([100.0, 110.0, 120.0, 130.0, 140.0], 1):
            distribution = {
                "p50_us": 10.0 + repetition,
                "p90_us": 20.0,
                "p95_us": 25.0,
                "p99_us": 30.0 + repetition,
                "p999_us": 40.0,
                "max_us": 50.0,
            }
            scenarios.append(
                {
                    "order": repetition,
                    "spec": {
                        "id": f"aggregate-{repetition}",
                        "family": "test",
                        "backend": "nfs-crust",
                        "operation": "get-known-size",
                        "mode": "closed-loop",
                        "object_size_bytes": REPORT.MIB,
                        "concurrency": 1,
                        "client_connections": 1,
                        "read_granularity_bytes": 128 * REPORT.KIB,
                        "directory_shards": 64,
                        "read_files_per_worker": 2,
                        "repetition": repetition,
                        "warmup_ms": 1_500,
                        "duration_ms": 6_000,
                        "fixed_operations_per_worker": None,
                        "offered_rate_ops_per_sec": None,
                        "max_outstanding": None,
                        "open_loop_load_basis": None,
                    },
                    "successes": 2_000,
                    "errors": 0,
                    "measurement_errors": 0,
                    "overload_drops": 0,
                    "reconciled_outcomes": 0,
                    "achieved_ops_per_sec": ops,
                    "achieved_mib_per_sec": ops,
                    "service_latency": distribution,
                    "queue_latency": distribution,
                    "total_response_latency": distribution,
                    "p99_sample_sufficient": True,
                    "p999_sample_sufficient": False,
                    "resource_delta": {
                        "user_cpu_ms": 100.0 * repetition,
                        "system_cpu_ms": 20.0 * repetition,
                        "host_rx_bytes": 2_000 * repetition,
                        "host_tx_bytes": 1_000 * repetition,
                    },
                }
            )
        group = REPORT.aggregate_scenarios(scenarios)[0]
        self.assertEqual(group["repetitions"], 5)
        self.assertEqual(group["metrics"]["ops"]["median"], 120.0)
        self.assertLessEqual(
            group["metrics"]["ops"]["ci_low"], group["metrics"]["ops"]["median"]
        )
        self.assertIn("cpu_ms_per_op", group["metrics"])
        self.assertIn("host_bytes_per_op", group["metrics"])
        self.assertTrue(group["all_p99_sample_sufficient"])
        self.assertFalse(group["all_p999_sample_sufficient"])

    def test_quick_report_renders_from_current_summary(self) -> None:
        summary = planned_summary(True, 1)
        summary["metadata"].update(
            {
                "started_at_unix_ms": 0,
                "source_revision": "abc123",
                "source_dirty": False,
                "mount_remote_read_mode": "o-direct",
                "tls_server_name": "fs-test.efs.us-east-1.amazonaws.com",
            }
        )
        summary["harness"] = {
            "rpc_transport": "tls",
            "nfs_session_slots": 64,
            "nfs_read_chunk_bytes": REPORT.MIB,
            "nfs_write_chunk_bytes": 504 * REPORT.KIB,
        }
        distribution = {
            "count": 10_000,
            "min_us": 1.0,
            "mean_us": 2.0,
            "p50_us": 2.0,
            "p90_us": 3.0,
            "p95_us": 4.0,
            "p99_us": 5.0,
            "p999_us": 6.0,
            "max_us": 7.0,
            "stddev_us": 1.0,
        }
        for scenario in summary["scenarios"]:
            scenario.update(
                {
                    "started_at_unix_ms": 0,
                    "actual_elapsed_seconds": 1.0,
                    "successes": 10_000,
                    "measurement_errors": 0,
                    "errors": 0,
                    "overload_drops": 0,
                    "reconciled_outcomes": 0,
                    "achieved_ops_per_sec": 100.0,
                    "achieved_mib_per_sec": 12.5,
                    "service_latency": distribution,
                    "queue_latency": distribution,
                    "total_response_latency": distribution,
                    "p99_sample_sufficient": True,
                    "p999_sample_sufficient": True,
                    "resource_delta": {
                        "user_cpu_ms": 100.0,
                        "system_cpu_ms": 20.0,
                        "process_max_rss_kib": 1_024,
                        "host_rx_bytes": 2_000,
                        "host_tx_bytes": 1_000,
                    },
                    "distinct_errors": [],
                    "cross_backend_validation": "passed",
                }
            )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
            fragment, groups = REPORT.generate_report(root)
        self.assertIn('<article id="nfs-crust-efs-report">', fragment)
        self.assertIn("<svg", fragment)
        self.assertEqual(len(groups), 72)


if __name__ == "__main__":
    unittest.main()
