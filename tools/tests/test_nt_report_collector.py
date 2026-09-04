import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "reporters" / "nt_report_collector.py"
SPEC = spec_from_file_location("nt_report_collector", SCRIPT)
COLLECTOR = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(COLLECTOR)


class NtReportCollectorTest(unittest.TestCase):
    def test_resolve_job_id_rejects_ambiguous_running_jobs(self):
        overview = {
            "jobs": [
                {"jid": "one", "name": "AIRiskOps MVP Increment 1", "state": "RUNNING"},
                {"jid": "two", "name": "AIRiskOps MVP Increment 1", "state": "RUNNING"},
            ]
        }
        with self.assertRaisesRegex(ValueError, "Multiple running"):
            COLLECTOR.resolve_job_id(overview, "AIRiskOps MVP Increment 1", None)

    def test_parse_generator_summary_reads_published_counts(self):
        with TemporaryDirectory() as directory:
            log_path = Path(directory) / "generator.log"
            log_path.write_text(
                "Live stream published for 60 seconds, scenario=mixed, mode=baseline, "
                "rps-range=50..50, total-requests=3000, total-findings=12000, "
                "triggered-findings=2010, invalid=0, late=0, detector-errors=0\n",
                encoding="utf-8",
            )
            summary = COLLECTOR.parse_generator_summary(log_path)
        self.assertEqual("3000", summary["total-requests"])
        self.assertEqual("12000", summary["total-findings"])
        self.assertEqual("0", summary["detector-errors"])

    def test_assess_marks_recovery_states_from_kafka_lag_points(self):
        lag_points = {
            "at_generator_end": {"kafka_lag": {"total": 100}},
            "after_settle": {"kafka_lag": {"total": 80}},
            "after_recovery": {"kafka_lag": {"total": 20}},
        }
        self.assertEqual(
            "still-draining",
            COLLECTOR.assess({"backpressure_ms_per_second": 0}, lag_points, 0),
        )
        lag_points["after_recovery"]["kafka_lag"]["total"] = 0
        self.assertEqual(
            "recovered",
            COLLECTOR.assess({"backpressure_ms_per_second": 0}, lag_points, 0),
        )

    def test_render_report_includes_log_navigation(self):
        snapshot = {
            "verdict": "stable",
            "metadata": {
                "run_id": "run-1",
                "run_started_at": "2026-09-05T00:00:00+00:00",
                "run_finished_at": "2026-09-05T00:01:00+00:00",
                "job_id": "job-1",
                "scenario": "mixed",
                "mode": "baseline",
                "rps": 50,
                "duration_seconds": 60,
                "sessions": 12,
                "seed": 42,
                "state_backend": "ROCKSDB",
                "incremental_checkpoints": True,
                "generator_exit_code": 0,
                "generator_log": "runtime/load-tests/run-1.generator.log",
                "raw_snapshot_path": "runtime/load-tests/run-1.json",
                "flink_url": "http://localhost:8081",
                "grafana_url": "http://localhost:3000",
                "prometheus_url": "http://localhost:9090",
                "generator_summary": {"total-requests": "3000"},
            },
            "metrics": {"backpressure_ms_per_second": 0, "jvm_cpu_load": 0.25},
            "kafka_lag_points": {
                "at_generator_end": {
                    "captured_at_epoch": 10.0,
                    "kafka_lag": {"total": 20, "partitions": []},
                },
                "after_settle": {
                    "captured_at_epoch": 40.0,
                    "kafka_lag": {"total": 10, "partitions": []},
                },
                "after_recovery": {
                    "captured_at_epoch": 100.0,
                    "kafka_lag": {"total": 0, "partitions": []},
                },
            },
            "taskmanager_vcpus": 8,
        }
        report = COLLECTOR.render_report(snapshot)
        self.assertIn("TaskManager logs", report)
        self.assertIn("logs --since", report)
        self.assertIn("2.0 vCPU", report)
        self.assertIn("Catch-up rate during settle", report)
        self.assertIn("Catch-up rate during recovery", report)
        self.assertIn("Total catch-up rate", report)


if __name__ == "__main__":
    unittest.main()
