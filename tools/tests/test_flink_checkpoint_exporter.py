import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "exporters" / "flink_checkpoint_exporter.py"
SPEC = spec_from_file_location("flink_checkpoint_exporter", SCRIPT)
EXPORTER = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(EXPORTER)


class FlinkCheckpointExporterTest(unittest.TestCase):
    def test_config_entries_to_map_uses_key_value_pairs(self):
        self.assertEqual(
            EXPORTER.config_entries_to_map(
                [
                    {"key": "execution.checkpointing.incremental", "value": "true"},
                    {"key": "execution.checkpointing.dir", "value": "file:///tmp/checkpoints"},
                ]
            ),
            {
                "execution.checkpointing.incremental": "true",
                "execution.checkpointing.dir": "file:///tmp/checkpoints",
            },
        )

    def test_parse_bool_accepts_true_case_insensitively(self):
        self.assertTrue(EXPORTER.parse_bool("TRUE"))
        self.assertFalse(EXPORTER.parse_bool("false"))
        self.assertFalse(EXPORTER.parse_bool(None))

    def test_format_metric_escapes_label_values(self):
        rendered = EXPORTER.format_metric(
            "sample_metric",
            12,
            {"job_name": 'AIRiskOps "MVP"', "operator_name": "Line\\Break"},
        )
        self.assertEqual(
            rendered,
            'sample_metric{job_name="AIRiskOps \\"MVP\\"",operator_name="Line\\\\Break"} 12',
        )

    def test_render_no_job_payload_marks_job_absent(self):
        exporter = EXPORTER.FlinkCheckpointExporter(
            EXPORTER.ExporterConfig(
                flink_base_url="http://jobmanager:8081",
                listen_host="0.0.0.0",
                listen_port=9261,
                scrape_timeout_seconds=5,
                cache_ttl_seconds=15,
                job_name="AIRiskOps MVP Increment 1",
            )
        )
        payload = exporter._render_no_job_payload()
        self.assertIn('airiskops_flink_job_present{job_name="AIRiskOps MVP Increment 1"} 0', payload)

    def test_render_latest_restore_metrics_uses_observed_elapsed(self):
        exporter = EXPORTER.FlinkCheckpointExporter(
            EXPORTER.ExporterConfig(
                flink_base_url="http://jobmanager:8081",
                listen_host="0.0.0.0",
                listen_port=9261,
                scrape_timeout_seconds=5,
                cache_ttl_seconds=15,
                job_name="AIRiskOps MVP Increment 1",
            )
        )
        checkpoints = {
            "counts": {"restored": 2},
            "latest": {
                "restored": {
                    "id": 77,
                    "restore_timestamp": 1_700,
                    "state_size": 2_048,
                    "is_savepoint": False,
                    "external_path": "file:///tmp/chk-77",
                }
            },
        }
        lines = exporter._render_latest_restore_metrics(
            "job-1",
            "AIRiskOps MVP Increment 1",
            checkpoints,
            {"start-time": 1_200},
        )
        payload = "\n".join(lines)
        self.assertIn('airiskops_flink_restore_count{job_id="job-1",job_name="AIRiskOps MVP Increment 1"} 2', payload)
        self.assertIn(
            'airiskops_flink_last_restore_state_size_bytes{job_id="job-1",job_name="AIRiskOps MVP Increment 1"} 2048',
            payload,
        )
        self.assertIn(
            'airiskops_flink_last_restore_observed_elapsed_ms{job_id="job-1",job_name="AIRiskOps MVP Increment 1"} 500',
            payload,
        )


if __name__ == "__main__":
    unittest.main()
