import unittest
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "generators" / "replay_metrics.py"
SPEC = spec_from_file_location("replay_metrics", SCRIPT)
REPLAY_METRICS = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(REPLAY_METRICS)


class ReplayMetricsTest(unittest.TestCase):
    def test_build_metrics_payload_contains_expected_metric_names_and_labels(self):
        payload = REPLAY_METRICS.build_metrics_payload(
            scenario="prompt_injection_burst",
            delivery_mode="combined-chaos",
            source_kind="live",
            agent_id="agent-risk-01",
            summary=REPLAY_METRICS.ReplayMetricSummary(
                requests_generated=25,
                responses_generated=25,
                findings_generated=100,
                triggered_findings_generated=31,
                invalid_generated=7,
                late_generated=5,
                detector_errors_generated=9,
                current_rps=5,
            ),
            status="running",
        )
        self.assertIn("aisafetyops_replay_run_info", payload)
        self.assertIn('scenario="prompt_injection_burst"', payload)
        self.assertIn('mode="combined-chaos"', payload)
        self.assertIn('source_kind="live"', payload)
        self.assertIn('agent_id="agent-risk-01"', payload)
        self.assertIn("aisafetyops_replay_detector_errors_generated_total", payload)
        self.assertIn('stream="findings" 100', payload)

    def test_safe_push_metrics_skips_when_url_missing(self):
        result = REPLAY_METRICS.safe_push_metrics(
            pushgateway_url=None,
            replay_metrics_job="aisafetyops-replay",
            payload="sample",
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
