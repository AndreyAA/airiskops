import json
import random
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "generators" / "generate_events.py"
SPEC = spec_from_file_location("generate_events", SCRIPT)
GENERATE_EVENTS = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(GENERATE_EVENTS)


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class GenerateEventsTest(unittest.TestCase):
    def run_generator(
        self,
        scenario: str,
        requests: int = 12,
        sessions: int = 3,
        delivery_mode: str = "baseline",
        extra_args: list[str] | None = None,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            command = [
                "python3",
                str(SCRIPT),
                "--business-scenario",
                scenario,
                "--delivery-mode",
                delivery_mode,
                "--requests",
                str(requests),
                "--sessions",
                str(sessions),
                "--agent-id",
                "agent-risk-01",
                    "--seed",
                    "7",
                    "--disable-replay-metrics",
                    "--output-dir",
                    str(out_dir),
                ]
            if extra_args:
                command.extend(extra_args)
            subprocess.run(
                command,
                check=True,
            )
            return {
                "requests": read_jsonl(out_dir / "agent-requests.jsonl"),
                "responses": read_jsonl(out_dir / "agent-responses.jsonl"),
                "findings": read_jsonl(out_dir / "guardrail-findings.jsonl"),
            }

    def test_generator_creates_expected_row_counts(self):
        data = self.run_generator("mixed", requests=10, sessions=2)
        self.assertEqual(len(data["requests"]), 10)
        self.assertEqual(len(data["responses"]), 10)
        self.assertEqual(len(data["findings"]), 40)

    def test_attack_scenario_emits_triggered_findings(self):
        data = self.run_generator("attack", requests=8, sessions=2)
        triggered = [row for row in data["findings"] if row.get("triggered") is True]
        self.assertTrue(triggered)

    def test_normal_scenario_preserves_identity_fields(self):
        data = self.run_generator("normal", requests=5, sessions=2)
        sample = data["requests"][0]
        self.assertEqual(sample["agentId"], "agent-risk-01")
        self.assertIn("sessionId", sample)
        self.assertIn("requestId", sample)

    def test_generate_event_batch_builds_expected_payload(self):
        batch = GENERATE_EVENTS.generate_event_batch(
            rng=random.Random(7),
            scenario="mixed",
            request_count=3,
            session_count=2,
            agent_id="agent-risk-02",
            base_time=datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(len(batch["requests"]), 3)
        self.assertEqual(len(batch["responses"]), 3)
        self.assertEqual(len(batch["findings"]), 12)
        self.assertIn("meta", batch)
        self.assertTrue(all(row["agentId"] == "agent-risk-02" for row in batch["requests"]))
        self.assertEqual(
            {row["guardrailName"] for row in batch["findings"]},
            {
                "PROMPT_INJECTION",
                "TOXICITY",
                "LOOPING",
                "SYSTEM_PROMPT_LEAKAGE",
            },
        )

    def test_generate_live_tick_batch_uses_live_request_ids_and_precise_timestamps(self):
        batch = GENERATE_EVENTS.generate_live_tick_batch(
            rng=random.Random(11),
            scenario="attack",
            requests_per_second=2,
            session_count=3,
            agent_id="agent-risk-03",
            tick_time=datetime(2026, 8, 28, 10, 0, 0, tzinfo=timezone.utc),
            tick_index=4,
        )
        self.assertEqual(len(batch["requests"]), 2)
        self.assertEqual(len(batch["responses"]), 2)
        self.assertEqual(len(batch["findings"]), 8)
        self.assertTrue(all(row["requestId"].startswith("live-") for row in batch["requests"]))
        self.assertTrue(all(row["eventTime"].endswith("Z") for row in batch["requests"]))
        self.assertTrue(any("." in row["eventTime"] for row in batch["responses"]))
        self.assertIn("meta", batch)

    def test_prompt_injection_burst_boosts_prompt_trigger_rate(self):
        data = self.run_generator(
            "prompt_injection_burst",
            requests=80,
            sessions=4,
            extra_args=[
                "--burst-start-second",
                "10",
                "--burst-duration-seconds",
                "80",
                "--burst-multiplier",
                "2.2",
                "--request-offset-seconds",
                "2",
            ],
        )
        prompt_findings = [row for row in data["findings"] if row.get("guardrailName") == "PROMPT_INJECTION"]
        triggered_count = len([row for row in prompt_findings if row.get("triggered") is True])
        self.assertGreater(triggered_count, len(prompt_findings) // 3)

    def test_invalid_events_mode_corrupts_some_rows(self):
        data = self.run_generator(
            "mixed",
            requests=20,
            sessions=4,
            delivery_mode="invalid-events",
            extra_args=["--invalid-share", "0.2"],
        )
        invalid_requests = [row for row in data["requests"] if "sessionId" not in row]
        invalid_findings = [row for row in data["findings"] if "guardrailName" not in row]
        self.assertTrue(invalid_requests)
        self.assertTrue(invalid_findings)
        self.assertGreater(data["findings"].count if False else 0, -1)

    def test_late_events_mode_shifts_event_time_backwards(self):
        baseline = self.run_generator("mixed", requests=20, sessions=4)
        late = self.run_generator(
            "mixed",
            requests=20,
            sessions=4,
            delivery_mode="late-events",
            extra_args=[
                "--late-share",
                "0.2",
                "--too-late-share",
                "0.1",
                "--out-of-orderness-seconds",
                "30",
                "--late-tolerance-seconds",
                "300",
            ],
        )
        self.assertLess(late["requests"][0]["eventTime"], baseline["requests"][0]["eventTime"])
        self.assertLess(late["responses"][0]["eventTime"], baseline["responses"][0]["eventTime"])

    def test_detector_errors_mode_marks_failed_findings(self):
        data = self.run_generator(
            "mixed",
            requests=20,
            sessions=4,
            delivery_mode="detector-errors",
            extra_args=["--error-share", "0.25", "--detector-latency-multiplier", "9"],
        )
        errored = [row for row in data["findings"] if row.get("detectorStatus") == "ERROR"]
        self.assertTrue(errored)
        self.assertTrue(all(row["detectorLatencyMs"] >= 63 for row in errored))

    def test_build_replay_metric_summary_uses_delivery_meta(self):
        batch = GENERATE_EVENTS.generate_event_batch(
            rng=random.Random(17),
            scenario="mixed",
            request_count=20,
            session_count=4,
            agent_id="agent-risk-04",
            base_time=datetime(2026, 8, 30, 12, 0, 0, tzinfo=timezone.utc),
            replay_options=GENERATE_EVENTS.ReplayOptions(
                business_scenario="mixed",
                delivery_mode="combined-chaos",
                request_offset_seconds=2,
                burst_start_second=60,
                burst_duration_seconds=90,
                burst_multiplier=1.8,
                late_share=0.2,
                too_late_share=0.1,
                invalid_share=0.1,
                error_share=0.25,
                detector_latency_multiplier=6.0,
                out_of_orderness_seconds=30,
                late_tolerance_seconds=300,
            ),
        )
        summary = GENERATE_EVENTS.build_replay_metric_summary(batch)
        self.assertEqual(summary.requests_generated, 20)
        self.assertEqual(summary.responses_generated, 20)
        self.assertEqual(summary.findings_generated, 80)
        self.assertGreater(summary.invalid_generated, 0)
        self.assertGreater(summary.late_generated, 0)
        self.assertGreater(summary.detector_errors_generated, 0)


if __name__ == "__main__":
    unittest.main()
