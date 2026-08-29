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
    def run_generator(self, scenario: str, requests: int = 12, sessions: int = 3):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--scenario",
                    scenario,
                    "--requests",
                    str(requests),
                    "--sessions",
                    str(sessions),
                    "--agent-id",
                    "agent-risk-01",
                    "--seed",
                    "7",
                    "--output-dir",
                    str(out_dir),
                ],
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


if __name__ == "__main__":
    unittest.main()
