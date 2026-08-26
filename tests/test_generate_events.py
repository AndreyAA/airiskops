import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "generate_events.py"


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


if __name__ == "__main__":
    unittest.main()
