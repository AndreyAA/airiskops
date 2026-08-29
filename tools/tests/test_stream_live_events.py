import random
import unittest
from argparse import Namespace
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "generators" / "stream_live_events.py"
SPEC = spec_from_file_location("stream_live_events", SCRIPT)
STREAM_LIVE_EVENTS = module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(STREAM_LIVE_EVENTS)


class StreamLiveEventsTest(unittest.TestCase):
    def test_resolve_rps_bounds_uses_fixed_requests_per_second_by_default(self):
        min_rps, max_rps = STREAM_LIVE_EVENTS.resolve_rps_bounds(
            Namespace(
                requests_per_second=3,
                min_requests_per_second=None,
                max_requests_per_second=None,
            )
        )
        self.assertEqual((min_rps, max_rps), (3, 3))

    def test_resolve_rps_bounds_supports_variable_range(self):
        min_rps, max_rps = STREAM_LIVE_EVENTS.resolve_rps_bounds(
            Namespace(
                requests_per_second=1,
                min_requests_per_second=2,
                max_requests_per_second=5,
            )
        )
        self.assertEqual((min_rps, max_rps), (2, 5))

    def test_resolve_rps_bounds_rejects_invalid_range(self):
        with self.assertRaises(ValueError):
            STREAM_LIVE_EVENTS.resolve_rps_bounds(
                Namespace(
                    requests_per_second=1,
                    min_requests_per_second=6,
                    max_requests_per_second=2,
                )
            )

    def test_resolve_requests_per_second_returns_values_within_range(self):
        rng = random.Random(17)
        values = [
            STREAM_LIVE_EVENTS.resolve_requests_per_second(rng, 2, 4)
            for _ in range(20)
        ]
        self.assertTrue(all(2 <= value <= 4 for value in values))
        self.assertGreater(len(set(values)), 1)


if __name__ == "__main__":
    unittest.main()
