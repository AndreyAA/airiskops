#!/usr/bin/env python3
import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = ROOT_DIR / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from generate_events import SCENARIOS, generate_live_tick_batch  # noqa: E402


TOPIC_TO_BATCH_KEY = {
    "agent-requests": "requests",
    "agent-responses": "responses",
    "guardrail-findings": "findings",
}
DEFAULT_DURATION_SECONDS = 300
DEFAULT_REQUESTS_PER_SECOND = 1
DEFAULT_SESSIONS = 12
DEFAULT_AGENT_ID = "agent-risk-01"
DEFAULT_SEED = 42
KAFKA_CONTAINER_SERVICE = "kafka"
KAFKA_PRODUCER_PATH = "/opt/kafka/bin/kafka-console-producer.sh"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="mixed")
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--requests-per-second", type=int, default=DEFAULT_REQUESTS_PER_SECOND)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--agent-id", default=DEFAULT_AGENT_ID)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args()


def create_producer(topic: str) -> subprocess.Popen[str]:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        KAFKA_CONTAINER_SERVICE,
        KAFKA_PRODUCER_PATH,
        "--bootstrap-server",
        KAFKA_BOOTSTRAP_SERVER,
        "--topic",
        topic,
    ]
    return subprocess.Popen(
        command,
        cwd=ROOT_DIR,
        stdin=subprocess.PIPE,
        text=True,
    )


def write_rows(producer: subprocess.Popen[str], rows: List[dict]) -> None:
    if producer.stdin is None:
        raise RuntimeError("Kafka producer stdin is not available")
    for row in rows:
        producer.stdin.write(json.dumps(row, ensure_ascii=True) + "\n")
    producer.stdin.flush()


def close_producer(producer: subprocess.Popen[str]) -> None:
    if producer.stdin is not None and not producer.stdin.closed:
        producer.stdin.close()
    producer.wait(timeout=30)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    producers = {topic: create_producer(topic) for topic in TOPIC_TO_BATCH_KEY}

    try:
        start_monotonic = time.monotonic()
        for tick_index in range(args.duration_seconds):
            tick_time = datetime.now(timezone.utc)
            batch = generate_live_tick_batch(
                rng=rng,
                scenario=args.scenario,
                requests_per_second=args.requests_per_second,
                session_count=args.sessions,
                agent_id=args.agent_id,
                tick_time=tick_time,
                tick_index=tick_index,
            )
            for topic, batch_key in TOPIC_TO_BATCH_KEY.items():
                write_rows(producers[topic], batch[batch_key])

            elapsed_seconds = time.monotonic() - start_monotonic
            remaining_sleep = (tick_index + 1) - elapsed_seconds
            if remaining_sleep > 0:
                time.sleep(remaining_sleep)

        print(
            f"Live stream published for {args.duration_seconds} seconds, "
            f"{args.requests_per_second} request(s)/second, scenario={args.scenario}"
        )
    finally:
        for producer in producers.values():
            close_producer(producer)


if __name__ == "__main__":
    main()
