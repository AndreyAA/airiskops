#!/usr/bin/env python3
# Streams live AISafetyOps events into Kafka at a fixed or variable RPS.
# Use when you want to observe changing metrics and dashboards over time on a
# running local stack instead of publishing a one-shot replay batch.
import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[2]
GENERATOR_DIR = Path(__file__).resolve().parent
if str(GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATOR_DIR))

from generate_events import SCENARIOS, generate_live_tick_batch  # noqa: E402


TOPIC_TO_BATCH_KEY = {
    "agent-requests": "requests",
    "agent-responses": "responses",
    "guardrail-findings": "findings",
}
DEFAULT_DURATION_SECONDS = 300
DEFAULT_REQUESTS_PER_SECOND = 1
DEFAULT_MIN_REQUESTS_PER_SECOND = DEFAULT_REQUESTS_PER_SECOND
DEFAULT_MAX_REQUESTS_PER_SECOND = DEFAULT_REQUESTS_PER_SECOND
DEFAULT_SESSIONS = 12
DEFAULT_AGENT_ID = "agent-risk-01"
DEFAULT_SEED = 42
KAFKA_CONTAINER_SERVICE = "kafka"
KAFKA_PRODUCER_PATH = "/opt/kafka/bin/kafka-console-producer.sh"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
COMPOSE_FILE = ROOT_DIR / "deployment" / "local" / "docker-compose.yml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="mixed")
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--requests-per-second", type=int, default=DEFAULT_REQUESTS_PER_SECOND)
    parser.add_argument("--min-requests-per-second", type=int, default=None)
    parser.add_argument("--max-requests-per-second", type=int, default=None)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--agent-id", default=DEFAULT_AGENT_ID)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args()


def resolve_rps_bounds(args: argparse.Namespace) -> tuple[int, int]:
    min_rps = (
        args.min_requests_per_second
        if args.min_requests_per_second is not None
        else args.requests_per_second
    )
    max_rps = (
        args.max_requests_per_second
        if args.max_requests_per_second is not None
        else args.requests_per_second
    )
    if min_rps <= 0:
        raise ValueError("min requests per second must be greater than zero")
    if max_rps <= 0:
        raise ValueError("max requests per second must be greater than zero")
    if min_rps > max_rps:
        raise ValueError("min requests per second must be less than or equal to max requests per second")
    return min_rps, max_rps


def resolve_requests_per_second(rng: random.Random, min_rps: int, max_rps: int) -> int:
    if min_rps == max_rps:
        return min_rps
    return rng.randint(min_rps, max_rps)


def create_producer(topic: str) -> subprocess.Popen[str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
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
    min_rps, max_rps = resolve_rps_bounds(args)
    producers = {topic: create_producer(topic) for topic in TOPIC_TO_BATCH_KEY}
    total_requests_sent = 0

    try:
        start_monotonic = time.monotonic()
        for tick_index in range(args.duration_seconds):
            tick_time = datetime.now(timezone.utc)
            requests_per_second = resolve_requests_per_second(rng, min_rps, max_rps)
            batch = generate_live_tick_batch(
                rng=rng,
                scenario=args.scenario,
                requests_per_second=requests_per_second,
                session_count=args.sessions,
                agent_id=args.agent_id,
                tick_time=tick_time,
                tick_index=tick_index,
            )
            for topic, batch_key in TOPIC_TO_BATCH_KEY.items():
                write_rows(producers[topic], batch[batch_key])
            total_requests_sent += requests_per_second

            elapsed_seconds = time.monotonic() - start_monotonic
            remaining_sleep = (tick_index + 1) - elapsed_seconds
            if remaining_sleep > 0:
                time.sleep(remaining_sleep)

        print(
            f"Live stream published for {args.duration_seconds} seconds, "
            f"scenario={args.scenario}, "
            f"rps-range={min_rps}..{max_rps}, "
            f"total-requests={total_requests_sent}"
        )
    finally:
        for producer in producers.values():
            close_producer(producer)


if __name__ == "__main__":
    main()
