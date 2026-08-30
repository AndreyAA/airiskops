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

from generate_events import (  # noqa: E402
    DELIVERY_MODES,
    SCENARIOS,
    build_replay_metric_summary,
    build_replay_options,
    generate_live_tick_batch,
)
from replay_metrics import (  # noqa: E402
    DEFAULT_PUSHGATEWAY_URL,
    DEFAULT_REPLAY_METRICS_JOB,
    build_metrics_payload,
    safe_push_metrics,
)


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
    parser.add_argument(
        "--business-scenario",
        "--scenario",
        dest="business_scenario",
        choices=sorted(SCENARIOS),
        default="mixed",
    )
    parser.add_argument(
        "--delivery-mode",
        "--mode",
        dest="delivery_mode",
        choices=DELIVERY_MODES,
        default="baseline",
    )
    parser.add_argument("--duration-seconds", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--requests-per-second", type=int, default=DEFAULT_REQUESTS_PER_SECOND)
    parser.add_argument("--min-requests-per-second", type=int, default=None)
    parser.add_argument("--max-requests-per-second", type=int, default=None)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--agent-id", default=DEFAULT_AGENT_ID)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--request-offset-seconds", type=int, default=1)
    parser.add_argument("--burst-start-second", type=int, default=60)
    parser.add_argument("--burst-duration-seconds", type=int, default=90)
    parser.add_argument("--burst-multiplier", type=float, default=1.8)
    parser.add_argument("--late-share", type=float, default=0.12)
    parser.add_argument("--too-late-share", type=float, default=0.05)
    parser.add_argument("--invalid-share", type=float, default=0.05)
    parser.add_argument("--error-share", type=float, default=0.08)
    parser.add_argument("--detector-latency-multiplier", type=float, default=6.0)
    parser.add_argument("--out-of-orderness-seconds", type=int, default=30)
    parser.add_argument("--late-tolerance-seconds", type=int, default=300)
    parser.add_argument("--pushgateway-url", default=DEFAULT_PUSHGATEWAY_URL)
    parser.add_argument("--replay-metrics-job", default=DEFAULT_REPLAY_METRICS_JOB)
    parser.add_argument("--disable-replay-metrics", action="store_true")
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
    replay_options = build_replay_options(args)
    producers = {topic: create_producer(topic) for topic in TOPIC_TO_BATCH_KEY}
    total_requests_sent = 0
    total_responses_sent = 0
    total_findings_sent = 0
    total_triggered_findings_sent = 0
    total_invalid_generated = 0
    total_late_generated = 0
    total_detector_errors_generated = 0

    try:
        start_monotonic = time.monotonic()
        for tick_index in range(args.duration_seconds):
            tick_time = datetime.now(timezone.utc)
            requests_per_second = resolve_requests_per_second(rng, min_rps, max_rps)
            batch = generate_live_tick_batch(
                rng=rng,
                scenario=args.business_scenario,
                requests_per_second=requests_per_second,
                session_count=args.sessions,
                agent_id=args.agent_id,
                tick_time=tick_time,
                tick_index=tick_index,
                replay_options=replay_options,
            )
            for topic, batch_key in TOPIC_TO_BATCH_KEY.items():
                write_rows(producers[topic], batch[batch_key])
            tick_summary = build_replay_metric_summary(batch)
            total_requests_sent += tick_summary.requests_generated
            total_responses_sent += tick_summary.responses_generated
            total_findings_sent += tick_summary.findings_generated
            total_triggered_findings_sent += tick_summary.triggered_findings_generated
            total_invalid_generated += tick_summary.invalid_generated
            total_late_generated += tick_summary.late_generated
            total_detector_errors_generated += tick_summary.detector_errors_generated
            if not args.disable_replay_metrics:
                push_error = safe_push_metrics(
                    args.pushgateway_url,
                    args.replay_metrics_job,
                    build_metrics_payload(
                        scenario=args.business_scenario,
                        delivery_mode=args.delivery_mode,
                        source_kind="live",
                        agent_id=args.agent_id,
                        summary=tick_summary,
                        status="running",
                    ),
                )
                if push_error is not None:
                    print(f"{push_error}. Status: не исправлено")

            elapsed_seconds = time.monotonic() - start_monotonic
            remaining_sleep = (tick_index + 1) - elapsed_seconds
            if remaining_sleep > 0:
                time.sleep(remaining_sleep)

        print(
            f"Live stream published for {args.duration_seconds} seconds, "
            f"scenario={args.business_scenario}, "
            f"mode={args.delivery_mode}, "
            f"rps-range={min_rps}..{max_rps}, "
            f"total-requests={total_requests_sent}"
        )
        if not args.disable_replay_metrics:
            final_summary = build_replay_metric_summary(
                {
                    "requests": [{}] * total_requests_sent,
                    "responses": [{}] * total_responses_sent,
                    "findings": [],
                    "meta": {
                        "totalRequests": total_requests_sent,
                        "totalResponses": total_responses_sent,
                        "totalFindings": total_findings_sent,
                        "triggeredFindings": total_triggered_findings_sent,
                        "invalidRequests": total_invalid_generated,
                        "invalidResponses": 0,
                        "invalidFindings": 0,
                        "lateRequests": total_late_generated,
                        "tooLateRequests": 0,
                        "detectorErrorFindings": total_detector_errors_generated,
                    },
                }
            )
            push_error = safe_push_metrics(
                args.pushgateway_url,
                args.replay_metrics_job,
                build_metrics_payload(
                    scenario=args.business_scenario,
                    delivery_mode=args.delivery_mode,
                    source_kind="live",
                    agent_id=args.agent_id,
                    summary=final_summary,
                    status="completed",
                ),
            )
            if push_error is not None:
                print(f"{push_error}. Status: не исправлено")
    finally:
        for producer in producers.values():
            close_producer(producer)


if __name__ == "__main__":
    main()
