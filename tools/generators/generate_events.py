#!/usr/bin/env python3
import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


GUARDRAILS = (
    "PROMPT_INJECTION",
    "TOXICITY",
    "LOOPING",
    "SYSTEM_PROMPT_LEAKAGE",
)


@dataclass(frozen=True)
class ScenarioProfile:
    prompt_range: tuple[float, float]
    toxicity_range: tuple[float, float]
    looping_probability: float
    leakage_probability: float
    attack_bias: float


SCENARIOS: Dict[str, ScenarioProfile] = {
    "normal": ScenarioProfile((0.01, 0.35), (0.01, 0.40), 0.02, 0.005, 0.0),
    "attack": ScenarioProfile((0.72, 0.99), (0.55, 0.98), 0.20, 0.08, 1.0),
    "mixed": ScenarioProfile((0.05, 0.95), (0.05, 0.92), 0.10, 0.04, 0.5),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), required=True)
    parser.add_argument("--requests", type=int, default=120)
    parser.add_argument("--sessions", type=int, default=12)
    parser.add_argument("--agent-id", default="agent-risk-01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def iso_ts(base: datetime, offset_seconds: int) -> str:
    return (base + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def iso_ts_precise(base: datetime, offset_millis: int) -> str:
    return (base + timedelta(milliseconds=offset_millis)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_jsonl(path: Path, rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def pick_session(index: int, session_count: int) -> str:
    return f"session-{(index % session_count) + 1:03d}"


def triggered_from_confidence(confidence: float, scenario: str) -> bool:
    if scenario == "normal":
        return confidence >= 0.75
    if scenario == "attack":
        return confidence >= 0.65
    return confidence >= 0.70


def build_request_event(agent_id: str, session_id: str, request_id: str, ts: str, idx: int) -> dict:
    return {
        "eventType": "AGENT_REQUEST",
        "agentId": agent_id,
        "tenantId": agent_id,
        "sessionId": session_id,
        "requestId": request_id,
        "turnId": f"turn-{idx:05d}",
        "eventTime": ts,
        "modelName": "gpt-4.1-mini",
        "userId": f"user-{(idx % 9) + 1:03d}",
        "channel": "web",
        "inputTokens": 150 + (idx % 120),
        "outputTokens": 0,
    }


def build_response_event(agent_id: str, session_id: str, request_id: str, ts: str, idx: int) -> dict:
    return {
        "eventType": "AGENT_RESPONSE",
        "agentId": agent_id,
        "tenantId": agent_id,
        "sessionId": session_id,
        "requestId": request_id,
        "turnId": f"turn-{idx:05d}",
        "eventTime": ts,
        "modelName": "gpt-4.1-mini",
        "userId": f"user-{(idx % 9) + 1:03d}",
        "channel": "web",
        "inputTokens": 0,
        "outputTokens": 250 + (idx % 220),
    }


def build_guardrail_findings(
    rng: random.Random,
    scenario: str,
    profile: ScenarioProfile,
    agent_id: str,
    session_id: str,
    request_id: str,
    ts: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
) -> List[dict]:
    prompt_conf = round(rng.uniform(*profile.prompt_range), 4)
    toxicity_conf = round(rng.uniform(*profile.toxicity_range), 4)
    looping = rng.random() < profile.looping_probability
    leakage = rng.random() < profile.leakage_probability
    return [
        {
            "eventType": "GUARDRAIL_FINDING",
            "guardrailName": "PROMPT_INJECTION",
            "guardrailVersion": "pi-v1",
            "policyVersion": "policy-v1",
            "agentId": agent_id,
            "tenantId": agent_id,
            "sessionId": session_id,
            "requestId": request_id,
            "eventTime": ts,
            "modelName": model_name,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "confidence": prompt_conf,
            "triggered": triggered_from_confidence(prompt_conf, scenario),
            "detectorLatencyMs": 15 + int(profile.attack_bias * 10),
            "detectorStatus": "OK",
        },
        {
            "eventType": "GUARDRAIL_FINDING",
            "guardrailName": "TOXICITY",
            "guardrailVersion": "tox-v1",
            "policyVersion": "policy-v1",
            "agentId": agent_id,
            "tenantId": agent_id,
            "sessionId": session_id,
            "requestId": request_id,
            "eventTime": ts,
            "modelName": model_name,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "confidence": toxicity_conf,
            "triggered": triggered_from_confidence(toxicity_conf, scenario),
            "detectorLatencyMs": 18 + int(profile.attack_bias * 12),
            "detectorStatus": "OK",
        },
        {
            "eventType": "GUARDRAIL_FINDING",
            "guardrailName": "LOOPING",
            "guardrailVersion": "loop-v1",
            "policyVersion": "policy-v1",
            "agentId": agent_id,
            "tenantId": agent_id,
            "sessionId": session_id,
            "requestId": request_id,
            "eventTime": ts,
            "modelName": model_name,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "triggered": looping,
            "detectorLatencyMs": 7,
            "detectorStatus": "OK",
        },
        {
            "eventType": "GUARDRAIL_FINDING",
            "guardrailName": "SYSTEM_PROMPT_LEAKAGE",
            "guardrailVersion": "spl-v1",
            "policyVersion": "policy-v1",
            "agentId": agent_id,
            "tenantId": agent_id,
            "sessionId": session_id,
            "requestId": request_id,
            "eventTime": ts,
            "modelName": model_name,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "triggered": leakage,
            "detectorLatencyMs": 9,
            "detectorStatus": "OK",
        },
    ]


def generate_event_batch(
    rng: random.Random,
    scenario: str,
    request_count: int,
    session_count: int,
    agent_id: str,
    base_time: datetime,
    request_offset_seconds: int = 2,
) -> Dict[str, List[dict]]:
    profile = SCENARIOS[scenario]
    requests: List[dict] = []
    responses: List[dict] = []
    findings: List[dict] = []

    for idx in range(request_count):
        session_id = pick_session(idx, session_count)
        request_id = f"req-{int(base_time.timestamp())}-{idx + 1:06d}"
        request_ts = iso_ts(base_time, idx * request_offset_seconds)
        response_ts = iso_ts(base_time, idx * request_offset_seconds + 1)
        model_name = "gpt-4.1-mini"
        input_tokens = 150 + (idx % 120)
        output_tokens = 250 + (idx % 220)

        requests.append(build_request_event(agent_id, session_id, request_id, request_ts, idx))
        responses.append(build_response_event(agent_id, session_id, request_id, response_ts, idx))
        findings.extend(
            build_guardrail_findings(
                rng=rng,
                scenario=scenario,
                profile=profile,
                agent_id=agent_id,
                session_id=session_id,
                request_id=request_id,
                ts=response_ts,
                model_name=model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
        )

    return {
        "requests": requests,
        "responses": responses,
        "findings": findings,
    }


def generate_live_tick_batch(
    rng: random.Random,
    scenario: str,
    requests_per_second: int,
    session_count: int,
    agent_id: str,
    tick_time: datetime,
    tick_index: int,
) -> Dict[str, List[dict]]:
    profile = SCENARIOS[scenario]
    requests: List[dict] = []
    responses: List[dict] = []
    findings: List[dict] = []

    for idx in range(requests_per_second):
        global_index = tick_index * requests_per_second + idx
        session_id = pick_session(global_index, session_count)
        request_id = f"live-{int(tick_time.timestamp())}-{global_index + 1:06d}"
        request_ts = iso_ts_precise(tick_time, idx * 100)
        response_ts = iso_ts_precise(tick_time, idx * 100 + 50)
        model_name = "gpt-4.1-mini"
        input_tokens = 150 + (global_index % 120)
        output_tokens = 250 + (global_index % 220)

        requests.append(build_request_event(agent_id, session_id, request_id, request_ts, global_index))
        responses.append(build_response_event(agent_id, session_id, request_id, response_ts, global_index))
        findings.extend(
            build_guardrail_findings(
                rng=rng,
                scenario=scenario,
                profile=profile,
                agent_id=agent_id,
                session_id=session_id,
                request_id=request_id,
                ts=response_ts,
                model_name=model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
        )

    return {
        "requests": requests,
        "responses": responses,
        "findings": findings,
    }


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_time = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
    batch = generate_event_batch(
        rng=rng,
        scenario=args.scenario,
        request_count=args.requests,
        session_count=args.sessions,
        agent_id=args.agent_id,
        base_time=base_time,
    )

    write_jsonl(out_dir / "agent-requests.jsonl", batch["requests"])
    write_jsonl(out_dir / "agent-responses.jsonl", batch["responses"])
    write_jsonl(out_dir / "guardrail-findings.jsonl", batch["findings"])


if __name__ == "__main__":
    main()
