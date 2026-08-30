#!/usr/bin/env python3
# Generates deterministic replay JSONL datasets for the AISafetyOps MVP.
# Use when you need reproducible local test traffic for Kafka replay,
# regression scenarios, or scenario/chaos validation from a fixed seed.
import argparse
import json
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

GENERATOR_DIR = Path(__file__).resolve().parent
if str(GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATOR_DIR))

PROMPT_INJECTION = "PROMPT_INJECTION"
TOXICITY = "TOXICITY"
LOOPING = "LOOPING"
SYSTEM_PROMPT_LEAKAGE = "SYSTEM_PROMPT_LEAKAGE"
GUARDRAILS = (
    PROMPT_INJECTION,
    TOXICITY,
    LOOPING,
    SYSTEM_PROMPT_LEAKAGE,
)
DEFAULT_AGENT_ID = "agent-risk-01"
DEFAULT_MODEL_NAME = "gpt-4.1-mini"
DEFAULT_POLICY_VERSION = "policy-v1"
DEFAULT_REQUEST_COUNT = 120
DEFAULT_SESSION_COUNT = 12
DEFAULT_REQUEST_OFFSET_SECONDS = 2
DEFAULT_SEED = 42
DEFAULT_BURST_START_SECOND = 60
DEFAULT_BURST_DURATION_SECONDS = 90
DEFAULT_BURST_MULTIPLIER = 1.8
DEFAULT_LATE_SHARE = 0.12
DEFAULT_TOO_LATE_SHARE = 0.05
DEFAULT_INVALID_SHARE = 0.05
DEFAULT_ERROR_SHARE = 0.08
DEFAULT_DETECTOR_LATENCY_MULTIPLIER = 6.0
DEFAULT_OUT_OF_ORDERNESS_SECONDS = 30
DEFAULT_LATE_TOLERANCE_SECONDS = 300
LATE_EVENT_GRACE_SECONDS = 15
TOO_LATE_EVENT_GRACE_SECONDS = 30
POLICY_REGRESSION_LOW_CONFIDENCE = 0.64
POLICY_REGRESSION_HIGH_CONFIDENCE = 0.78


@dataclass(frozen=True)
class ScenarioProfile:
    prompt_range: tuple[float, float]
    toxicity_range: tuple[float, float]
    looping_probability: float
    leakage_probability: float
    attack_bias: float


@dataclass(frozen=True)
class ReplayOptions:
    business_scenario: str
    delivery_mode: str
    request_offset_seconds: int
    burst_start_second: int
    burst_duration_seconds: int
    burst_multiplier: float
    late_share: float
    too_late_share: float
    invalid_share: float
    error_share: float
    detector_latency_multiplier: float
    out_of_orderness_seconds: int
    late_tolerance_seconds: int


@dataclass(frozen=True)
class DeliveryMutationStats:
    invalid_requests: int
    invalid_responses: int
    invalid_findings: int
    late_requests: int
    too_late_requests: int
    detector_error_findings: int


SCENARIOS: Dict[str, ScenarioProfile] = {
    "normal": ScenarioProfile((0.01, 0.35), (0.01, 0.40), 0.02, 0.005, 0.0),
    "attack": ScenarioProfile((0.72, 0.99), (0.55, 0.98), 0.20, 0.08, 1.0),
    "mixed": ScenarioProfile((0.05, 0.95), (0.05, 0.92), 0.10, 0.04, 0.5),
    "prompt_injection_burst": ScenarioProfile((0.10, 0.78), (0.05, 0.45), 0.04, 0.01, 0.45),
    "toxicity_campaign": ScenarioProfile((0.04, 0.40), (0.18, 0.82), 0.04, 0.01, 0.35),
    "looping_false_positive_check": ScenarioProfile((0.02, 0.22), (0.02, 0.20), 0.42, 0.0, 0.1),
    "policy_regression_case": ScenarioProfile((0.60, 0.80), (0.58, 0.79), 0.03, 0.01, 0.2),
}
DELIVERY_MODES = (
    "baseline",
    "late-events",
    "invalid-events",
    "detector-errors",
    "combined-chaos",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--business-scenario",
        "--scenario",
        dest="business_scenario",
        choices=sorted(SCENARIOS),
        required=True,
    )
    parser.add_argument(
        "--delivery-mode",
        "--mode",
        dest="delivery_mode",
        choices=DELIVERY_MODES,
        default="baseline",
    )
    parser.add_argument("--requests", type=int, default=DEFAULT_REQUEST_COUNT)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSION_COUNT)
    parser.add_argument("--agent-id", default=DEFAULT_AGENT_ID)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--request-offset-seconds", type=int, default=DEFAULT_REQUEST_OFFSET_SECONDS)
    parser.add_argument("--burst-start-second", type=int, default=DEFAULT_BURST_START_SECOND)
    parser.add_argument("--burst-duration-seconds", type=int, default=DEFAULT_BURST_DURATION_SECONDS)
    parser.add_argument("--burst-multiplier", type=float, default=DEFAULT_BURST_MULTIPLIER)
    parser.add_argument("--late-share", type=float, default=DEFAULT_LATE_SHARE)
    parser.add_argument("--too-late-share", type=float, default=DEFAULT_TOO_LATE_SHARE)
    parser.add_argument("--invalid-share", type=float, default=DEFAULT_INVALID_SHARE)
    parser.add_argument("--error-share", type=float, default=DEFAULT_ERROR_SHARE)
    parser.add_argument(
        "--detector-latency-multiplier",
        type=float,
        default=DEFAULT_DETECTOR_LATENCY_MULTIPLIER,
    )
    parser.add_argument("--out-of-orderness-seconds", type=int, default=DEFAULT_OUT_OF_ORDERNESS_SECONDS)
    parser.add_argument("--late-tolerance-seconds", type=int, default=DEFAULT_LATE_TOLERANCE_SECONDS)
    return parser.parse_args()


def build_replay_options(args: argparse.Namespace) -> ReplayOptions:
    return ReplayOptions(
        business_scenario=args.business_scenario,
        delivery_mode=args.delivery_mode,
        request_offset_seconds=args.request_offset_seconds,
        burst_start_second=args.burst_start_second,
        burst_duration_seconds=args.burst_duration_seconds,
        burst_multiplier=args.burst_multiplier,
        late_share=clamp_fraction(args.late_share, "late-share"),
        too_late_share=clamp_fraction(args.too_late_share, "too-late-share"),
        invalid_share=clamp_fraction(args.invalid_share, "invalid-share"),
        error_share=clamp_fraction(args.error_share, "error-share"),
        detector_latency_multiplier=validate_positive_float(
            args.detector_latency_multiplier,
            "detector-latency-multiplier",
        ),
        out_of_orderness_seconds=validate_non_negative_int(
            args.out_of_orderness_seconds,
            "out-of-orderness-seconds",
        ),
        late_tolerance_seconds=validate_non_negative_int(
            args.late_tolerance_seconds,
            "late-tolerance-seconds",
        ),
    )


def validate_positive_float(value: float, field_name: str) -> float:
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def validate_non_negative_int(value: int, field_name: str) -> int:
    if value < 0:
        raise ValueError(f"{field_name} must be greater than or equal to zero")
    return value


def clamp_fraction(value: float, field_name: str) -> float:
    if not 0 <= value <= 1:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return value


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
        "modelName": DEFAULT_MODEL_NAME,
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
        "modelName": DEFAULT_MODEL_NAME,
        "userId": f"user-{(idx % 9) + 1:03d}",
        "channel": "web",
        "inputTokens": 0,
        "outputTokens": 250 + (idx % 220),
    }


def is_burst_active(logical_second: int, options: ReplayOptions) -> bool:
    burst_end_second = options.burst_start_second + options.burst_duration_seconds
    return options.burst_start_second <= logical_second < burst_end_second


def clamp_confidence(value: float) -> float:
    return round(max(0.0, min(value, 0.9999)), 4)


def apply_business_scenario(
    scenario: str,
    logical_second: int,
    prompt_conf: float,
    toxicity_conf: float,
    looping: bool,
    leakage: bool,
    policy_version: str,
    options: ReplayOptions,
) -> dict:
    if scenario == "prompt_injection_burst" and is_burst_active(logical_second, options):
        prompt_conf = clamp_confidence(prompt_conf * options.burst_multiplier + 0.18)
        toxicity_conf = clamp_confidence(toxicity_conf * 0.75)
        leakage = False
    elif scenario == "toxicity_campaign" and is_burst_active(logical_second, options):
        toxicity_conf = clamp_confidence(toxicity_conf * options.burst_multiplier + 0.16)
        prompt_conf = clamp_confidence(prompt_conf * 0.80)
    elif scenario == "looping_false_positive_check":
        looping = logical_second % 3 == 0
        leakage = False
        prompt_conf = clamp_confidence(min(prompt_conf, 0.18))
        toxicity_conf = clamp_confidence(min(toxicity_conf, 0.22))
    elif scenario == "policy_regression_case":
        prompt_conf = POLICY_REGRESSION_HIGH_CONFIDENCE if logical_second % 2 == 0 else POLICY_REGRESSION_LOW_CONFIDENCE
        toxicity_conf = POLICY_REGRESSION_HIGH_CONFIDENCE if logical_second % 3 == 0 else POLICY_REGRESSION_LOW_CONFIDENCE
        policy_version = "policy-regression-v1"

    return {
        "prompt_conf": prompt_conf,
        "toxicity_conf": toxicity_conf,
        "looping": looping,
        "leakage": leakage,
        "policy_version": policy_version,
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
    logical_second: int,
    options: ReplayOptions,
) -> List[dict]:
    prompt_conf = round(rng.uniform(*profile.prompt_range), 4)
    toxicity_conf = round(rng.uniform(*profile.toxicity_range), 4)
    looping = rng.random() < profile.looping_probability
    leakage = rng.random() < profile.leakage_probability
    scenario_values = apply_business_scenario(
        scenario=scenario,
        logical_second=logical_second,
        prompt_conf=prompt_conf,
        toxicity_conf=toxicity_conf,
        looping=looping,
        leakage=leakage,
        policy_version=DEFAULT_POLICY_VERSION,
        options=options,
    )
    prompt_conf = scenario_values["prompt_conf"]
    toxicity_conf = scenario_values["toxicity_conf"]
    looping = scenario_values["looping"]
    leakage = scenario_values["leakage"]
    policy_version = scenario_values["policy_version"]
    return [
        {
            "eventType": "GUARDRAIL_FINDING",
            "guardrailName": PROMPT_INJECTION,
            "guardrailVersion": "pi-v1",
            "policyVersion": policy_version,
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
            "guardrailName": TOXICITY,
            "guardrailVersion": "tox-v1",
            "policyVersion": policy_version,
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
            "guardrailName": LOOPING,
            "guardrailVersion": "loop-v1",
            "policyVersion": policy_version,
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
            "guardrailName": SYSTEM_PROMPT_LEAKAGE,
            "guardrailVersion": "spl-v1",
            "policyVersion": policy_version,
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


def mutate_invalid_row(row: dict) -> dict:
    invalid_row = dict(row)
    event_type = invalid_row.get("eventType")
    if event_type == "AGENT_REQUEST":
        invalid_row.pop("sessionId", None)
    elif event_type == "AGENT_RESPONSE":
        invalid_row["eventTime"] = "not-a-timestamp"
    elif event_type == "GUARDRAIL_FINDING":
        invalid_row.pop("guardrailName", None)
    return invalid_row


def shift_event_time(row: dict, delta_seconds: int) -> dict:
    shifted_row = dict(row)
    event_time = datetime.fromisoformat(shifted_row["eventTime"].replace("Z", "+00:00"))
    shifted_row["eventTime"] = (event_time - timedelta(seconds=delta_seconds)).isoformat().replace("+00:00", "Z")
    return shifted_row


def apply_delivery_mode(batch: Dict[str, List[dict]], options: ReplayOptions) -> Dict[str, List[dict]]:
    mutation_stats = DeliveryMutationStats(0, 0, 0, 0, 0, 0)
    if options.delivery_mode == "baseline":
        return {
            **batch,
            "meta": build_batch_meta(batch, mutation_stats),
        }

    requests = list(batch["requests"])
    responses = list(batch["responses"])
    findings = list(batch["findings"])
    triplet_count = min(len(requests), len(responses))
    if triplet_count == 0:
        return batch

    request_index_to_findings: Dict[int, List[int]] = {}
    for finding_index, _finding in enumerate(findings):
        request_index_to_findings.setdefault(finding_index // len(GUARDRAILS), []).append(finding_index)

    late_count = int(triplet_count * options.late_share)
    too_late_count = int(triplet_count * options.too_late_share)
    invalid_count = int(triplet_count * options.invalid_share)
    error_count = int(len(findings) * options.error_share)
    request_indexes = list(range(triplet_count))

    def apply_late_rows(start: int, count: int, delta_seconds: int) -> None:
        for request_index in request_indexes[start:start + count]:
            requests[request_index] = shift_event_time(requests[request_index], delta_seconds)
            responses[request_index] = shift_event_time(responses[request_index], delta_seconds)
            for finding_index in request_index_to_findings.get(request_index, []):
                findings[finding_index] = shift_event_time(findings[finding_index], delta_seconds)

    if options.delivery_mode in ("late-events", "combined-chaos"):
        late_delta_seconds = options.out_of_orderness_seconds + LATE_EVENT_GRACE_SECONDS
        too_late_delta_seconds = (
            options.out_of_orderness_seconds
            + options.late_tolerance_seconds
            + TOO_LATE_EVENT_GRACE_SECONDS
        )
        apply_late_rows(0, late_count, late_delta_seconds)
        apply_late_rows(late_count, too_late_count, too_late_delta_seconds)
        mutation_stats = DeliveryMutationStats(
            mutation_stats.invalid_requests,
            mutation_stats.invalid_responses,
            mutation_stats.invalid_findings,
            late_count,
            too_late_count,
            mutation_stats.detector_error_findings,
        )

    if options.delivery_mode in ("invalid-events", "combined-chaos"):
        for request_index in request_indexes[-invalid_count:]:
            requests[request_index] = mutate_invalid_row(requests[request_index])
            responses[request_index] = mutate_invalid_row(responses[request_index])
            for finding_index in request_index_to_findings.get(request_index, []):
                findings[finding_index] = mutate_invalid_row(findings[finding_index])
        mutation_stats = DeliveryMutationStats(
            invalid_count,
            invalid_count,
            invalid_count * len(GUARDRAILS),
            mutation_stats.late_requests,
            mutation_stats.too_late_requests,
            mutation_stats.detector_error_findings,
        )

    if options.delivery_mode in ("detector-errors", "combined-chaos"):
        for finding in findings[:error_count]:
            finding["detectorStatus"] = "ERROR"
            finding["detectorLatencyMs"] = int(
                max(1, finding["detectorLatencyMs"] * options.detector_latency_multiplier)
            )
            if "confidence" in finding:
                finding["confidence"] = clamp_confidence(finding["confidence"] * 0.55)
                finding["triggered"] = False
        mutation_stats = DeliveryMutationStats(
            mutation_stats.invalid_requests,
            mutation_stats.invalid_responses,
            mutation_stats.invalid_findings,
            mutation_stats.late_requests,
            mutation_stats.too_late_requests,
            error_count,
        )

    return {
        "requests": requests,
        "responses": responses,
        "findings": findings,
        "meta": build_batch_meta(
            {
                "requests": requests,
                "responses": responses,
                "findings": findings,
            },
            mutation_stats,
        ),
    }


def build_batch_meta(batch: Dict[str, List[dict]], mutation_stats: DeliveryMutationStats) -> dict:
    return {
        "invalidRequests": mutation_stats.invalid_requests,
        "invalidResponses": mutation_stats.invalid_responses,
        "invalidFindings": mutation_stats.invalid_findings,
        "lateRequests": mutation_stats.late_requests,
        "tooLateRequests": mutation_stats.too_late_requests,
        "detectorErrorFindings": mutation_stats.detector_error_findings,
        "triggeredFindings": len([row for row in batch["findings"] if row.get("triggered") is True]),
        "totalRequests": len(batch["requests"]),
        "totalResponses": len(batch["responses"]),
        "totalFindings": len(batch["findings"]),
    }


def build_replay_summary(batch: Dict[str, List[dict]]) -> dict[str, int]:
    meta = batch.get("meta", {})
    return {
        "requestsGenerated": meta.get("totalRequests", len(batch["requests"])),
        "responsesGenerated": meta.get("totalResponses", len(batch["responses"])),
        "findingsGenerated": meta.get("totalFindings", len(batch["findings"])),
        "triggeredFindingsGenerated": meta.get(
            "triggeredFindings",
            len([row for row in batch["findings"] if row.get("triggered") is True]),
        ),
        "invalidGenerated": (
            meta.get("invalidRequests", 0)
            + meta.get("invalidResponses", 0)
            + meta.get("invalidFindings", 0)
        ),
        "lateGenerated": meta.get("lateRequests", 0) + meta.get("tooLateRequests", 0),
        "detectorErrorsGenerated": meta.get("detectorErrorFindings", 0),
    }


def generate_event_batch(
    rng: random.Random,
    scenario: str,
    request_count: int,
    session_count: int,
    agent_id: str,
    base_time: datetime,
    request_offset_seconds: int = DEFAULT_REQUEST_OFFSET_SECONDS,
    replay_options: ReplayOptions | None = None,
) -> Dict[str, List[dict]]:
    options = replay_options or ReplayOptions(
        business_scenario=scenario,
        delivery_mode="baseline",
        request_offset_seconds=request_offset_seconds,
        burst_start_second=DEFAULT_BURST_START_SECOND,
        burst_duration_seconds=DEFAULT_BURST_DURATION_SECONDS,
        burst_multiplier=DEFAULT_BURST_MULTIPLIER,
        late_share=DEFAULT_LATE_SHARE,
        too_late_share=DEFAULT_TOO_LATE_SHARE,
        invalid_share=DEFAULT_INVALID_SHARE,
        error_share=DEFAULT_ERROR_SHARE,
        detector_latency_multiplier=DEFAULT_DETECTOR_LATENCY_MULTIPLIER,
        out_of_orderness_seconds=DEFAULT_OUT_OF_ORDERNESS_SECONDS,
        late_tolerance_seconds=DEFAULT_LATE_TOLERANCE_SECONDS,
    )
    profile = SCENARIOS[scenario]
    requests: List[dict] = []
    responses: List[dict] = []
    findings: List[dict] = []

    for idx in range(request_count):
        session_id = pick_session(idx, session_count)
        request_id = f"req-{int(base_time.timestamp())}-{idx + 1:06d}"
        logical_second = idx * request_offset_seconds
        request_ts = iso_ts(base_time, logical_second)
        response_ts = iso_ts(base_time, logical_second + 1)
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
                model_name=DEFAULT_MODEL_NAME,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                logical_second=logical_second,
                options=options,
            )
        )

    return apply_delivery_mode(
        {
            "requests": requests,
            "responses": responses,
            "findings": findings,
        },
        options,
    )


def generate_live_tick_batch(
    rng: random.Random,
    scenario: str,
    requests_per_second: int,
    session_count: int,
    agent_id: str,
    tick_time: datetime,
    tick_index: int,
    replay_options: ReplayOptions | None = None,
) -> Dict[str, List[dict]]:
    options = replay_options or ReplayOptions(
        business_scenario=scenario,
        delivery_mode="baseline",
        request_offset_seconds=1,
        burst_start_second=DEFAULT_BURST_START_SECOND,
        burst_duration_seconds=DEFAULT_BURST_DURATION_SECONDS,
        burst_multiplier=DEFAULT_BURST_MULTIPLIER,
        late_share=DEFAULT_LATE_SHARE,
        too_late_share=DEFAULT_TOO_LATE_SHARE,
        invalid_share=DEFAULT_INVALID_SHARE,
        error_share=DEFAULT_ERROR_SHARE,
        detector_latency_multiplier=DEFAULT_DETECTOR_LATENCY_MULTIPLIER,
        out_of_orderness_seconds=DEFAULT_OUT_OF_ORDERNESS_SECONDS,
        late_tolerance_seconds=DEFAULT_LATE_TOLERANCE_SECONDS,
    )
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
                model_name=DEFAULT_MODEL_NAME,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                logical_second=tick_index,
                options=options,
            )
        )

    return apply_delivery_mode(
        {
            "requests": requests,
            "responses": responses,
            "findings": findings,
        },
        options,
    )


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_time = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)
    replay_options = build_replay_options(args)
    batch = generate_event_batch(
        rng=rng,
        scenario=args.business_scenario,
        request_count=args.requests,
        session_count=args.sessions,
        agent_id=args.agent_id,
        base_time=base_time,
        request_offset_seconds=args.request_offset_seconds,
        replay_options=replay_options,
    )

    write_jsonl(out_dir / "agent-requests.jsonl", batch["requests"])
    write_jsonl(out_dir / "agent-responses.jsonl", batch["responses"])
    write_jsonl(out_dir / "guardrail-findings.jsonl", batch["findings"])
    summary = build_replay_summary(batch)
    print(
        "Replay dataset generated: "
        f"scenario={args.business_scenario}, "
        f"mode={args.delivery_mode}, "
        f"requests={summary['requestsGenerated']}, "
        f"findings={summary['findingsGenerated']}, "
        f"triggered-findings={summary['triggeredFindingsGenerated']}, "
        f"invalid={summary['invalidGenerated']}, "
        f"late={summary['lateGenerated']}, "
        f"detector-errors={summary['detectorErrorsGenerated']}"
    )


if __name__ == "__main__":
    main()
