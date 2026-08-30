#!/usr/bin/env python3
# Publishes replay and live-generator control metrics to Prometheus Pushgateway.
# Use when scenario tooling should be visible in Grafana/Prometheus separately
# from the Flink job metrics themselves.
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
from urllib import error, request


DEFAULT_PUSHGATEWAY_URL = "http://localhost:9091"
DEFAULT_REPLAY_METRICS_JOB = "aisafetyops-replay"
DEFAULT_PUSH_TIMEOUT_SECONDS = 3
CONTENT_TYPE_HEADER = "text/plain; version=0.0.4; charset=utf-8"
REQUEST_METHOD_PUT = "PUT"
METRIC_HELP_TEMPLATE = "# HELP {name} {help_text}\n# TYPE {name} gauge\n"


@dataclass(frozen=True)
class ReplayMetricSummary:
    requests_generated: int
    responses_generated: int
    findings_generated: int
    triggered_findings_generated: int
    invalid_generated: int
    late_generated: int
    detector_errors_generated: int
    current_rps: int


def summarize_batch(batch: Dict[str, List[dict]]) -> ReplayMetricSummary:
    requests = batch["requests"]
    responses = batch["responses"]
    findings = batch["findings"]
    invalid_generated = len([row for row in requests if "sessionId" not in row])
    invalid_generated += len([row for row in responses if not is_valid_iso_timestamp(row.get("eventTime"))])
    invalid_generated += len([row for row in findings if "guardrailName" not in row])
    late_generated = len(
        [
            row for row in requests
            if row.get("requestId", "").startswith("req-") and row.get("eventTime", "").startswith("2026-08-26T11:")
        ]
    )
    detector_errors_generated = len([row for row in findings if row.get("detectorStatus") == "ERROR"])
    triggered_findings_generated = len([row for row in findings if row.get("triggered") is True])
    return ReplayMetricSummary(
        requests_generated=len(requests),
        responses_generated=len(responses),
        findings_generated=len(findings),
        triggered_findings_generated=triggered_findings_generated,
        invalid_generated=invalid_generated,
        late_generated=late_generated,
        detector_errors_generated=detector_errors_generated,
        current_rps=len(requests),
    )


def build_metrics_payload(
    scenario: str,
    delivery_mode: str,
    source_kind: str,
    agent_id: str,
    summary: ReplayMetricSummary,
    status: str,
) -> str:
    labels = common_labels(
        scenario=scenario,
        delivery_mode=delivery_mode,
        source_kind=source_kind,
        agent_id=agent_id,
        status=status,
    )
    stream_labels = common_labels(
        scenario=scenario,
        delivery_mode=delivery_mode,
        source_kind=source_kind,
        agent_id=agent_id,
    )
    return "".join(
        [
            metric_block(
                "aisafetyops_replay_run_info",
                "Replay/live control-plane status for the active scenario run.",
                f"{labels} 1",
            ),
            metric_block(
                "aisafetyops_replay_events_generated_total",
                "Generated replay/live events grouped by logical stream.",
                f'{stream_labels},stream="requests" {summary.requests_generated}',
                f'{stream_labels},stream="responses" {summary.responses_generated}',
                f'{stream_labels},stream="findings" {summary.findings_generated}',
            ),
            metric_block(
                "aisafetyops_replay_triggered_findings_generated_total",
                "Generated triggered guardrail findings for the active scenario run.",
                f"{stream_labels} {summary.triggered_findings_generated}",
            ),
            metric_block(
                "aisafetyops_replay_invalid_generated_total",
                "Generated invalid payload count for the active scenario run.",
                f"{stream_labels} {summary.invalid_generated}",
            ),
            metric_block(
                "aisafetyops_replay_late_generated_total",
                "Generated late payload count for the active scenario run.",
                f"{stream_labels} {summary.late_generated}",
            ),
            metric_block(
                "aisafetyops_replay_detector_errors_generated_total",
                "Generated detector error findings for the active scenario run.",
                f"{stream_labels} {summary.detector_errors_generated}",
            ),
            metric_block(
                "aisafetyops_replay_current_rps",
                "Current request-per-second level for replay/live generator.",
                f"{stream_labels} {summary.current_rps}",
            ),
        ]
    )


def common_labels(
    scenario: str,
    delivery_mode: str,
    source_kind: str,
    agent_id: str,
    status: str | None = None,
) -> str:
    label_parts = [
        f'scenario="{sanitize_label_value(scenario)}"',
        f'mode="{sanitize_label_value(delivery_mode)}"',
        f'source_kind="{sanitize_label_value(source_kind)}"',
        f'agent_id="{sanitize_label_value(agent_id)}"',
    ]
    if status is not None:
        label_parts.append(f'status="{sanitize_label_value(status)}"')
    return "{" + ",".join(label_parts) + "}"


def metric_block(metric_name: str, help_text: str, *samples: str) -> str:
    return METRIC_HELP_TEMPLATE.format(name=metric_name, help_text=help_text) + "".join(
        f"{metric_name}{sample}\n" for sample in samples
    )


def sanitize_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def is_valid_iso_timestamp(value: str | None) -> bool:
    if value is None or not isinstance(value, str):
        return False
    return value.endswith("Z") and "T" in value and "not-a-timestamp" not in value


def push_metrics(pushgateway_url: str, replay_metrics_job: str, payload: str) -> None:
    gateway_url = pushgateway_url.rstrip("/")
    target_url = f"{gateway_url}/metrics/job/{replay_metrics_job}"
    push_request = request.Request(
        url=target_url,
        data=payload.encode("utf-8"),
        method=REQUEST_METHOD_PUT,
        headers={"Content-Type": CONTENT_TYPE_HEADER},
    )
    with request.urlopen(push_request, timeout=DEFAULT_PUSH_TIMEOUT_SECONDS):
        return


def safe_push_metrics(pushgateway_url: str | None, replay_metrics_job: str, payload: str) -> str | None:
    if pushgateway_url is None or not pushgateway_url.strip():
        return None
    try:
        push_metrics(pushgateway_url, replay_metrics_job, payload)
    except error.URLError as exception:
        return f"Replay metrics push failed: {exception}"
    return None
