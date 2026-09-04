#!/usr/bin/env python3
# Builds a Markdown report for one completed local AIRiskOps load-test run.
# Use from run-nt-baseline.sh after the generator exits and metrics have settled.
import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT_DIR = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT_DIR / "deployment" / "local" / "docker-compose.yml"
DEFAULT_JOB_NAME = "AIRiskOps MVP Increment 1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir")
    parser.add_argument("--run-id")
    parser.add_argument("--run-start-epoch", type=float)
    parser.add_argument("--run-end-epoch", type=float)
    parser.add_argument("--scenario")
    parser.add_argument("--mode")
    parser.add_argument("--rps", type=int)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--sessions", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--agent-id")
    parser.add_argument("--generator-log")
    parser.add_argument("--generator-exit-code", type=int)
    parser.add_argument("--job-id")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--flink-url", default="http://localhost:8081")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--grafana-url", default="http://localhost:3000")
    parser.add_argument("--write-kafka-lag")
    parser.add_argument("--write-checkpoint-snapshot")
    parser.add_argument("--kafka-lag-at-generator-end")
    parser.add_argument("--kafka-lag-after-settle")
    parser.add_argument("--kafka-lag-after-recovery")
    parser.add_argument("--checkpoint-at-run-start")
    parser.add_argument("--checkpoint-after-recovery")
    return parser.parse_args()


def fetch_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=10) as response:  # nosec B310: local URLs are CLI arguments.
        return json.loads(response.read().decode("utf-8"))


def query_prometheus(base_url: str, query: str) -> dict[str, Any]:
    return fetch_json(f"{base_url}/api/v1/query?{urlencode({'query': query})}")


def query_prometheus_range(base_url: str, query: str, start_epoch: float, end_epoch: float) -> dict[str, Any]:
    parameters = {"query": query, "start": f"{start_epoch:.3f}", "end": f"{end_epoch:.3f}", "step": "15s"}
    return fetch_json(f"{base_url}/api/v1/query_range?{urlencode(parameters)}")


def vector_value(payload: dict[str, Any]) -> float | None:
    results = payload.get("data", {}).get("result", [])
    if not results:
        return None
    try:
        return float(results[0]["value"][1])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


def matrix_max_value(payload: dict[str, Any]) -> float | None:
    maximum: float | None = None
    for series in payload.get("data", {}).get("result", []):
        for sample in series.get("values", []):
            try:
                value = float(sample[1])
            except (IndexError, TypeError, ValueError):
                continue
            maximum = value if maximum is None else max(maximum, value)
    return maximum


def resolve_job_id(overview: dict[str, Any], job_name: str, requested_job_id: str | None) -> str:
    jobs = overview.get("jobs", [])
    if requested_job_id:
        if any(job.get("jid") == requested_job_id for job in jobs):
            return requested_job_id
        raise ValueError(f"Flink job not found: {requested_job_id}")

    running = [job["jid"] for job in jobs if job.get("name") == job_name and job.get("state") == "RUNNING"]
    if len(running) == 1:
        return running[0]
    if not running:
        raise ValueError(f"No running Flink job named: {job_name}")
    raise ValueError(f"Multiple running Flink jobs named {job_name}; pass --job-id explicitly")


def parse_generator_summary(log_path: Path) -> dict[str, str]:
    if not log_path.exists():
        return {}
    summary = ""
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("Live stream published for "):
            summary = line
    return dict(re.findall(r"([a-z-]+)=([^,\s]+)", summary))


def collect_kafka_lag() -> dict[str, Any]:
    command = [
        "docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092",
        "--describe", "--group", "airiskops-mvp",
    ]
    completed = subprocess.run(command, cwd=ROOT_DIR, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "no command output"
        raise OSError(f"Kafka lag collection failed (exit {completed.returncode}): {details}")
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[0] != "airiskops-mvp":
            continue
        try:
            rows.append({"topic": fields[1], "partition": int(fields[2]), "lag": int(fields[5])})
        except ValueError:
            continue
    if not rows:
        raise OSError("Kafka lag collection returned no partition rows for consumer group airiskops-mvp")
    return {"total": sum(row["lag"] for row in rows), "partitions": rows, "stderr": completed.stderr.strip()}


def write_kafka_lag_snapshot(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    payload = {
        "captured_at": now.isoformat(),
        "captured_at_epoch": now.timestamp(),
        "kafka_lag": collect_kafka_lag(),
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[nt-report] Kafka lag snapshot: {output_path}")


def read_kafka_lag_snapshot(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_checkpoint_snapshot(
    output_path: Path,
    flink_url: str,
    job_name: str,
    requested_job_id: str | None,
    prometheus_url: str,
) -> None:
    overview = fetch_json(f"{flink_url}/jobs/overview")
    job_id = resolve_job_id(overview, job_name, requested_job_id)
    payload = query_prometheus(
        prometheus_url,
        f'max(flink_jobmanager_job_numberOfFailedCheckpoints{{job_id="{job_id}"}})',
    )
    failed_checkpoints = vector_value(payload)
    if failed_checkpoints is None:
        raise OSError(f"Failed to collect failed-checkpoint counter for Flink job {job_id}")
    now = datetime.now(timezone.utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "captured_at": now.isoformat(),
                "captured_at_epoch": now.timestamp(),
                "job_id": job_id,
                "failed_checkpoints": failed_checkpoints,
            },
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    print(f"[nt-report] Checkpoint snapshot: {output_path}")


def read_checkpoint_snapshot(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def collect_taskmanager_vcpus() -> int | None:
    command = ["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T", "taskmanager", "nproc"]
    completed = subprocess.run(command, cwd=ROOT_DIR, text=True, capture_output=True, check=False)
    try:
        return int(completed.stdout.strip())
    except ValueError:
        return None


def value_or_na(value: float | None, unit: str = "", digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{unit}"


def kib_to_mib(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / (1024 * 1024):.1f} MiB"


def assess(metrics: dict[str, float | None], kafka_lag_points: dict[str, dict[str, Any]], generator_exit_code: int) -> str:
    if generator_exit_code != 0:
        return "failed"
    if (metrics.get("failed_checkpoints_during_run") or 0) > 0:
        return "degraded"
    if (metrics.get("backpressure_ms_per_second") or 0) >= 100:
        return "degraded"
    end_lag = kafka_lag_points["at_generator_end"]["kafka_lag"]["total"]
    settle_lag = kafka_lag_points["after_settle"]["kafka_lag"]["total"]
    recovery_lag = kafka_lag_points["after_recovery"]["kafka_lag"]["total"]
    if recovery_lag == 0 and (end_lag > 0 or settle_lag > 0):
        return "recovered"
    if recovery_lag > 0 and recovery_lag < settle_lag:
        return "still-draining"
    if recovery_lag > 0:
        return "not-recovering"
    return "stable"


def render_report(snapshot: dict[str, Any]) -> str:
    metadata = snapshot["metadata"]
    metrics = snapshot["metrics"]
    lag_points = snapshot["kafka_lag_points"]
    lag_at_end = lag_points["at_generator_end"]
    lag_after_settle = lag_points["after_settle"]
    lag_after_recovery = lag_points["after_recovery"]
    settle_elapsed = lag_after_settle["captured_at_epoch"] - lag_at_end["captured_at_epoch"]
    recovery_elapsed = lag_after_recovery["captured_at_epoch"] - lag_after_settle["captured_at_epoch"]
    total_elapsed = lag_after_recovery["captured_at_epoch"] - lag_at_end["captured_at_epoch"]
    settle_delta = lag_at_end["kafka_lag"]["total"] - lag_after_settle["kafka_lag"]["total"]
    recovery_delta = lag_after_settle["kafka_lag"]["total"] - lag_after_recovery["kafka_lag"]["total"]
    total_delta = lag_at_end["kafka_lag"]["total"] - lag_after_recovery["kafka_lag"]["total"]
    settle_rate = settle_delta / settle_elapsed if settle_elapsed > 0 else None
    recovery_rate = recovery_delta / recovery_elapsed if recovery_elapsed > 0 else None
    total_rate = total_delta / total_elapsed if total_elapsed > 0 else None
    vcpus = snapshot.get("taskmanager_vcpus")
    cpu_equivalent = None
    if metrics.get("jvm_cpu_load") is not None and vcpus is not None:
        cpu_equivalent = metrics["jvm_cpu_load"] * vcpus
    incident_e2e = metrics.get("incident_event_to_emit_ms")
    incident_e2e_display = (
        "n/a (no current incident emission)"
        if incident_e2e is None or incident_e2e <= 0
        else value_or_na(incident_e2e, " ms")
    )

    lines = [
        f"# AIRiskOps load-test report: {metadata['run_id']}",
        "",
        f"Verdict: **{snapshot['verdict']}**",
        "",
        "## Run",
        "",
        f"- Started: `{metadata['run_started_at']}`",
        f"- Finished: `{metadata['run_finished_at']}`",
        f"- Runtime metrics interval: `{metadata['run_started_at']}` to `{metadata['metrics_end_at']}`",
        f"- Job ID: `{metadata['job_id']}`",
        f"- Scenario: `{metadata['scenario']}`, mode `{metadata['mode']}`",
        f"- Load: `{metadata['rps']} RPS` for `{metadata['duration_seconds']} s`; sessions `{metadata['sessions']}`, seed `{metadata['seed']}`",
        f"- State backend: `{metadata['state_backend']}`; incremental checkpoints `{metadata['incremental_checkpoints']}`",
        f"- TaskManager available vCPU: `{vcpus if vcpus is not None else 'n/a'}`",
        "",
        "## Generator",
        "",
        f"- Exit code: `{metadata['generator_exit_code']}`",
        f"- Requests: `{metadata['generator_summary'].get('total-requests', 'n/a')}`",
        f"- Findings: `{metadata['generator_summary'].get('total-findings', 'n/a')}`",
        f"- Triggered findings: `{metadata['generator_summary'].get('triggered-findings', 'n/a')}`",
        f"- Invalid / late / detector errors: `{metadata['generator_summary'].get('invalid', 'n/a')}` / `{metadata['generator_summary'].get('late', 'n/a')}` / `{metadata['generator_summary'].get('detector-errors', 'n/a')}`",
        "",
        "## Runtime Snapshot",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Aggregate E2E latest event to emit | {value_or_na(metrics.get('aggregate_event_to_emit_ms'), ' ms')} |",
        f"| Aggregate E2E window end to emit | {value_or_na(metrics.get('aggregate_window_end_to_emit_ms'), ' ms')} |",
        f"| Incident E2E latest event to emit | {incident_e2e_display} |",
        f"| Busy time max | {value_or_na(metrics.get('busy_ms_per_second'), ' ms/s')} |",
        f"| Backpressure max | {value_or_na(metrics.get('backpressure_ms_per_second'), ' ms/s')} |",
        f"| JVM heap used max | {kib_to_mib(metrics.get('jvm_heap_used_bytes'))} |",
        f"| JVM CPU load max | {value_or_na((metrics.get('jvm_cpu_load') or 0) * 100 if metrics.get('jvm_cpu_load') is not None else None, '%')} |",
        f"| JVM CPU capacity equivalent | {value_or_na(cpu_equivalent, ' vCPU')} |",
        f"| Checkpoint duration max | {value_or_na(metrics.get('checkpoint_duration_ms'), ' ms')} |",
        f"| Failed checkpoints at start | {value_or_na(metrics.get('failed_checkpoints_at_start'), '', 0)} |",
        f"| Failed checkpoints after recovery | {value_or_na(metrics.get('failed_checkpoints_after_recovery'), '', 0)} |",
        f"| Failed checkpoints during run | {value_or_na(metrics.get('failed_checkpoints_during_run'), '', 0)} |",
        f"| Watermark lag | {value_or_na(metrics.get('watermark_lag_ms'), ' ms')} |",
        f"| Kafka lag at generator end | {lag_at_end['kafka_lag']['total']} messages |",
        f"| Kafka lag after settle | {lag_after_settle['kafka_lag']['total']} messages |",
        f"| Kafka lag after recovery | {lag_after_recovery['kafka_lag']['total']} messages |",
        f"| Lag reduction during settle | {settle_delta} messages |",
        f"| Catch-up rate during settle | {value_or_na(settle_rate, ' messages/s')} |",
        f"| Lag reduction during recovery | {recovery_delta} messages |",
        f"| Catch-up rate during recovery | {value_or_na(recovery_rate, ' messages/s')} |",
        f"| Total lag reduction | {total_delta} messages |",
        f"| Total catch-up rate | {value_or_na(total_rate, ' messages/s')} |",
        "",
        "## Interpretation",
        "",
        "- `stable`: generator succeeded, Kafka lag is zero, no failed checkpoints during this run, and backpressure is below `100 ms/s`.",
        "- `recovered`: lag was non-zero immediately after the generator but reached zero during recovery.",
        "- `still-draining`: lag decreased during recovery but remained non-zero.",
        "- `not-recovering`: lag remained non-zero without a measured decrease during recovery.",
        "- `degraded`: failed checkpoints during this run or backpressure at or above `100 ms/s` were observed.",
        "- Aggregate E2E includes event-time watermark wait; it is not a pure CPU latency metric.",
        "- Incident E2E can be `n/a` when the selected scenario does not emit incidents.",
        "",
        "## Artefacts And Logs",
        "",
        f"- Generator log: `{metadata['generator_log']}`",
        f"- Raw snapshot: `{metadata['raw_snapshot_path']}`",
        f"- Flink job: {metadata['flink_url']}/#/job/{metadata['job_id']}/overview",
        f"- Grafana: {metadata['grafana_url']}",
        f"- Prometheus: {metadata['prometheus_url']}",
        "",
        "TaskManager logs:",
        "",
        "```bash",
        f"docker compose -f deployment/local/docker-compose.yml logs --since '{metadata['run_started_at']}' taskmanager",
        "```",
        "",
        "JobManager logs:",
        "",
        "```bash",
        f"docker compose -f deployment/local/docker-compose.yml logs --since '{metadata['run_started_at']}' jobmanager",
        "```",
        "",
        "Kafka logs:",
        "",
        "```bash",
        f"docker compose -f deployment/local/docker-compose.yml logs --since '{metadata['run_started_at']}' kafka",
        "```",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    if args.write_kafka_lag:
        write_kafka_lag_snapshot(Path(args.write_kafka_lag))
        return
    if args.write_checkpoint_snapshot:
        write_checkpoint_snapshot(
            Path(args.write_checkpoint_snapshot),
            args.flink_url,
            args.job_name,
            args.job_id,
            args.prometheus_url,
        )
        return
    required = {
        "report-dir": args.report_dir,
        "run-id": args.run_id,
        "run-start-epoch": args.run_start_epoch,
        "run-end-epoch": args.run_end_epoch,
        "scenario": args.scenario,
        "mode": args.mode,
        "rps": args.rps,
        "duration-seconds": args.duration_seconds,
        "sessions": args.sessions,
        "seed": args.seed,
        "agent-id": args.agent_id,
        "generator-log": args.generator_log,
        "generator-exit-code": args.generator_exit_code,
        "kafka-lag-at-generator-end": args.kafka_lag_at_generator_end,
        "kafka-lag-after-settle": args.kafka_lag_after_settle,
        "kafka-lag-after-recovery": args.kafka_lag_after_recovery,
        "checkpoint-at-run-start": args.checkpoint_at_run_start,
        "checkpoint-after-recovery": args.checkpoint_after_recovery,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ValueError(f"missing report arguments: {', '.join(missing)}")
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    overview = fetch_json(f"{args.flink_url}/jobs/overview")
    job_id = resolve_job_id(overview, args.job_name, args.job_id)
    lag_points = {
        "at_generator_end": read_kafka_lag_snapshot(args.kafka_lag_at_generator_end),
        "after_settle": read_kafka_lag_snapshot(args.kafka_lag_after_settle),
        "after_recovery": read_kafka_lag_snapshot(args.kafka_lag_after_recovery),
    }
    checkpoint_at_start = read_checkpoint_snapshot(args.checkpoint_at_run_start)
    checkpoint_after_recovery = read_checkpoint_snapshot(args.checkpoint_after_recovery)
    if checkpoint_at_start["job_id"] != job_id or checkpoint_after_recovery["job_id"] != job_id:
        raise ValueError("Flink job changed during the load test; checkpoint delta is not comparable")
    metrics_end_epoch = lag_points["after_recovery"]["captured_at_epoch"]
    if metrics_end_epoch < args.run_start_epoch:
        raise ValueError("Kafka recovery snapshot predates the load-test start")
    metric_queries = {
        "aggregate_event_to_emit_ms": f'max(flink_taskmanager_job_task_operator_airiskops_quality_window_guardrail_last_e2e_latest_event_to_emit_ms{{job_id="{job_id}",window="1m"}})',
        "aggregate_window_end_to_emit_ms": f'max(flink_taskmanager_job_task_operator_airiskops_quality_window_guardrail_last_e2e_window_end_to_emit_ms{{job_id="{job_id}",window="1m"}})',
        "incident_event_to_emit_ms": f'max(flink_taskmanager_job_task_operator_airiskops_incident_last_e2e_latest_event_to_emit_ms{{job_id="{job_id}"}})',
        "busy_ms_per_second": f'max(flink_taskmanager_job_task_busyTimeMsPerSecond{{job_id="{job_id}"}})',
        "backpressure_ms_per_second": f'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{{job_id="{job_id}"}})',
        "jvm_heap_used_bytes": "flink_taskmanager_Status_JVM_Memory_Heap_Used",
        "jvm_cpu_load": "flink_taskmanager_Status_JVM_CPU_Load",
        "checkpoint_duration_ms": f'max(flink_jobmanager_job_lastCheckpointDuration{{job_id="{job_id}"}})',
        "state_backend_code": f'max(flink_taskmanager_job_task_operator_airiskops_runtime_contract_state_backend_code{{job_id="{job_id}"}})',
        "incremental_checkpoints": f'max(flink_taskmanager_job_task_operator_airiskops_runtime_contract_incremental_checkpoints_enabled{{job_id="{job_id}"}})',
    }
    raw_queries = {
        name: query_prometheus_range(args.prometheus_url, query, args.run_start_epoch, metrics_end_epoch)
        for name, query in metric_queries.items()
    }
    metrics = {name: matrix_max_value(payload) for name, payload in raw_queries.items()}
    watermark_query = f'time() * 1000 - max(flink_taskmanager_job_task_currentInputWatermark{{job_id="{job_id}"}})'
    raw_queries["watermark_lag_ms"] = query_prometheus(args.prometheus_url, watermark_query)
    metrics["watermark_lag_ms"] = vector_value(raw_queries["watermark_lag_ms"])
    backend_code = metrics.pop("state_backend_code")
    incremental = metrics.pop("incremental_checkpoints")
    metrics["failed_checkpoints_at_start"] = checkpoint_at_start["failed_checkpoints"]
    metrics["failed_checkpoints_after_recovery"] = checkpoint_after_recovery["failed_checkpoints"]
    metrics["failed_checkpoints_during_run"] = max(
        0,
        checkpoint_after_recovery["failed_checkpoints"] - checkpoint_at_start["failed_checkpoints"],
    )
    state_backend = {0.0: "DEFAULT", 1.0: "ROCKSDB"}.get(backend_code, "UNKNOWN")
    run_started_at = datetime.fromtimestamp(args.run_start_epoch, timezone.utc).isoformat()
    run_finished_at = datetime.fromtimestamp(args.run_end_epoch, timezone.utc).isoformat()
    raw_path = report_dir / f"{args.run_id}.json"
    markdown_path = report_dir / f"{args.run_id}.md"
    snapshot = {
        "metadata": {
            "run_id": args.run_id,
            "run_started_at": run_started_at,
            "run_finished_at": run_finished_at,
            "metrics_end_at": datetime.fromtimestamp(metrics_end_epoch, timezone.utc).isoformat(),
            "job_id": job_id,
            "scenario": args.scenario,
            "mode": args.mode,
            "rps": args.rps,
            "duration_seconds": args.duration_seconds,
            "sessions": args.sessions,
            "seed": args.seed,
            "agent_id": args.agent_id,
            "state_backend": state_backend,
            "incremental_checkpoints": bool(incremental),
            "generator_exit_code": args.generator_exit_code,
            "generator_log": args.generator_log,
            "raw_snapshot_path": str(raw_path),
            "flink_url": args.flink_url,
            "grafana_url": args.grafana_url,
            "prometheus_url": args.prometheus_url,
            "generator_summary": parse_generator_summary(Path(args.generator_log)),
        },
        "metrics": metrics,
        "kafka_lag_points": lag_points,
        "checkpoint_snapshots": {
            "at_run_start": checkpoint_at_start,
            "after_recovery": checkpoint_after_recovery,
        },
        "taskmanager_vcpus": collect_taskmanager_vcpus(),
        "prometheus_queries": raw_queries,
        "flink_overview": overview,
    }
    snapshot["verdict"] = assess(metrics, snapshot["kafka_lag_points"], args.generator_exit_code)
    raw_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_report(snapshot), encoding="utf-8")
    print(f"[nt-report] verdict: {snapshot['verdict']}")
    print(f"[nt-report] report: {markdown_path}")
    print(f"[nt-report] raw metrics: {raw_path}")
    print(f"[nt-report] generator log: {args.generator_log}")
    print(f"[nt-report] Flink job: {args.flink_url}/#/job/{job_id}/overview")
    print(f"[nt-report] Grafana: {args.grafana_url}")
    print(f"[nt-report] Prometheus: {args.prometheus_url}")
    print("[nt-report] TaskManager logs:")
    print(f"docker compose -f deployment/local/docker-compose.yml logs --since '{run_started_at}' taskmanager")
    print("[nt-report] JobManager logs:")
    print(f"docker compose -f deployment/local/docker-compose.yml logs --since '{run_started_at}' jobmanager")
    print("[nt-report] Kafka logs:")
    print(f"docker compose -f deployment/local/docker-compose.yml logs --since '{run_started_at}' kafka")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"nt_report_collector: {error}", file=sys.stderr)
        sys.exit(1)
