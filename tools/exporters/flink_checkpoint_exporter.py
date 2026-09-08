#!/usr/bin/env python3
# Exposes Flink checkpoint and restore statistics as Prometheus metrics.
# Use when you need Grafana panels for checkpoint state sizes, subtask skew,
# incremental checkpoint mode, and restore-related signals from Flink REST.
import argparse
import json
import re
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlopen


DEFAULT_FLINK_BASE_URL = "http://jobmanager:8081"
DEFAULT_LISTEN_HOST = "0.0.0.0"
DEFAULT_LISTEN_PORT = 9261
DEFAULT_SCRAPE_TIMEOUT_SECONDS = 5
DEFAULT_CACHE_TTL_SECONDS = 15
DEFAULT_JOB_NAME = "AIRiskOps MVP Increment 1"
JSON_HEADERS = {"Accept": "application/json"}


@dataclass(frozen=True)
class ExporterConfig:
    flink_base_url: str
    listen_host: str
    listen_port: int
    scrape_timeout_seconds: float
    cache_ttl_seconds: float
    job_name: str


def parse_args() -> ExporterConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flink-base-url", default=DEFAULT_FLINK_BASE_URL)
    parser.add_argument("--listen-host", default=DEFAULT_LISTEN_HOST)
    parser.add_argument("--listen-port", type=int, default=DEFAULT_LISTEN_PORT)
    parser.add_argument("--scrape-timeout-seconds", type=float, default=DEFAULT_SCRAPE_TIMEOUT_SECONDS)
    parser.add_argument("--cache-ttl-seconds", type=float, default=DEFAULT_CACHE_TTL_SECONDS)
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    args = parser.parse_args()
    return ExporterConfig(
        flink_base_url=args.flink_base_url.rstrip("/"),
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        scrape_timeout_seconds=args.scrape_timeout_seconds,
        cache_ttl_seconds=args.cache_ttl_seconds,
        job_name=args.job_name,
    )


class FlinkRestClient:
    def __init__(self, base_url: str, timeout_seconds: float):
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout_seconds = timeout_seconds

    def get_json(self, path: str) -> Any:
        request_url = urljoin(self.base_url, path.lstrip("/"))
        request = self._build_request(request_url)
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.load(response)

    @staticmethod
    def _build_request(url: str):
        from urllib.request import Request

        return Request(url, headers=JSON_HEADERS)


class FlinkCheckpointExporter:
    def __init__(self, config: ExporterConfig):
        self.config = config
        self.client = FlinkRestClient(config.flink_base_url, config.scrape_timeout_seconds)
        self._lock = threading.Lock()
        self._cached_payload = ""
        self._cached_at_monotonic = 0.0

    def render_metrics(self) -> str:
        with self._lock:
            now = time.monotonic()
            if self._cached_payload and now - self._cached_at_monotonic < self.config.cache_ttl_seconds:
                return self._cached_payload
            payload = self._build_metrics_payload()
            self._cached_payload = payload
            self._cached_at_monotonic = now
            return payload

    def _build_metrics_payload(self) -> str:
        running_job = self._select_running_job(self.client.get_json("/jobs/overview"))
        if running_job is None:
            return self._render_no_job_payload()

        job_id = running_job["jid"]
        job_name = running_job["name"]
        job_details = self.client.get_json(f"/jobs/{job_id}")
        checkpoints = self.client.get_json(f"/jobs/{job_id}/checkpoints")
        checkpoint_config = self.client.get_json(f"/jobs/{job_id}/checkpoints/config")
        jobmanager_config = self.client.get_json("/jobmanager/config")
        operator_names = {
            vertex["id"]: vertex["name"]
            for vertex in job_details.get("vertices", [])
            if "id" in vertex and "name" in vertex
        }

        metrics: list[str] = []
        metrics.extend(self._render_job_presence(job_id, job_name))
        metrics.extend(self._render_checkpoint_config_metrics(job_id, job_name, checkpoint_config, jobmanager_config))
        metrics.extend(self._render_checkpoint_count_metrics(job_id, job_name, checkpoints))
        metrics.extend(self._render_latest_restore_metrics(job_id, job_name, checkpoints, running_job))
        metrics.extend(self._render_latest_completed_checkpoint_metrics(job_id, job_name, checkpoints))
        metrics.extend(self._render_operator_metrics(job_id, job_name, checkpoints, operator_names))
        return "\n".join(metrics) + "\n"

    def _select_running_job(self, jobs_overview: dict[str, Any]) -> dict[str, Any] | None:
        jobs = jobs_overview.get("jobs", [])
        running_jobs = [job for job in jobs if job.get("state") == "RUNNING"]
        for job in running_jobs:
            if job.get("name") == self.config.job_name:
                return job
        return running_jobs[0] if running_jobs else None

    def _render_no_job_payload(self) -> str:
        metrics = [
            "# HELP airiskops_flink_job_present 1 if the target Flink job is running, else 0",
            "# TYPE airiskops_flink_job_present gauge",
            'airiskops_flink_job_present{job_name="%s"} 0' % escape_label_value(self.config.job_name),
        ]
        return "\n".join(metrics)

    def _render_job_presence(self, job_id: str, job_name: str) -> list[str]:
        return [
            "# HELP airiskops_flink_job_present 1 if the target Flink job is running, else 0",
            "# TYPE airiskops_flink_job_present gauge",
            format_metric(
                "airiskops_flink_job_present",
                1,
                {"job_id": job_id, "job_name": job_name},
            ),
        ]

    def _render_checkpoint_config_metrics(
        self,
        job_id: str,
        job_name: str,
        checkpoint_config: dict[str, Any],
        jobmanager_config_entries: list[dict[str, str]],
    ) -> list[str]:
        entries = config_entries_to_map(jobmanager_config_entries)
        interval_millis = checkpoint_config.get("interval", 0)
        incremental_enabled = resolve_bool_override(
            checkpoint_config.get("incremental"),
            entries.get("execution.checkpointing.incremental"),
        )
        state_backend = checkpoint_config.get("state_backend", "")
        checkpoint_storage = (
            checkpoint_config.get("checkpoint_storage")
            or entries.get("execution.checkpointing.storage", "")
        )
        checkpoint_dir = (
            checkpoint_config.get("checkpoint_directory")
            or checkpoint_config.get("checkpoints_directory")
            or entries.get("execution.checkpointing.dir", "")
        )
        labels = {"job_id": job_id, "job_name": job_name}

        return [
            "# HELP airiskops_flink_checkpoint_interval_seconds Configured checkpoint interval in seconds",
            "# TYPE airiskops_flink_checkpoint_interval_seconds gauge",
            format_metric("airiskops_flink_checkpoint_interval_seconds", interval_millis / 1000.0, labels),
            "# HELP airiskops_flink_checkpoint_incremental_enabled 1 if incremental checkpoints are enabled, else 0",
            "# TYPE airiskops_flink_checkpoint_incremental_enabled gauge",
            format_metric("airiskops_flink_checkpoint_incremental_enabled", 1 if incremental_enabled else 0, labels),
            "# HELP airiskops_flink_checkpoint_config_info Static checkpoint configuration labels",
            "# TYPE airiskops_flink_checkpoint_config_info gauge",
            format_metric(
                "airiskops_flink_checkpoint_config_info",
                1,
                {
                    **labels,
                    "state_backend": state_backend or "unknown",
                    "checkpoint_storage": checkpoint_storage or "unspecified",
                    "checkpoint_dir": checkpoint_dir or "unspecified",
                },
            ),
        ]

    def _render_checkpoint_count_metrics(
        self,
        job_id: str,
        job_name: str,
        checkpoints: dict[str, Any],
    ) -> list[str]:
        counts = checkpoints.get("counts", {})
        labels = {"job_id": job_id, "job_name": job_name}
        lines = [
            "# HELP airiskops_flink_checkpoint_count Count of checkpoints by outcome category",
            "# TYPE airiskops_flink_checkpoint_count gauge",
        ]
        for metric_key in ("completed", "failed", "in_progress", "restored", "total"):
            lines.append(
                format_metric(
                    "airiskops_flink_checkpoint_count",
                    counts.get(metric_key, 0),
                    {**labels, "kind": metric_key},
                )
            )
        return lines

    def _render_latest_restore_metrics(
        self,
        job_id: str,
        job_name: str,
        checkpoints: dict[str, Any],
        running_job: dict[str, Any],
    ) -> list[str]:
        latest_restored = checkpoints.get("latest", {}).get("restored")
        labels = {"job_id": job_id, "job_name": job_name}
        lines = [
            "# HELP airiskops_flink_restore_count Number of restores observed by Flink for the job",
            "# TYPE airiskops_flink_restore_count gauge",
            format_metric(
                "airiskops_flink_restore_count",
                checkpoints.get("counts", {}).get("restored", 0),
                labels,
            ),
        ]
        if not latest_restored:
            return lines

        restore_timestamp = latest_restored.get("restore_timestamp", 0)
        state_size = latest_restored.get("state_size", 0)
        start_time = running_job.get("start-time", 0)
        observed_elapsed_ms = max(0, restore_timestamp - start_time) if restore_timestamp and start_time else 0
        lines.extend(
            [
                "# HELP airiskops_flink_last_restore_timestamp_ms Timestamp when the latest restore completed",
                "# TYPE airiskops_flink_last_restore_timestamp_ms gauge",
                format_metric("airiskops_flink_last_restore_timestamp_ms", restore_timestamp, labels),
                "# HELP airiskops_flink_last_restore_state_size_bytes State size referenced by the latest restore",
                "# TYPE airiskops_flink_last_restore_state_size_bytes gauge",
                format_metric("airiskops_flink_last_restore_state_size_bytes", state_size, labels),
                "# HELP airiskops_flink_last_restore_observed_elapsed_ms Observed elapsed time from job start to the restore timestamp",
                "# TYPE airiskops_flink_last_restore_observed_elapsed_ms gauge",
                format_metric("airiskops_flink_last_restore_observed_elapsed_ms", observed_elapsed_ms, labels),
                "# HELP airiskops_flink_last_restore_info Metadata for the latest restored checkpoint",
                "# TYPE airiskops_flink_last_restore_info gauge",
                format_metric(
                    "airiskops_flink_last_restore_info",
                    1,
                    {
                        **labels,
                        "restored_checkpoint_id": str(latest_restored.get("id", 0)),
                        "external_path": latest_restored.get("external_path") or "none",
                        "restore_type": "savepoint" if latest_restored.get("is_savepoint") else "checkpoint",
                    },
                ),
            ]
        )
        return lines

    def _render_latest_completed_checkpoint_metrics(
        self,
        job_id: str,
        job_name: str,
        checkpoints: dict[str, Any],
    ) -> list[str]:
        latest_completed = checkpoints.get("latest", {}).get("completed")
        labels = {"job_id": job_id, "job_name": job_name}
        if not latest_completed:
            return []

        return [
            "# HELP airiskops_flink_checkpoint_last_completed_id Latest completed checkpoint id",
            "# TYPE airiskops_flink_checkpoint_last_completed_id gauge",
            format_metric("airiskops_flink_checkpoint_last_completed_id", latest_completed.get("id", 0), labels),
            "# HELP airiskops_flink_checkpoint_last_completed_timestamp_ms Timestamp when the latest completed checkpoint was triggered",
            "# TYPE airiskops_flink_checkpoint_last_completed_timestamp_ms gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_timestamp_ms",
                latest_completed.get("trigger_timestamp", 0),
                labels,
            ),
            "# HELP airiskops_flink_checkpoint_last_completed_duration_ms End-to-end duration of the latest completed checkpoint",
            "# TYPE airiskops_flink_checkpoint_last_completed_duration_ms gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_duration_ms",
                latest_completed.get("end_to_end_duration", 0),
                labels,
            ),
            "# HELP airiskops_flink_checkpoint_last_completed_state_size_bytes Full state size referenced by the latest completed checkpoint",
            "# TYPE airiskops_flink_checkpoint_last_completed_state_size_bytes gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_state_size_bytes",
                latest_completed.get("state_size", 0),
                labels,
            ),
            "# HELP airiskops_flink_checkpoint_last_completed_checkpointed_size_bytes Persisted checkpoint size of the latest completed checkpoint",
            "# TYPE airiskops_flink_checkpoint_last_completed_checkpointed_size_bytes gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_checkpointed_size_bytes",
                latest_completed.get("checkpointed_size", 0),
                labels,
            ),
            "# HELP airiskops_flink_checkpoint_last_completed_processed_data_bytes Processed data during the latest completed checkpoint",
            "# TYPE airiskops_flink_checkpoint_last_completed_processed_data_bytes gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_processed_data_bytes",
                latest_completed.get("processed_data", 0),
                labels,
            ),
            "# HELP airiskops_flink_checkpoint_last_completed_persisted_data_bytes Persisted data during the latest completed checkpoint",
            "# TYPE airiskops_flink_checkpoint_last_completed_persisted_data_bytes gauge",
            format_metric(
                "airiskops_flink_checkpoint_last_completed_persisted_data_bytes",
                latest_completed.get("persisted_data", 0),
                labels,
            ),
        ]

    def _render_operator_metrics(
        self,
        job_id: str,
        job_name: str,
        checkpoints: dict[str, Any],
        operator_names: dict[str, str],
    ) -> list[str]:
        latest_completed = checkpoints.get("latest", {}).get("completed")
        if not latest_completed:
            return []

        checkpoint_id = latest_completed.get("id")
        tasks = latest_completed.get("tasks", {})
        base_labels = {"job_id": job_id, "job_name": job_name}
        lines = [
            "# HELP airiskops_flink_operator_checkpoint_state_size_bytes State size by operator for the latest completed checkpoint",
            "# TYPE airiskops_flink_operator_checkpoint_state_size_bytes gauge",
            "# HELP airiskops_flink_operator_checkpointed_size_bytes Persisted checkpoint size by operator for the latest completed checkpoint",
            "# TYPE airiskops_flink_operator_checkpointed_size_bytes gauge",
            "# HELP airiskops_flink_operator_checkpoint_duration_ms End-to-end checkpoint duration by operator for the latest completed checkpoint",
            "# TYPE airiskops_flink_operator_checkpoint_duration_ms gauge",
            "# HELP airiskops_flink_subtask_checkpoint_state_size_bytes State size by operator subtask for the latest completed checkpoint",
            "# TYPE airiskops_flink_subtask_checkpoint_state_size_bytes gauge",
            "# HELP airiskops_flink_subtask_checkpointed_size_bytes Persisted checkpoint size by operator subtask for the latest completed checkpoint",
            "# TYPE airiskops_flink_subtask_checkpointed_size_bytes gauge",
            "# HELP airiskops_flink_subtask_checkpoint_duration_ms End-to-end checkpoint duration by operator subtask for the latest completed checkpoint",
            "# TYPE airiskops_flink_subtask_checkpoint_duration_ms gauge",
        ]

        for operator_id, task_stats in tasks.items():
            operator_name = operator_names.get(operator_id, operator_id)
            operator_labels = {
                **base_labels,
                "operator_id": operator_id,
                "operator_name": operator_name,
            }
            lines.extend(
                [
                    format_metric(
                        "airiskops_flink_operator_checkpoint_state_size_bytes",
                        task_stats.get("state_size", 0),
                        operator_labels,
                    ),
                    format_metric(
                        "airiskops_flink_operator_checkpointed_size_bytes",
                        task_stats.get("checkpointed_size", 0),
                        operator_labels,
                    ),
                    format_metric(
                        "airiskops_flink_operator_checkpoint_duration_ms",
                        task_stats.get("end_to_end_duration", 0),
                        operator_labels,
                    ),
                ]
            )
            for subtask in self._fetch_subtask_stats(job_id, checkpoint_id, operator_id):
                subtask_labels = {
                    **operator_labels,
                    "subtask_index": str(subtask.get("index", 0)),
                }
                lines.extend(
                    [
                        format_metric(
                            "airiskops_flink_subtask_checkpoint_state_size_bytes",
                            subtask.get("state_size", 0),
                            subtask_labels,
                        ),
                        format_metric(
                            "airiskops_flink_subtask_checkpointed_size_bytes",
                            subtask.get("checkpointed_size", 0),
                            subtask_labels,
                        ),
                        format_metric(
                            "airiskops_flink_subtask_checkpoint_duration_ms",
                            subtask.get("end_to_end_duration", 0),
                            subtask_labels,
                        ),
                    ]
                )
        return lines

    def _fetch_subtask_stats(self, job_id: str, checkpoint_id: int, operator_id: str) -> list[dict[str, Any]]:
        details = self.client.get_json(f"/jobs/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{operator_id}")
        return details.get("subtasks", [])


def config_entries_to_map(entries: list[dict[str, str]]) -> dict[str, str]:
    return {
        entry.get("key", ""): entry.get("value", "")
        for entry in entries
        if entry.get("key")
    }


def parse_bool(value: str | None) -> bool:
    return str(value).strip().lower() == "true"


def resolve_bool_override(primary: Any, fallback: Any) -> bool:
    if primary is not None:
        return parse_bool(primary)
    return parse_bool(fallback)


def escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def format_metric(name: str, value: Any, labels: dict[str, str]) -> str:
    serialized_labels = ",".join(
        f'{key}="{escape_label_value(str(labels[key]))}"'
        for key in sorted(labels)
    )
    return f"{name}{{{serialized_labels}}} {format_metric_value(value)}"


def format_metric_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


class ExporterRequestHandler(BaseHTTPRequestHandler):
    exporter: FlinkCheckpointExporter

    def do_GET(self):
        if self.path not in ("/metrics", "/metrics/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found\n")
            return

        try:
            payload = self.exporter.render_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
            payload = render_exporter_error_metrics(str(error)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def render_exporter_error_metrics(error_message: str) -> str:
    return "\n".join(
        [
            "# HELP airiskops_flink_checkpoint_exporter_up 1 if exporter refresh succeeds, else 0",
            "# TYPE airiskops_flink_checkpoint_exporter_up gauge",
            "airiskops_flink_checkpoint_exporter_up 0",
            "# HELP airiskops_flink_checkpoint_exporter_error_info Exporter error details",
            "# TYPE airiskops_flink_checkpoint_exporter_error_info gauge",
            format_metric(
                "airiskops_flink_checkpoint_exporter_error_info",
                1,
                {"message": sanitize_error_label(error_message)},
            ),
        ]
    ) + "\n"


def sanitize_error_label(error_message: str) -> str:
    collapsed = re.sub(r"\s+", " ", error_message.strip())
    return collapsed[:200] if collapsed else "unknown"


def main() -> None:
    config = parse_args()
    exporter = FlinkCheckpointExporter(config)
    ExporterRequestHandler.exporter = exporter
    server = ThreadingHTTPServer((config.listen_host, config.listen_port), ExporterRequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
