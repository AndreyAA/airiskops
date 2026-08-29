package com.bank.aisafetyops.app.config;

/**
 * Centralized names and defaults for job CLI and YAML configuration options.
 *
 * <p>Keeping these keys in one place avoids drift between scripts, YAML files,
 * and Java code when the local MVP evolves.
 */
public final class JobConfigOptions {
    public static final String ARG_CONFIG_FILE = "configFile";
    public static final String ARG_BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String ARG_TOPICS = "topics";
    public static final String ARG_GROUP_ID = "groupId";
    public static final String ARG_NORMALIZED_EVENTS_TOPIC = "normalizedEventsTopic";
    public static final String ARG_INVALID_EVENTS_TOPIC = "invalidEventsTopic";
    public static final String ARG_LATE_EVENTS_TOPIC = "lateEventsTopic";
    public static final String ARG_GUARDRAIL_AGGREGATES_TOPIC = "guardrailAggregatesTopic";
    public static final String ARG_OUT_OF_ORDERNESS_SECONDS = "outOfOrdernessSeconds";
    public static final String ARG_IDLE_TIMEOUT_MINUTES = "idleTimeoutMinutes";
    public static final String ARG_LATE_TOLERANCE_MINUTES = "lateToleranceMinutes";
    public static final String ARG_CHECKPOINT_INTERVAL_SECONDS = "checkpointIntervalSeconds";
    public static final String ARG_AUTO_WATERMARK_INTERVAL_SECONDS = "autoWatermarkIntervalSeconds";
    public static final String ARG_START_FROM_EARLIEST = "startFromEarliest";

    public static final String DEFAULT_CONFIG_FILE = "config/job/local-job.yaml";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPICS = "agent-requests,agent-responses,guardrail-findings";
    public static final String DEFAULT_GROUP_ID = "aisafetyops-mvp";
    public static final String DEFAULT_NORMALIZED_EVENTS_TOPIC = "normalized-events";
    public static final String DEFAULT_INVALID_EVENTS_TOPIC = "invalid-events";
    public static final String DEFAULT_LATE_EVENTS_TOPIC = "late-events";
    public static final String DEFAULT_GUARDRAIL_AGGREGATES_TOPIC = "guardrail-aggregates";
    public static final long DEFAULT_OUT_OF_ORDERNESS_SECONDS = 30L;
    public static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 1L;
    public static final long DEFAULT_LATE_TOLERANCE_MINUTES = 5L;
    public static final long DEFAULT_CHECKPOINT_INTERVAL_SECONDS = 30L;
    public static final long DEFAULT_AUTO_WATERMARK_INTERVAL_SECONDS = 5L;
    public static final boolean DEFAULT_START_FROM_EARLIEST = true;

    public static final String TOPIC_SEPARATOR = ",";

    private JobConfigOptions() {
    }
}
