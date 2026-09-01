package com.bank.airiskops.app.config;

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
    public static final String ARG_BASIC_INCIDENTS_TOPIC = "basicIncidentsTopic";
    public static final String ARG_GUARDRAIL_QUALITY_METRICS_TOPIC = "guardrailQualityMetricsTopic";
    public static final String ARG_OUT_OF_ORDERNESS_SECONDS = "outOfOrdernessSeconds";
    public static final String ARG_IDLE_TIMEOUT_MINUTES = "idleTimeoutMinutes";
    public static final String ARG_LATE_TOLERANCE_MINUTES = "lateToleranceMinutes";
    public static final String ARG_CHECKPOINT_INTERVAL_SECONDS = "checkpointIntervalSeconds";
    public static final String ARG_AUTO_WATERMARK_INTERVAL_SECONDS = "autoWatermarkIntervalSeconds";
    public static final String ARG_WINDOW_TYPE = "windowType";
    public static final String ARG_AGGREGATE_WINDOW_MINUTES = "aggregateWindowMinutes";
    public static final String ARG_DELIVERY_GUARANTEE = "deliveryGuarantee";
    public static final String ARG_START_FROM_EARLIEST = "startFromEarliest";
    public static final String ARG_INCIDENT_ENABLED = "incidentEnabled";
    public static final String ARG_INCIDENT_EMIT_UPDATES = "incidentEmitUpdates";
    public static final String ARG_INCIDENT_SESSION_TIMEOUT_MINUTES = "incidentSessionTimeoutMinutes";
    public static final String ARG_INCIDENT_MAX_REQUEST_IDS = "incidentMaxRequestIdsPerIncident";
    public static final String ARG_PROMPT_INJECTION_BURST_MIN_FINDINGS = "incidentPromptInjectionBurstMinFindings";
    public static final String ARG_TOXICITY_CAMPAIGN_MIN_FINDINGS = "incidentToxicityCampaignMinFindings";
    public static final String ARG_LOOPING_MIN_OCCURRENCES = "incidentLoopingMinOccurrences";
    public static final String ARG_PI_AND_TOXIC_ENABLED = "incidentPiAndToxicEnabled";
    public static final String ARG_PI_AND_TOXIC_WINDOW_MINUTES = "incidentPiAndToxicWindowMinutes";
    public static final String ARG_PI_AND_TOXIC_SEVERITY = "incidentPiAndToxicSeverity";
    public static final String ARG_PI_AND_TOXIC_MIN_PROMPT_INJECTION_TRIGGERED_COUNT =
            "incidentPiAndToxicMinPromptInjectionTriggeredCount";
    public static final String ARG_PI_AND_TOXIC_MIN_TOXICITY_TRIGGERED_COUNT =
            "incidentPiAndToxicMinToxicityTriggeredCount";
    public static final String ARG_PI_AND_TOXIC_MIN_PROMPT_INJECTION_CONFIDENCE =
            "incidentPiAndToxicMinPromptInjectionConfidence";
    public static final String ARG_PI_AND_TOXIC_MIN_TOXICITY_CONFIDENCE =
            "incidentPiAndToxicMinToxicityConfidence";
    public static final String ARG_POLICY_ENABLED = "policyEnabled";
    public static final String ARG_POLICY_BOOTSTRAP_FILE = "policyBootstrapFile";
    public static final String ARG_POLICY_REQUIRE_BOOTSTRAP = "policyRequireBootstrap";
    public static final String ARG_POLICY_UPDATES_TOPIC = "policyUpdatesTopic";
    public static final String ARG_POLICY_REJECT_OLDER_VERSIONS = "policyRejectOlderVersions";

    public static final String DEFAULT_CONFIG_FILE = "config/job/local-job.yaml";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPICS = "agent-requests,agent-responses,guardrail-findings";
    public static final String DEFAULT_GROUP_ID = "airiskops-mvp";
    public static final String DEFAULT_NORMALIZED_EVENTS_TOPIC = "normalized-events";
    public static final String DEFAULT_INVALID_EVENTS_TOPIC = "invalid-events";
    public static final String DEFAULT_LATE_EVENTS_TOPIC = "late-events";
    public static final String DEFAULT_GUARDRAIL_AGGREGATES_TOPIC = "guardrail-aggregates";
    public static final String DEFAULT_BASIC_INCIDENTS_TOPIC = "basic-incidents";
    public static final String DEFAULT_GUARDRAIL_QUALITY_METRICS_TOPIC = "guardrail-quality-metrics";
    public static final long DEFAULT_OUT_OF_ORDERNESS_SECONDS = 30L;
    public static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 1L;
    public static final long DEFAULT_LATE_TOLERANCE_MINUTES = 5L;
    public static final long DEFAULT_CHECKPOINT_INTERVAL_SECONDS = 30L;
    public static final long DEFAULT_AUTO_WATERMARK_INTERVAL_SECONDS = 5L;
    public static final String DEFAULT_WINDOW_TYPE = "tumbling-event-time";
    public static final String DEFAULT_AGGREGATE_WINDOW_MINUTES = "1,5";
    public static final String DEFAULT_DELIVERY_GUARANTEE = "AT_LEAST_ONCE";
    public static final boolean DEFAULT_START_FROM_EARLIEST = true;
    public static final boolean DEFAULT_INCIDENT_ENABLED = true;
    public static final boolean DEFAULT_INCIDENT_EMIT_UPDATES = false;
    public static final long DEFAULT_INCIDENT_SESSION_TIMEOUT_MINUTES = 30L;
    public static final int DEFAULT_INCIDENT_MAX_REQUEST_IDS = 50;
    public static final int DEFAULT_PROMPT_INJECTION_BURST_MIN_FINDINGS = 3;
    public static final int DEFAULT_TOXICITY_CAMPAIGN_MIN_FINDINGS = 3;
    public static final int DEFAULT_LOOPING_MIN_OCCURRENCES = 2;
    public static final boolean DEFAULT_PI_AND_TOXIC_ENABLED = true;
    public static final long DEFAULT_PI_AND_TOXIC_WINDOW_MINUTES = 5L;
    public static final String DEFAULT_PI_AND_TOXIC_SEVERITY = "HIGH";
    public static final int DEFAULT_PI_AND_TOXIC_MIN_PROMPT_INJECTION_TRIGGERED_COUNT = 1;
    public static final int DEFAULT_PI_AND_TOXIC_MIN_TOXICITY_TRIGGERED_COUNT = 1;
    public static final boolean DEFAULT_POLICY_ENABLED = true;
    public static final String DEFAULT_POLICY_BOOTSTRAP_FILE = "runtime/policies/active-policy.yaml";
    public static final boolean DEFAULT_POLICY_REQUIRE_BOOTSTRAP = false;
    public static final String DEFAULT_POLICY_UPDATES_TOPIC = "policy-updates";
    public static final boolean DEFAULT_POLICY_REJECT_OLDER_VERSIONS = true;

    public static final String TOPIC_SEPARATOR = ",";

    private JobConfigOptions() {
    }
}
