package com.bank.airiskops.app.config;

import com.bank.airiskops.infra.config.IncidentPolicyLoader;
import com.bank.airiskops.infra.config.YamlJobConfigLoader;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentSeverity;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Runtime configuration for the AIRiskOps Flink job.
 *
 * <p>This record combines CLI arguments and YAML defaults into a single object
 * that can be passed through topology setup without leaking parameter parsing
 * details into application logic.
 */
public record JobConfig(
        String bootstrapServers,
        List<String> topics,
        String consumerGroupId,
        OutputTopics outputTopics,
        IncidentConfig incidentConfig,
        PolicyConfig policyConfig,
        IncidentPolicy bootstrapIncidentPolicy,
        RuntimeContractConfig runtimeContract,
        Duration outOfOrderness,
        Duration idleTimeout,
        Duration lateTolerance,
        Duration checkpointInterval,
        Duration autoWatermarkInterval,
        boolean startFromEarliest
) {
    public static JobConfig fromArgs(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        Map<String, Object> yamlConfig = loadYamlConfig(parameters);

        String bootstrapServers = readString(
                parameters,
                yamlConfig,
                JobConfigOptions.ARG_BOOTSTRAP_SERVERS,
                JobConfigOptions.DEFAULT_BOOTSTRAP_SERVERS
        );
        // Topics stay configurable even for the local MVP so that we can keep the
        // transport contract stable while swapping only the serialization layer later.
        List<String> topics = readTopics(parameters, yamlConfig);

        PolicyConfig policyConfig = new PolicyConfig(
                readBoolean(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_POLICY_ENABLED,
                        JobConfigOptions.DEFAULT_POLICY_ENABLED
                ),
                Path.of(readString(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_POLICY_BOOTSTRAP_FILE,
                        JobConfigOptions.DEFAULT_POLICY_BOOTSTRAP_FILE
                )),
                readBoolean(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_POLICY_REQUIRE_BOOTSTRAP,
                        JobConfigOptions.DEFAULT_POLICY_REQUIRE_BOOTSTRAP
                ),
                readString(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_POLICY_UPDATES_TOPIC,
                        JobConfigOptions.DEFAULT_POLICY_UPDATES_TOPIC
                ),
                readBoolean(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_POLICY_REJECT_OLDER_VERSIONS,
                        JobConfigOptions.DEFAULT_POLICY_REJECT_OLDER_VERSIONS
                )
        );
        RuntimeContractConfig runtimeContract = new RuntimeContractConfig(
                readString(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_WINDOW_TYPE,
                        JobConfigOptions.DEFAULT_WINDOW_TYPE
                ),
                readDurationMinutesList(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_AGGREGATE_WINDOW_MINUTES,
                        JobConfigOptions.DEFAULT_AGGREGATE_WINDOW_MINUTES
                ),
                PipelineDeliveryGuarantee.fromConfigValue(readString(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_DELIVERY_GUARANTEE,
                        JobConfigOptions.DEFAULT_DELIVERY_GUARANTEE
                ))
        );

        return new JobConfig(
                bootstrapServers,
                topics,
                readString(parameters, yamlConfig, JobConfigOptions.ARG_GROUP_ID, JobConfigOptions.DEFAULT_GROUP_ID),
                new OutputTopics(
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_NORMALIZED_EVENTS_TOPIC,
                                JobConfigOptions.DEFAULT_NORMALIZED_EVENTS_TOPIC
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_INVALID_EVENTS_TOPIC,
                                JobConfigOptions.DEFAULT_INVALID_EVENTS_TOPIC
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_LATE_EVENTS_TOPIC,
                                JobConfigOptions.DEFAULT_LATE_EVENTS_TOPIC
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_GUARDRAIL_AGGREGATES_TOPIC,
                                JobConfigOptions.DEFAULT_GUARDRAIL_AGGREGATES_TOPIC
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_BASIC_INCIDENTS_TOPIC,
                                JobConfigOptions.DEFAULT_BASIC_INCIDENTS_TOPIC
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_GUARDRAIL_QUALITY_METRICS_TOPIC,
                                JobConfigOptions.DEFAULT_GUARDRAIL_QUALITY_METRICS_TOPIC
                        )
                ),
                new IncidentConfig(
                        readBoolean(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_INCIDENT_ENABLED,
                                JobConfigOptions.DEFAULT_INCIDENT_ENABLED
                        ),
                        readString(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_BASIC_INCIDENTS_TOPIC,
                                JobConfigOptions.DEFAULT_BASIC_INCIDENTS_TOPIC
                        ),
                        readBoolean(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_INCIDENT_EMIT_UPDATES,
                                JobConfigOptions.DEFAULT_INCIDENT_EMIT_UPDATES
                        ),
                        Duration.ofMinutes(readLong(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_INCIDENT_SESSION_TIMEOUT_MINUTES,
                                JobConfigOptions.DEFAULT_INCIDENT_SESSION_TIMEOUT_MINUTES
                        )),
                        readInt(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_INCIDENT_MAX_REQUEST_IDS,
                                JobConfigOptions.DEFAULT_INCIDENT_MAX_REQUEST_IDS
                        ),
                        readInt(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_PROMPT_INJECTION_BURST_MIN_FINDINGS,
                                JobConfigOptions.DEFAULT_PROMPT_INJECTION_BURST_MIN_FINDINGS
                        ),
                        readInt(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_TOXICITY_CAMPAIGN_MIN_FINDINGS,
                                JobConfigOptions.DEFAULT_TOXICITY_CAMPAIGN_MIN_FINDINGS
                        ),
                        readInt(
                                parameters,
                                yamlConfig,
                                JobConfigOptions.ARG_LOOPING_MIN_OCCURRENCES,
                                JobConfigOptions.DEFAULT_LOOPING_MIN_OCCURRENCES
                        ),
                        new PiAndToxicRuleConfig(
                                readBoolean(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_ENABLED,
                                        JobConfigOptions.DEFAULT_PI_AND_TOXIC_ENABLED
                                ),
                                Duration.ofMinutes(readLong(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_WINDOW_MINUTES,
                                        JobConfigOptions.DEFAULT_PI_AND_TOXIC_WINDOW_MINUTES
                                )),
                                IncidentSeverity.valueOf(readString(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_SEVERITY,
                                        JobConfigOptions.DEFAULT_PI_AND_TOXIC_SEVERITY
                                ).toUpperCase()),
                                readInt(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_MIN_PROMPT_INJECTION_TRIGGERED_COUNT,
                                        JobConfigOptions.DEFAULT_PI_AND_TOXIC_MIN_PROMPT_INJECTION_TRIGGERED_COUNT
                                ),
                                readInt(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_MIN_TOXICITY_TRIGGERED_COUNT,
                                        JobConfigOptions.DEFAULT_PI_AND_TOXIC_MIN_TOXICITY_TRIGGERED_COUNT
                                ),
                                readDouble(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_MIN_PROMPT_INJECTION_CONFIDENCE
                                ),
                                readDouble(
                                        parameters,
                                        yamlConfig,
                                        JobConfigOptions.ARG_PI_AND_TOXIC_MIN_TOXICITY_CONFIDENCE
                                )
                        )
                ),
                policyConfig,
                loadBootstrapIncidentPolicy(policyConfig),
                runtimeContract,
                Duration.ofSeconds(readLong(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_OUT_OF_ORDERNESS_SECONDS,
                        JobConfigOptions.DEFAULT_OUT_OF_ORDERNESS_SECONDS
                )),
                Duration.ofMinutes(readLong(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_IDLE_TIMEOUT_MINUTES,
                        JobConfigOptions.DEFAULT_IDLE_TIMEOUT_MINUTES
                )),
                Duration.ofMinutes(readLong(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_LATE_TOLERANCE_MINUTES,
                        JobConfigOptions.DEFAULT_LATE_TOLERANCE_MINUTES
                )),
                Duration.ofSeconds(readLong(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_CHECKPOINT_INTERVAL_SECONDS,
                        JobConfigOptions.DEFAULT_CHECKPOINT_INTERVAL_SECONDS
                )),
                Duration.ofSeconds(readLong(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_AUTO_WATERMARK_INTERVAL_SECONDS,
                        JobConfigOptions.DEFAULT_AUTO_WATERMARK_INTERVAL_SECONDS
                )),
                readBoolean(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_START_FROM_EARLIEST,
                        JobConfigOptions.DEFAULT_START_FROM_EARLIEST
                )
        );
    }

    private static IncidentPolicy loadBootstrapIncidentPolicy(PolicyConfig policyConfig) {
        if (!policyConfig.enabled()) {
            return null;
        }
        if (policyConfig.requireBootstrapPolicy()) {
            return IncidentPolicyLoader.loadRequired(policyConfig.bootstrapFile());
        }
        return IncidentPolicyLoader.loadIfExists(policyConfig.bootstrapFile());
    }

    private static Map<String, Object> loadYamlConfig(ParameterTool parameters) {
        String configFile = parameters.get(JobConfigOptions.ARG_CONFIG_FILE);
        if (configFile != null && !configFile.isBlank()) {
            return YamlJobConfigLoader.loadRequired(Path.of(configFile));
        }
        return YamlJobConfigLoader.loadIfExists(Path.of(JobConfigOptions.DEFAULT_CONFIG_FILE));
    }

    private static List<String> readTopics(ParameterTool parameters, Map<String, Object> yamlConfig) {
        String topicsCsv = parameters.get(JobConfigOptions.ARG_TOPICS);
        if (topicsCsv != null) {
            return List.of(topicsCsv.split(JobConfigOptions.TOPIC_SEPARATOR));
        }

        Object yamlTopics = yamlConfig.get(JobConfigOptions.ARG_TOPICS);
        if (yamlTopics instanceof List<?> values) {
            return values.stream().map(String::valueOf).toList();
        }
        if (yamlTopics instanceof String value) {
            return List.of(value.split(JobConfigOptions.TOPIC_SEPARATOR));
        }
        return List.of(JobConfigOptions.DEFAULT_TOPICS.split(JobConfigOptions.TOPIC_SEPARATOR));
    }

    private static List<Duration> readDurationMinutesList(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key,
            String defaultValue
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return parseDurationMinutes(cliValue.split(JobConfigOptions.TOPIC_SEPARATOR));
        }

        Object yamlValue = yamlConfig.get(key);
        if (yamlValue instanceof List<?> values) {
            return parseDurationMinutes(values.stream().map(String::valueOf).toArray(String[]::new));
        }
        if (yamlValue instanceof String value) {
            return parseDurationMinutes(value.split(JobConfigOptions.TOPIC_SEPARATOR));
        }
        return parseDurationMinutes(defaultValue.split(JobConfigOptions.TOPIC_SEPARATOR));
    }

    private static List<Duration> parseDurationMinutes(String[] rawValues) {
        return List.of(rawValues).stream()
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(Long::parseLong)
                .map(Duration::ofMinutes)
                .toList();
    }

    private static String readString(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key,
            String defaultValue
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return cliValue;
        }
        Object yamlValue = yamlConfig.get(key);
        return yamlValue != null ? String.valueOf(yamlValue) : defaultValue;
    }

    private static long readLong(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key,
            long defaultValue
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return Long.parseLong(cliValue);
        }
        Object yamlValue = yamlConfig.get(key);
        if (yamlValue instanceof Number number) {
            return number.longValue();
        }
        if (yamlValue instanceof String value) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }

    private static int readInt(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key,
            int defaultValue
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return Integer.parseInt(cliValue);
        }
        Object yamlValue = yamlConfig.get(key);
        if (yamlValue instanceof Number number) {
            return number.intValue();
        }
        if (yamlValue instanceof String value) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    private static boolean readBoolean(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key,
            boolean defaultValue
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return Boolean.parseBoolean(cliValue);
        }
        Object yamlValue = yamlConfig.get(key);
        if (yamlValue instanceof Boolean value) {
            return value;
        }
        if (yamlValue instanceof String value) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    private static Double readDouble(
            ParameterTool parameters,
            Map<String, Object> yamlConfig,
            String key
    ) {
        String cliValue = parameters.get(key);
        if (cliValue != null) {
            return Double.parseDouble(cliValue);
        }
        Object yamlValue = yamlConfig.get(key);
        if (yamlValue instanceof Number number) {
            return number.doubleValue();
        }
        if (yamlValue instanceof String value && !value.isBlank()) {
            return Double.parseDouble(value);
        }
        return null;
    }
}
