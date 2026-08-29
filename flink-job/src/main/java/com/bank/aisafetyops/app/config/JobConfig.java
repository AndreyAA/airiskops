package com.bank.aisafetyops.app.config;

import com.bank.aisafetyops.infra.config.YamlJobConfigLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;

public record JobConfig(
        String bootstrapServers,
        List<String> topics,
        String consumerGroupId,
        OutputTopics outputTopics,
        Duration outOfOrderness,
        Duration idleTimeout,
        Duration lateTolerance,
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
                        )
                ),
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
                readBoolean(
                        parameters,
                        yamlConfig,
                        JobConfigOptions.ARG_START_FROM_EARLIEST,
                        JobConfigOptions.DEFAULT_START_FROM_EARLIEST
                )
        );
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
}
