package com.bank.aisafetyops.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JobConfigTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsValuesFromYamlAndAllowsCliOverrides() throws Exception {
        Path configFile = tempDir.resolve("job-config.yaml");
        Files.writeString(configFile, """
                bootstrapServers: kafka:9092
                topics:
                  - agent-requests
                  - guardrail-findings
                groupId: yaml-group
                normalizedEventsTopic: normalized-out
                invalidEventsTopic: invalid-out
                lateEventsTopic: late-out
                guardrailAggregatesTopic: guardrail-aggregates-out
                outOfOrdernessSeconds: 45
                idleTimeoutMinutes: 2
                lateToleranceMinutes: 7
                startFromEarliest: false
                """);

        JobConfig config = JobConfig.fromArgs(new String[]{
                "--configFile", configFile.toString(),
                "--groupId", "cli-group",
                "--lateToleranceMinutes", "9"
        });

        assertEquals("kafka:9092", config.bootstrapServers());
        assertEquals(2, config.topics().size());
        assertEquals("cli-group", config.consumerGroupId());
        assertEquals("normalized-out", config.outputTopics().normalizedEventsTopic());
        assertEquals("invalid-out", config.outputTopics().invalidEventsTopic());
        assertEquals("late-out", config.outputTopics().lateEventsTopic());
        assertEquals("guardrail-aggregates-out", config.outputTopics().guardrailAggregatesTopic());
        assertEquals(Duration.ofSeconds(45), config.outOfOrderness());
        assertEquals(Duration.ofMinutes(2), config.idleTimeout());
        assertEquals(Duration.ofMinutes(9), config.lateTolerance());
        assertFalse(config.startFromEarliest());
    }
}
