package com.bank.airiskops.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.model.IncidentSeverity;
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
        Path policyFile = tempDir.resolve("active-policy.yaml");
        Files.writeString(policyFile, """
                version: "policy-v9"
                updatedBy: "test"
                updatedAt: "2026-08-30T00:00:00Z"
                defaults:
                  promptInjection:
                    medium: 0.55
                    high: 0.77
                    critical: 0.91
                  toxicity:
                    medium: 0.70
                    high: 0.88
                    critical: 0.95
                  looping:
                    severity: "MEDIUM"
                  systemPromptLeakage:
                    severity: "CRITICAL"
                agents:
                  agent-risk-01:
                    promptInjection:
                      high: 0.73
                """);
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
                basicIncidentsTopic: basic-incidents-out
                guardrailQualityMetricsTopic: quality-out
                windowType: tumbling-event-time
                aggregateWindowMinutes:
                  - 1
                  - 5
                deliveryGuarantee: NONE
                outOfOrdernessSeconds: 45
                idleTimeoutMinutes: 2
                lateToleranceMinutes: 7
                checkpointIntervalSeconds: 40
                autoWatermarkIntervalSeconds: 8
                startFromEarliest: false
                incidentEnabled: true
                incidentEmitUpdates: true
                incidentSessionTimeoutMinutes: 15
                incidentMaxRequestIdsPerIncident: 25
                incidentPromptInjectionBurstMinFindings: 4
                incidentToxicityCampaignMinFindings: 5
                incidentLoopingMinOccurrences: 3
                incidentPiAndToxicEnabled: false
                incidentPiAndToxicWindowMinutes: 7
                incidentPiAndToxicSeverity: MEDIUM
                incidentPiAndToxicMinPromptInjectionTriggeredCount: 2
                incidentPiAndToxicMinToxicityTriggeredCount: 4
                incidentPiAndToxicMinPromptInjectionConfidence: 0.81
                incidentPiAndToxicMinToxicityConfidence: 0.93
                policyEnabled: true
                policyBootstrapFile: %s
                policyRequireBootstrap: true
                policyUpdatesTopic: policy-updates-local
                policyRejectOlderVersions: false
                runtimeState:
                  backendType: rocksdb
                  incrementalCheckpointsEnabled: true
                  checkpointsDir: file:///opt/flink/state/checkpoints
                  savepointsDir: file:///opt/flink/state/savepoints
                  rocksdbLocalDir: /opt/flink/state/rocksdb
                """.formatted(policyFile));

        JobConfig config = JobConfig.fromArgs(new String[]{
                "--configFile", configFile.toString(),
                "--groupId", "cli-group",
                "--lateToleranceMinutes", "9",
                "--autoWatermarkIntervalSeconds", "6",
                "--deliveryGuarantee", "AT_LEAST_ONCE",
                "--incidentPromptInjectionBurstMinFindings", "6",
                "--incidentPiAndToxicSeverity", "HIGH"
        });

        assertEquals("kafka:9092", config.bootstrapServers());
        assertEquals(2, config.topics().size());
        assertEquals("cli-group", config.consumerGroupId());
        assertEquals("normalized-out", config.outputTopics().normalizedEventsTopic());
        assertEquals("invalid-out", config.outputTopics().invalidEventsTopic());
        assertEquals("late-out", config.outputTopics().lateEventsTopic());
        assertEquals("guardrail-aggregates-out", config.outputTopics().guardrailAggregatesTopic());
        assertEquals("basic-incidents-out", config.outputTopics().basicIncidentsTopic());
        assertEquals("quality-out", config.outputTopics().guardrailQualityMetricsTopic());
        assertEquals("tumbling-event-time", config.runtimeContract().windowType());
        assertEquals(2, config.runtimeContract().aggregateWindows().size());
        assertEquals(Duration.ofMinutes(1), config.runtimeContract().aggregateWindows().get(0));
        assertEquals(Duration.ofMinutes(5), config.runtimeContract().aggregateWindows().get(1));
        assertEquals(PipelineDeliveryGuarantee.AT_LEAST_ONCE, config.runtimeContract().deliveryGuarantee());
        assertEquals(Duration.ofSeconds(45), config.outOfOrderness());
        assertEquals(Duration.ofMinutes(2), config.idleTimeout());
        assertEquals(Duration.ofMinutes(9), config.lateTolerance());
        assertEquals(Duration.ofSeconds(40), config.checkpointInterval());
        assertEquals(Duration.ofSeconds(6), config.autoWatermarkInterval());
        assertFalse(config.startFromEarliest());
        assertTrue(config.incidentConfig().enabled());
        assertTrue(config.incidentConfig().emitUpdates());
        assertEquals(Duration.ofMinutes(15), config.incidentConfig().sessionInactivityTimeout());
        assertEquals(25, config.incidentConfig().maxRequestIdsPerIncident());
        assertEquals(6, config.incidentConfig().promptInjectionBurstMinFindings());
        assertEquals(5, config.incidentConfig().toxicityCampaignMinFindings());
        assertEquals(3, config.incidentConfig().loopingMinOccurrences());
        assertFalse(config.incidentConfig().piAndToxic().enabled());
        assertEquals(Duration.ofMinutes(7), config.incidentConfig().piAndToxic().window());
        assertEquals(IncidentSeverity.HIGH, config.incidentConfig().piAndToxic().severity());
        assertEquals(2, config.incidentConfig().piAndToxic().minPromptInjectionTriggeredCount());
        assertEquals(4, config.incidentConfig().piAndToxic().minToxicityTriggeredCount());
        assertEquals(0.81d, config.incidentConfig().piAndToxic().minPromptInjectionConfidence());
        assertEquals(0.93d, config.incidentConfig().piAndToxic().minToxicityConfidence());
        assertTrue(config.policyConfig().enabled());
        assertEquals(policyFile, config.policyConfig().bootstrapFile());
        assertTrue(config.policyConfig().requireBootstrapPolicy());
        assertEquals("policy-updates-local", config.policyConfig().updatesTopic());
        assertFalse(config.policyConfig().rejectOlderVersions());
        assertEquals(StateBackendType.ROCKSDB, config.runtimeState().backendType());
        assertTrue(config.runtimeState().incrementalCheckpointsEnabled());
        assertEquals("file:///opt/flink/state/checkpoints", config.runtimeState().checkpointsDir());
        assertEquals("file:///opt/flink/state/savepoints", config.runtimeState().savepointsDir());
        assertEquals("/opt/flink/state/rocksdb", config.runtimeState().rocksdbLocalDir());
        assertEquals("policy-v9", config.bootstrapIncidentPolicy().version());
        assertEquals(0.73d, config.bootstrapIncidentPolicy().resolveForAgent("agent-risk-01").promptInjection().high());
    }

    @Test
    void usesDefaultRuntimeStateProfileWhenYamlSectionIsMissing() throws Exception {
        Path configFile = tempDir.resolve("default-job-config.yaml");
        Files.writeString(configFile, """
                bootstrapServers: kafka:9092
                policyEnabled: false
                """);

        JobConfig config = JobConfig.fromArgs(new String[]{
                "--configFile", configFile.toString()
        });

        assertEquals(StateBackendType.DEFAULT, config.runtimeState().backendType());
        assertFalse(config.runtimeState().incrementalCheckpointsEnabled());
        assertNull(config.runtimeState().checkpointsDir());
        assertNull(config.runtimeState().savepointsDir());
        assertNull(config.runtimeState().rocksdbLocalDir());
    }

    @Test
    void rejectsIncompleteRocksDbRuntimeStateConfig() throws Exception {
        Path configFile = tempDir.resolve("invalid-rocksdb-config.yaml");
        Files.writeString(configFile, """
                bootstrapServers: kafka:9092
                policyEnabled: false
                runtimeState:
                  backendType: rocksdb
                  incrementalCheckpointsEnabled: true
                  checkpointsDir: file:///opt/flink/state/checkpoints
                """);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--configFile", configFile.toString()})
        );

        assertTrue(error.getMessage().contains("runtimeState.savepointsDir"));
    }
}
