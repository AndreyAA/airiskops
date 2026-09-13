package com.bank.airiskops.app.usecase;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.app.config.EmbeddingProvider;
import com.bank.airiskops.app.config.IncidentConfig;
import com.bank.airiskops.app.config.JobConfig;
import com.bank.airiskops.app.config.OutputTopics;
import com.bank.airiskops.app.config.PipelineDeliveryGuarantee;
import com.bank.airiskops.app.config.PiAndToxicRuleConfig;
import com.bank.airiskops.app.config.PolicyConfig;
import com.bank.airiskops.app.config.RuntimeContractConfig;
import com.bank.airiskops.app.config.RuntimeStateConfig;
import com.bank.airiskops.app.config.SimilarPromptInjectionRuleConfig;
import com.bank.airiskops.model.IncidentGuardrailPolicy;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentPolicyDefaults;
import com.bank.airiskops.model.IncidentSeverity;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class IncrementOneTopologyBuilderTest {
    @Test
    void configureBuildsTopologyWithIncidentAndRuntimeContractBranches() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = testJobConfig();

        assertDoesNotThrow(() -> IncrementOneTopologyBuilder.configure(env, config));
    }

    @Test
    void disabledSimilarityOmitsSimilarityOperatorsFromStreamGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IncrementOneTopologyBuilder.configure(env, testJobConfig(false));

        Collection<StreamNode> nodes = env.getStreamGraph().getStreamNodes();
        assertFalse(hasOperator(nodes, "Embed Similar Prompt Injection Evidence"));
        assertFalse(hasOperator(nodes, "Similar Prompt Injection Campaign"));
        assertTrue(hasOperator(nodes, "Session Incident Evaluator"));
        assertTrue(hasOperator(nodes, "Kafka Basic Incidents"));
    }

    @Test
    void enabledSimilarityKeepsSimilarityOperatorsInStreamGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IncrementOneTopologyBuilder.configure(env, testJobConfig(true));

        Collection<StreamNode> nodes = env.getStreamGraph().getStreamNodes();
        assertTrue(hasOperator(nodes, "Embed Similar Prompt Injection Evidence"));
        assertTrue(hasOperator(nodes, "Similar Prompt Injection Campaign"));
        assertTrue(hasUid(nodes, "embed-similar-prompt-injection"));
        assertTrue(hasUid(nodes, "similar-prompt-injection-campaign"));
    }

    @Test
    void disabledSimilarityDoesNotRequireOnnxArtifacts() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertDoesNotThrow(() -> IncrementOneTopologyBuilder.configure(
                env,
                testJobConfig(false, EmbeddingProvider.LANGCHAIN4J_ONNX)
        ));
    }

    @Test
    void serializeToJsonDeclaresStringOutputType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertEquals(
                String.class,
                IncrementOneTopologyBuilder
                        .serializeToJson(env.fromData("value"))
                        .getTransformation()
                        .getOutputType()
                        .getTypeClass()
        );
    }

    private static JobConfig testJobConfig() {
        return testJobConfig(false);
    }

    private static JobConfig testJobConfig(boolean similarPromptInjectionEnabled) {
        return testJobConfig(similarPromptInjectionEnabled, EmbeddingProvider.DETERMINISTIC);
    }

    private static JobConfig testJobConfig(
            boolean similarPromptInjectionEnabled,
            EmbeddingProvider embeddingProvider
    ) {
        return new JobConfig(
                "localhost:9092",
                List.of("agent-requests", "agent-responses", "guardrail-findings"),
                "airiskops-test",
                new OutputTopics(
                        "normalized-events",
                        "invalid-events",
                        "late-events",
                        "guardrail-aggregates",
                        "basic-incidents",
                        "guardrail-quality-metrics"
                ),
                new IncidentConfig(
                        true,
                        "basic-incidents",
                        true,
                        Duration.ofMinutes(15),
                        25,
                        3,
                        3,
                        2,
                        new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, null, null),
                        new SimilarPromptInjectionRuleConfig(
                                similarPromptInjectionEnabled,
                                Duration.ofMinutes(5),
                                0.5d,
                                3,
                                2,
                                IncidentSeverity.HIGH,
                                "deterministic-hash-v1",
                                "query: ",
                                1000,
                                20,
                                100,
                                50,
                                20,
                                5,
                                500,
                                embeddingProvider,
                                null,
                                null,
                                null,
                                384,
                                256,
                                1
                        )
                ),
                new PolicyConfig(
                        true,
                        Path.of("config/policies/default-policy.yaml"),
                        true,
                        "policy-updates",
                        true
                ),
                bootstrapPolicy(),
                new RuntimeContractConfig(
                        "tumbling-event-time",
                        List.of(Duration.ofMinutes(1), Duration.ofMinutes(5)),
                        PipelineDeliveryGuarantee.AT_LEAST_ONCE
                ),
                RuntimeStateConfig.defaults(),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                Duration.ofMinutes(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                true
        );
    }

    private static boolean hasOperator(Collection<StreamNode> nodes, String operatorName) {
        return nodes.stream().anyMatch(node -> node.getOperatorName().contains(operatorName));
    }

    private static boolean hasUid(Collection<StreamNode> nodes, String uid) {
        return nodes.stream().anyMatch(node -> uid.equals(node.getTransformationUID()));
    }

    private static IncidentPolicy bootstrapPolicy() {
        IncidentGuardrailPolicy defaultPolicy = new IncidentGuardrailPolicy(0.4d, 0.7d, 0.9d, IncidentSeverity.MEDIUM);
        return new IncidentPolicy(
                "policy-v1",
                "test",
                "2026-08-30T00:00:00Z",
                new IncidentPolicyDefaults(defaultPolicy, defaultPolicy, defaultPolicy, defaultPolicy),
                Map.of()
        );
    }
}
