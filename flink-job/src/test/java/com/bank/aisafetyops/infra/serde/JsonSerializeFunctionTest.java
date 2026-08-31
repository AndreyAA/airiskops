package com.bank.aisafetyops.infra.serde;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.model.BasicIncident;
import com.bank.aisafetyops.model.GuardrailQualityMetric;
import com.bank.aisafetyops.model.IncidentSeverity;
import com.bank.aisafetyops.model.WindowNames;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonSerializeFunctionTest {
    private final JsonSerializeFunction<Object> serializer = new JsonSerializeFunction<>();

    @Test
    void serializesBasicIncidentRecords() throws Exception {
        String json = serializer.map(new BasicIncident(
                "incident-1",
                "agent-risk-01",
                "agent-risk-01",
                "session-001",
                "PROMPT_INJECTION_BURST",
                IncidentSeverity.HIGH,
                new ArrayList<>(List.of("req-001", "req-002")),
                new ArrayList<>(List.of("PROMPT_INJECTION")),
                new ArrayList<>(List.of("guard-v1")),
                new ArrayList<>(List.of("policy-v1")),
                "policy-v2",
                1_000L,
                2_000L,
                2_000L,
                2L,
                1,
                "Prompt injection burst detected"
        ));

        assertTrue(json.contains("\"incidentId\":\"incident-1\""));
        assertTrue(json.contains("\"severity\":\"HIGH\""));
    }

    @Test
    void serializesGuardrailQualityMetricRecords() throws Exception {
        String json = serializer.map(new GuardrailQualityMetric(
                "agent-risk-01",
                "agent-risk-01",
                "PROMPT_INJECTION",
                "pi-v1",
                "policy-v1",
                "gpt-4.1-mini",
                WindowNames.WINDOW_1_MINUTE,
                1_000L,
                61_000L,
                10L,
                4L,
                0.4d,
                1L,
                0.1d,
                2L,
                0.2d,
                0.8d,
                8L,
                10L,
                20.0d,
                80L
        ));

        assertTrue(json.contains("\"guardrailName\":\"PROMPT_INJECTION\""));
        assertTrue(json.contains("\"confidenceCoverageRate\":0.8"));
    }
}
