package com.bank.aisafetyops.infra.serde;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.model.BasicIncident;
import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.GuardrailQualityMetric;
import com.bank.aisafetyops.model.IncidentSeverity;
import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.SafetyEvent;
import com.bank.aisafetyops.model.WindowNames;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonSerdeTest {
    @Test
    void serializesInvalidEventToJson() {
        String json = JsonSerde.toJson(new InvalidEvent("bad schema", "{\"broken\":true}"));
        assertTrue(json.contains("\"reason\":\"bad schema\""));
    }

    @Test
    void serializesLateEventToJson() {
        SafetyEvent event = new SafetyEvent(
                "agent-risk-01",
                "local",
                "agent-risk-01",
                "session-001",
                "req-001",
                null,
                1_000L,
                EventType.AGENT_REQUEST,
                "gpt-4.1-mini",
                "user-1",
                "web",
                10,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "{}"
        );

        String json = JsonSerde.toJson(new LateEvent("too old", event, 5_000L));
        assertTrue(json.contains("\"currentWatermark\":5000"));
        assertTrue(json.contains("\"requestId\":\"req-001\""));
    }

    @Test
    void serializesBasicIncidentToJson() {
        String json = JsonSerde.toJson(new BasicIncident(
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
        assertTrue(json.contains("\"appliedPolicyVersion\":\"policy-v2\""));
    }

    @Test
    void serializesGuardrailQualityMetricToJson() {
        String json = JsonSerde.toJson(new GuardrailQualityMetric(
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
        assertTrue(json.contains("\"detectorErrorRate\":0.1"));
        assertTrue(json.contains("\"confidenceCoverageRate\":0.8"));
    }
}
