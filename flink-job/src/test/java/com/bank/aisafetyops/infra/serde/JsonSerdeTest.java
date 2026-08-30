package com.bank.aisafetyops.infra.serde;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.model.BasicIncident;
import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.IncidentSeverity;
import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.SafetyEvent;
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
}
