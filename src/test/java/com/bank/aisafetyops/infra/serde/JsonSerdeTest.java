package com.bank.aisafetyops.infra.serde;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.SafetyEvent;
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
}
