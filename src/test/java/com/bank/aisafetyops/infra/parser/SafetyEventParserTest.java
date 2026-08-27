package com.bank.aisafetyops.infra.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.model.EventType;
import org.junit.jupiter.api.Test;

class SafetyEventParserTest {
    private static final String AGENT_ID = "agent-risk-01";
    private static final String SESSION_ID = "session-001";
    private static final String REQUEST_ID = "req-000001";
    private static final String EVENT_TIME_REQUEST = "2026-08-26T12:00:00Z";
    private static final String EVENT_TIME_FINDING = "2026-08-26T12:00:01Z";
    private static final String MODEL_NAME = "gpt-4.1-mini";
    private static final String PROMPT_INJECTION = "PROMPT_INJECTION";
    private static final String LOOPING = "LOOPING";
    private static final String GUARDRAIL_VERSION = "pi-v1";
    private static final String POLICY_VERSION = "policy-v1";
    private static final String ERROR_TRIGGERED_REQUIRED = "Missing required triggered field for guardrail finding";
    private static final double CONFIDENCE = 0.91d;

    private final SafetyEventParser parser = new SafetyEventParser();

    @Test
    void parsesAgentRequest() {
        String payload = """
                {
                  "eventType": "AGENT_REQUEST",
                  "agentId": "%s",
                  "tenantId": "%s",
                  "sessionId": "%s",
                  "requestId": "%s",
                  "eventTime": "%s",
                  "modelName": "%s",
                  "inputTokens": 150
                }
                """.formatted(AGENT_ID, AGENT_ID, SESSION_ID, REQUEST_ID, EVENT_TIME_REQUEST, MODEL_NAME);

        ParseResult result = parser.parse(payload);

        assertTrue(result.isValid(), () -> String.valueOf(result.invalidEvent()));
        assertEquals(EventType.AGENT_REQUEST, result.event().eventType());
        assertEquals(AGENT_ID, result.event().agentId());
        assertEquals(SESSION_ID, result.event().sessionId());
    }

    @Test
    void rejectsGuardrailFindingWithoutTriggered() {
        String payload = """
                {
                  "eventType": "GUARDRAIL_FINDING",
                  "agentId": "%s",
                  "tenantId": "%s",
                  "sessionId": "%s",
                  "requestId": "%s",
                  "eventTime": "%s",
                  "guardrailName": "%s"
                }
                """.formatted(AGENT_ID, AGENT_ID, SESSION_ID, REQUEST_ID, EVENT_TIME_FINDING, LOOPING);

        ParseResult result = parser.parse(payload);

        assertFalse(result.isValid());
        assertEquals(ERROR_TRIGGERED_REQUIRED, result.invalidEvent().reason());
    }

    @Test
    void parsesConfidenceBasedGuardrailFinding() {
        String payload = """
                {
                  "eventType": "GUARDRAIL_FINDING",
                  "agentId": "%s",
                  "tenantId": "%s",
                  "sessionId": "%s",
                  "requestId": "%s",
                  "eventTime": "%s",
                  "guardrailName": "%s",
                  "guardrailVersion": "%s",
                  "policyVersion": "%s",
                  "confidence": %s,
                  "triggered": true
                }
                """.formatted(
                        AGENT_ID,
                        AGENT_ID,
                        SESSION_ID,
                        REQUEST_ID,
                        EVENT_TIME_FINDING,
                        PROMPT_INJECTION,
                        GUARDRAIL_VERSION,
                        POLICY_VERSION,
                        CONFIDENCE
                );

        ParseResult result = parser.parse(payload);

        assertTrue(result.isValid());
        assertEquals(PROMPT_INJECTION, result.event().guardrailName());
        assertEquals(CONFIDENCE, result.event().confidence());
        assertEquals(Boolean.TRUE, result.event().triggered());
    }
}
