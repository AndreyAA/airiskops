package com.bank.airiskops.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class SessionRiskSnapshotTest {
    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final String TENANT_ID = "tenant-01";
    private static final String AGENT_ID = "agent-01";
    private static final String SESSION_ID = "session-01";
    private static final long BASE_TIME = 1_788_220_800_000L;

    @Test
    void internalWindowEvidenceHolderIsNotARecord() {
        Class<?> evidenceClass = findWindowEvidenceClass();

        assertFalse(evidenceClass.isRecord());
        assertTrue(java.io.Serializable.class.isAssignableFrom(evidenceClass));
    }

    @Test
    void javaSerializationRoundTripPreservesWindowEvidence() throws Exception {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        snapshot.recordFinding(buildFinding("req-pi", GuardrailNames.PROMPT_INJECTION, 0.81d, BASE_TIME + 1_000L), 10, WINDOW);
        snapshot.recordFinding(buildFinding("req-tox", GuardrailNames.TOXICITY, 0.93d, BASE_TIME + 2_000L), 10, WINDOW);

        byte[] serialized = InstantiationUtil.serializeObject(snapshot);
        SessionRiskSnapshot restored = InstantiationUtil.deserializeObject(serialized, SessionRiskSnapshot.class.getClassLoader());
        SessionRiskSnapshot.PiAndToxicWindowStats restoredStats =
                restored.piAndToxicWindowStats(BASE_TIME + 2_000L, WINDOW, null, null);

        assertEquals(TENANT_ID, restored.tenantId());
        assertEquals(AGENT_ID, restored.agentId());
        assertEquals(SESSION_ID, restored.sessionId());
        assertEquals(2, restored.triggeredFindingsCount());
        assertEquals(1, restoredStats.promptInjectionCount());
        assertEquals(0.81d, restoredStats.maxPromptInjectionConfidence());
        assertEquals(1, restoredStats.toxicityCount());
        assertEquals(0.93d, restoredStats.maxToxicityConfidence());
    }

    @Test
    void prunesOutdatedWindowEvidenceBeforeCalculatingStats() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        snapshot.recordFinding(buildFinding("req-old-pi", GuardrailNames.PROMPT_INJECTION, 0.61d, BASE_TIME), 10, WINDOW);
        snapshot.recordFinding(
                buildFinding("req-new-pi", GuardrailNames.PROMPT_INJECTION, 0.88d, BASE_TIME + WINDOW.toMillis() + 1_000L),
                10,
                WINDOW
        );
        snapshot.recordFinding(
                buildFinding("req-new-tox", GuardrailNames.TOXICITY, 0.79d, BASE_TIME + WINDOW.toMillis() + 2_000L),
                10,
                WINDOW
        );

        SessionRiskSnapshot.PiAndToxicWindowStats stats =
                snapshot.piAndToxicWindowStats(BASE_TIME + WINDOW.toMillis() + 2_000L, WINDOW, null, null);

        assertEquals(1, stats.promptInjectionCount());
        assertEquals(0.88d, stats.maxPromptInjectionConfidence());
        assertEquals(1, stats.toxicityCount());
        assertEquals(0.79d, stats.maxToxicityConfidence());
    }

    private static Class<?> findWindowEvidenceClass() {
        for (Class<?> nestedClass : SessionRiskSnapshot.class.getDeclaredClasses()) {
            if ("WindowFindingEvidence".equals(nestedClass.getSimpleName())) {
                return nestedClass;
            }
        }
        throw new AssertionError("WindowFindingEvidence nested class not found");
    }

    private static SafetyEvent buildFinding(String requestId, String guardrailName, Double confidence, long eventTimeMillis) {
        return new SafetyEvent(
                TENANT_ID,
                "local",
                AGENT_ID,
                SESSION_ID,
                requestId,
                "turn-" + requestId,
                eventTimeMillis,
                EventType.GUARDRAIL_FINDING,
                "gpt-4.1-mini",
                "user-001",
                "web",
                100,
                50,
                guardrailName,
                "guard-v1",
                "policy-v1",
                confidence,
                true,
                10L,
                "OK",
                "{}"
        );
    }
}
