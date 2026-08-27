package com.bank.aisafetyops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.GuardrailNames;
import com.bank.aisafetyops.model.SafetyEvent;
import org.junit.jupiter.api.Test;

class GuardrailWindowAggregateFunctionTest {
    private static final long EVENT_TIME = 1_787_745_600_000L;
    private static final String TENANT_ID = "agent-risk-01";
    private static final String AGENT_ID = "agent-risk-01";
    private static final String SESSION_ID = "session-001";
    private static final String MODEL_NAME = "gpt-4.1-mini";
    private static final String POLICY_VERSION = "policy-v1";

    private final GuardrailWindowAggregateFunction function = new GuardrailWindowAggregateFunction();

    @Test
    void aggregatesConfidenceBasedGuardrailMetrics() {
        GuardrailAggregateAccumulator accumulator = function.createAccumulator();

        function.add(buildFinding(GuardrailNames.PROMPT_INJECTION, "pi-v1", 0.91d, true, 15L, "OK"), accumulator);
        function.add(buildFinding(GuardrailNames.PROMPT_INJECTION, "pi-v1", 0.73d, false, 25L, "ERROR"), accumulator);

        GuardrailAggregateAccumulator result = function.getResult(accumulator);

        assertEquals(2L, result.totalEvents());
        assertEquals(2L, result.guardrailFindingCount());
        assertEquals(1L, result.triggeredCount());
        assertEquals(0L, result.loopingTriggeredCount());
        assertEquals(0L, result.systemPromptLeakageTriggeredCount());
        assertEquals(2L, result.detectorLatencyCount());
        assertEquals(1L, result.detectorErrorCount());
        assertEquals(0.73d, result.minConfidence());
        assertEquals(0.82d, result.avgConfidence(), 0.0001d);
        assertEquals(0.91d, result.maxConfidence());
        assertEquals(15L, result.minDetectorLatencyMs());
        assertEquals(20.0d, result.avgDetectorLatencyMs(), 0.0001d);
        assertEquals(25L, result.maxDetectorLatencyMs());
    }

    @Test
    void aggregatesBooleanGuardrailMetricsWithoutConfidence() {
        GuardrailAggregateAccumulator accumulator = function.createAccumulator();

        function.add(buildFinding(GuardrailNames.LOOPING, "loop-v1", null, true, 7L, "OK"), accumulator);
        function.add(buildFinding(GuardrailNames.SYSTEM_PROMPT_LEAKAGE, "spl-v1", null, true, 9L, "OK"), accumulator);

        GuardrailAggregateAccumulator result = function.getResult(accumulator);

        assertEquals(2L, result.totalEvents());
        assertEquals(2L, result.triggeredCount());
        assertEquals(1L, result.loopingTriggeredCount());
        assertEquals(1L, result.systemPromptLeakageTriggeredCount());
        assertNull(result.minConfidence());
        assertNull(result.avgConfidence());
        assertNull(result.maxConfidence());
    }

    private static SafetyEvent buildFinding(
            String guardrailName,
            String guardrailVersion,
            Double confidence,
            boolean triggered,
            Long detectorLatencyMs,
            String detectorStatus
    ) {
        return new SafetyEvent(
                TENANT_ID,
                null,
                AGENT_ID,
                SESSION_ID,
                "req-000001",
                "turn-00001",
                EVENT_TIME,
                EventType.GUARDRAIL_FINDING,
                MODEL_NAME,
                "user-001",
                "web",
                0,
                0,
                guardrailName,
                guardrailVersion,
                POLICY_VERSION,
                confidence,
                triggered,
                detectorLatencyMs,
                detectorStatus,
                "{}"
        );
    }
}
