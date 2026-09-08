package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.GuardrailQualityMetric;
import com.bank.airiskops.model.GuardrailWindowAggregate;
import com.bank.airiskops.model.WindowNames;
import org.junit.jupiter.api.Test;

class GuardrailQualityMetricFunctionTest {
    @Test
    void buildsConfidenceCoverageMetricsForConfidenceBasedGuardrails() {
        GuardrailWindowAggregate aggregate = new GuardrailWindowAggregate(
                "agent-risk-01",
                "agent-risk-01",
                GuardrailNames.PROMPT_INJECTION,
                "pi-v1",
                "policy-v1",
                "gpt-4.1-mini",
                WindowNames.WINDOW_1_MINUTE,
                1_000L,
                61_000L,
                58_000L,
                10L,
                10L,
                4L,
                0L,
                0L,
                200L,
                300L,
                8L,
                0.22d,
                0.55d,
                0.93d,
                0.51d,
                0.89d,
                0.77d,
                0.95d,
                10L,
                25.0d,
                80L,
                2L,
                3_000L,
                1_500L
        );

        GuardrailQualityMetric qualityMetric = GuardrailQualityMetricFunction.buildQualityMetric(aggregate);

        assertEquals(0.4d, qualityMetric.triggerRate(), 0.0001d);
        assertEquals(0.2d, qualityMetric.detectorErrorRate(), 0.0001d);
        assertEquals(2L, qualityMetric.missingConfidenceCount());
        assertEquals(0.2d, qualityMetric.missingConfidenceRate(), 0.0001d);
        assertEquals(0.8d, qualityMetric.confidenceCoverageRate(), 0.0001d);
        assertEquals(8L, qualityMetric.confidenceCount());
        assertEquals(58_000L, qualityMetric.latestEventTimeMillis());
        assertEquals(3_000L, qualityMetric.e2eLatestEventToEmitMs());
        assertEquals(1_500L, qualityMetric.e2eWindowEndToEmitMs());
    }

    @Test
    void skipsConfidenceCoverageForBooleanGuardrails() {
        GuardrailWindowAggregate aggregate = new GuardrailWindowAggregate(
                "agent-risk-01",
                "agent-risk-01",
                GuardrailNames.LOOPING,
                "loop-v1",
                "policy-v1",
                "gpt-4.1-mini",
                WindowNames.WINDOW_5_MINUTES,
                1_000L,
                301_000L,
                299_000L,
                6L,
                6L,
                2L,
                2L,
                0L,
                100L,
                150L,
                0L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                5L,
                7.0d,
                9L,
                0L,
                2_000L,
                500L
        );

        GuardrailQualityMetric qualityMetric = GuardrailQualityMetricFunction.buildQualityMetric(aggregate);

        assertEquals(0.3333d, qualityMetric.triggerRate(), 0.0001d);
        assertEquals(0.0d, qualityMetric.detectorErrorRate(), 0.0001d);
        assertNull(qualityMetric.missingConfidenceCount());
        assertNull(qualityMetric.missingConfidenceRate());
        assertNull(qualityMetric.confidenceCoverageRate());
        assertEquals(0L, qualityMetric.confidenceCount());
        assertEquals(2_000L, qualityMetric.e2eLatestEventToEmitMs());
        assertEquals(500L, qualityMetric.e2eWindowEndToEmitMs());
    }
}
