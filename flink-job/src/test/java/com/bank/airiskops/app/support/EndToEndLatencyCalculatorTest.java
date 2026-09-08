package com.bank.airiskops.app.support;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class EndToEndLatencyCalculatorTest {
    @Test
    void shouldCalculateEventToEmitLatency() {
        assertEquals(250L, EndToEndLatencyCalculator.eventToEmitMillis(1_000L, 1_250L));
    }

    @Test
    void shouldClampNegativeEventToEmitLatencyToZero() {
        assertEquals(0L, EndToEndLatencyCalculator.eventToEmitMillis(2_000L, 1_500L));
    }

    @Test
    void shouldCalculateWindowEndToEmitLatency() {
        assertEquals(400L, EndToEndLatencyCalculator.windowEndToEmitMillis(5_000L, 5_400L));
    }

    @Test
    void shouldClampNegativeWindowEndToEmitLatencyToZero() {
        assertEquals(0L, EndToEndLatencyCalculator.windowEndToEmitMillis(6_000L, 5_500L));
    }
}
