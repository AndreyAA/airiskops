package com.bank.airiskops.app.support;

/**
 * Calculates non-negative end-to-end latency values for emitted AIRiskOps outputs.
 *
 * <p>The local MVP currently approximates end-to-end latency as the difference
 * between the current processing time at emission and the relevant domain-time
 * milestone carried by the event or derived output contract.
 */
public final class EndToEndLatencyCalculator {
    private EndToEndLatencyCalculator() {
    }

    public static long eventToEmitMillis(long eventTimeMillis, long emittedAtProcessingTimeMillis) {
        return Math.max(0L, emittedAtProcessingTimeMillis - eventTimeMillis);
    }

    public static long windowEndToEmitMillis(long windowEndMillis, long emittedAtProcessingTimeMillis) {
        return Math.max(0L, emittedAtProcessingTimeMillis - windowEndMillis);
    }
}
