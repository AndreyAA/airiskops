package com.bank.aisafetyops.model;

/**
 * Side-output record for events that missed the current watermark tolerance.
 */
public record LateEvent(
        String reason,
        SafetyEvent event,
        long currentWatermark
) {
}
