package com.bank.aisafetyops.model;

/**
 * Side-output record describing why a raw payload failed validation.
 */
public record InvalidEvent(
        String reason,
        String rawPayload
) {
}
