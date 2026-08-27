package com.bank.aisafetyops.model;

public record InvalidEvent(
        String reason,
        String rawPayload
) {
}
