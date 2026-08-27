package com.bank.aisafetyops.model;

public record LateEvent(
        String reason,
        SafetyEvent event,
        long currentWatermark
) {
}
