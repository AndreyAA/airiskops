package com.bank.airiskops.model;

/**
 * Session-level correlation key for the first incident layer.
 *
 * <p>The current agreement is to correlate operational patterns by
 * {@code agentId + sessionId}. {@code requestId} remains part of drill-down,
 * but not of the primary keyed state boundary.
 */
public record SessionIncidentKey(
        String agentId,
        String sessionId
) {
}
