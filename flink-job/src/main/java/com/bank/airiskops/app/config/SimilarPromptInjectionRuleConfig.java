package com.bank.airiskops.app.config;

import com.bank.airiskops.model.IncidentSeverity;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Runtime limits and matching policy for cross-session prompt-injection campaigns.
 */
public record SimilarPromptInjectionRuleConfig(
        boolean enabled, Duration window, double similarityThreshold, int minUniqueRequests,
        int minDistinctSessions, IncidentSeverity severity, String embeddingModelVersion,
        String embeddingInputPrefix, int maxEvidenceLength, int maxClustersPerKey,
        int maxTrackedRequestIdsPerCluster, int maxRequestIdsPerIncident, int maxSessionIdsPerCluster,
        int maxEvidenceSamplesPerCluster, int maxStoredEvidenceSampleLength
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
