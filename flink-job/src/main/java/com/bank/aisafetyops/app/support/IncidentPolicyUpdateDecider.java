package com.bank.aisafetyops.app.support;

import com.bank.aisafetyops.model.IncidentPolicy;
import java.time.Instant;

/**
 * Decides whether a runtime policy update should replace the active policy.
 *
 * <p>Increment 3.2b uses {@code updatedAt} as the primary ordering signal and
 * rejects obviously older updates when configured to do so.
 */
public final class IncidentPolicyUpdateDecider {
    private IncidentPolicyUpdateDecider() {
    }

    public static PolicyUpdateDecision decide(
            IncidentPolicy currentPolicy,
            IncidentPolicy candidatePolicy,
            boolean rejectOlderVersions
    ) {
        if (candidatePolicy == null) {
            return PolicyUpdateDecision.reject("null-candidate");
        }
        if (currentPolicy == null) {
            return PolicyUpdateDecision.accept();
        }
        if (!rejectOlderVersions) {
            return PolicyUpdateDecision.accept();
        }
        Instant currentUpdatedAt = parseInstant(currentPolicy.updatedAt());
        Instant candidateUpdatedAt = parseInstant(candidatePolicy.updatedAt());
        if (currentUpdatedAt != null && candidateUpdatedAt != null && candidateUpdatedAt.isBefore(currentUpdatedAt)) {
            return PolicyUpdateDecision.reject("older-updatedAt");
        }
        if (currentUpdatedAt == null && candidateUpdatedAt == null
                && candidatePolicy.version() != null
                && currentPolicy.version() != null
                && candidatePolicy.version().compareTo(currentPolicy.version()) < 0) {
            return PolicyUpdateDecision.reject("older-version");
        }
        return PolicyUpdateDecision.accept();
    }

    private static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Result of policy update validation.
     */
    public record PolicyUpdateDecision(
            boolean accepted,
            String rejectionReason
    ) {
        public static PolicyUpdateDecision accept() {
            return new PolicyUpdateDecision(true, null);
        }

        public static PolicyUpdateDecision reject(String rejectionReason) {
            return new PolicyUpdateDecision(false, rejectionReason);
        }
    }
}
