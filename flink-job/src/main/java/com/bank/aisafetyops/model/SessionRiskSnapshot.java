package com.bank.aisafetyops.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable keyed-state snapshot used by the minimal incident evaluator.
 *
 * <p>The snapshot stores only the session-level evidence required for the
 * first incident MVP: compact counters, highest confidences, bounded request
 * drill-down, and rule emission bookkeeping.
 */
public final class SessionRiskSnapshot implements Serializable {
    private static final double NO_CONFIDENCE = -1.0d;

    private String tenantId;
    private String agentId;
    private String sessionId;
    private long firstEventTimeMillis;
    private long lastEventTimeMillis;
    private long cleanupDeadlineMillis;
    private long triggeredFindingsCount;
    private int promptInjectionCount;
    private int toxicityCount;
    private int loopingCount;
    private int systemPromptLeakageCount;
    private double maxPromptInjectionConfidence = NO_CONFIDENCE;
    private double maxToxicityConfidence = NO_CONFIDENCE;
    private final List<String> requestIds = new ArrayList<>();
    private final List<String> guardrailNames = new ArrayList<>();
    private final List<String> guardrailVersions = new ArrayList<>();
    private final List<String> policyVersions = new ArrayList<>();
    private boolean promptInjectionBurstEmitted;
    private boolean toxicityCampaignEmitted;
    private boolean leakageWithInjectionEmitted;
    private boolean loopingPersistenceEmitted;
    private int promptInjectionBurstRevision;
    private int toxicityCampaignRevision;
    private int leakageWithInjectionRevision;
    private int loopingPersistenceRevision;

    public void recordFinding(SafetyEvent event, int maxRequestIdsPerIncident) {
        tenantId = event.tenantId();
        agentId = event.agentId();
        sessionId = event.sessionId();
        if (firstEventTimeMillis == 0L) {
            firstEventTimeMillis = event.eventTimeMillis();
        }
        lastEventTimeMillis = Math.max(lastEventTimeMillis, event.eventTimeMillis());
        triggeredFindingsCount++;
        appendDistinct(requestIds, event.requestId(), maxRequestIdsPerIncident);
        appendDistinct(guardrailNames, event.guardrailName(), Integer.MAX_VALUE);
        appendDistinct(guardrailVersions, event.guardrailVersion(), Integer.MAX_VALUE);
        appendDistinct(policyVersions, event.policyVersion(), Integer.MAX_VALUE);

        if (GuardrailNames.PROMPT_INJECTION.equals(event.guardrailName())) {
            promptInjectionCount++;
            maxPromptInjectionConfidence = Math.max(maxPromptInjectionConfidence, safeConfidence(event.confidence()));
            return;
        }
        if (GuardrailNames.TOXICITY.equals(event.guardrailName())) {
            toxicityCount++;
            maxToxicityConfidence = Math.max(maxToxicityConfidence, safeConfidence(event.confidence()));
            return;
        }
        if (GuardrailNames.LOOPING.equals(event.guardrailName())) {
            loopingCount++;
            return;
        }
        if (GuardrailNames.SYSTEM_PROMPT_LEAKAGE.equals(event.guardrailName())) {
            systemPromptLeakageCount++;
        }
    }

    public String tenantId() {
        return tenantId;
    }

    public String agentId() {
        return agentId;
    }

    public String sessionId() {
        return sessionId;
    }

    public long firstEventTimeMillis() {
        return firstEventTimeMillis;
    }

    public long lastEventTimeMillis() {
        return lastEventTimeMillis;
    }

    public long cleanupDeadlineMillis() {
        return cleanupDeadlineMillis;
    }

    public void setCleanupDeadlineMillis(long cleanupDeadlineMillis) {
        this.cleanupDeadlineMillis = cleanupDeadlineMillis;
    }

    public long triggeredFindingsCount() {
        return triggeredFindingsCount;
    }

    public int promptInjectionCount() {
        return promptInjectionCount;
    }

    public int toxicityCount() {
        return toxicityCount;
    }

    public int loopingCount() {
        return loopingCount;
    }

    public int systemPromptLeakageCount() {
        return systemPromptLeakageCount;
    }

    public double maxPromptInjectionConfidence() {
        return maxPromptInjectionConfidence;
    }

    public double maxToxicityConfidence() {
        return maxToxicityConfidence;
    }

    public List<String> requestIds() {
        return new ArrayList<>(requestIds);
    }

    public List<String> guardrailNames() {
        return new ArrayList<>(guardrailNames);
    }

    public List<String> guardrailVersions() {
        return new ArrayList<>(guardrailVersions);
    }

    public List<String> policyVersions() {
        return new ArrayList<>(policyVersions);
    }

    public boolean promptInjectionBurstEmitted() {
        return promptInjectionBurstEmitted;
    }

    public int incrementPromptInjectionBurstRevision() {
        promptInjectionBurstEmitted = true;
        return ++promptInjectionBurstRevision;
    }

    public boolean toxicityCampaignEmitted() {
        return toxicityCampaignEmitted;
    }

    public int incrementToxicityCampaignRevision() {
        toxicityCampaignEmitted = true;
        return ++toxicityCampaignRevision;
    }

    public boolean leakageWithInjectionEmitted() {
        return leakageWithInjectionEmitted;
    }

    public int incrementLeakageWithInjectionRevision() {
        leakageWithInjectionEmitted = true;
        return ++leakageWithInjectionRevision;
    }

    public boolean loopingPersistenceEmitted() {
        return loopingPersistenceEmitted;
    }

    public int incrementLoopingPersistenceRevision() {
        loopingPersistenceEmitted = true;
        return ++loopingPersistenceRevision;
    }

    private static double safeConfidence(Double confidence) {
        return confidence == null ? NO_CONFIDENCE : confidence;
    }

    private static void appendDistinct(List<String> values, String nextValue, int maxSize) {
        if (nextValue == null || nextValue.isBlank() || values.contains(nextValue) || values.size() >= maxSize) {
            return;
        }
        values.add(nextValue);
    }
}
