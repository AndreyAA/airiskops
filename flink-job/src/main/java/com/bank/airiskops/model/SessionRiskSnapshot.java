package com.bank.airiskops.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.time.Duration;

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
    private boolean piAndToxicEmitted;
    private int piAndToxicRevision;
    private final List<WindowFindingEvidence> promptInjectionWindowEvidence = new ArrayList<>();
    private final List<WindowFindingEvidence> toxicityWindowEvidence = new ArrayList<>();

    public void recordFinding(SafetyEvent event, int maxRequestIdsPerIncident, Duration piAndToxicWindow) {
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
            promptInjectionWindowEvidence.add(new WindowFindingEvidence(event.eventTimeMillis(), event.confidence()));
            pruneWindowEvidence(piAndToxicWindow);
            return;
        }
        if (GuardrailNames.TOXICITY.equals(event.guardrailName())) {
            toxicityCount++;
            maxToxicityConfidence = Math.max(maxToxicityConfidence, safeConfidence(event.confidence()));
            toxicityWindowEvidence.add(new WindowFindingEvidence(event.eventTimeMillis(), event.confidence()));
            pruneWindowEvidence(piAndToxicWindow);
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

    public boolean piAndToxicEmitted() {
        return piAndToxicEmitted;
    }

    public int incrementPiAndToxicRevision() {
        piAndToxicEmitted = true;
        return ++piAndToxicRevision;
    }

    public PiAndToxicWindowStats piAndToxicWindowStats(
            long referenceTimeMillis,
            Duration window,
            Double minPromptInjectionConfidence,
            Double minToxicityConfidence
    ) {
        return new PiAndToxicWindowStats(
                countQualified(promptInjectionWindowEvidence, referenceTimeMillis, window, minPromptInjectionConfidence),
                maxQualifiedConfidence(promptInjectionWindowEvidence, referenceTimeMillis, window, minPromptInjectionConfidence),
                countQualified(toxicityWindowEvidence, referenceTimeMillis, window, minToxicityConfidence),
                maxQualifiedConfidence(toxicityWindowEvidence, referenceTimeMillis, window, minToxicityConfidence)
        );
    }

    private static double safeConfidence(Double confidence) {
        return confidence == null ? NO_CONFIDENCE : confidence;
    }

    private void pruneWindowEvidence(Duration piAndToxicWindow) {
        long minimumEventTimeMillis = lastEventTimeMillis - piAndToxicWindow.toMillis();
        pruneBefore(promptInjectionWindowEvidence, minimumEventTimeMillis);
        pruneBefore(toxicityWindowEvidence, minimumEventTimeMillis);
    }

    private static void pruneBefore(List<WindowFindingEvidence> evidence, long minimumEventTimeMillis) {
        Iterator<WindowFindingEvidence> iterator = evidence.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().eventTimeMillis() < minimumEventTimeMillis) {
                iterator.remove();
            }
        }
    }

    private static int countQualified(
            List<WindowFindingEvidence> evidence,
            long referenceTimeMillis,
            Duration window,
            Double minConfidence
    ) {
        int qualifiedCount = 0;
        for (WindowFindingEvidence finding : evidence) {
            if (isInsideWindow(finding.eventTimeMillis(), referenceTimeMillis, window)
                    && passesConfidenceThreshold(finding.confidence(), minConfidence)) {
                qualifiedCount++;
            }
        }
        return qualifiedCount;
    }

    private static double maxQualifiedConfidence(
            List<WindowFindingEvidence> evidence,
            long referenceTimeMillis,
            Duration window,
            Double minConfidence
    ) {
        double maxConfidence = NO_CONFIDENCE;
        for (WindowFindingEvidence finding : evidence) {
            if (isInsideWindow(finding.eventTimeMillis(), referenceTimeMillis, window)
                    && passesConfidenceThreshold(finding.confidence(), minConfidence)) {
                maxConfidence = Math.max(maxConfidence, safeConfidence(finding.confidence()));
            }
        }
        return maxConfidence;
    }

    private static boolean isInsideWindow(long eventTimeMillis, long referenceTimeMillis, Duration window) {
        return eventTimeMillis >= referenceTimeMillis - window.toMillis() && eventTimeMillis <= referenceTimeMillis;
    }

    private static boolean passesConfidenceThreshold(Double confidence, Double minConfidence) {
        if (minConfidence == null) {
            return true;
        }
        return confidence != null && confidence >= minConfidence;
    }

    private static void appendDistinct(List<String> values, String nextValue, int maxSize) {
        if (nextValue == null || nextValue.isBlank() || values.contains(nextValue) || values.size() >= maxSize) {
            return;
        }
        values.add(nextValue);
    }

    /**
     * Qualified PI_AND_TOXIC evidence inside the active event-time window.
     */
    public record PiAndToxicWindowStats(
            int promptInjectionCount,
            double maxPromptInjectionConfidence,
            int toxicityCount,
            double maxToxicityConfidence
    ) implements Serializable {
    }

    /**
     * Flink state stores this type through generic Kryo serialization, so it must stay
     * a regular Serializable class rather than a Java record.
     */
    private static final class WindowFindingEvidence implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long eventTimeMillis;
        private final Double confidence;

        private WindowFindingEvidence(long eventTimeMillis, Double confidence) {
            this.eventTimeMillis = eventTimeMillis;
            this.confidence = confidence;
        }

        private long eventTimeMillis() {
            return eventTimeMillis;
        }

        private Double confidence() {
            return confidence;
        }
    }
}
