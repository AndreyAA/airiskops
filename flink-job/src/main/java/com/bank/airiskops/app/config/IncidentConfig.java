package com.bank.airiskops.app.config;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Runtime configuration for the minimal incident layer.
 *
 * <p>The current implementation keeps only operationally meaningful knobs here:
 * feature enablement, session correlation lifetime, output routing, and the
 * first rule thresholds that risk engineers are likely to tune between local
 * and future bank environments.
 */
public record IncidentConfig(
        boolean enabled,
        String incidentsTopic,
        boolean emitUpdates,
        Duration sessionInactivityTimeout,
        int maxRequestIdsPerIncident,
        int promptInjectionBurstMinFindings,
        int toxicityCampaignMinFindings,
        int loopingMinOccurrences,
        PiAndToxicRuleConfig piAndToxic,
        SimilarPromptInjectionRuleConfig similarPromptInjection
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** Compatibility constructor retained for existing rule tests and integrations. */
    public IncidentConfig(boolean enabled, String incidentsTopic, boolean emitUpdates, Duration sessionInactivityTimeout,
                          int maxRequestIdsPerIncident, int promptInjectionBurstMinFindings,
                          int toxicityCampaignMinFindings, int loopingMinOccurrences, PiAndToxicRuleConfig piAndToxic) {
        this(enabled, incidentsTopic, emitUpdates, sessionInactivityTimeout, maxRequestIdsPerIncident,
                promptInjectionBurstMinFindings, toxicityCampaignMinFindings, loopingMinOccurrences, piAndToxic,
                new SimilarPromptInjectionRuleConfig(true, Duration.ofMinutes(5), .5, 3, 2,
                        com.bank.airiskops.model.IncidentSeverity.HIGH, "multilingual-e5-small-onnx-qint8",
                        "query: ", 1000, 20, 100, 50, 20, 5, 500));
    }
}
