package com.bank.aisafetyops.app.config;

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
        int loopingMinOccurrences
) {
}
