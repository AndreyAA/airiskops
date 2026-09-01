package com.bank.airiskops.app.config;

import com.bank.airiskops.model.IncidentSeverity;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Runtime configuration for the PI_AND_TOXIC session incident rule.
 *
 * <p>The rule correlates prompt-injection and toxicity findings inside a
 * bounded event-time window and keeps all thresholds explicit in config so the
 * MVP can be tuned without changing code.
 */
public record PiAndToxicRuleConfig(
        boolean enabled,
        Duration window,
        IncidentSeverity severity,
        int minPromptInjectionTriggeredCount,
        int minToxicityTriggeredCount,
        Double minPromptInjectionConfidence,
        Double minToxicityConfidence
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
