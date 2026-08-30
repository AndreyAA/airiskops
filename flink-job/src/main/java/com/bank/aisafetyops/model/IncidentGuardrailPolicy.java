package com.bank.aisafetyops.model;

/**
 * Policy thresholds for one guardrail family.
 */
public record IncidentGuardrailPolicy(
        Double medium,
        Double high,
        Double critical,
        IncidentSeverity severity
) {
}
