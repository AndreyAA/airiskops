package com.bank.aisafetyops.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Policy thresholds for one guardrail family.
 */
public record IncidentGuardrailPolicy(
        Double medium,
        Double high,
        Double critical,
        IncidentSeverity severity
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
