package com.bank.airiskops.model;

import java.util.List;

/**
 * Canonical incident rule names emitted by the first session correlation step.
 */
public final class IncidentRuleNames {
    public static final String PROMPT_INJECTION_BURST = "PROMPT_INJECTION_BURST";
    public static final String TOXICITY_CAMPAIGN = "TOXICITY_CAMPAIGN";
    public static final String LEAKAGE_WITH_INJECTION = "LEAKAGE_WITH_INJECTION";
    public static final String LOOPING_PERSISTENCE = "LOOPING_PERSISTENCE";
    public static final String PI_AND_TOXIC = "PI_AND_TOXIC";
    public static final String SIMILAR_PROMPT_INJECTION_CAMPAIGN = "SIMILAR_PROMPT_INJECTION_CAMPAIGN";

    private static final List<String> SESSION_RULES = List.of(
            PROMPT_INJECTION_BURST,
            TOXICITY_CAMPAIGN,
            LEAKAGE_WITH_INJECTION,
            LOOPING_PERSISTENCE,
            PI_AND_TOXIC
    );

    /**
     * Returns the bounded set of rules emitted by the session incident evaluator.
     *
     * <p>Metrics for these rules are registered when the operator opens so a short replay cannot
     * finish before Prometheus observes the counter's zero baseline.
     */
    public static List<String> sessionRules() {
        return SESSION_RULES;
    }

    private IncidentRuleNames() {
    }
}
