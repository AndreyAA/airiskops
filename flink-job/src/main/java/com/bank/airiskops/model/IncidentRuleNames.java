package com.bank.airiskops.model;

/**
 * Canonical incident rule names emitted by the first session correlation step.
 */
public final class IncidentRuleNames {
    public static final String PROMPT_INJECTION_BURST = "PROMPT_INJECTION_BURST";
    public static final String TOXICITY_CAMPAIGN = "TOXICITY_CAMPAIGN";
    public static final String LEAKAGE_WITH_INJECTION = "LEAKAGE_WITH_INJECTION";
    public static final String LOOPING_PERSISTENCE = "LOOPING_PERSISTENCE";

    private IncidentRuleNames() {
    }
}
