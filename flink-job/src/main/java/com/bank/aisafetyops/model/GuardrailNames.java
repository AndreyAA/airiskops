package com.bank.aisafetyops.model;

import java.util.Set;

public final class GuardrailNames {
    public static final String PROMPT_INJECTION = "PROMPT_INJECTION";
    public static final String TOXICITY = "TOXICITY";
    public static final String LOOPING = "LOOPING";
    public static final String SYSTEM_PROMPT_LEAKAGE = "SYSTEM_PROMPT_LEAKAGE";

    private static final Set<String> CONFIDENCE_BASED_GUARDRAILS = Set.of(
            PROMPT_INJECTION,
            TOXICITY
    );

    private GuardrailNames() {
    }

    public static boolean requiresConfidence(String guardrailName) {
        return guardrailName != null && CONFIDENCE_BASED_GUARDRAILS.contains(guardrailName);
    }
}
