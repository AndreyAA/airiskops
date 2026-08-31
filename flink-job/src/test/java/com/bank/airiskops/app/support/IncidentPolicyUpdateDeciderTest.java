package com.bank.airiskops.app.support;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.model.IncidentGuardrailPolicy;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentPolicyDefaults;
import com.bank.airiskops.model.IncidentSeverity;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IncidentPolicyUpdateDeciderTest {
    @Test
    void rejectsOlderPolicyWhenConfigured() {
        IncidentPolicy current = policy("policy-v2", "2026-08-30T10:00:00Z");
        IncidentPolicy candidate = policy("policy-v1", "2026-08-30T09:59:00Z");

        var decision = IncidentPolicyUpdateDecider.decide(current, candidate, true);

        assertFalse(decision.accepted());
        assertTrue(decision.rejectionReason().contains("older"));
    }

    @Test
    void acceptsOlderPolicyWhenRejectionIsDisabled() {
        IncidentPolicy current = policy("policy-v2", "2026-08-30T10:00:00Z");
        IncidentPolicy candidate = policy("policy-v1", "2026-08-30T09:59:00Z");

        var decision = IncidentPolicyUpdateDecider.decide(current, candidate, false);

        assertTrue(decision.accepted());
    }

    private static IncidentPolicy policy(String version, String updatedAt) {
        return new IncidentPolicy(
                version,
                "test",
                updatedAt,
                new IncidentPolicyDefaults(
                        new IncidentGuardrailPolicy(0.55d, 0.75d, 0.90d, null),
                        new IncidentGuardrailPolicy(0.70d, 0.90d, 0.95d, null),
                        new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.MEDIUM),
                        new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.CRITICAL)
                ),
                Map.of()
        );
    }
}
