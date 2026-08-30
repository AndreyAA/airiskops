package com.bank.aisafetyops.app.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.aisafetyops.app.config.IncidentConfig;
import com.bank.aisafetyops.model.AgentIncidentPolicyOverride;
import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.IncidentGuardrailPolicy;
import com.bank.aisafetyops.model.IncidentPolicy;
import com.bank.aisafetyops.model.IncidentPolicyDefaults;
import com.bank.aisafetyops.model.GuardrailNames;
import com.bank.aisafetyops.model.IncidentRuleNames;
import com.bank.aisafetyops.model.IncidentSeverity;
import com.bank.aisafetyops.model.SafetyEvent;
import com.bank.aisafetyops.model.SessionRiskSnapshot;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IncidentEmissionPlannerTest {
    private static final String TENANT_ID = "agent-risk-01";
    private static final String AGENT_ID = "agent-risk-01";
    private static final long BASE_TIME = 1_788_134_400_000L;
    private static final IncidentConfig INCIDENT_CONFIG = new IncidentConfig(
            true,
            "basic-incidents",
            false,
            Duration.ofMinutes(30),
            10,
            3,
            3,
            2
    );
    private static final IncidentPolicy POLICY = new IncidentPolicy(
            "policy-v2",
            "test",
            "2026-08-30T00:00:00Z",
            new IncidentPolicyDefaults(
                    new IncidentGuardrailPolicy(0.55d, 0.75d, 0.90d, null),
                    new IncidentGuardrailPolicy(0.70d, 0.90d, 0.95d, null),
                    new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.MEDIUM),
                    new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.CRITICAL)
            ),
            Map.of(
                    AGENT_ID,
                    new AgentIncidentPolicyOverride(
                            new IncidentGuardrailPolicy(null, 0.72d, 0.88d, null),
                            null,
                            null,
                            null
                    )
            )
    );

    @Test
    void plansPromptInjectionIncidentWhenBurstThresholdIsReached() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.70d, BASE_TIME + 1_000L), 10);
        snapshot.recordFinding(buildFinding("req-002", "session-001", GuardrailNames.PROMPT_INJECTION, 0.92d, BASE_TIME + 2_000L), 10);
        SafetyEvent triggeringEvent = buildFinding("req-003", "session-001", GuardrailNames.PROMPT_INJECTION, 0.88d, BASE_TIME + 3_000L);
        snapshot.recordFinding(triggeringEvent, 10);

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals(IncidentRuleNames.PROMPT_INJECTION_BURST, emissions.get(0).ruleName());
        assertEquals(IncidentSeverity.HIGH, emissions.get(0).severity());
        assertEquals(1, emissions.get(0).revision());
        assertEquals("policy-v2", emissions.get(0).appliedPolicyVersion());
    }

    @Test
    void plansCriticalLeakageIncidentWhenLeakageFollowsHighConfidenceInjection() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.95d, BASE_TIME + 1_000L), 10);
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.SYSTEM_PROMPT_LEAKAGE, null, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10);

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals(IncidentRuleNames.LEAKAGE_WITH_INJECTION, emissions.get(0).ruleName());
        assertEquals(IncidentSeverity.CRITICAL, emissions.get(0).severity());
    }

    @Test
    void plannerDoesNotRepeatEmissionWhenUpdatesAreDisabled() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 1_000L), 10);
        SafetyEvent secondEvent = buildFinding("req-002", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 2_000L);
        snapshot.recordFinding(secondEvent, 10);

        List<IncidentEmissionPlanner.PlannedIncidentEmission> firstEmission = planner.plan(snapshot, secondEvent);
        assertEquals(1, firstEmission.size());

        SafetyEvent thirdEvent = buildFinding("req-003", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 3_000L);
        snapshot.recordFinding(thirdEvent, 10);
        List<IncidentEmissionPlanner.PlannedIncidentEmission> repeatedEmission = planner.plan(snapshot, thirdEvent);

        assertTrue(repeatedEmission.isEmpty());
    }

    @Test
    void fallsBackToConfigPolicyWhenBootstrapPolicyIsMissing() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, null);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.91d, BASE_TIME + 1_000L), 10);
        snapshot.recordFinding(buildFinding("req-002", "session-001", GuardrailNames.PROMPT_INJECTION, 0.92d, BASE_TIME + 2_000L), 10);
        SafetyEvent triggeringEvent = buildFinding("req-003", "session-001", GuardrailNames.PROMPT_INJECTION, 0.93d, BASE_TIME + 3_000L);
        snapshot.recordFinding(triggeringEvent, 10);

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals("config-fallback", emissions.get(0).appliedPolicyVersion());
    }

    private static SafetyEvent buildFinding(
            String requestId,
            String sessionId,
            String guardrailName,
            Double confidence,
            long eventTime
    ) {
        return new SafetyEvent(
                TENANT_ID,
                "local",
                AGENT_ID,
                sessionId,
                requestId,
                "turn-" + requestId,
                eventTime,
                EventType.GUARDRAIL_FINDING,
                "gpt-4.1-mini",
                "user-001",
                "web",
                100,
                50,
                guardrailName,
                "guard-v1",
                "policy-v1",
                confidence,
                true,
                10L,
                "OK",
                "{}"
        );
    }
}
