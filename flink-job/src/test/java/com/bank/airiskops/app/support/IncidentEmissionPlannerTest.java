package com.bank.airiskops.app.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.app.config.IncidentConfig;
import com.bank.airiskops.app.config.PiAndToxicRuleConfig;
import com.bank.airiskops.model.AgentIncidentPolicyOverride;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.IncidentGuardrailPolicy;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentPolicyDefaults;
import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.IncidentRuleNames;
import com.bank.airiskops.model.IncidentSeverity;
import com.bank.airiskops.model.SafetyEvent;
import com.bank.airiskops.model.SessionRiskSnapshot;
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
            2,
            new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, null, null)
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

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.70d, BASE_TIME + 1_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        snapshot.recordFinding(buildFinding("req-002", "session-001", GuardrailNames.PROMPT_INJECTION, 0.92d, BASE_TIME + 2_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        SafetyEvent triggeringEvent = buildFinding("req-003", "session-001", GuardrailNames.PROMPT_INJECTION, 0.88d, BASE_TIME + 3_000L);
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

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

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.95d, BASE_TIME + 1_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.SYSTEM_PROMPT_LEAKAGE, null, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals(IncidentRuleNames.LEAKAGE_WITH_INJECTION, emissions.get(0).ruleName());
        assertEquals(IncidentSeverity.CRITICAL, emissions.get(0).severity());
    }

    @Test
    void plannerDoesNotRepeatEmissionWhenUpdatesAreDisabled() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 1_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        SafetyEvent secondEvent = buildFinding("req-002", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 2_000L);
        snapshot.recordFinding(secondEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> firstEmission = planner.plan(snapshot, secondEvent);
        assertEquals(1, firstEmission.size());

        SafetyEvent thirdEvent = buildFinding("req-003", "session-001", GuardrailNames.LOOPING, null, BASE_TIME + 3_000L);
        snapshot.recordFinding(thirdEvent, 10, INCIDENT_CONFIG.piAndToxic().window());
        List<IncidentEmissionPlanner.PlannedIncidentEmission> repeatedEmission = planner.plan(snapshot, thirdEvent);

        assertTrue(repeatedEmission.isEmpty());
    }

    @Test
    void fallsBackToConfigPolicyWhenBootstrapPolicyIsMissing() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, null);

        snapshot.recordFinding(buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.91d, BASE_TIME + 1_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        snapshot.recordFinding(buildFinding("req-002", "session-001", GuardrailNames.PROMPT_INJECTION, 0.92d, BASE_TIME + 2_000L), 10, INCIDENT_CONFIG.piAndToxic().window());
        SafetyEvent triggeringEvent = buildFinding("req-003", "session-001", GuardrailNames.PROMPT_INJECTION, 0.93d, BASE_TIME + 3_000L);
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals("config-fallback", emissions.get(0).appliedPolicyVersion());
    }

    @Test
    void plansPiAndToxicIncidentInsideConfiguredWindow() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME + 1_000L),
                10,
                INCIDENT_CONFIG.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.TOXICITY, 0.91d, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.size());
        assertEquals(IncidentRuleNames.PI_AND_TOXIC, emissions.get(0).ruleName());
        assertEquals(IncidentSeverity.HIGH, emissions.get(0).severity());
        assertTrue(emissions.get(0).summary().contains("promptInjectionFindings=1"));
        assertTrue(emissions.get(0).summary().contains("toxicityFindings=1"));
        assertTrue(emissions.get(0).summary().contains("maxPromptInjectionConfidence=0.820"));
        assertTrue(emissions.get(0).summary().contains("maxToxicityConfidence=0.910"));
    }

    @Test
    void doesNotPlanPiAndToxicWhenFindingsFallOutsideWindow() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME + 1_000L),
                10,
                INCIDENT_CONFIG.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding(
                "req-002",
                "session-001",
                GuardrailNames.TOXICITY,
                0.91d,
                BASE_TIME + INCIDENT_CONFIG.piAndToxic().window().toMillis() + 1_001L
        );
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(0, emissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
    }

    @Test
    void doesNotPlanPiAndToxicWhenConfidenceThresholdRejectsPromptInjection() {
        IncidentConfig configWithThreshold = new IncidentConfig(
                true,
                "basic-incidents",
                false,
                Duration.ofMinutes(30),
                10,
                3,
                3,
                2,
                new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, 0.90d, null)
        );
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(configWithThreshold, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME + 1_000L),
                10,
                configWithThreshold.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.TOXICITY, 0.91d, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10, configWithThreshold.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(0, emissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
    }

    @Test
    void countsBoundaryEventInsidePiAndToxicWindow() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME),
                10,
                INCIDENT_CONFIG.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding(
                "req-002",
                "session-001",
                GuardrailNames.TOXICITY,
                0.91d,
                BASE_TIME + INCIDENT_CONFIG.piAndToxic().window().toMillis()
        );
        snapshot.recordFinding(triggeringEvent, 10, INCIDENT_CONFIG.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(1, emissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
    }

    @Test
    void doesNotPlanPiAndToxicWhenTriggeredCountThresholdIsNotMet() {
        IncidentConfig stricterConfig = new IncidentConfig(
                true,
                "basic-incidents",
                false,
                Duration.ofMinutes(30),
                10,
                3,
                3,
                2,
                new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 2, 1, null, null)
        );
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(stricterConfig, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME + 1_000L),
                10,
                stricterConfig.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.TOXICITY, 0.91d, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10, stricterConfig.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(0, emissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
    }

    @Test
    void doesNotPlanPiAndToxicWhenPromptInjectionConfidenceIsMissingAndThresholdIsConfigured() {
        IncidentConfig configWithThreshold = new IncidentConfig(
                true,
                "basic-incidents",
                false,
                Duration.ofMinutes(30),
                10,
                3,
                3,
                2,
                new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, 0.80d, null)
        );
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(configWithThreshold, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, null, BASE_TIME + 1_000L),
                10,
                configWithThreshold.piAndToxic().window()
        );
        SafetyEvent triggeringEvent = buildFinding("req-002", "session-001", GuardrailNames.TOXICITY, 0.91d, BASE_TIME + 2_000L);
        snapshot.recordFinding(triggeringEvent, 10, configWithThreshold.piAndToxic().window());

        List<IncidentEmissionPlanner.PlannedIncidentEmission> emissions = planner.plan(snapshot, triggeringEvent);

        assertEquals(0, emissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
    }

    @Test
    void doesNotRepeatPiAndToxicEmissionWhenUpdatesAreDisabled() {
        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(INCIDENT_CONFIG, POLICY);

        snapshot.recordFinding(
                buildFinding("req-001", "session-001", GuardrailNames.PROMPT_INJECTION, 0.82d, BASE_TIME + 1_000L),
                10,
                INCIDENT_CONFIG.piAndToxic().window()
        );
        SafetyEvent firstToxicityEvent = buildFinding("req-002", "session-001", GuardrailNames.TOXICITY, 0.91d, BASE_TIME + 2_000L);
        snapshot.recordFinding(firstToxicityEvent, 10, INCIDENT_CONFIG.piAndToxic().window());
        assertEquals(1, planner.plan(snapshot, firstToxicityEvent).stream()
                .filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName()))
                .count());

        SafetyEvent secondToxicityEvent = buildFinding("req-003", "session-001", GuardrailNames.TOXICITY, 0.95d, BASE_TIME + 3_000L);
        snapshot.recordFinding(secondToxicityEvent, 10, INCIDENT_CONFIG.piAndToxic().window());
        List<IncidentEmissionPlanner.PlannedIncidentEmission> repeatedEmissions = planner.plan(snapshot, secondToxicityEvent);

        assertEquals(0, repeatedEmissions.stream().filter(emission -> IncidentRuleNames.PI_AND_TOXIC.equals(emission.ruleName())).count());
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
