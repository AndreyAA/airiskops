package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.app.config.IncidentConfig;
import com.bank.airiskops.app.config.PiAndToxicRuleConfig;
import com.bank.airiskops.app.support.IncidentEmissionPlanner;
import com.bank.airiskops.model.AgentIncidentPolicyOverride;
import com.bank.airiskops.model.BasicIncident;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.IncidentGuardrailPolicy;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentPolicyDefaults;
import com.bank.airiskops.model.IncidentRuleNames;
import com.bank.airiskops.model.IncidentSeverity;
import com.bank.airiskops.model.SafetyEvent;
import com.bank.airiskops.model.SessionIncidentKey;
import com.bank.airiskops.model.SessionRiskSnapshot;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class PolicyAwareSessionIncidentEvaluatorFunctionTest {
    private static final long BASE_TIME = 1_788_480_000_000L;
    private static final MapStateDescriptor<String, IncidentPolicy> POLICY_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("policy-state", TypeInformation.of(String.class), TypeInformation.of(IncidentPolicy.class));

    @Test
    void acceptsNewerBroadcastPolicyAndRejectsOlderOne() throws Exception {
        PolicyAwareSessionIncidentEvaluatorFunction function = new PolicyAwareSessionIncidentEvaluatorFunction(
                config(3),
                true,
                policy("policy-v1", "2026-09-01T02:00:00Z", 0.95d),
                POLICY_STATE_DESCRIPTOR
        );

        try (KeyedBroadcastOperatorTestHarness<SessionIncidentKey, SafetyEvent, IncidentPolicy, BasicIncident> harness =
                     ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                             function,
                             event -> new SessionIncidentKey(event.agentId(), event.sessionId()),
                             TypeInformation.of(SessionIncidentKey.class),
                             POLICY_STATE_DESCRIPTOR
                     )) {
            harness.open();
            harness.processBroadcastElement(policy("policy-v2", "2026-09-01T03:00:00Z", 0.70d), BASE_TIME);
            assertEquals("policy-v2", harness.getBroadcastState(POLICY_STATE_DESCRIPTOR).get("active").version());

            harness.processElement(promptInjection("req-1", BASE_TIME + 1_000L, 0.71d), BASE_TIME + 1_000L);
            harness.processWatermark(BASE_TIME + 1_000L + config(3).sessionInactivityTimeout().toMillis());
            assertTrue(harness.extractOutputValues().isEmpty());

            harness.processBroadcastElement(policy("policy-v0", "2026-09-01T01:00:00Z", 0.60d), BASE_TIME + 2_000L);
            assertEquals("policy-v2", harness.getBroadcastState(POLICY_STATE_DESCRIPTOR).get("active").version());
        }
    }

    @Test
    void fallsBackToBootstrapPolicyWhenOlderUpdateIsRejectedBeforeAnyAcceptedUpdate() throws Exception {
        PolicyAwareSessionIncidentEvaluatorFunction function = new PolicyAwareSessionIncidentEvaluatorFunction(
                config(3),
                true,
                policy("policy-v1", "2026-09-01T02:00:00Z", 0.95d),
                POLICY_STATE_DESCRIPTOR
        );

        try (KeyedBroadcastOperatorTestHarness<SessionIncidentKey, SafetyEvent, IncidentPolicy, BasicIncident> harness =
                     ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
                             function,
                             event -> new SessionIncidentKey(event.agentId(), event.sessionId()),
                             TypeInformation.of(SessionIncidentKey.class),
                             POLICY_STATE_DESCRIPTOR
                     )) {
            harness.open();
            harness.processBroadcastElement(policy("policy-v0", "2026-09-01T01:00:00Z", 0.70d), BASE_TIME);
            harness.processElement(promptInjection("req-1", BASE_TIME + 1_000L, 0.71d), BASE_TIME + 1_000L);

            assertNull(harness.getBroadcastState(POLICY_STATE_DESCRIPTOR).get("active"));
            assertTrue(harness.extractOutputValues().isEmpty());
        }
    }

    @Test
    void emitIncidentBuildsOutputAndParseUpdatedAtHandlesEdgeCases() throws Exception {
        PolicyAwareSessionIncidentEvaluatorFunction function = new PolicyAwareSessionIncidentEvaluatorFunction(
                config(2),
                true,
                policy("policy-v1", "2026-09-01T02:00:00Z", 0.95d),
                POLICY_STATE_DESCRIPTOR
        );
        SimpleCounter emittedCounter = new SimpleCounter();
        SimpleCounter updatedCounter = new SimpleCounter();
        SimpleCounter ruleCounter = new SimpleCounter();
        SimpleCounter severityCounter = new SimpleCounter();
        HashMap<String, org.apache.flink.metrics.Counter> ruleCounters = new HashMap<>();
        HashMap<String, org.apache.flink.metrics.Counter> severityCounters = new HashMap<>();
        ruleCounters.put(IncidentRuleNames.PROMPT_INJECTION_BURST, ruleCounter);
        severityCounters.put(IncidentSeverity.HIGH.name(), severityCounter);

        setField(function, "emittedCounter", emittedCounter);
        setField(function, "updatedCounter", updatedCounter);
        setField(function, "ruleCounters", ruleCounters);
        setField(function, "severityCounters", severityCounters);
        setField(function, "airiskOpsMetricGroup", new UnregisteredMetricsGroup());

        SessionRiskSnapshot snapshot = new SessionRiskSnapshot();
        snapshot.recordFinding(promptInjection("req-1", BASE_TIME + 1_000L, 0.71d), 10, Duration.ofMinutes(5));
        snapshot.recordFinding(promptInjection("req-2", BASE_TIME + 2_000L, 0.92d), 10, Duration.ofMinutes(5));

        Method emitIncident = PolicyAwareSessionIncidentEvaluatorFunction.class.getDeclaredMethod(
                "emitIncident",
                Collector.class,
                SessionRiskSnapshot.class,
                SafetyEvent.class,
                IncidentEmissionPlanner.PlannedIncidentEmission.class
        );
        emitIncident.setAccessible(true);
        ListCollector collector = new ListCollector();
        emitIncident.invoke(
                function,
                collector,
                snapshot,
                promptInjection("req-2", BASE_TIME + 2_000L, 0.92d),
                new IncidentEmissionPlanner.PlannedIncidentEmission(
                        IncidentRuleNames.PROMPT_INJECTION_BURST,
                        IncidentSeverity.HIGH,
                        2,
                        true,
                        "policy-v2",
                        "Prompt injection burst detected"
                )
        );

        Method parseUpdatedAt = PolicyAwareSessionIncidentEvaluatorFunction.class.getDeclaredMethod(
                "parseUpdatedAt",
                IncidentPolicy.class
        );
        parseUpdatedAt.setAccessible(true);

        assertEquals(1, collector.values.size());
        assertEquals("policy-v2", collector.values.get(0).appliedPolicyVersion());
        assertEquals(1L, emittedCounter.getCount());
        assertEquals(1L, updatedCounter.getCount());
        assertEquals(1L, ruleCounter.getCount());
        assertEquals(1L, severityCounter.getCount());
        assertEquals(1_756_688_400_000L, parseUpdatedAt.invoke(null, policy("policy-v3", "2025-09-01T01:00:00Z", 0.8d)));
        assertEquals(0L, parseUpdatedAt.invoke(null, policy("policy-v4", "not-an-instant", 0.8d)));
        assertEquals(0L, parseUpdatedAt.invoke(null, new IncidentPolicy("policy-v5", "risk-eng", " ", null, null)));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static IncidentConfig config(int promptInjectionBurstMinFindings) {
        return new IncidentConfig(
                true,
                "basic-incidents",
                false,
                Duration.ofMinutes(30),
                10,
                promptInjectionBurstMinFindings,
                3,
                2,
                new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, null, null)
        );
    }

    private static IncidentPolicy policy(String version, String updatedAt, double highThreshold) {
        return new IncidentPolicy(
                version,
                "risk-eng",
                updatedAt,
                new IncidentPolicyDefaults(
                        new IncidentGuardrailPolicy(0.55d, highThreshold, 0.95d, null),
                        new IncidentGuardrailPolicy(0.70d, 0.90d, 0.95d, null),
                        new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.MEDIUM),
                        new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.CRITICAL)
                ),
                Map.of(
                        "agent-1",
                        new AgentIncidentPolicyOverride(null, null, null, null)
                )
        );
    }

    private static SafetyEvent promptInjection(String requestId, long eventTimeMillis, double confidence) {
        return new SafetyEvent(
                "tenant-1",
                "local",
                "agent-1",
                "session-1",
                requestId,
                "turn-" + requestId,
                eventTimeMillis,
                EventType.GUARDRAIL_FINDING,
                "gpt-4.1-mini",
                "user-1",
                "web",
                100,
                50,
                GuardrailNames.PROMPT_INJECTION,
                "pi-v1",
                "policy-v1",
                confidence,
                true,
                15L,
                "OK",
                "{}"
        );
    }

    private static final class ListCollector implements Collector<BasicIncident> {
        private final List<BasicIncident> values = new ArrayList<>();

        @Override
        public void collect(BasicIncident record) {
            values.add(record);
        }

        @Override
        public void close() {
        }
    }
}
