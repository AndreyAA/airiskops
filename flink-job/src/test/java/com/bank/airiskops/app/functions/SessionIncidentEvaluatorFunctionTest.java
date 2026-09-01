package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class SessionIncidentEvaluatorFunctionTest {
    private static final long BASE_TIME = 1_788_393_600_000L;

    @Test
    void managesSessionStateAndCleanupTimerWithoutEmittingBelowThreshold() throws Exception {
        IncidentConfig config = config(false, 3);
        SessionIncidentEvaluatorFunction function = new SessionIncidentEvaluatorFunction(config, policy("policy-v1", 0.90d));

        try (KeyedOneInputStreamOperatorTestHarness<SessionIncidentKey, SafetyEvent, BasicIncident> harness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             function,
                             event -> new SessionIncidentKey(event.agentId(), event.sessionId()),
                             TypeInformation.of(SessionIncidentKey.class)
                     )) {
            harness.open();
            harness.processElement(promptInjection("req-1", BASE_TIME + 1_000L, 0.71d), BASE_TIME + 1_000L);
            harness.processElement(promptInjection("req-2", BASE_TIME + 2_000L, 0.72d), BASE_TIME + 2_000L);

            assertEquals(1, harness.numEventTimeTimers());
            assertEquals(1, harness.numKeyedStateEntries());
            assertTrue(harness.extractOutputValues().isEmpty());

            harness.processWatermark(BASE_TIME + 1_000L + config.sessionInactivityTimeout().toMillis());
            assertEquals(1, harness.numKeyedStateEntries());

            harness.processWatermark(BASE_TIME + 2_000L + config.sessionInactivityTimeout().toMillis());
            assertEquals(0, harness.numKeyedStateEntries());
        }
    }

    @Test
    void emitIncidentBuildsOutputAndIncrementsUpdateCounters() throws Exception {
        SessionIncidentEvaluatorFunction function = new SessionIncidentEvaluatorFunction(config(true, 2), policy("policy-v1", 0.90d));
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

        SafetyEvent event = promptInjection("req-2", BASE_TIME + 2_000L, 0.92d);
        IncidentEmissionPlanner.PlannedIncidentEmission emission = new IncidentEmissionPlanner.PlannedIncidentEmission(
                IncidentRuleNames.PROMPT_INJECTION_BURST,
                IncidentSeverity.HIGH,
                2,
                true,
                "policy-v1",
                "Prompt injection burst detected"
        );
        ListCollector collector = new ListCollector();

        Method emitIncident = SessionIncidentEvaluatorFunction.class.getDeclaredMethod(
                "emitIncident",
                Collector.class,
                SessionRiskSnapshot.class,
                SafetyEvent.class,
                IncidentEmissionPlanner.PlannedIncidentEmission.class
        );
        emitIncident.setAccessible(true);
        emitIncident.invoke(function, collector, snapshot, event, emission);

        assertEquals(1, collector.values.size());
        assertEquals("agent-1|session-1|PROMPT_INJECTION_BURST|" + (BASE_TIME + 1_000L), collector.values.get(0).incidentId());
        assertEquals("policy-v1", collector.values.get(0).appliedPolicyVersion());
        assertEquals(2L, collector.values.get(0).triggeredFindingsCount());
        assertEquals(1L, emittedCounter.getCount());
        assertEquals(1L, updatedCounter.getCount());
        assertEquals(1L, ruleCounter.getCount());
        assertEquals(1L, severityCounter.getCount());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static IncidentConfig config(boolean emitUpdates, int promptInjectionBurstMinFindings) {
        return new IncidentConfig(
                true,
                "basic-incidents",
                emitUpdates,
                Duration.ofMinutes(30),
                10,
                promptInjectionBurstMinFindings,
                3,
                2,
                new PiAndToxicRuleConfig(true, Duration.ofMinutes(5), IncidentSeverity.HIGH, 1, 1, null, null)
        );
    }

    private static IncidentPolicy policy(String version, double highThreshold) {
        return new IncidentPolicy(
                version,
                "test",
                "2026-09-01T00:00:00Z",
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
