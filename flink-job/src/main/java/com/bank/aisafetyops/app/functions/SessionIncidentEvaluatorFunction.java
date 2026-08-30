package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.app.config.IncidentConfig;
import com.bank.aisafetyops.app.support.IncidentEmissionPlanner;
import com.bank.aisafetyops.model.BasicIncident;
import com.bank.aisafetyops.model.IncidentPolicy;
import com.bank.aisafetyops.model.SafetyEvent;
import com.bank.aisafetyops.model.SessionIncidentKey;
import com.bank.aisafetyops.model.SessionRiskSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Correlates triggered guardrail findings into minimal session incidents.
 *
 * <p>The function keeps bounded session evidence in keyed state and emits
 * business-facing incident signals as soon as the first operational rule is
 * satisfied. Event-time cleanup is used so the current semantics remain
 * visible and testable without relying on implicit state TTL behavior.
 */
public final class SessionIncidentEvaluatorFunction
        extends KeyedProcessFunction<SessionIncidentKey, SafetyEvent, BasicIncident> {
    private static final String AISAFETYOPS_GROUP = "aisafetyops";
    private static final String INCIDENT_GROUP = "incident";
    private static final String RULE_GROUP = "rule";
    private static final String SEVERITY_GROUP = "severity";
    private static final String EMITTED_METRIC = "incidents_emitted_total";
    private static final String UPDATED_METRIC = "incident_updates_total";
    private static final String OPEN_SESSIONS_METRIC = "open_sessions";

    private final IncidentConfig config;
    private final IncidentPolicy policy;

    private transient ValueState<SessionRiskSnapshot> snapshotState;
    private transient Counter emittedCounter;
    private transient Counter updatedCounter;
    private transient AtomicInteger openSessionsGaugeValue;
    private transient Map<String, Counter> ruleCounters;
    private transient Map<String, Counter> severityCounters;
    private transient MetricGroup aisafetyOpsMetricGroup;
    private transient IncidentEmissionPlanner emissionPlanner;

    public SessionIncidentEvaluatorFunction(IncidentConfig config, IncidentPolicy policy) {
        this.config = config;
        this.policy = policy;
    }

    @Override
    public void open(Configuration parameters) {
        snapshotState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "session-incident-snapshot",
                SessionRiskSnapshot.class
        ));
        aisafetyOpsMetricGroup = getRuntimeContext().getMetricGroup()
                .addGroup(AISAFETYOPS_GROUP)
                .addGroup(INCIDENT_GROUP);
        emittedCounter = aisafetyOpsMetricGroup.counter(EMITTED_METRIC);
        updatedCounter = aisafetyOpsMetricGroup.counter(UPDATED_METRIC);
        openSessionsGaugeValue = new AtomicInteger();
        aisafetyOpsMetricGroup.gauge(OPEN_SESSIONS_METRIC, openSessionsGaugeValue::get);
        ruleCounters = new ConcurrentHashMap<>();
        severityCounters = new ConcurrentHashMap<>();
        emissionPlanner = new IncidentEmissionPlanner(config, policy);
    }

    @Override
    public void processElement(
            SafetyEvent event,
            Context context,
            Collector<BasicIncident> out
    ) throws Exception {
        SessionRiskSnapshot snapshot = snapshotState.value();
        if (snapshot == null) {
            snapshot = new SessionRiskSnapshot();
            openSessionsGaugeValue.incrementAndGet();
        } else if (snapshot.cleanupDeadlineMillis() > 0L) {
            context.timerService().deleteEventTimeTimer(snapshot.cleanupDeadlineMillis());
        }

        snapshot.recordFinding(event, config.maxRequestIdsPerIncident());
        long cleanupDeadlineMillis = event.eventTimeMillis() + config.sessionInactivityTimeout().toMillis();
        snapshot.setCleanupDeadlineMillis(cleanupDeadlineMillis);
        context.timerService().registerEventTimeTimer(cleanupDeadlineMillis);

        List<IncidentEmissionPlanner.PlannedIncidentEmission> plannedEmissions = emissionPlanner.plan(snapshot, event);
        for (IncidentEmissionPlanner.PlannedIncidentEmission plannedEmission : plannedEmissions) {
            emitIncident(out, snapshot, event, plannedEmission);
        }

        snapshotState.update(snapshot);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<BasicIncident> out
    ) throws Exception {
        SessionRiskSnapshot snapshot = snapshotState.value();
        if (snapshot == null || timestamp != snapshot.cleanupDeadlineMillis()) {
            return;
        }
        snapshotState.clear();
        openSessionsGaugeValue.updateAndGet(currentValue -> Math.max(0, currentValue - 1));
    }

    private void emitIncident(
            Collector<BasicIncident> out,
            SessionRiskSnapshot snapshot,
            SafetyEvent event,
            IncidentEmissionPlanner.PlannedIncidentEmission plannedEmission
    ) {
        BasicIncident incident = new BasicIncident(
                buildIncidentId(snapshot, plannedEmission.ruleName()),
                snapshot.tenantId(),
                snapshot.agentId(),
                snapshot.sessionId(),
                plannedEmission.ruleName(),
                plannedEmission.severity(),
                new ArrayList<>(snapshot.requestIds()),
                new ArrayList<>(snapshot.guardrailNames()),
                new ArrayList<>(snapshot.guardrailVersions()),
                new ArrayList<>(snapshot.policyVersions()),
                plannedEmission.appliedPolicyVersion(),
                snapshot.firstEventTimeMillis(),
                snapshot.lastEventTimeMillis(),
                event.eventTimeMillis(),
                snapshot.triggeredFindingsCount(),
                plannedEmission.revision(),
                plannedEmission.summary()
        );
        emittedCounter.inc();
        ruleCounter(plannedEmission.ruleName()).inc();
        severityCounter(plannedEmission.severity().name()).inc();
        if (plannedEmission.update()) {
            updatedCounter.inc();
        }
        out.collect(incident);
    }

    private Counter ruleCounter(String ruleName) {
        return ruleCounters.computeIfAbsent(
                ruleName,
                key -> aisafetyOpsMetricGroup.addGroup(RULE_GROUP, key).counter(EMITTED_METRIC)
        );
    }

    private Counter severityCounter(String severityName) {
        return severityCounters.computeIfAbsent(
                severityName,
                key -> aisafetyOpsMetricGroup.addGroup(SEVERITY_GROUP, key.toLowerCase()).counter(EMITTED_METRIC)
        );
    }

    private static String buildIncidentId(SessionRiskSnapshot snapshot, String ruleName) {
        return snapshot.agentId() + "|" + snapshot.sessionId() + "|" + ruleName + "|" + snapshot.firstEventTimeMillis();
    }
}
