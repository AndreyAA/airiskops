package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.app.config.IncidentConfig;
import com.bank.aisafetyops.app.support.IncidentEmissionPlanner;
import com.bank.aisafetyops.app.support.IncidentPolicyUpdateDecider;
import com.bank.aisafetyops.model.BasicIncident;
import com.bank.aisafetyops.model.IncidentPolicy;
import com.bank.aisafetyops.model.SafetyEvent;
import com.bank.aisafetyops.model.SessionIncidentKey;
import com.bank.aisafetyops.model.SessionRiskSnapshot;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Session incident evaluator with runtime policy updates from broadcast state.
 *
 * <p>The bootstrap policy loaded at startup remains the initial baseline, but
 * accepted Kafka updates can replace it without redeploying the job.
 */
public final class PolicyAwareSessionIncidentEvaluatorFunction
        extends KeyedBroadcastProcessFunction<SessionIncidentKey, SafetyEvent, IncidentPolicy, BasicIncident> {
    private static final String AISAFETYOPS_GROUP = "aisafetyops";
    private static final String INCIDENT_GROUP = "incident";
    private static final String POLICY_GROUP = "policy";
    private static final String RULE_GROUP = "rule";
    private static final String SEVERITY_GROUP = "severity";
    private static final String EMITTED_METRIC = "incidents_emitted_total";
    private static final String UPDATED_METRIC = "incident_updates_total";
    private static final String OPEN_SESSIONS_METRIC = "open_sessions";
    private static final String POLICY_ACCEPTED_UPDATES_METRIC = "accepted_updates_total";
    private static final String POLICY_REJECTED_UPDATES_METRIC = "rejected_updates_total";
    private static final String POLICY_LAST_UPDATE_EPOCH_MS_METRIC = "last_update_epoch_ms";
    private static final String POLICY_STATE_KEY = "active";

    private final IncidentConfig incidentConfig;
    private final boolean rejectOlderVersions;
    private final IncidentPolicy bootstrapPolicy;
    private final MapStateDescriptor<String, IncidentPolicy> broadcastStateDescriptor;

    private transient ValueState<SessionRiskSnapshot> snapshotState;
    private transient Counter emittedCounter;
    private transient Counter updatedCounter;
    private transient Counter acceptedPolicyUpdatesCounter;
    private transient Counter rejectedPolicyUpdatesCounter;
    private transient AtomicInteger openSessionsGaugeValue;
    private transient AtomicLong lastPolicyUpdateEpochMillis;
    private transient Map<String, Counter> ruleCounters;
    private transient Map<String, Counter> severityCounters;
    private transient MetricGroup aisafetyOpsMetricGroup;

    public PolicyAwareSessionIncidentEvaluatorFunction(
            IncidentConfig incidentConfig,
            boolean rejectOlderVersions,
            IncidentPolicy bootstrapPolicy,
            MapStateDescriptor<String, IncidentPolicy> broadcastStateDescriptor
    ) {
        this.incidentConfig = incidentConfig;
        this.rejectOlderVersions = rejectOlderVersions;
        this.bootstrapPolicy = bootstrapPolicy;
        this.broadcastStateDescriptor = broadcastStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) {
        snapshotState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "session-incident-snapshot",
                SessionRiskSnapshot.class
        ));
        aisafetyOpsMetricGroup = getRuntimeContext().getMetricGroup().addGroup(AISAFETYOPS_GROUP);
        MetricGroup incidentMetricGroup = aisafetyOpsMetricGroup.addGroup(INCIDENT_GROUP);
        MetricGroup policyMetricGroup = aisafetyOpsMetricGroup.addGroup(POLICY_GROUP);
        emittedCounter = incidentMetricGroup.counter(EMITTED_METRIC);
        updatedCounter = incidentMetricGroup.counter(UPDATED_METRIC);
        acceptedPolicyUpdatesCounter = policyMetricGroup.counter(POLICY_ACCEPTED_UPDATES_METRIC);
        rejectedPolicyUpdatesCounter = policyMetricGroup.counter(POLICY_REJECTED_UPDATES_METRIC);
        openSessionsGaugeValue = new AtomicInteger();
        lastPolicyUpdateEpochMillis = new AtomicLong(parseUpdatedAt(bootstrapPolicy));
        incidentMetricGroup.gauge(OPEN_SESSIONS_METRIC, openSessionsGaugeValue::get);
        policyMetricGroup.gauge(POLICY_LAST_UPDATE_EPOCH_MS_METRIC, lastPolicyUpdateEpochMillis::get);
        ruleCounters = new ConcurrentHashMap<>();
        severityCounters = new ConcurrentHashMap<>();
    }

    @Override
    public void processElement(
            SafetyEvent event,
            ReadOnlyContext context,
            Collector<BasicIncident> out
    ) throws Exception {
        SessionRiskSnapshot snapshot = snapshotState.value();
        if (snapshot == null) {
            snapshot = new SessionRiskSnapshot();
            openSessionsGaugeValue.incrementAndGet();
        } else if (snapshot.cleanupDeadlineMillis() > 0L) {
            context.timerService().deleteEventTimeTimer(snapshot.cleanupDeadlineMillis());
        }

        snapshot.recordFinding(event, incidentConfig.maxRequestIdsPerIncident());
        long cleanupDeadlineMillis = event.eventTimeMillis() + incidentConfig.sessionInactivityTimeout().toMillis();
        snapshot.setCleanupDeadlineMillis(cleanupDeadlineMillis);
        context.timerService().registerEventTimeTimer(cleanupDeadlineMillis);

        IncidentPolicy activePolicy = resolveActivePolicy(context.getBroadcastState(broadcastStateDescriptor));
        IncidentEmissionPlanner planner = new IncidentEmissionPlanner(incidentConfig, activePolicy);
        for (IncidentEmissionPlanner.PlannedIncidentEmission plannedEmission : planner.plan(snapshot, event)) {
            emitIncident(out, snapshot, event, plannedEmission);
        }

        snapshotState.update(snapshot);
    }

    @Override
    public void processBroadcastElement(
            IncidentPolicy candidatePolicy,
            Context context,
            Collector<BasicIncident> out
    ) throws Exception {
        var state = context.getBroadcastState(broadcastStateDescriptor);
        IncidentPolicy currentPolicy = state.get(POLICY_STATE_KEY);
        if (currentPolicy == null) {
            currentPolicy = bootstrapPolicy;
        }
        IncidentPolicyUpdateDecider.PolicyUpdateDecision decision = IncidentPolicyUpdateDecider.decide(
                currentPolicy,
                candidatePolicy,
                rejectOlderVersions
        );
        if (!decision.accepted()) {
            rejectedPolicyUpdatesCounter.inc();
            return;
        }
        state.put(POLICY_STATE_KEY, candidatePolicy);
        acceptedPolicyUpdatesCounter.inc();
        lastPolicyUpdateEpochMillis.set(parseUpdatedAt(candidatePolicy));
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

    private IncidentPolicy resolveActivePolicy(ReadOnlyBroadcastState<String, IncidentPolicy> state) throws Exception {
        IncidentPolicy policyFromBroadcast = state.get(POLICY_STATE_KEY);
        return policyFromBroadcast != null ? policyFromBroadcast : bootstrapPolicy;
    }

    private Counter ruleCounter(String ruleName) {
        return ruleCounters.computeIfAbsent(
                ruleName,
                key -> aisafetyOpsMetricGroup.addGroup(INCIDENT_GROUP).addGroup(RULE_GROUP, key).counter(EMITTED_METRIC)
        );
    }

    private Counter severityCounter(String severityName) {
        return severityCounters.computeIfAbsent(
                severityName,
                key -> aisafetyOpsMetricGroup
                        .addGroup(INCIDENT_GROUP)
                        .addGroup(SEVERITY_GROUP, key.toLowerCase())
                        .counter(EMITTED_METRIC)
        );
    }

    private static long parseUpdatedAt(IncidentPolicy policy) {
        if (policy == null || policy.updatedAt() == null || policy.updatedAt().isBlank()) {
            return 0L;
        }
        try {
            return java.time.Instant.parse(policy.updatedAt()).toEpochMilli();
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static String buildIncidentId(SessionRiskSnapshot snapshot, String ruleName) {
        return snapshot.agentId() + "|" + snapshot.sessionId() + "|" + ruleName + "|" + snapshot.firstEventTimeMillis();
    }
}
