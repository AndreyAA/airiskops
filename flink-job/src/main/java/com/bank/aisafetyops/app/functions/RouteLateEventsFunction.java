package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.SafetyEvent;
import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class RouteLateEventsFunction extends ProcessFunction<SafetyEvent, SafetyEvent> {
    private static final String LATE_EVENT_REASON = "Event is older than current watermark tolerance";
    private static final long UNINITIALIZED_WATERMARK = Long.MIN_VALUE;
    private static final String LATE_EVENTS_METRIC = "late_events_total";
    private static final String ON_TIME_EVENTS_METRIC = "on_time_events_total";

    private final long lateToleranceMillis;
    private final OutputTag<LateEvent> lateOutputTag;
    private transient Counter lateCounter;
    private transient Counter onTimeCounter;

    public RouteLateEventsFunction(Duration lateTolerance, OutputTag<LateEvent> lateOutputTag) {
        this.lateToleranceMillis = lateTolerance.toMillis();
        this.lateOutputTag = lateOutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        // These counters complement the built-in Flink throughput metrics with
        // domain-relevant visibility: how much data is still usable in the main
        // stream and how much is already considered late for NRTP analytics.
        lateCounter = getRuntimeContext().getMetricGroup().counter(LATE_EVENTS_METRIC);
        onTimeCounter = getRuntimeContext().getMetricGroup().counter(ON_TIME_EVENTS_METRIC);
    }

    @Override
    public void processElement(SafetyEvent event, Context ctx, Collector<SafetyEvent> out) {
        long currentWatermark = ctx.timerService().currentWatermark();
        // We deliberately route late records into a side output instead of dropping
        // them silently. For AISafetyOps this matters because an "old" guardrail
        // event can still be valuable for replay, auditing and root-cause analysis.
        if (currentWatermark != UNINITIALIZED_WATERMARK && event.eventTimeMillis() + lateToleranceMillis < currentWatermark) {
            lateCounter.inc();
            ctx.output(lateOutputTag, new LateEvent(LATE_EVENT_REASON, event, currentWatermark));
            return;
        }
        onTimeCounter.inc();
        out.collect(event);
    }
}
