package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.model.GuardrailAggregateKey;
import com.bank.aisafetyops.model.GuardrailWindowAggregate;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class GuardrailWindowProcessFunction extends
        ProcessWindowFunction<GuardrailAggregateAccumulator, GuardrailWindowAggregate, GuardrailAggregateKey, TimeWindow> {
    private static final String AISAFETYOPS_GROUP = "aisafetyops";
    private static final String WINDOW_GROUP = "window";
    private static final String GUARDRAIL_GROUP = "guardrail";
    private static final String METRIC_PREFIX = "guardrail_aggregate_records_total_";
    private static final String EMITTED_AGGREGATES_METRIC = "aggregates_emitted_total";
    private static final String EMITTED_EVENTS_METRIC = "events_total";
    private static final String EMITTED_TRIGGERED_METRIC = "triggered_total";
    private static final String EMITTED_DETECTOR_ERRORS_METRIC = "detector_errors_total";
    private static final String EMITTED_INPUT_TOKENS_METRIC = "input_tokens_total";
    private static final String EMITTED_OUTPUT_TOKENS_METRIC = "output_tokens_total";

    private final String windowName;
    private transient Counter emittedAggregateCounter;
    private transient MetricGroup aisafetyOpsMetricGroup;
    private transient Map<String, GuardrailMetricSet> guardrailMetricSets;

    public GuardrailWindowProcessFunction(String windowName) {
        this.windowName = windowName;
    }

    @Override
    public void open(Configuration parameters) {
        emittedAggregateCounter = getRuntimeContext().getMetricGroup().counter(METRIC_PREFIX + windowName);
        aisafetyOpsMetricGroup = getRuntimeContext().getMetricGroup().addGroup(AISAFETYOPS_GROUP);
        guardrailMetricSets = new ConcurrentHashMap<>();
    }

    @Override
    public void process(
            GuardrailAggregateKey key,
            Context context,
            Iterable<GuardrailAggregateAccumulator> elements,
            Collector<GuardrailWindowAggregate> out
    ) {
        Iterator<GuardrailAggregateAccumulator> iterator = elements.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        GuardrailAggregateAccumulator aggregate = iterator.next();
        emittedAggregateCounter.inc();
        metricSetFor(key.guardrailName()).record(aggregate);
        out.collect(new GuardrailWindowAggregate(
                aggregate.tenantId(),
                key.agentId(),
                key.guardrailName(),
                key.guardrailVersion(),
                key.policyVersion(),
                key.modelName(),
                windowName,
                context.window().getStart(),
                context.window().getEnd(),
                aggregate.totalEvents(),
                aggregate.guardrailFindingCount(),
                aggregate.triggeredCount(),
                aggregate.loopingTriggeredCount(),
                aggregate.systemPromptLeakageTriggeredCount(),
                aggregate.inputTokens(),
                aggregate.outputTokens(),
                aggregate.minConfidence(),
                aggregate.avgConfidence(),
                aggregate.maxConfidence(),
                aggregate.minDetectorLatencyMs(),
                aggregate.avgDetectorLatencyMs(),
                aggregate.maxDetectorLatencyMs(),
                aggregate.detectorErrorCount()
        ));
    }

    private GuardrailMetricSet metricSetFor(String guardrailName) {
        return guardrailMetricSets.computeIfAbsent(
                guardrailName == null ? "UNKNOWN" : guardrailName,
                this::createMetricSet
        );
    }

    private GuardrailMetricSet createMetricSet(String guardrailName) {
        MetricGroup metricGroup = aisafetyOpsMetricGroup
                .addGroup(WINDOW_GROUP, windowName)
                .addGroup(GUARDRAIL_GROUP, guardrailName);
        return new GuardrailMetricSet(
                metricGroup.counter(EMITTED_AGGREGATES_METRIC),
                metricGroup.counter(EMITTED_EVENTS_METRIC),
                metricGroup.counter(EMITTED_TRIGGERED_METRIC),
                metricGroup.counter(EMITTED_DETECTOR_ERRORS_METRIC),
                metricGroup.counter(EMITTED_INPUT_TOKENS_METRIC),
                metricGroup.counter(EMITTED_OUTPUT_TOKENS_METRIC)
        );
    }

    private record GuardrailMetricSet(
            Counter emittedAggregatesCounter,
            Counter eventsCounter,
            Counter triggeredCounter,
            Counter detectorErrorsCounter,
            Counter inputTokensCounter,
            Counter outputTokensCounter
    ) {
        private void record(GuardrailAggregateAccumulator aggregate) {
            emittedAggregatesCounter.inc();
            eventsCounter.inc(aggregate.totalEvents());
            triggeredCounter.inc(aggregate.triggeredCount());
            detectorErrorsCounter.inc(aggregate.detectorErrorCount());
            inputTokensCounter.inc(aggregate.inputTokens());
            outputTokensCounter.inc(aggregate.outputTokens());
        }
    }
}
