package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.app.support.PercentileCalculator;
import com.bank.aisafetyops.model.GuardrailAggregateKey;
import com.bank.aisafetyops.model.GuardrailNames;
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

/**
 * Finalizes guardrail window aggregates and exports matching business metrics.
 *
 * <p>The function converts the mutable accumulator into the immutable
 * {@code GuardrailWindowAggregate} contract and updates Prometheus-visible
 * counters scoped by window name and guardrail.
 */
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
    private static final String LAST_P50_CONFIDENCE_METRIC = "last_p50_confidence";
    private static final String LAST_P95_CONFIDENCE_METRIC = "last_p95_confidence";
    private static final String LAST_TRIGGERED_P50_CONFIDENCE_METRIC = "last_triggered_p50_confidence";
    private static final String LAST_TRIGGERED_P95_CONFIDENCE_METRIC = "last_triggered_p95_confidence";
    private static final int P50 = 50;
    private static final int P95 = 95;

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
        ConfidencePercentiles percentiles = calculatePercentiles(key.guardrailName(), aggregate);
        emittedAggregateCounter.inc();
        metricSetFor(key.guardrailName()).record(aggregate, percentiles);
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
                percentiles.p50Confidence(),
                percentiles.p95Confidence(),
                percentiles.triggeredP50Confidence(),
                percentiles.triggeredP95Confidence(),
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
        DoubleGaugeValue lastP50Confidence = new DoubleGaugeValue();
        DoubleGaugeValue lastP95Confidence = new DoubleGaugeValue();
        DoubleGaugeValue lastTriggeredP50Confidence = new DoubleGaugeValue();
        DoubleGaugeValue lastTriggeredP95Confidence = new DoubleGaugeValue();
        metricGroup.gauge(LAST_P50_CONFIDENCE_METRIC, lastP50Confidence);
        metricGroup.gauge(LAST_P95_CONFIDENCE_METRIC, lastP95Confidence);
        metricGroup.gauge(LAST_TRIGGERED_P50_CONFIDENCE_METRIC, lastTriggeredP50Confidence);
        metricGroup.gauge(LAST_TRIGGERED_P95_CONFIDENCE_METRIC, lastTriggeredP95Confidence);
        return new GuardrailMetricSet(
                metricGroup.counter(EMITTED_AGGREGATES_METRIC),
                metricGroup.counter(EMITTED_EVENTS_METRIC),
                metricGroup.counter(EMITTED_TRIGGERED_METRIC),
                metricGroup.counter(EMITTED_DETECTOR_ERRORS_METRIC),
                metricGroup.counter(EMITTED_INPUT_TOKENS_METRIC),
                metricGroup.counter(EMITTED_OUTPUT_TOKENS_METRIC),
                lastP50Confidence,
                lastP95Confidence,
                lastTriggeredP50Confidence,
                lastTriggeredP95Confidence
        );
    }

    private static ConfidencePercentiles calculatePercentiles(
            String guardrailName,
            GuardrailAggregateAccumulator aggregate
    ) {
        if (!GuardrailNames.requiresConfidence(guardrailName)) {
            return ConfidencePercentiles.empty();
        }
        return new ConfidencePercentiles(
                PercentileCalculator.nearestRank(aggregate.confidenceValues(), P50),
                PercentileCalculator.nearestRank(aggregate.confidenceValues(), P95),
                PercentileCalculator.nearestRank(aggregate.triggeredConfidenceValues(), P50),
                PercentileCalculator.nearestRank(aggregate.triggeredConfidenceValues(), P95)
        );
    }

    private record GuardrailMetricSet(
            Counter emittedAggregatesCounter,
            Counter eventsCounter,
            Counter triggeredCounter,
            Counter detectorErrorsCounter,
            Counter inputTokensCounter,
            Counter outputTokensCounter,
            DoubleGaugeValue lastP50Confidence,
            DoubleGaugeValue lastP95Confidence,
            DoubleGaugeValue lastTriggeredP50Confidence,
            DoubleGaugeValue lastTriggeredP95Confidence
    ) {
        private void record(
                GuardrailAggregateAccumulator aggregate,
                ConfidencePercentiles percentiles
        ) {
            emittedAggregatesCounter.inc();
            eventsCounter.inc(aggregate.totalEvents());
            triggeredCounter.inc(aggregate.triggeredCount());
            detectorErrorsCounter.inc(aggregate.detectorErrorCount());
            inputTokensCounter.inc(aggregate.inputTokens());
            outputTokensCounter.inc(aggregate.outputTokens());
            lastP50Confidence.set(percentiles.p50Confidence());
            lastP95Confidence.set(percentiles.p95Confidence());
            lastTriggeredP50Confidence.set(percentiles.triggeredP50Confidence());
            lastTriggeredP95Confidence.set(percentiles.triggeredP95Confidence());
        }
    }

    private record ConfidencePercentiles(
            Double p50Confidence,
            Double p95Confidence,
            Double triggeredP50Confidence,
            Double triggeredP95Confidence
    ) {
        private static ConfidencePercentiles empty() {
            return new ConfidencePercentiles(null, null, null, null);
        }
    }

    private static final class DoubleGaugeValue implements org.apache.flink.metrics.Gauge<Double> {
        private static final double NO_VALUE = Double.NaN;

        private volatile double value = NO_VALUE;

        private void set(Double nextValue) {
            value = nextValue == null ? NO_VALUE : nextValue;
        }

        @Override
        public Double getValue() {
            return value;
        }
    }
}
