package com.bank.airiskops.app.functions;

import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.GuardrailQualityMetric;
import com.bank.airiskops.model.GuardrailWindowAggregate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * Derives detector-quality signals from emitted guardrail aggregates.
 *
 * <p>The function keeps detector quality separate from the main business
 * findings stream, which allows dashboards and downstream tooling to answer
 * whether a risk signal is real or caused by degraded detector behavior.
 */
public final class GuardrailQualityMetricFunction extends RichMapFunction<GuardrailWindowAggregate, GuardrailQualityMetric> {
    private static final String AIRISKOPS_GROUP = "airiskops";
    private static final String QUALITY_GROUP = "quality";
    private static final String WINDOW_GROUP = "window";
    private static final String GUARDRAIL_GROUP = "guardrail";
    private static final String EMITTED_METRIC = "emitted_total";
    private static final String FINDINGS_METRIC = "findings_total";
    private static final String TRIGGERED_METRIC = "triggered_total";
    private static final String DETECTOR_ERRORS_METRIC = "detector_errors_total";
    private static final String LAST_TRIGGER_RATE_METRIC = "last_trigger_rate";
    private static final String LAST_DETECTOR_ERROR_RATE_METRIC = "last_detector_error_rate";
    private static final String LAST_MISSING_CONFIDENCE_RATE_METRIC = "last_missing_confidence_rate";
    private static final String LAST_CONFIDENCE_COVERAGE_RATE_METRIC = "last_confidence_coverage_rate";
    private static final String LAST_AVG_DETECTOR_LATENCY_MS_METRIC = "last_avg_detector_latency_ms";
    private static final String LAST_MAX_DETECTOR_LATENCY_MS_METRIC = "last_max_detector_latency_ms";

    private transient MetricGroup airiskOpsMetricGroup;
    private transient Map<String, QualityMetricSet> metricSets;

    @Override
    public void open(Configuration parameters) {
        airiskOpsMetricGroup = getRuntimeContext().getMetricGroup().addGroup(AIRISKOPS_GROUP);
        metricSets = new ConcurrentHashMap<>();
    }

    @Override
    public GuardrailQualityMetric map(GuardrailWindowAggregate aggregate) {
        GuardrailQualityMetric qualityMetric = buildQualityMetric(aggregate);
        metricSetFor(aggregate.windowName(), aggregate.guardrailName()).record(qualityMetric);
        return qualityMetric;
    }

    static GuardrailQualityMetric buildQualityMetric(GuardrailWindowAggregate aggregate) {
        double triggerRate = rate(aggregate.triggeredCount(), aggregate.guardrailFindingCount());
        double detectorErrorRate = rate(aggregate.detectorErrorCount(), aggregate.guardrailFindingCount());
        Long missingConfidenceCount = null;
        Double missingConfidenceRate = null;
        Double confidenceCoverageRate = null;

        if (GuardrailNames.requiresConfidence(aggregate.guardrailName())) {
            long missingCount = Math.max(0L, aggregate.guardrailFindingCount() - aggregate.confidenceCount());
            missingConfidenceCount = missingCount;
            missingConfidenceRate = rate(missingCount, aggregate.guardrailFindingCount());
            confidenceCoverageRate = rate(aggregate.confidenceCount(), aggregate.guardrailFindingCount());
        }

        return new GuardrailQualityMetric(
                aggregate.tenantId(),
                aggregate.agentId(),
                aggregate.guardrailName(),
                aggregate.guardrailVersion(),
                aggregate.policyVersion(),
                aggregate.modelName(),
                aggregate.windowName(),
                aggregate.windowStartMillis(),
                aggregate.windowEndMillis(),
                aggregate.guardrailFindingCount(),
                aggregate.triggeredCount(),
                triggerRate,
                aggregate.detectorErrorCount(),
                detectorErrorRate,
                missingConfidenceCount,
                missingConfidenceRate,
                confidenceCoverageRate,
                aggregate.confidenceCount(),
                aggregate.minDetectorLatencyMs(),
                aggregate.avgDetectorLatencyMs(),
                aggregate.maxDetectorLatencyMs()
        );
    }

    private QualityMetricSet metricSetFor(String windowName, String guardrailName) {
        String key = windowName + "|" + guardrailName;
        return metricSets.computeIfAbsent(key, ignored -> createMetricSet(windowName, guardrailName));
    }

    private QualityMetricSet createMetricSet(String windowName, String guardrailName) {
        MetricGroup metricGroup = airiskOpsMetricGroup
                .addGroup(QUALITY_GROUP)
                .addGroup(WINDOW_GROUP, windowName)
                .addGroup(GUARDRAIL_GROUP, guardrailName == null ? "UNKNOWN" : guardrailName);
        DoubleGaugeValue lastTriggerRate = new DoubleGaugeValue();
        DoubleGaugeValue lastDetectorErrorRate = new DoubleGaugeValue();
        DoubleGaugeValue lastMissingConfidenceRate = new DoubleGaugeValue();
        DoubleGaugeValue lastConfidenceCoverageRate = new DoubleGaugeValue();
        DoubleGaugeValue lastAvgDetectorLatencyMs = new DoubleGaugeValue();
        DoubleGaugeValue lastMaxDetectorLatencyMs = new DoubleGaugeValue();
        metricGroup.gauge(LAST_TRIGGER_RATE_METRIC, lastTriggerRate);
        metricGroup.gauge(LAST_DETECTOR_ERROR_RATE_METRIC, lastDetectorErrorRate);
        metricGroup.gauge(LAST_MISSING_CONFIDENCE_RATE_METRIC, lastMissingConfidenceRate);
        metricGroup.gauge(LAST_CONFIDENCE_COVERAGE_RATE_METRIC, lastConfidenceCoverageRate);
        metricGroup.gauge(LAST_AVG_DETECTOR_LATENCY_MS_METRIC, lastAvgDetectorLatencyMs);
        metricGroup.gauge(LAST_MAX_DETECTOR_LATENCY_MS_METRIC, lastMaxDetectorLatencyMs);
        return new QualityMetricSet(
                metricGroup.counter(EMITTED_METRIC),
                metricGroup.counter(FINDINGS_METRIC),
                metricGroup.counter(TRIGGERED_METRIC),
                metricGroup.counter(DETECTOR_ERRORS_METRIC),
                lastTriggerRate,
                lastDetectorErrorRate,
                lastMissingConfidenceRate,
                lastConfidenceCoverageRate,
                lastAvgDetectorLatencyMs,
                lastMaxDetectorLatencyMs
        );
    }

    private static double rate(long numerator, long denominator) {
        if (denominator <= 0) {
            return 0.0d;
        }
        return (double) numerator / denominator;
    }

    private record QualityMetricSet(
            Counter emittedCounter,
            Counter findingsCounter,
            Counter triggeredCounter,
            Counter detectorErrorsCounter,
            DoubleGaugeValue lastTriggerRate,
            DoubleGaugeValue lastDetectorErrorRate,
            DoubleGaugeValue lastMissingConfidenceRate,
            DoubleGaugeValue lastConfidenceCoverageRate,
            DoubleGaugeValue lastAvgDetectorLatencyMs,
            DoubleGaugeValue lastMaxDetectorLatencyMs
    ) {
        private void record(GuardrailQualityMetric qualityMetric) {
            emittedCounter.inc();
            findingsCounter.inc(qualityMetric.guardrailFindingCount());
            triggeredCounter.inc(qualityMetric.triggeredCount());
            detectorErrorsCounter.inc(qualityMetric.detectorErrorCount());
            lastTriggerRate.set(qualityMetric.triggerRate());
            lastDetectorErrorRate.set(qualityMetric.detectorErrorRate());
            lastMissingConfidenceRate.set(qualityMetric.missingConfidenceRate());
            lastConfidenceCoverageRate.set(qualityMetric.confidenceCoverageRate());
            lastAvgDetectorLatencyMs.set(qualityMetric.avgDetectorLatencyMs());
            lastMaxDetectorLatencyMs.set(qualityMetric.maxDetectorLatencyMs() == null
                    ? null
                    : qualityMetric.maxDetectorLatencyMs().doubleValue());
        }
    }

    private static final class DoubleGaugeValue implements Gauge<Double> {
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
