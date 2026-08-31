package com.bank.airiskops.app.functions;

import com.bank.airiskops.app.config.RuntimeContractConfig;
import com.bank.airiskops.model.SafetyEvent;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * Registers explicit runtime contract metrics and passes AIRiskOps events through.
 *
 * <p>The function makes the operational semantics of the job visible in
 * Prometheus/Grafana: window type, configured aggregate windows, bounded
 * out-of-orderness, late tolerance, checkpoint interval, watermark cadence,
 * and sink delivery guarantee.
 */
public final class RuntimeContractMetricsFunction extends RichMapFunction<SafetyEvent, SafetyEvent> {
    private static final String AIRISKOPS_GROUP = "airiskops";
    private static final String RUNTIME_CONTRACT_GROUP = "runtime_contract";
    private static final String WINDOW_GROUP = "window";
    private static final String WINDOW_TYPE_GROUP = "window_type";
    private static final String DELIVERY_GUARANTEE_GROUP = "delivery_guarantee";
    private static final String ANALYSIS_MODE_GROUP = "analysis_mode";
    private static final String AGGREGATE_WINDOWS_GROUP = "aggregate_windows";
    private static final String NRT_ANALYSIS_MODE = "nrtp";
    private static final String CONTRACT_INFO_METRIC = "info";
    private static final String OUT_OF_ORDERNESS_SECONDS_METRIC = "out_of_orderness_seconds";
    private static final String LATE_TOLERANCE_SECONDS_METRIC = "late_tolerance_seconds";
    private static final String IDLE_TIMEOUT_SECONDS_METRIC = "idle_timeout_seconds";
    private static final String CHECKPOINT_INTERVAL_SECONDS_METRIC = "checkpoint_interval_seconds";
    private static final String AUTO_WATERMARK_INTERVAL_SECONDS_METRIC = "auto_watermark_interval_seconds";
    private static final String WINDOW_SIZE_SECONDS_METRIC = "window_size_seconds";

    private final RuntimeContractConfig runtimeContract;
    private final Duration outOfOrderness;
    private final Duration lateTolerance;
    private final Duration idleTimeout;
    private final Duration checkpointInterval;
    private final Duration autoWatermarkInterval;

    public RuntimeContractMetricsFunction(
            RuntimeContractConfig runtimeContract,
            Duration outOfOrderness,
            Duration lateTolerance,
            Duration idleTimeout,
            Duration checkpointInterval,
            Duration autoWatermarkInterval
    ) {
        this.runtimeContract = runtimeContract;
        this.outOfOrderness = outOfOrderness;
        this.lateTolerance = lateTolerance;
        this.idleTimeout = idleTimeout;
        this.checkpointInterval = checkpointInterval;
        this.autoWatermarkInterval = autoWatermarkInterval;
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup runtimeMetricGroup = getRuntimeContext()
                .getMetricGroup()
                .addGroup(AIRISKOPS_GROUP)
                .addGroup(RUNTIME_CONTRACT_GROUP);
        runtimeMetricGroup
                .addGroup(WINDOW_TYPE_GROUP, runtimeContract.metricWindowType())
                .addGroup(DELIVERY_GUARANTEE_GROUP, runtimeContract.deliveryGuarantee().metricLabelValue())
                .addGroup(ANALYSIS_MODE_GROUP, NRT_ANALYSIS_MODE)
                .addGroup(AGGREGATE_WINDOWS_GROUP, runtimeContract.aggregateWindowsLabel())
                .gauge(CONTRACT_INFO_METRIC, new LongGaugeValue(1L));
        runtimeMetricGroup.gauge(OUT_OF_ORDERNESS_SECONDS_METRIC, gaugeOfSeconds(outOfOrderness));
        runtimeMetricGroup.gauge(LATE_TOLERANCE_SECONDS_METRIC, gaugeOfSeconds(lateTolerance));
        runtimeMetricGroup.gauge(IDLE_TIMEOUT_SECONDS_METRIC, gaugeOfSeconds(idleTimeout));
        runtimeMetricGroup.gauge(CHECKPOINT_INTERVAL_SECONDS_METRIC, gaugeOfSeconds(checkpointInterval));
        runtimeMetricGroup.gauge(AUTO_WATERMARK_INTERVAL_SECONDS_METRIC, gaugeOfSeconds(autoWatermarkInterval));
        registerWindowSizeGauges(runtimeMetricGroup, runtimeContract.aggregateWindows(), runtimeContract.aggregateWindowNames());
    }

    @Override
    public SafetyEvent map(SafetyEvent value) {
        return value;
    }

    private static Gauge<Long> gaugeOfSeconds(Duration duration) {
        return new LongGaugeValue(duration.toSeconds());
    }

    private static void registerWindowSizeGauges(
            MetricGroup runtimeMetricGroup,
            List<Duration> aggregateWindows,
            List<String> windowNames
    ) {
        for (int index = 0; index < aggregateWindows.size(); index++) {
            runtimeMetricGroup
                    .addGroup(WINDOW_GROUP, windowNames.get(index))
                    .gauge(WINDOW_SIZE_SECONDS_METRIC, new LongGaugeValue(aggregateWindows.get(index).toSeconds()));
        }
    }

    private static final class LongGaugeValue implements Gauge<Long> {
        private final long value;

        private LongGaugeValue(long value) {
            this.value = value;
        }

        @Override
        public Long getValue() {
            return value;
        }
    }
}
