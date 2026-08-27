package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.model.GuardrailAggregateKey;
import com.bank.aisafetyops.model.GuardrailWindowAggregate;
import java.util.Iterator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class GuardrailWindowProcessFunction extends
        ProcessWindowFunction<GuardrailAggregateAccumulator, GuardrailWindowAggregate, GuardrailAggregateKey, TimeWindow> {
    private static final String METRIC_PREFIX = "guardrail_aggregate_records_total_";

    private final String windowName;
    private transient Counter emittedAggregateCounter;

    public GuardrailWindowProcessFunction(String windowName) {
        this.windowName = windowName;
    }

    @Override
    public void open(Configuration parameters) {
        emittedAggregateCounter = getRuntimeContext().getMetricGroup().counter(METRIC_PREFIX + windowName);
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
}
