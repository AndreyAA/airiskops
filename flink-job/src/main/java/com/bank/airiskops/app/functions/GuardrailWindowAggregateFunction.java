package com.bank.airiskops.app.functions;

import com.bank.airiskops.model.SafetyEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregate function for guardrail finding windows.
 *
 * <p>This operator updates a mutable accumulator per incoming finding and keeps
 * window aggregation lightweight before the richer window result is produced by
 * {@code GuardrailWindowProcessFunction}.
 */
public final class GuardrailWindowAggregateFunction
        implements AggregateFunction<SafetyEvent, GuardrailAggregateAccumulator, GuardrailAggregateAccumulator> {
    @Override
    public GuardrailAggregateAccumulator createAccumulator() {
        return new GuardrailAggregateAccumulator();
    }

    @Override
    public GuardrailAggregateAccumulator add(SafetyEvent value, GuardrailAggregateAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public GuardrailAggregateAccumulator getResult(GuardrailAggregateAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public GuardrailAggregateAccumulator merge(
            GuardrailAggregateAccumulator left,
            GuardrailAggregateAccumulator right
    ) {
        return left.merge(right);
    }
}
