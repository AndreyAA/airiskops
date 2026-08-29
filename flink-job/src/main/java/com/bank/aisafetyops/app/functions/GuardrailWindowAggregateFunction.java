package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.model.SafetyEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

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
