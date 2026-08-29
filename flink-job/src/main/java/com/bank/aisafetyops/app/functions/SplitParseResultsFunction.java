package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.app.support.JobTopology;
import com.bank.aisafetyops.infra.parser.ParseResult;
import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.SafetyEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public final class SplitParseResultsFunction extends ProcessFunction<ParseResult, SafetyEvent> {
    @Override
    public void processElement(ParseResult value, Context ctx, Collector<SafetyEvent> out) {
        if (value.isValid()) {
            out.collect(value.event());
            return;
        }
        // Invalid records must stay inspectable. Emitting them to a side output is
        // what later allows us to wire dead-letter topics or quality dashboards
        // without changing the parsing step itself.
        InvalidEvent invalidEvent = value.invalidEvent();
        ctx.output(JobTopology.INVALID_EVENTS, invalidEvent);
    }
}
