package com.bank.airiskops.app.functions;

import com.bank.airiskops.app.support.JobTopology;
import com.bank.airiskops.infra.parser.ParseResult;
import com.bank.airiskops.model.InvalidEvent;
import com.bank.airiskops.model.SafetyEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Splits parse results into valid domain events and invalid side-output records.
 *
 * <p>This keeps malformed payloads inspectable without polluting the main stream
 * that later receives timestamps, watermarks, and stateful processing.
 */
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
