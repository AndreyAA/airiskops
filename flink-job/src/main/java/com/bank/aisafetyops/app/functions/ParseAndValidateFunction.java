package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.infra.parser.ParseResult;
import com.bank.aisafetyops.infra.parser.SafetyEventParser;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * Parses raw JSON payloads into validated domain events.
 *
 * <p>This is the first business-safe boundary in the topology: successful
 * records move forward as {@code ParseResult.success}, and malformed or
 * incomplete payloads are retained for downstream inspection as invalid events.
 */
public final class ParseAndValidateFunction extends RichMapFunction<String, ParseResult> {
    private static final String VALID_EVENTS_METRIC = "valid_events_total";
    private static final String INVALID_EVENTS_METRIC = "invalid_events_total";

    private transient SafetyEventParser parser;
    private transient Counter validCounter;
    private transient Counter invalidCounter;

    @Override
    public void open(Configuration parameters) {
        parser = new SafetyEventParser();
        // These counters are the first business-safe observability layer of the MVP:
        // before we trust any downstream metric, we need to know how much raw data
        // survived parsing and how much was rejected at the boundary.
        validCounter = getRuntimeContext().getMetricGroup().counter(VALID_EVENTS_METRIC);
        invalidCounter = getRuntimeContext().getMetricGroup().counter(INVALID_EVENTS_METRIC);
    }

    @Override
    public ParseResult map(String rawPayload) {
        // Parsing is intentionally isolated from the rest of the topology. This lets
        // us change JSON -> Avro/Protobuf later in one place instead of rewriting
        // stateful operators.
        ParseResult result = parser.parse(rawPayload);
        if (result.isValid()) {
            validCounter.inc();
        } else {
            invalidCounter.inc();
        }
        return result;
    }
}
