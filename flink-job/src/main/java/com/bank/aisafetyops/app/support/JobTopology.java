package com.bank.aisafetyops.app.support;

import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.WindowNames;
import org.apache.flink.util.OutputTag;

public final class JobTopology {
    public static final String JOB_NAME = "AISafetyOps MVP Increment 1";

    public static final String INVALID_EVENTS_TAG = "invalid-events";
    public static final String LATE_EVENTS_TAG = "late-events";

    public static final OutputTag<InvalidEvent> INVALID_EVENTS = new OutputTag<>(INVALID_EVENTS_TAG) {
    };
    public static final OutputTag<LateEvent> LATE_EVENTS = new OutputTag<>(LATE_EVENTS_TAG) {
    };

    public static final String SOURCE_UID = "kafka-raw-events";
    public static final String SOURCE_NAME = "Kafka Raw Events";
    public static final String PARSE_UID = "parse-and-validate";
    public static final String PARSE_NAME = "Parse And Validate";
    public static final String SPLIT_UID = "split-parse-results";
    public static final String SPLIT_NAME = "Split Parse Results";
    public static final String LATE_ROUTE_UID = "route-late-events";
    public static final String LATE_ROUTE_NAME = "Route Late Events";
    public static final String NORMALIZED_SINK_UID = "kafka-normalized-events";
    public static final String NORMALIZED_SINK_NAME = "Kafka Normalized Events";
    public static final String INVALID_SINK_UID = "kafka-invalid-events";
    public static final String INVALID_SINK_NAME = "Kafka Invalid Events";
    public static final String LATE_SINK_UID = "kafka-late-events";
    public static final String LATE_SINK_NAME = "Kafka Late Events";
    public static final String GUARDRAIL_FILTER_NAME = "Filter Guardrail Findings";
    public static final String GUARDRAIL_AGGREGATES_1M_UID = "guardrail-aggregates-1m";
    public static final String GUARDRAIL_AGGREGATES_1M_NAME = "Guardrail Aggregates 1m";
    public static final String GUARDRAIL_AGGREGATES_5M_UID = "guardrail-aggregates-5m";
    public static final String GUARDRAIL_AGGREGATES_5M_NAME = "Guardrail Aggregates 5m";
    public static final String GUARDRAIL_AGGREGATES_SERIALIZE_UID = "serialize-guardrail-aggregates";
    public static final String GUARDRAIL_AGGREGATES_SERIALIZE_NAME = "Serialize Guardrail Aggregates";
    public static final String GUARDRAIL_AGGREGATES_SINK_UID = "kafka-guardrail-aggregates";
    public static final String GUARDRAIL_AGGREGATES_SINK_NAME = "Kafka Guardrail Aggregates";
    public static final String GUARDRAIL_WINDOW_1M_NAME = WindowNames.WINDOW_1_MINUTE;
    public static final String GUARDRAIL_WINDOW_5M_NAME = WindowNames.WINDOW_5_MINUTES;

    private JobTopology() {
    }
}
