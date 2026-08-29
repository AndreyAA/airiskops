package com.bank.aisafetyops.app.usecase;

import com.bank.aisafetyops.app.config.JobConfig;
import com.bank.aisafetyops.app.functions.GuardrailAggregateKeySelector;
import com.bank.aisafetyops.app.functions.GuardrailWindowAggregateFunction;
import com.bank.aisafetyops.app.functions.GuardrailWindowProcessFunction;
import com.bank.aisafetyops.app.functions.ParseAndValidateFunction;
import com.bank.aisafetyops.app.functions.RouteLateEventsFunction;
import com.bank.aisafetyops.app.functions.SerializeGuardrailAggregateFunction;
import com.bank.aisafetyops.app.functions.SplitParseResultsFunction;
import com.bank.aisafetyops.app.support.FlinkEnvironmentDefaults;
import com.bank.aisafetyops.app.support.JobTopology;
import com.bank.aisafetyops.infra.parser.ParseResult;
import com.bank.aisafetyops.infra.serde.JsonSerde;
import com.bank.aisafetyops.infra.sink.KafkaSinkFactory;
import com.bank.aisafetyops.infra.source.KafkaSourceFactory;
import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.GuardrailAggregateKey;
import com.bank.aisafetyops.model.GuardrailWindowAggregate;
import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.LateEvent;
import com.bank.aisafetyops.model.SafetyEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public final class IncrementOneTopologyBuilder {
    private IncrementOneTopologyBuilder() {
    }

    public static void configure(StreamExecutionEnvironment env, JobConfig config) {
        // Increment 1 already uses checkpointing and periodic watermark emission
        // so that the local MVP behaves close to the runtime discipline required
        // later in NRTP production flows.
        env.enableCheckpointing(FlinkEnvironmentDefaults.CHECKPOINT_INTERVAL.toMillis());
        env.getConfig().setAutoWatermarkInterval(FlinkEnvironmentDefaults.AUTO_WATERMARK_INTERVAL.toMillis());

        DataStream<String> rawEvents = env
                .fromSource(
                        KafkaSourceFactory.build(config),
                        WatermarkStrategy.noWatermarks(),
                        JobTopology.SOURCE_UID)
                .uid(JobTopology.SOURCE_UID)
                .name(JobTopology.SOURCE_NAME);

        SingleOutputStreamOperator<ParseResult> parsedResults = rawEvents
                .map(new ParseAndValidateFunction())
                .uid(JobTopology.PARSE_UID)
                .name(JobTopology.PARSE_NAME);

        SingleOutputStreamOperator<SafetyEvent> validEvents = parsedResults
                .process(new SplitParseResultsFunction())
                .uid(JobTopology.SPLIT_UID)
                .name(JobTopology.SPLIT_NAME);

        // The watermark configuration follows the current NRTP agreement:
        // analysis windows are 1-5 minutes, but we still need bounded disorder
        // handling so a delayed guardrail finding does not freeze the whole stream.
        WatermarkStrategy<SafetyEvent> watermarks = WatermarkStrategy
                .<SafetyEvent>forBoundedOutOfOrderness(config.outOfOrderness())
                .withTimestampAssigner((SerializableTimestampAssigner<SafetyEvent>) (event, timestamp) -> event.eventTimeMillis())
                .withIdleness(config.idleTimeout());

        SingleOutputStreamOperator<SafetyEvent> onTimeEvents = validEvents
                .assignTimestampsAndWatermarks(watermarks)
                .process(new RouteLateEventsFunction(config.lateTolerance(), JobTopology.LATE_EVENTS))
                .uid(JobTopology.LATE_ROUTE_UID)
                .name(JobTopology.LATE_ROUTE_NAME);

        // Kafka output topics make the MVP inspectable and reusable outside Flink:
        // downstream readers, replay checks and manual incident analysis can all
        // consume the same normalized/invalid/late contracts.
        onTimeEvents
                .map(JsonSerde::toJson)
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().normalizedEventsTopic()))
                .uid(JobTopology.NORMALIZED_SINK_UID)
                .name(JobTopology.NORMALIZED_SINK_NAME);
        validEvents
                .getSideOutput(JobTopology.INVALID_EVENTS)
                .map((InvalidEvent invalidEvent) -> JsonSerde.toJson(invalidEvent))
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().invalidEventsTopic()))
                .uid(JobTopology.INVALID_SINK_UID)
                .name(JobTopology.INVALID_SINK_NAME);
        onTimeEvents
                .getSideOutput(JobTopology.LATE_EVENTS)
                .map((LateEvent lateEvent) -> JsonSerde.toJson(lateEvent))
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().lateEventsTopic()))
                .uid(JobTopology.LATE_SINK_UID)
                .name(JobTopology.LATE_SINK_NAME);

        // Stage 2 guardrail visibility starts only from already validated on-time
        // events, so aggregate dashboards inherit the same trust boundary as the
        // operational stream used for investigations.
        DataStream<SafetyEvent> guardrailFindings = onTimeEvents
                .filter(event -> event.eventType() == EventType.GUARDRAIL_FINDING)
                .name(JobTopology.GUARDRAIL_FILTER_NAME);

        DataStream<GuardrailWindowAggregate> guardrailAggregates1m = buildGuardrailAggregates(
                guardrailFindings,
                config,
                Time.minutes(1),
                JobTopology.GUARDRAIL_WINDOW_1M_NAME,
                JobTopology.GUARDRAIL_AGGREGATES_1M_UID,
                JobTopology.GUARDRAIL_AGGREGATES_1M_NAME
        );
        DataStream<GuardrailWindowAggregate> guardrailAggregates5m = buildGuardrailAggregates(
                guardrailFindings,
                config,
                Time.minutes(5),
                JobTopology.GUARDRAIL_WINDOW_5M_NAME,
                JobTopology.GUARDRAIL_AGGREGATES_5M_UID,
                JobTopology.GUARDRAIL_AGGREGATES_5M_NAME
        );

        guardrailAggregates1m
                .union(guardrailAggregates5m)
                .map(new SerializeGuardrailAggregateFunction())
                .uid(JobTopology.GUARDRAIL_AGGREGATES_SERIALIZE_UID)
                .name(JobTopology.GUARDRAIL_AGGREGATES_SERIALIZE_NAME)
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().guardrailAggregatesTopic()))
                .uid(JobTopology.GUARDRAIL_AGGREGATES_SINK_UID)
                .name(JobTopology.GUARDRAIL_AGGREGATES_SINK_NAME);
    }

    private static DataStream<GuardrailWindowAggregate> buildGuardrailAggregates(
            DataStream<SafetyEvent> guardrailFindings,
            JobConfig config,
            Time windowSize,
            String windowName,
            String uid,
            String operatorName
    ) {
        WindowedStream<SafetyEvent, GuardrailAggregateKey, TimeWindow> windowedStream = guardrailFindings
                .keyBy(new GuardrailAggregateKeySelector())
                .window(TumblingEventTimeWindows.of(windowSize))
                // Replay and real integrations can legitimately deliver guardrail
                // findings after related request/response events. We keep these
                // records inside the agreed NRTP tolerance instead of dropping
                // them at the window boundary.
                .allowedLateness(Time.milliseconds(config.lateTolerance().toMillis()));

        return windowedStream.aggregate(
                        new GuardrailWindowAggregateFunction(),
                        new GuardrailWindowProcessFunction(windowName)
                )
                .uid(uid)
                .name(operatorName);
    }
}
