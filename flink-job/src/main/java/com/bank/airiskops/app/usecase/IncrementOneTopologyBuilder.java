package com.bank.airiskops.app.usecase;

import com.bank.airiskops.app.config.JobConfig;
import com.bank.airiskops.app.functions.GuardrailAggregateKeySelector;
import com.bank.airiskops.app.functions.GuardrailQualityMetricFunction;
import com.bank.airiskops.app.functions.GuardrailWindowAggregateFunction;
import com.bank.airiskops.app.functions.GuardrailWindowProcessFunction;
import com.bank.airiskops.app.functions.ParseAndValidateFunction;
import com.bank.airiskops.app.functions.ParseIncidentPolicyUpdateFunction;
import com.bank.airiskops.app.functions.PolicyAwareSessionIncidentEvaluatorFunction;
import com.bank.airiskops.app.functions.RouteLateEventsFunction;
import com.bank.airiskops.app.functions.RuntimeContractMetricsFunction;
import com.bank.airiskops.app.functions.SessionIncidentKeySelector;
import com.bank.airiskops.app.functions.SerializeGuardrailAggregateFunction;
import com.bank.airiskops.app.functions.SplitParseResultsFunction;
import com.bank.airiskops.app.support.JobTopology;
import com.bank.airiskops.infra.parser.ParseResult;
import com.bank.airiskops.infra.serde.JsonSerde;
import com.bank.airiskops.infra.serde.JsonSerializeFunction;
import com.bank.airiskops.infra.sink.KafkaSinkFactory;
import com.bank.airiskops.infra.source.KafkaSourceFactory;
import com.bank.airiskops.model.BasicIncident;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.GuardrailAggregateKey;
import com.bank.airiskops.model.GuardrailQualityMetric;
import com.bank.airiskops.model.GuardrailWindowAggregate;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.InvalidEvent;
import com.bank.airiskops.model.LateEvent;
import com.bank.airiskops.model.SafetyEvent;
import com.bank.airiskops.model.WindowNames;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Builds the current AIRiskOps MVP streaming topology.
 *
 * <p>The topology ingests raw Kafka events, validates and normalizes them,
 * routes invalid and late records to side outputs, and computes 1-minute and
 * 5-minute event-time aggregates for guardrail findings.
 */
public final class IncrementOneTopologyBuilder {
    private IncrementOneTopologyBuilder() {
    }

    public static void configure(StreamExecutionEnvironment env, JobConfig config) {
        // Increment 1 already uses checkpointing and periodic watermark emission
        // so that the local MVP behaves close to the runtime discipline required
        // later in NRTP production flows.
        env.enableCheckpointing(config.checkpointInterval().toMillis());
        env.getConfig().setAutoWatermarkInterval(config.autoWatermarkInterval().toMillis());

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
        serializeToJson(onTimeEvents)
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

        DataStream<SafetyEvent> instrumentedOnTimeEvents = onTimeEvents
                .map(new RuntimeContractMetricsFunction(
                        config.runtimeContract(),
                        config.runtimeState(),
                        config.outOfOrderness(),
                        config.lateTolerance(),
                        config.idleTimeout(),
                        config.checkpointInterval(),
                        config.autoWatermarkInterval()
                ))
                .uid(JobTopology.RUNTIME_CONTRACT_UID)
                .name(JobTopology.RUNTIME_CONTRACT_NAME);

        // Stage 2 guardrail visibility starts only from already validated on-time
        // events, so aggregate dashboards inherit the same trust boundary as the
        // operational stream used for investigations.
        DataStream<SafetyEvent> guardrailFindings = instrumentedOnTimeEvents
                .filter(event -> event.eventType() == EventType.GUARDRAIL_FINDING)
                .name(JobTopology.GUARDRAIL_FILTER_NAME);

        DataStream<GuardrailWindowAggregate> combinedAggregates = buildGuardrailAggregates(guardrailFindings, config);

        combinedAggregates
                .map(new SerializeGuardrailAggregateFunction())
                .uid(JobTopology.GUARDRAIL_AGGREGATES_SERIALIZE_UID)
                .name(JobTopology.GUARDRAIL_AGGREGATES_SERIALIZE_NAME)
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().guardrailAggregatesTopic()))
                .uid(JobTopology.GUARDRAIL_AGGREGATES_SINK_UID)
                .name(JobTopology.GUARDRAIL_AGGREGATES_SINK_NAME);

        DataStream<GuardrailQualityMetric> qualityMetrics = combinedAggregates
                .map(new GuardrailQualityMetricFunction())
                .uid(JobTopology.GUARDRAIL_QUALITY_UID)
                .name(JobTopology.GUARDRAIL_QUALITY_NAME);

        serializeToJson(qualityMetrics)
                .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().guardrailQualityMetricsTopic()))
                .uid(JobTopology.GUARDRAIL_QUALITY_SINK_UID)
                .name(JobTopology.GUARDRAIL_QUALITY_SINK_NAME);

        if (config.incidentConfig().enabled()) {
            BroadcastStream<IncidentPolicy> policyUpdates = env
                    .fromSource(
                            KafkaSourceFactory.buildSingleTopic(
                                    config.bootstrapServers(),
                                    config.policyConfig().updatesTopic(),
                                    config.consumerGroupId() + "-policy-updates",
                                    config.startFromEarliest()
                            ),
                            WatermarkStrategy.noWatermarks(),
                            JobTopology.POLICY_SOURCE_UID
                    )
                    .uid(JobTopology.POLICY_SOURCE_UID)
                    .name(JobTopology.POLICY_SOURCE_NAME)
                    .flatMap(new ParseIncidentPolicyUpdateFunction())
                    .uid(JobTopology.POLICY_PARSE_UID)
                    .name(JobTopology.POLICY_PARSE_NAME)
                    .broadcast(JobTopology.INCIDENT_POLICY_BROADCAST_STATE);

            DataStream<BasicIncident> incidents = guardrailFindings
                    .filter(event -> Boolean.TRUE.equals(event.triggered()))
                    .name(JobTopology.TRIGGERED_GUARDRAIL_FILTER_NAME)
                    .keyBy(new SessionIncidentKeySelector())
                    .connect(policyUpdates)
                    // Session incidents intentionally run as a separate branch
                    // from the aggregate layer so we can evolve correlation logic
                    // without changing the existing NRT window contracts.
                    .process(new PolicyAwareSessionIncidentEvaluatorFunction(
                            config.incidentConfig(),
                            config.policyConfig().rejectOlderVersions(),
                            config.bootstrapIncidentPolicy(),
                            JobTopology.INCIDENT_POLICY_BROADCAST_STATE
                    ))
                    .uid(JobTopology.INCIDENT_EVALUATOR_UID)
                    .name(JobTopology.INCIDENT_EVALUATOR_NAME);

            serializeToJson(incidents)
                    .sinkTo(KafkaSinkFactory.build(config, config.outputTopics().basicIncidentsTopic()))
                    .uid(JobTopology.INCIDENT_SINK_UID)
                    .name(JobTopology.INCIDENT_SINK_NAME);
        }
    }

    static <T> SingleOutputStreamOperator<String> serializeToJson(DataStream<T> stream) {
        return stream
                .map(new JsonSerializeFunction<>())
                .returns(Types.STRING);
    }

    private static DataStream<GuardrailWindowAggregate> buildGuardrailAggregates(
            DataStream<SafetyEvent> guardrailFindings,
            JobConfig config
    ) {
        List<DataStream<GuardrailWindowAggregate>> aggregateStreams = new ArrayList<>();
        for (Duration aggregateWindow : config.runtimeContract().aggregateWindows()) {
            String windowName = WindowNames.forDuration(aggregateWindow);
            aggregateStreams.add(buildGuardrailAggregateWindow(
                    guardrailFindings,
                    config,
                    Time.milliseconds(aggregateWindow.toMillis()),
                    windowName,
                    "guardrail-aggregates-" + windowName,
                    "Guardrail Aggregates " + windowName
            ));
        }
        DataStream<GuardrailWindowAggregate> firstStream = aggregateStreams.get(0);
        if (aggregateStreams.size() == 1) {
            return firstStream;
        }
        DataStream<GuardrailWindowAggregate>[] remainingStreams = aggregateStreams
                .subList(1, aggregateStreams.size())
                .toArray(DataStream[]::new);
        return firstStream.union(remainingStreams);
    }

    private static DataStream<GuardrailWindowAggregate> buildGuardrailAggregateWindow(
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
