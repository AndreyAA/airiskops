package com.bank.airiskops.infra.source;

import com.bank.airiskops.app.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Builds the Kafka source for raw AIRiskOps input events.
 *
 * <p>The factory keeps connector-specific setup outside the topology builder so
 * transport changes remain localized to the infrastructure layer.
 */
public final class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    public static KafkaSource<String> build(JobConfig config) {
        return build(config.bootstrapServers(), config.topics(), config.consumerGroupId(), config.startFromEarliest());
    }

    public static KafkaSource<String> buildSingleTopic(
            String bootstrapServers,
            String topic,
            String groupId,
            boolean startFromEarliest
    ) {
        return build(bootstrapServers, java.util.List.of(topic), groupId, startFromEarliest);
    }

    private static KafkaSource<String> build(
            String bootstrapServers,
            java.util.List<String> topics,
            String groupId,
            boolean startFromEarliest
    ) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(startFromEarliest
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .build();
    }
}
