package com.bank.aisafetyops.infra.source;

import com.bank.aisafetyops.app.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Builds the Kafka source for raw AISafetyOps input events.
 *
 * <p>The factory keeps connector-specific setup outside the topology builder so
 * transport changes remain localized to the infrastructure layer.
 */
public final class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    public static KafkaSource<String> build(JobConfig config) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.topics())
                .setGroupId(config.consumerGroupId())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(config.startFromEarliest()
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .build();
    }
}
