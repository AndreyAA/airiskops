package com.bank.aisafetyops.infra.sink;

import com.bank.aisafetyops.app.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Builds Kafka sinks for JSON payloads emitted by the AISafetyOps pipeline.
 *
 * <p>The factory centralizes delivery guarantees and serializer wiring so
 * topology code only has to choose the target topic.
 */
public final class KafkaSinkFactory {
    private KafkaSinkFactory() {
    }

    public static KafkaSink<String> build(JobConfig config, String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(config.runtimeContract().deliveryGuarantee().toFlinkDeliveryGuarantee())
                .build();
    }
}
