package com.kaf.stream;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ExampleEnrichedRecordSerializer implements KafkaRecordSerializationSchema<ExampleEnrichedRecord> {

    private static final long serialVersionUID = -432957416189445L;

    private String topic;
    private static final ObjectMapper objectMapper = JsonMapper.builder().build();

    public ExampleEnrichedRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ExampleEnrichedRecord r, KafkaSinkContext context, Long timestamp) {
        try {
            return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(r));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("serialize() failed " + r, e);
        }
    }
}