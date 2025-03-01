package com.kaf.stream;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class ExampleRecordDeserializer extends AbstractDeserializationSchema<ExampleRecord> {
    private static final long serialVersionUID = -432957416189445L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public ExampleRecord deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ExampleRecord.class);
    }
}