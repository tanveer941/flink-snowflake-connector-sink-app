package com.kaf.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

import io.deltastream.flink.connector.snowflake.sink.SnowflakeSink;
import io.deltastream.flink.connector.snowflake.sink.SnowflakeSinkBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import io.deltastream.flink.connector.snowflake.sink.context.SnowflakeSinkContext;
import io.deltastream.flink.connector.snowflake.sink.serialization.SnowflakeRowSerializationSchema;


import java.util.Properties;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

public class FlinkSnowflakeJob {

    private static void configureSourceBuilder(KafkaSourceBuilder<ExampleRecord> builder) {
        builder.setProperty("security.protocol", "SASL_SSL");
        builder.setProperty("sasl.mechanism", "AWS_MSK_IAM");
        builder.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        builder.setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
    }
    private static void configureSinkBuilder(KafkaSinkBuilder<ExampleEnrichedRecord> builder) {
        builder.setProperty("security.protocol", "SASL_SSL");
        builder.setProperty("sasl.mechanism", "AWS_MSK_IAM");
        builder.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        builder.setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
    }

    private static KafkaSource<ExampleRecord> getKafkaSource(Properties props, Properties managedFlinkConfig) {
        KafkaSourceBuilder<ExampleRecord> builder = KafkaSource.builder();

        if (!managedFlinkConfig.containsKey("local")) {
            configureSourceBuilder(builder);
        }

        return builder
                .setBootstrapServers(managedFlinkConfig.getProperty("bootstrap.servers"))
                .setTopics(managedFlinkConfig.getProperty("topics"))
                .setGroupId(managedFlinkConfig.getProperty("consumer.group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ExampleRecordDeserializer())
                .build();
    }

    private static KafkaSink<ExampleEnrichedRecord> getKafkaSink(Properties props, Properties managedFlinkConfig) {
        KafkaSinkBuilder<ExampleEnrichedRecord> builder = KafkaSink.builder();

        if (!managedFlinkConfig.containsKey("local")) {
            configureSinkBuilder(builder);
        }

        return builder
                .setBootstrapServers(managedFlinkConfig.getProperty("bootstrap.servers"))
                .setRecordSerializer(new ExampleEnrichedRecordSerializer("output"))
//                 .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static class RowPassThroughSerializer
        implements SnowflakeRowSerializationSchema<Object> {

        private static final long serialVersionUID = -23875899103249615L;

        @Override
        public Map<String, Object> serialize(
                Object element, SnowflakeSinkContext sinkContext) {
            return (Map<String, Object>) element;
        }
    }

    public static void main(String[] args) throws Exception{
        // Create a new SnowflakeSink
        SnowflakeSink SnowSink = SnowflakeSink.builder()
            .url("https://autodesk_dev.us-east-1.snowflakecomputing.com")
            .user("svc_aad_d_edh_sales_analytics")
            .privateKey("MIIFNTBfBgkqhkiG9w0BBQ0wUjAxBgkqhkiG9w0BBQwwJAQQ1xBbQvtIODLJwxdsgCTPnAICCAAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEAM0bsx/fqxBEv6s/LVZOVQEggTQj3SwFiQImKv8S9nV3TSQcioabpgs3GwkQaArmSUP89iuZutWrKUwihPT1jF7nt1TaPgkr2URQSapSa9qYxUdQ9tobfEzlvx/lx91L6bZWT73KYG9SIiKeqvmj8RmcWD3etlEHJk+YtUhWphBPqWTMHelFGGDw1Zj1AzYknSYCZvRpmJ1/8/N0nA8ocd40h7nmXii3NhrKYk18meG8bMJoIvUgIWc+EhBi3jCo+ix7jycoLCh6zZ4csFfAm4QhbFxEFhRwiWvAs9jw8pshyTg/2V+OhyutnJtpUq4mKMHJkLlcG68ylP24xxC5CkkAA3JYu9Ne6r3jT8Cyylo81MyxfGr3KfhCL0JKo821y7rke9yq6T5lf2Px9cyagVt6C+dbgCvfch9hfYrQZKHK6HyL/oUpkqmTbJ8TdjuobBRI8STrZoSAKHj4SE++eEUxkncYF6F97nZSfKlajxGDm/XzeDH9lNcK//M4Ul3+gsn5Y6qW9FNF4ke5+Ql1+pdfzKTsf6xBqW7HY4X5pSmLXTe0Tp9AVsCIse8cmAN8nuZrPIsNt9WBDH0IyiPXOOmgxlMgcXkwak+rGudjDeb4YUGPxK5Uq+xpb35aRVZsZBnX0YgbTj0M1rmJYuqRugGFaieakYgRge079hzvIj2okx+jjSwYE7oMzDXApyTf29pPtExhYHpoAqAAMxOjljrtQMN8LytV3fdVkpzrVGlYBuuuolq42eXbT5jlVbrWfJnm5+HebUebUXgE9yua04kQG6CeHcG+DTBLFBTj4iJ/IneKe2gJplwZIntcy5m28ZJSCrzSR7xmrjmRgShy3HYwtaOBvlYDNB7E5e0QSZRLc66P3j8HN3wnuGKR7wepLosDnk8t5n1G+y42H+r3w/0dOVqD1F5VhB7UTdcuqVlK/lTPDKDYsrJPn4mz0Cn3yJtI6C0T0mYSq3USg1sdFsh8F4oJRMBeqWBGkuRknwZ2dp9ozlYl12DG5fqlIMsipY2uJ38xETLaYJI232gtrvR1xdRQn16mg/MPgtuY9e28A99hlxwPxdXyBQDBSkUP9nxlItyF5xvR1A2uJIS7lxiOOIiCxxgfwHs9hFYpMtx+Flnkqk2dM/FpwkZpDKGrQP5Fh9MfIEOExgrLEqFpFvf0hRLYixxnXBp5QB/LzUHhSu3egDtHC0H+iKfnqbeF76y1YAW0mRDXK+PV73ahyrYLv2ydgS88tGtUo6wVOgl8BCK5zq75F8TGm8zcxVmLn0K3n/WmdTeVERLM2NsnQ+UpmCT7ODcvR5nKaPljuswQXS4JMmiK0LO/a3pxI+df0csXnK4x/c3ic3KpEsWVIFjDeu3L+T5LlK1ExwDGkDQO/6mYHN7SesX62D8ACr+MC1Cq3ggELaojitNUpq1jShgaHjsq38hcRJRHMgNLn1YXX/u8r+f8MWo2dlKPUA0gqElGt8bMQF56EYAWZy9LU+yPd/evaTm3ipRuCyL5uMaMiyisZv1SxHzldibmcJdh8e0vx+8nGEUNenWZ0rE+M9TpyEzj0kclTppaNArSvaQgC4CA3vbRlH7cUBR75IcxfFsuOEZvH1/4r67sVYKdxKLjXLAGiGZDwb1reQt1lDmTgg8JPfSLhLmJ+ZdUsPH7E5HkYc=")
            .keyPassphrase("thales-api")
            .role("EDH_INGEST_THALES_READWRITE")
            .bufferTimeMillis(2000L)
            .database("EDH_INGEST")
            .schema("THALES_INGEST")
            .table("EDH_TEST_USAGE")
            .serializationSchema(new RowPassThroughSerializer())
            .build("flink-snowflake-sink");
//

        Properties config = new Properties();
        config.put("bootstrap.servers", "127.0.0.1:9092");
        config.put("topics", "snow-topic");
        config.put("local", "1");
        config.put("consumer.group.id", "local-test-123");

//         buildCreate a new Flink job
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, // number of restart attempts
            org.apache.flink.api.common.time.Time.of(10, java.util.concurrent.TimeUnit.SECONDS) // delay between attempts
        ));
        env.setParallelism(2); // Set the parallelism to 2
        KafkaSource<ExampleRecord> kafkaSource = getKafkaSource(parameters.getProperties(), config);
//        KafkaSink<ExampleEnrichedRecord> kafkaSink = getKafkaSink(parameters.getProperties(), config);

        DataStream<ExampleRecord> ds = env.fromSource(kafkaSource,
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                "JSON source").returns(ExampleRecord.class);

        // This is an example job that takes a JSON string input from a Kafka topic
        // deserializes it to a Java data class, uses data from the input to add additional fields,
        DataStream<ExampleEnrichedRecord> enrichmentStage = ds.map(new RecordEnrinchmentFunction());
        DataStream<Map<String, Object>> mapStage = enrichmentStage.map(new ExampleEnrichedRecordToMapConverter());
        // Add the SnowflakeSink to the Flink job
        mapStage.sinkTo(SnowSink);
//         // Execute the Flink job
        env.execute("Flink Snowflake Job");
    }
}