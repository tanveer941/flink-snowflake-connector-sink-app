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
            .url("https://<account-id>.<region>.snowflakecomputing.com")
            .user("<user>")
            .privateKey("<private-key>")
            .keyPassphrase("<key-passphrase>")
            .role("<role>")
            .bufferTimeMillis(2000L)
            .database("<database>")
            .schema("<schema>")
            .table("<table>")
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