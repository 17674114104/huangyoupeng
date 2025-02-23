package com.example.connectors;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaConnector {
    public static FlinkKafkaConsumer<String> createConsumer(String topic, String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "flink-kafka-consumer");

        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}