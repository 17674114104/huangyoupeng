//package com.example.jobs;
//
//import com.ververica.cdc.debezium.DebeziumSourceFunction;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class MysqlCDCToKafkaJob {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        configureEnvironment(env);
//
//        // 加载YAML配置
//        Configuration config = loadYamlConfig("flink-config.yaml");
//
//        // 构建MySQL CDC Source
//        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
//                .hostname(config.getString("cdc.mysql.host"))
//                .port(config.getInteger("cdc.mysql.port"))
//                .databaseList(config.getString("cdc.mysql.databases"))
//                .tableList(config.getString("cdc.mysql.tables"))
//                .username(config.getString("cdc.mysql.username"))
//                .password(config.getString("cdc.mysql.password"))
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .serverTimeZone(config.getString("cdc.mysql.server-time-zone"))
//                .serverIdRange(config.getString("cdc.mysql.server-id-range"))
//                .build();
//
//        // 构建Kafka Sink
//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                .setBootstrapServers(config.getString("kafka.bootstrap.servers"))
//                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
//                        .setTopic(config.getString("kafka.topic"))
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .build();
//
//        // 构建处理流水线
//        env.addSource(mysqlSource)
//                .name("MySQL CDC Source")
//                .uid("mysql-cdc-source")
//                .rebalance()
//                .sinkTo(kafkaSink)
//                .name("Kafka Sink")
//                .uid("kafka-sink");
//
//        env.execute("MySQL CDC to Kafka Pipeline");
//    }
//
//    private static void configureEnvironment(StreamExecutionEnvironment env) {
//        env.enableCheckpointing(60000);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, Time.of(30, TimeUnit.SECONDS)));
//    }
//
//    private static Configuration loadYamlConfig(String path) {
//        // 实现YAML配置加载逻辑
//    }
//
//}
