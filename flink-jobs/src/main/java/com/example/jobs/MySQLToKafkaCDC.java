package com.example.jobs;

public class MySQLToKafkaCDC {

//    public static void main(String[] args) throws Exception {
//        FlinkConfig flinkConfig = new FlinkConfig(args);
//
//        Configuration conf = new Configuration();
//        conf.setString("taskmanager.memory.network.min", "512mb");
//        conf.setString("taskmanager.memory.network.fraction", "0.1");
//        conf.setString("taskmanager.memory.network.max", "512mb");
//        conf.setBoolean("rest.flamegraph.enabled", true);
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        ExecutionConfig executionConfig = env.getConfig();
//        env.getConfig().setAutoWatermarkInterval(1000);
//
//        SourceFunction<String> mysqlSource = MySQLCDCConnector.createSource(
//                flinkConfig.getMysqlHostname(),
//                flinkConfig.getMysqlPort(),
//                flinkConfig.getMysqlUsername(),
//                flinkConfig.getMysqlPassword(),
//                flinkConfig.getMysqlDatabase(),
//                flinkConfig.getMysqlTable()
//        );
//
//        SinkFunction<String> kafkaSink = (SinkFunction<String>) KafkaConnector.createSink("mysql-cdc-topic", flinkConfig.getKafkaBootstrapServers());
//
//        DataStream<String> cdcStream = env.addSource(mysqlSource)
//                .name("MySQL-CDC-Source");
//
//        cdcStream
//                .addSink(kafkaSink)
//                .name("Kafka-Sink");
//
//        env.execute("MySQL CDC to Kafka");
//    }
}