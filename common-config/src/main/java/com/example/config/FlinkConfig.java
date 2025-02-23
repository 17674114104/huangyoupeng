package com.example.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class FlinkConfig {
    private final ParameterTool parameterTool;

    public FlinkConfig(String[] args) {
        this.parameterTool = ParameterTool.fromArgs(args);
    }

    public String getKafkaBootstrapServers() {
        return parameterTool.get("kafka.bootstrap-servers", "localhost:9092");
    }

    public String getMysqlUrl() {
        return parameterTool.get("mysql.url", "jdbc:mysql://localhost:3306/test_database");
    }

    public String getMysqlUsername() {
        return parameterTool.get("mysql.username", "test_user");
    }

    public String getMysqlPassword() {
        return parameterTool.get("mysql.password", "test_password");
    }

    public String getHiveMetastoreUris() {
        return parameterTool.get("hive.metastore.uris", "thrift://localhost:9083");
    }

    public String getClickHouseUrl() {
        return parameterTool.get("clickhouse.url", "jdbc:clickhouse://localhost:8123/default");
    }

    public String getClickHouseUser() {
        return parameterTool.get("clickhouse.user", "default");
    }

    public String getClickHousePassword() {
        return parameterTool.get("clickhouse.password", "");
    }
}