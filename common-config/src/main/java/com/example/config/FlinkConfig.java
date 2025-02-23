package com.example.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class FlinkConfig {
    private final ParameterTool parameterTool;

//    public FlinkConfig(String[] args) {
//        this.parameterTool = ParameterTool.fromArgs(args);
//    }

    public FlinkConfig(String[] args) throws IOException {
        String configFilePath = System.getProperty("config.file.path", "/default/path/config.properties");
        parameterTool = ParameterTool.fromPropertiesFile(configFilePath);
    }
    // 从系统属性获取配置文件路径


    public String getKafkaBootstrapServers() {
        return parameterTool.get("kafka.bootstrap-servers", "localhost:9092");
    }

    public String getMysqlHostname() {
        return parameterTool.get("mysql.hostname", "localhost");
    }

    public String getMysqlPort() {
        return parameterTool.get("mysql.port", "3306");
    }

    public String getMysqlUsername() {
        return parameterTool.get("mysql.username", "test_user");
    }

    public String getMysqlPassword() {
        return parameterTool.get("mysql.password", "test_password");
    }

    public String getMysqlDatabase() {
        return parameterTool.get("mysql.database", "test_database");
    }

    public String getMysqlTable() {
        return parameterTool.get("mysql.table", "test_table");
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