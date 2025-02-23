package com.example.connectors;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveConnector {

    public static void registerHiveCatalog(StreamTableEnvironment tableEnv, String hiveMetastoreUris) {
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/opt/hive-conf"; // a local path
        String version = "2.3.6";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        hive.getHiveConf().set("hive.metastore.uris", hiveMetastoreUris);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
    }
}