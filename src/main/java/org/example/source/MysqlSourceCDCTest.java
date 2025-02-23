package org.example.source;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.demo01.EventType;
import org.example.demo01.UserEvent;

public class MysqlSourceCDCTest {

    public static void main(String[] args) throws Exception {


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                //rm-wz9u5z016py624uz9.mysql.rds.aliyuncs.com
                .hostname("rm-wz9u5z016py624uz9.mysql.rds.aliyuncs.com") //内网地址
//                .hostname("rm-wz9u5z016py624uz9uo.mysql.rds.aliyuncs.com")
                .port(3306)
                .databaseList("flink_test") // monitor all tables under inventory database
                .tableList("flink_test.login_flow") // set captured table
                .username("flink_test")
                .password("flink_test123456")
                .deserializer(new JsonDebeziumDeserializationSchema())// converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(60000);  //checkpoint需要什么条件?com/ververica/cdc/connectors/mysql/source/enumerator/MySqlSourceEnumerator.snapshotState()

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .print("最终数据===>").setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("MySqlCdcPrint");
    }

    public static MySqlSource<UserEvent> registerFlow = MySqlSource.<UserEvent>builder()
            .hostname("rm-wz9u5z016py624uz9.mysql.rds.aliyuncs.com") //内网地址
//            .hostname("rm-wz9u5z016py624uz9uo.mysql.rds.aliyuncs.com")
            .port(3306)
            .databaseList("flink_test") // monitor all tables under inventory database
            .tableList("flink_test.register_flow") // set captured table
            .username("flink_test")
            .password("flink_test123456")
            .deserializer(new UserEventDeserializationSchema(EventType.REGISTER))// converts SourceRecord to JSON String
            .build();

    public static  MySqlSource<UserEvent> loginFlow = MySqlSource.<UserEvent>builder()
            .hostname("rm-wz9u5z016py624uz9.mysql.rds.aliyuncs.com") //内网地址
//            .hostname("rm-wz9u5z016py624uz9uo.mysql.rds.aliyuncs.com")
            .port(3306)
            .databaseList("flink_test") // monitor all tables under inventory database
            .tableList("flink_test.login_flow") // set captured table
            .username("flink_test")
            .password("flink_test123456")
            .deserializer(new UserEventDeserializationSchema(EventType.LOGIN))// converts SourceRecord to JSON String
            .build();
}
