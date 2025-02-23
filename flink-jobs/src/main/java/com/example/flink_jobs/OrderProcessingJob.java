package com.example.flink_jobs;

import com.example.flink_jobs.config.ConfigLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderProcessingJob {

    public static void main(String[] args) throws Exception {
        // 加载配置
        FlinkConfig config = ConfigLoader.loadConfig("application.yml", FlinkConfig.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupCheckpoint(env, config);

        // 1. 从Kafka读取数据
        DataStream<OrderEvent> sourceStream = env.fromSource(
                createKafkaSource(config),
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source"
        );

        // 2. 关联Redis维度数据
        DataStream<EnrichedOrder> enrichedStream = AsyncDataStream.unorderedWait(
                sourceStream,
                new RedisAsyncLookupFunction<>(config, "user_profiles"),
                30, TimeUnit.SECONDS, 100
        );

        // 3. 写入ClickHouse和MySQL
        enrichedStream.addSink(ClickHouseSink.create(config));
        enrichedStream.addSink(MySqlSink.create(config));

        env.execute("Real-time Order Processing");
    }

    private static KafkaSource<OrderEvent> createKafkaSource(FlinkConfig config) {
        return KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(config.getKafka().getBrokers())
                .setTopics(config.getKafka().getSourceTopic())
                .setGroupId(config.getKafka().getGroupId())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(OrderEvent.class))
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
    }

    private static void setupCheckpoint(StreamExecutionEnvironment env, FlinkConfig config) {
        env.enableCheckpointing(config.getCheckpointInterval());
        env.getCheckpointConfig().setCheckpointStorage(config.getCheckpointPath());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    }
}
