//package org.example.demo01;
//
//
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//
//public class RetentionRateJob {
//
//    private static final long DAY_MILLIS = 24 * 60 * 60 * 1000L;  // 一天的毫秒数
//    private static final int[] RETENTION_DAYS = new int[]{1,2,3,4,5,6, 7, 30, 60, 90};
//
//    public static void main(String[] args) throws Exception {
//        // 创建 Flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic.EventTime);
//
//
//
//        // 配置 Kafka 消费源等（简化示例）
//
//        DataStream<UserEvent> eventStream = env.addSource(new UserEventSource1());
//        // 从 Kafka 或其他数据源读取数据流
//
//        // 为每个用户按注册时间分组，并进行留存计算
//        DataStream<RetentionResult> retentionStream = eventStream
//                .keyBy(UserEvent::getEventDate)   // 按用getEventDate分组
//                .process(new UserRetentionProcessFunction());
//
//        // 异步写入外部存储（例如 Kafka、ClickHouse 等）
//        retentionStream.addSink(new MyRetentionSink());
//
//        // 启动作业
//        env.execute("User Retention Rate Calculation");
//    }
//
//    // 处理每个用户的注册和登录事件
//    public static class UserRetentionProcessFunction extends KeyedProcessFunction<String, UserEvent, RetentionResult> {
//
//        private ValueState<Long> registrationTimeState;  // 保存注册时间
//        private MapState<Integer, Boolean> retentionState; // 保存不同天数的留存状态
//
//        @Override
//        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//            registrationTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("registrationTime", Long.class));
//            retentionState = getRuntimeContext().getMapState(new MapStateDescriptor<>("retentionState", Integer.class, Boolean.class));
//        }
//
//        @Override
//        public void processElement(UserEvent event, Context ctx, Collector<RetentionResult> out) throws Exception {
//            RetentionResult retentionResult = new RetentionResult(event.getEventDate());
//
//            if ("register".equals(event.getEventType())) {
//                // 处理注册事件，记录注册时间
//                registrationTimeState.update(event.getEventTime());
//                retentionResult.addRegistration(0); // 注册第一天
//            } else if ("login".equals(event.getEventType())) {
//                // 处理登录事件，检查该用户的注册时间以及登录的天数，更新留存数据
//                Long regTime = registrationTimeState.value();
//                if (regTime != null) {
//                    long delta = event.getEventTime() - regTime;
//
//                    for (int day : RETENTION_DAYS) {
//                        // 判断该登录事件是否属于某个留存天数
//                        if (delta >= (day - 1) * DAY_MILLIS && delta < day * DAY_MILLIS) {
//                            retentionResult.addRetention(day);  // 该天留存
//                        }
//                    }
//                }
//            }
//
//            // 输出每个用户的留存率数据
//            retentionResult.calculateAndPrintRetentionRates();
//            out.collect(retentionResult);
//        }
//    }
//
//    // 自定义 Sink，用于将数据写入外部系统（例如 Kafka、ClickHouse 等）
//    public static class MyRetentionSink implements org.apache.flink.streaming.api.functions.sink.SinkFunction<RetentionResult> {
//
//        @Override
//        public void invoke(RetentionResult value, Context context) throws Exception {
//            // 异步或同步写入外部系统
//            System.out.println("Outputting retention data: " + value);
//        }
//    }
//}
