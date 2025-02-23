package org.example.demo01;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.source.MysqlSourceCDCTest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class UserRetentionAnalysis2 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 增加网络缓冲区的最小和最大数量
        conf.setString("taskmanager.memory.network.min", "512mb");
        conf.setString("taskmanager.memory.network.fraction", "0.1");
        conf.setString("taskmanager.memory.network.max", "512mb");
        conf.setBoolean("rest.flamegraph.enabled", true);




        // 启用查询状态
//        conf.setString("queryable-state.proxy.ports", "9067");
//        conf.setString("queryable-state.server.ports", "9069");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置 ExecutionConfig
        ExecutionConfig executionConfig = env.getConfig();
        // 设置水印间隔
        env.getConfig().setAutoWatermarkInterval(1000);

        // 数据源（假设已实现）
        DataStream<UserEvent> registerEvents = env.fromSource(MysqlSourceCDCTest.registerFlow,WatermarkStrategy.noWatermarks(),"MySQL-注册事件").name("MySQL-注册事件").uid("source-kafka-user-register-topic-v1");
        DataStream<UserEvent> loginEvents = env.fromSource(MysqlSourceCDCTest.loginFlow,  WatermarkStrategy.noWatermarks(), // 如果需要处理事件时间，可以配置 WatermarkStrategy
                "MySQL-登录事件").name("MySQL-登录事件").uid("source-kafka-user-login-topic-v1");
        // 定义水印策略
//        WatermarkStrategy watermarkStrategy = WatermarkStrategy
//                .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner((event, timestamp) -> event.eventTime);

//        DataStream<UserEvent> timedEvents = events.assignTimestampsAndWatermarks(watermarkStrategy);

        // 分离注册事件和登录事件
        DataStream<UserEvent> registrations = registerEvents
                .map((item)->{
                    System.out.println("收到一个注册:"+item);
                    return item;
                })
                .filter(event -> EventType.REGISTER.equals(event.eventType));

        DataStream<UserEvent> logins = loginEvents.map((item)->{
                    System.out.println("收到一个登录:"+item);
                    return item;
                })
                .filter(event -> EventType.LOGIN.equals(event.eventType));

        // 处理注册事件（每日去重）
        DataStream<Tuple2<String, LocalDate>> dailyRegistrations = registrations
                .keyBy(event -> event.userId)
                .process(new DailyRegistrationProcessor()).name("注册去重").uid("process-uk-user-register-v1");

        // 处理登录事件（每日首次登录）
        DataStream<Tuple2<String, LocalDate>> dailyLogins = logins
                .keyBy(event -> event.userId)
                .process(new DailyLoginProcessor()).name("登录去重").uid("process-uk-user-login-v1");

        // 用户ID   关联注册时间 得到用户属于X次留
        DataStream<Tuple3<LocalDate, Integer, String>> userLoginRetens = dailyLogins
                .keyBy(t->t.f0)
                .connect(dailyRegistrations.keyBy(t -> t.f0))
                .process(new UserRetentionCalculator()).name("用户关联注册时间得到X次留").uid("connect-process-user-x-stay-v1");

        // 用户注册时间 keyby 计算留存指标

        userLoginRetens.keyBy(t -> t.f0)
                .connect(dailyRegistrations.keyBy(t -> t.f1))
                .process(new RetentionCalculator()).name("注册时间计算留存指标").uid("calculator-process-user-x-retention-v1")
                .print();

        env.execute("User Retention Analysis");

    }

    // 每日注册用户处理（去重）
    public static class DailyRegistrationProcessor
            extends KeyedProcessFunction<String, UserEvent, Tuple2<String, LocalDate>> {

        private ValueState<LocalDate> registrationDateState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<LocalDate> descriptor = new ValueStateDescriptor<>(
                    "registrationDate",
                    LocalDate.class
            );
            // 设置状态TTL为90天
//            descriptor.enableTimeToLive( Time.days(90));
            registrationDateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                UserEvent event,
                Context ctx,
                Collector<Tuple2<String, LocalDate>> out) throws Exception {
            LocalDate registerDate = toLocalDate(event.eventTime);
            LocalDate storedDate = registrationDateState.value();

            if (storedDate == null || !storedDate.equals(registerDate)) {
                registrationDateState.update(registerDate);
                System.out.println(Tuple2.of(event.userId, registerDate));
                out.collect(Tuple2.of(event.userId, registerDate));
            }
        }
    }

    // 每日首次登录处理
    public static class DailyLoginProcessor
            extends KeyedProcessFunction<String, UserEvent, Tuple2<String, LocalDate>> {

        private transient ValueState<LocalDate> lastLoginDateState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<LocalDate> descriptor = new ValueStateDescriptor<>(
                    "lastLoginDate",
                    LocalDate.class
            );
//            descriptor.enableTimeToLive(Time.days(90));
            lastLoginDateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                UserEvent event,
                Context ctx,
                Collector<Tuple2<String, LocalDate>> out) throws Exception {
            LocalDate loginDate = toLocalDate(event.eventTime);
            LocalDate lastLogin = lastLoginDateState.value();

            if (lastLogin == null || !lastLogin.equals(loginDate)) {
                lastLoginDateState.update(loginDate);
                System.out.println(Tuple2.of(event.userId, loginDate));
                out.collect(Tuple2.of(event.userId, loginDate));
            }
        }
    }

    // 留存计算核心逻辑
    public static class UserRetentionCalculator extends KeyedCoProcessFunction<
                String, Tuple2<String, LocalDate>, Tuple2<String, LocalDate>, Tuple3<LocalDate, Integer, String>> {

        private transient ValueState<LocalDate> registerDateSate;
        private transient ValueState<LocalDate> loginDateSate;


        @Override
        public void open(Configuration parameters) {
            //
            ValueStateDescriptor<LocalDate> registerDateDesc = new ValueStateDescriptor<>(
                    "registerDateSate",
                        Types.LOCAL_DATE
            );
            registerDateSate = getRuntimeContext().getState(registerDateDesc);

            ValueStateDescriptor<LocalDate> loginDateDesc = new ValueStateDescriptor<>(
                    "loginDateSate",
                    Types.LOCAL_DATE
            );
            loginDateSate = getRuntimeContext().getState(loginDateDesc);
        }

        @Override
        public void processElement1(
                Tuple2<String, LocalDate> login,
                Context ctx,
                Collector<Tuple3<LocalDate, Integer, String>> out) throws Exception {
            LocalDate registerDate = registerDateSate.value();
            if (registerDate != null) {
                outDataFunction(registerDate, login.f1, login.f0, out);
            }else{
                loginDateSate.update(login.f1);
            }
        }

        @Override
        public void processElement2(
                Tuple2<String, LocalDate> register,
                Context ctx,
                Collector<Tuple3<LocalDate, Integer, String>> out) throws IOException {

            registerDateSate.update(register.f1);

            LocalDate loginDate = loginDateSate.value();
            if (loginDate != null) {
                outDataFunction(register.f1, loginDate, register.f0, out);
                loginDateSate.clear();
            }
        }

        private void outDataFunction(LocalDate registerDate ,LocalDate loginDate, String userId, Collector<Tuple3<LocalDate, Integer, String>> out){
            long daysBetween = Duration.between(
                    registerDate.atStartOfDay(),
                    loginDate.atStartOfDay()
            ).toDays();
            System.out.println(Tuple3.of(registerDate, (int) daysBetween, userId));
            out.collect(Tuple3.of(registerDate, (int) daysBetween, userId));
        }
    }

    public static class RetentionCalculator extends KeyedCoProcessFunction<
            LocalDate, Tuple3<LocalDate, Integer, String>, Tuple2<String, LocalDate>, RetentionResult> {

        private transient ValueState<RetentionMetrics> retentionMetrics;
        private transient ValueState<Long> nextTimerTimestamp;

        @Override
        public void open(Configuration parameters) {
            // 留存指标状态（注册日期 -> 指标）
            ValueStateDescriptor<RetentionMetrics> metricsDesc = new ValueStateDescriptor<>(
                    "retentionMetrics",
                    TypeInformation.of(RetentionMetrics.class)
            );
            retentionMetrics = getRuntimeContext().getState(metricsDesc);

            ValueStateDescriptor<Long> nextTimerTimestampDesc = new ValueStateDescriptor<>(
                    "nextTimerTimestamp",
                    Types.LONG
            );
            nextTimerTimestamp = getRuntimeContext().getState(nextTimerTimestampDesc);
        }


        //计算累加数据
        private void computeDataFunction(Tuple3<LocalDate, Integer, String> login, Tuple2<String, LocalDate> registerDate,  Context ctx) throws IOException {
            RetentionMetrics metrics = retentionMetrics.value();
            if (registerDate!=null){
                if(metrics == null) {
                    metrics = new RetentionMetrics(registerDate.f1);
                }
                // 更新总注册数
                metrics.incrementRegistrations();
            }
            if (login!= null) {
                if(metrics == null) {
                    metrics = new RetentionMetrics(login.f0);
                }
                //次留人数+1
                metrics.updateRetention(login.f1);
            }
            retentionMetrics.update(metrics);


            long currentTime = ctx.timerService().currentProcessingTime();
            if (nextTimerTimestamp.value() == null) {
                // 首次处理，注册第一个每分钟定时器
                long nextMinuteTimestamp = currentTime + Duration.ofSeconds(5).toMillis();
                nextTimerTimestamp.update(nextMinuteTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextMinuteTimestamp);
            }
        }
        @Override
        public void processElement1(
                //注册时间 X次留 用户ID
                Tuple3<LocalDate, Integer, String> login,
                Context ctx,
                Collector<RetentionResult> out) throws Exception {

                computeDataFunction(login, null, ctx);

        }

        @Override
        public void processElement2(
                Tuple2<String, LocalDate> registerDate,
                Context ctx,
                Collector<RetentionResult> out) throws Exception {

            computeDataFunction(null, registerDate, ctx);
        }




        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<RetentionResult> out) throws Exception {
            if (timestamp == nextTimerTimestamp.value()) {
                // 定时器触发，执行逻辑
                System.out.println("每分钟触发一次逻辑，当前时间: " + timestamp);
                // 注册下一个分钟的定时器
                long nextMinuteTimestamp = timestamp + Duration.ofSeconds(5).toMillis();
                nextTimerTimestamp.update(nextMinuteTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextMinuteTimestamp);
            }
            LocalDate date = toLocalDate(timestamp);
            RetentionMetrics metrics = retentionMetrics.value();
            if (metrics != null) {
                System.out.println("time metrics"+ metrics.toResult());
                out.collect(metrics.toResult());
            }else{
                System.out.println(date+" time metrics is null");
            }
        }
    }

    // 辅助方法
    public static LocalDate toLocalDate(long timestamp) {
        return Instant.ofEpochMilli(timestamp*1000)
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

    public static long toEpochMilli(LocalDate date) {
        return date.atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    // 留存结果数据结构
    public static class RetentionResult {
        public LocalDate registerDate;
        public Map<Integer, Double> retentionRates = new HashMap<>();

        public RetentionResult(LocalDate registerDate) {
            this.registerDate = registerDate;
        }

        @Override
        public String toString() {
            return "RetentionResult{" +
                    "registerDate=" + registerDate +
                    ", retentionRates=" + retentionRates +
                    '}';
        }
    }

    // 留存指标聚合
    public static class RetentionMetrics{
        private LocalDate registerDate;
        private int totalRegistrations;
        private Map<Integer, Integer> retentionDays = new HashMap<>();
        public RetentionMetrics(){}

        public RetentionMetrics(LocalDate registerDate) {
            this.registerDate = registerDate;
        }

        public void incrementRegistrations() {
            totalRegistrations++;
        }

        public void updateRetention(int days) {
            retentionDays.merge(days, 1, Integer::sum);
        }

        public RetentionResult toResult() {
            RetentionResult result = new RetentionResult(registerDate);
            retentionDays.forEach((days, count) ->
                    result.retentionRates.put(
                            days,
                            (double) count / totalRegistrations
                    )
            );
            return result;
        }

        @Override
        public String toString() {
            return "RetentionMetrics{" +
                    "registerDate=" + registerDate +
                    ", totalRegistrations=" + totalRegistrations +
                    ", retentionDays=" + retentionDays +
                    '}';
        }
    }
}