package com.example.jobs;

import com.example.config.FlinkConfig;
import com.example.connectors.KafkaConnector;
import com.example.jobs.pojo.EventType;
import com.example.jobs.pojo.UserEvent;
import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class UserRetentionAnalysis {

    public static void main(String[] args) throws Exception {
        FlinkConfig flinkConfig = new FlinkConfig(args);

        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.network.min", "512mb");
        conf.setString("taskmanager.memory.network.fraction", "0.1");
        conf.setString("taskmanager.memory.network.max", "512mb");
        conf.setBoolean("rest.flamegraph.enabled", true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        ExecutionConfig executionConfig = env.getConfig();
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<UserEvent> registerEvents = env.addSource(KafkaConnector.createConsumer("register-topic", flinkConfig.getKafkaBootstrapServers()))
                .name("Kafka-Register-Events")
                .uid("source-kafka-register-events");

        DataStream<UserEvent> loginEvents = env.addSource(KafkaConnector.createConsumer("login-topic", flinkConfig.getKafkaBootstrapServers()))
                .name("Kafka-Login-Events")
                .uid("source-kafka-login-events");

        DataStream<UserEvent> registrations = registerEvents
                .map((item) -> {
                    System.out.println("Received registration: " + item);
                    return item;
                })
                .filter(event -> EventType.REGISTER.equals(event.eventType));

        DataStream<UserEvent> logins = loginEvents.map((item) -> {
                    System.out.println("Received login: " + item);
                    return item;
                })
                .filter(event -> EventType.LOGIN.equals(event.eventType));

        DataStream<Tuple2<String, LocalDate>> dailyRegistrations = registrations
                .keyBy(event -> event.userId)
                .process(new DailyRegistrationProcessor())
                .name("Registration-Deduplication")
                .uid("process-registration-deduplication");

        DataStream<Tuple2<String, LocalDate>> dailyLogins = logins
                .keyBy(event -> event.userId)
                .process(new DailyLoginProcessor())
                .name("Login-Deduplication")
                .uid("process-login-deduplication");

        DataStream<Tuple3<LocalDate, Integer, String>> userLoginRetens = dailyLogins
                .keyBy(t -> t.f0)
                .connect(dailyRegistrations.keyBy(t -> t.f0))
                .process(new UserRetentionCalculator())
                .name("User-Retention-Calculation")
                .uid("connect-process-user-retention-calculation");

        userLoginRetens.keyBy(t -> t.f0)
                .connect(dailyRegistrations.keyBy(t -> t.f1))
                .process(new RetentionCalculator())
                .name("Retention-Metrics-Calculation")
                .uid("process-retention-metrics-calculation")
                .addSink(new MySQLSink(flinkConfig));

        env.execute("User Retention Analysis");
    }

    public static class MySQLSink extends RichSinkFunction<Tuple3<LocalDate, Integer, String>> {
        private transient Connection connection;
        private transient PreparedStatement preparedStatement;
        private final FlinkConfig flinkConfig;

        public MySQLSink(FlinkConfig flinkConfig) {
            this.flinkConfig = flinkConfig;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = ClickHouseConnector.getConnection(
                    flinkConfig.getClickHouseUrl(),
                    flinkConfig.getClickHouseUser(),
                    flinkConfig.getClickHousePassword());
            String sql = "INSERT INTO user_retention (register_date, retention_days, user_id) VALUES (?, ?, ?)";
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(Tuple3<LocalDate, Integer, String> value, Context context) throws SQLException {
            preparedStatement.setDate(1, java.sql.Date.valueOf(value.f0));
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setString(3, value.f2);
            preparedStatement.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static class DailyRegistrationProcessor extends KeyedProcessFunction<String, UserEvent, Tuple2<String, LocalDate>> {
        private ValueState<LocalDate> registrationDateState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<LocalDate> descriptor = new ValueStateDescriptor<>(
                    "registrationDate",
                    LocalDate.class
            );
            registrationDateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<Tuple2<String, LocalDate>> out) throws Exception {
            LocalDate registerDate = toLocalDate(event.eventTime);
            LocalDate storedDate = registrationDateState.value();

            if (storedDate == null || !storedDate.equals(registerDate)) {
                registrationDateState.update(registerDate);
                System.out.println(Tuple2.of(event.userId, registerDate));
                out.collect(Tuple2.of(event.userId, registerDate));
            }
        }
    }

    public static class DailyLoginProcessor extends KeyedProcessFunction<String, UserEvent, Tuple2<String, LocalDate>> {
        private transient ValueState<LocalDate> lastLoginDateState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<LocalDate> descriptor = new ValueStateDescriptor<>(
                    "lastLoginDate",
                    LocalDate.class
            );
            lastLoginDateState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<Tuple2<String, LocalDate>> out) throws Exception {
            LocalDate loginDate = toLocalDate(event.eventTime);
            LocalDate lastLogin = lastLoginDateState.value();

            if (lastLogin == null || !lastLogin.equals(loginDate)) {
                lastLoginDateState.update(loginDate);
                System.out.println(Tuple2.of(event.userId, loginDate));
                out.collect(Tuple2.of(event.userId, loginDate));
            }
        }
    }

    public static class UserRetentionCalculator extends KeyedCoProcessFunction<String, Tuple2<String, LocalDate>, Tuple2<String, LocalDate>, Tuple3<LocalDate, Integer, String>> {
        private transient ValueState<LocalDate> registerDateState;
        private transient ValueState<LocalDate> loginDateState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<LocalDate> registerDateDesc = new ValueStateDescriptor<>(
                    "registerDateState",
                    Types.LOCAL_DATE
            );
            registerDateState = getRuntimeContext().getState(registerDateDesc);

            ValueStateDescriptor<LocalDate> loginDateDesc = new ValueStateDescriptor<>(
                    "loginDateState",
                    Types.LOCAL_DATE
            );
            loginDateState = getRuntimeContext().getState(loginDateDesc);
        }

        @Override
        public void processElement1(Tuple2<String, LocalDate> login, Context ctx, Collector<Tuple3<LocalDate, Integer, String>> out) throws Exception {
            LocalDate registerDate = registerDateState.value();
            if (registerDate != null) {
                outDataFunction(registerDate, login.f1, login.f0, out);
            } else {
                loginDateState.update(login.f1);
            }
        }

        @Override
        public void processElement2(Tuple2<String, LocalDate> register, Context ctx, Collector<Tuple3<LocalDate, Integer, String>> out) throws IOException {
            registerDateState.update(register.f1);
            LocalDate loginDate = loginDateState.value();
            if (loginDate != null) {
                outDataFunction(register.f1, loginDate, register.f0, out);
                loginDateState.clear();
            }
        }

        private void outDataFunction(LocalDate registerDate, LocalDate loginDate, String userId, Collector<Tuple3<LocalDate, Integer, String>> out) {
            long daysBetween = Duration.between(
                    registerDate.atStartOfDay(),
                    loginDate.atStartOfDay()
            ).toDays();
            System.out.println(Tuple3.of(registerDate, (int) daysBetween, userId));
            out.collect(Tuple3.of(registerDate, (int) daysBetween, userId));
        }
    }

    public static class RetentionCalculator extends KeyedCoProcessFunction<LocalDate, Tuple3<LocalDate, Integer, String>, Tuple2<String, LocalDate>, RetentionResult> {
        private transient ValueState<RetentionMetrics> retentionMetrics;
        private transient ValueState<Long> nextTimerTimestamp;

        @Override
        public void open(Configuration parameters) {
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

        private void computeDataFunction(Tuple3<LocalDate, Integer, String> login, Tuple2<String, LocalDate> registerDate, Context ctx) throws IOException {
            RetentionMetrics metrics = retentionMetrics.value();
            if (registerDate != null) {
                if (metrics == null) {
                    metrics = new RetentionMetrics(registerDate.f1);
                }
                metrics.incrementRegistrations();
            }
            if (login != null) {
                if (metrics == null) {
                    metrics = new RetentionMetrics(login.f0);
                }
                metrics.updateRetention(login.f1);
            }
            retentionMetrics.update(metrics);

            long currentTime = ctx.timerService().currentProcessingTime();
            if (nextTimerTimestamp.value() == null) {
                long nextMinuteTimestamp = currentTime + Duration.ofSeconds(5).toMillis();
                nextTimerTimestamp.update(nextMinuteTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextMinuteTimestamp);
            }
        }

        @Override
        public void processElement1(Tuple3<LocalDate, Integer, String> login, Context ctx, Collector<RetentionResult> out) throws Exception {
            computeDataFunction(login, null, ctx);
        }

        @Override
        public void processElement2(Tuple2<String, LocalDate> registerDate, Context ctx, Collector<RetentionResult> out) throws Exception {
            computeDataFunction(null, registerDate, ctx);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<RetentionResult> out) throws Exception {
            if (timestamp == nextTimerTimestamp.value()) {
                long nextMinuteTimestamp = timestamp + Duration.ofSeconds(5).toMillis();
                nextTimerTimestamp.update(nextMinuteTimestamp);
                ctx.timerService().registerProcessingTimeTimer(nextMinuteTimestamp);
            }
            LocalDate date = toLocalDate(timestamp);
            RetentionMetrics metrics = retentionMetrics.value();
            if (metrics != null) {
                System.out.println("time metrics" + metrics.toResult());
                out.collect(metrics.toResult());
            } else {
                System.out.println(date + " time metrics is null");
            }
        }
    }

    public static LocalDate toLocalDate(long timestamp) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
    }

    public static long toEpochMilli(LocalDate date) {
        return date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

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

    public static class RetentionMetrics {
        private LocalDate registerDate;
        private int totalRegistrations;
        private Map<Integer, Integer> retentionDays = new HashMap<>();

        public RetentionMetrics() {
        }

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