package org.example.demo01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

// 自定义数据源：模拟生成 Kafka 的事件流
public  class RegisterUserEventSource implements SourceFunction<UserEvent> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] eventTypes = {"register", "login"}; // 定义事件类型
    private final LocalDate startDate = LocalDate.now().minusDays(10); // 模拟从 10 天前开始的数据

    public static Map<String,LocalDate> registerUserMap = new HashMap<>();

    @Override
    public void run(SourceContext<UserEvent> ctx) throws Exception {
        int userCount = 10; // 模拟 10000 个用户

        for (int i = 0; i < userCount; i++) {
            if (!isRunning) {
                break;
            }
            // 每个用户生成一次注册事件
            String userId = "user_" + i;
            LocalDate registerDay = startDate.plusDays(random.nextInt(3)); // 注册时间集中在过去 3 天内
            String registerDate = registerDay.toString();
            long registerTimestamp = registerDay.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
            System.out.println(new UserEvent(userId, EventType.REGISTER, registerTimestamp, registerDate));
            registerUserMap.put(userId, registerDay);
            ctx.collect(new UserEvent(userId, EventType.REGISTER, registerTimestamp, registerDate));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}