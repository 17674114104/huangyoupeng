package org.example.demo01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Random;

// 自定义数据源：模拟生成 Kafka 的事件流
public  class UserEventSource implements SourceFunction<UserEvent> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] eventTypes = {"register", "login"}; // 定义事件类型
    private final LocalDate startDate = LocalDate.now().minusDays(10); // 模拟从 10 天前开始的数据

    @Override
    public void run(SourceContext<UserEvent> ctx) throws Exception {
        int userCount = 1; // 模拟 10000 个用户

        for (int i = 0; i < userCount; i++) {
            if (!isRunning) {
                break;
            }

            // 每个用户生成一次注册事件
            String userId = "user_" + i;
            LocalDate registerDay = startDate.plusDays(random.nextInt(3)); // 注册时间集中在过去 3 天内
            String registerDate = registerDay.toString();
            long registerTimestamp = registerDay.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
            ctx.collect(new UserEvent(userId, EventType.REGISTER, registerTimestamp, registerDate));

            // 模拟该用户在未来几天内的登录事件
            int loginDays = random.nextInt(8); // 模拟最多 7 天内的登录
            for (int j = 0; j < loginDays; j++) {
                if (!isRunning) {
                    break;
                }

                LocalDate loginDay = registerDay.plusDays(j + 1);
                String loginDate = loginDay.toString();
                long loginTimestamp = loginDay.atStartOfDay().plusHours(random.nextInt(24)).plusMinutes(random.nextInt(60)).toEpochSecond(ZoneOffset.UTC) * 1000;
                ctx.collect(new UserEvent(userId, EventType.REGISTER, loginTimestamp,  loginDate));
            }

            // 控制生成速度
            Thread.sleep(100); // 每 1ms 生成一个用户事件
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}