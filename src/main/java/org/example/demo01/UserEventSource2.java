package org.example.demo01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDate;
import java.time.ZoneOffset;

// 自定义数据源：模拟单个用户从 2月1号注册并在 2月1号到 2月7号每天登录
public class UserEventSource2 implements SourceFunction<UserEvent> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<UserEvent> ctx) throws Exception {
        String userId = "user_1"; // 单一用户
        LocalDate registerDay = LocalDate.of(2025, 2, 1); // 注册时间：2月1号
        String registerDate = registerDay.toString();
        long registerTimestamp = registerDay.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        //ctx.collect(new UserEvent(userId,"register",  registerTimestamp,  registerDate));

        // 模拟该用户从 2月1号到 2月7号每天登录
        for (int i = 0; i < 7; i++) {
            LocalDate loginDay = registerDay.plusDays(i); // 登录日期：2月1号到2月7号
            String loginDate = loginDay.toString();
            long loginTimestamp = loginDay.atStartOfDay().plusHours(10).toEpochSecond(ZoneOffset.UTC) * 1000; // 固定时间点：每天 10:00
            UserEvent userEvent = new UserEvent(userId, EventType.LOGIN,  loginTimestamp,  loginDate);
            System.out.printf("userEvent"+userEvent);
            ctx.collect(userEvent);
            // 控制生成速度
            Thread.sleep(1000); // 每 10ms 生成一个事件
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
