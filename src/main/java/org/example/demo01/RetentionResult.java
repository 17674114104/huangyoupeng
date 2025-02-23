package org.example.demo01;

import java.util.HashMap;
import java.util.Map;

public class RetentionResult {
    private String eventDate;    // 日期
    private Map<Integer, Integer> registeredUsers = new HashMap<>();  // 各天的注册用户数
    private Map<Integer, Integer> retainedUsers = new HashMap<>();   // 各天的留存用户数

    public RetentionResult(String eventDate) {
        this.eventDate = eventDate;
    }

    public void addRegistration(int day) {
        registeredUsers.put(day, registeredUsers.getOrDefault(day, 0) + 1);
    }

    public void addRetention(int day) {
        retainedUsers.put(day, retainedUsers.getOrDefault(day, 0) + 1);
    }

    public void calculateAndPrintRetentionRates() {
        // 假设我们要计算这些天数的留存率
        for (int day : new int[]{1, 2, 3, 4, 5, 6, 7, 30, 60, 90}) {
            int registered = registeredUsers.getOrDefault(day, 0);
            int retained = retainedUsers.getOrDefault(day, 0);
            if (registered == 0) {
                System.out.println("No registrations for day " + day);
                continue;
            }
            double retentionRate = (double) retained / registered * 100;
            System.out.println("Day " + day + " retention: " + retained + " retained, " + registered + " registered, " + String.format("%.2f", retentionRate) + "% retention rate");
        }  }
}
