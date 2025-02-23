package com.example.jobs.pojo;


public class UserEvent  {
    public String userId;        // 用户ID
    public EventType eventType;     // 事件类型：register 或 login
    public long eventTime;       // 事件时间戳
    public String eventDate;     // 事件日期，方便在输出时按日期分组

    public UserEvent(String userId, EventType eventType, long eventTime, String eventDate) {
        this.userId = userId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.eventDate = eventDate;
    }

    public String getUserId() {
        return userId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public String getEventDate() {
        return eventDate;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", eventDate='" + eventDate + '\'' +
                '}';
    }
}
