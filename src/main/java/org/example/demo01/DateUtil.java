package org.example.demo01;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class DateUtil implements Serializable {
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.of("UTC")); // 指定时区

    public static String format(TemporalAccessor temporal) {
        return formatter.format(temporal);
    }

    public static String format(Long time) {
        return formatter.format(Instant.ofEpochMilli(time));
    }


}
