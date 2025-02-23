package org.example.source;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.example.demo01.EventType;
import org.example.demo01.UserEvent;


public class UserEventDeserializationSchema implements DebeziumDeserializationSchema<UserEvent> {

    private EventType eventType;

    @Override
    public void deserialize(SourceRecord record, Collector<UserEvent> out) {
        Struct valueStruct = (Struct) record.value();
        Struct afterStruct = valueStruct.getStruct("after");
        System.out.println("接收到数据变更 record: " + record);
        if (afterStruct != null) {
            String userId = String.valueOf(afterStruct.getInt32("user_id"));
            long eventTime = afterStruct.getInt32("event_time");
            UserEvent userEvent = new UserEvent(userId, eventType, eventTime, null);
            out.collect(userEvent);
        }
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }

    public UserEventDeserializationSchema(){}

    public UserEventDeserializationSchema(EventType eventType ){
        this.eventType = eventType;
    }
}
