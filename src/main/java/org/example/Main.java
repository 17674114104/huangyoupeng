package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class Main {
    public static void main(String[] args) throws Exception {
// 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入流（此处用简单的从集合创建 DataStream 举例）
        DataStream<String> text = env.fromElements("Flink", "is", "awesome", "in", "local");

        // 对数据进行转换
        DataStream<String> transformed = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        // 输出结果
        transformed.print();

        // 执行 Flink 作业
        env.execute("Flink Local Job");
    }
}