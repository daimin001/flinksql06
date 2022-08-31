package com.djt.chap04;

import com.djt.entity.ClickLogs;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;


/**
 * DataStream转换为Table时指定EventTime属性
 */
public class EventTimeDefDataStream2Table2 {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //3、读取数据并提取时间戳指定水印生成策略
        WatermarkStrategy<ClickLogs> watermarkStrategy = WatermarkStrategy
                .<ClickLogs>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ClickLogs>() {
            @Override
            public long extractTimestamp(ClickLogs element, long recordTimestamp) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    return sdf.parse(element.getCTime()).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        DataStream<ClickLogs> clickLogs = env.fromElements(
                "Mary,./home,2022-02-02 12:00:00",
                "Bob,./cart,2022-02-02 12:00:00",
                "Mary,./prod?id=1,2022-02-02 12:00:05",
                "Liz,./home,2022-02-02 12:01:00",
                "Bob,./prod?id=3,2022-02-02 12:01:30",
                "Mary,./prod?id=7,2022-02-02 12:01:45"
        ).map(event -> {
            String[] props = event.split(",");
            return ClickLogs
                    .builder()
                    .user(props[0])
                    .url(props[1])
                    .cTime(props[2])
                    .build();
        }).assignTimestampsAndWatermarks(watermarkStrategy);

        //4、流转换为动态表
        //当字段名不存在时，重新添加一个字段作为时间属性
        Table table = tEnv.fromDataStream(clickLogs,$("user"),$("url"),$("cTime"),$("eTime").rowtime());
        //当字段名存在，且类型为Timestamp或者Long时，直接以现有字段作为时间属性(这里肯定报错)
//        Table table = tEnv.fromDataStream(clickLogs,$("user"),$("url"),$("cTime").rowtime());

        table.printSchema();

        //5、转换为流并打印
        tEnv.toDataStream(table, Row.class).print();

        //6、执行
        env.execute("EventTimeDefDataStream2Table2");
    }
}
