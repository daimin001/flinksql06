package com.djt.chap04;

import com.djt.entity.ClickLogs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * DataStream转换为Table时指定EventTime属性
 */
public class EventTimeDefDataStream2Table1 {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //3、读取数据
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
        });

        //4、流转换为动态表
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                //3就是毫秒，0就是秒
                .column("cTime", DataTypes.TIMESTAMP(0))
                //以cTime字段减5秒作为水印
//                .watermark("cTime", $("cTime").minus(lit(5).seconds()))
                .watermark("cTime", "cTime - INTERVAL '5' SECOND")
                .build();
        Table table = tEnv.fromDataStream(clickLogs,schema);

        table.printSchema();
    }
}
