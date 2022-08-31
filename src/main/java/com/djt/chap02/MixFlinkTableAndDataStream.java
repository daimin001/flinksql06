package com.djt.chap02;

import com.djt.entity.ClickLogs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

/**
 * 混合使用Flink Table API和DataStream
 */
public class MixFlinkTableAndDataStream {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //3、读取数据(source)
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
        Table table = tEnv.fromDataStream(clickLogs);

        //5、执行Table API查询/SQL查询
        Table resultTable = table
                .where($("user").isEqual("Mary"))
                .select($("user"), $("url"), $("cTime"));

        //6、将Table转换为DataStream
        DataStream<ClickLogs> selectedClickLogs = tEnv.toDataStream(resultTable, ClickLogs.class);

        //7、处理结果：打印/输出
        selectedClickLogs.print();

        //8、执行
        env.execute("FlinkTableFirstExample");
    }
}
