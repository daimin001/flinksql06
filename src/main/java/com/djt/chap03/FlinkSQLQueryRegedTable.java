package com.djt.chap03;

import com.djt.entity.ClickLogs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Flink SQL查询已注册的表
 */
public class FlinkSQLQueryRegedTable {
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
        Table table = tEnv.fromDataStream(clickLogs);

        //5、注册表
        tEnv.createTemporaryView("sourceTable",table);

        //6、SQL查询已注册表
        Table resultTable = tEnv.sqlQuery("select user,count(url) as cnt from sourceTable group by user");

        //7、转换成流直接打印或者输出
        tEnv.toRetractStream(resultTable, Row.class).print();

        //8、执行任务
        env.execute("FlinkSQLQueryUnregTable");
    }
}
