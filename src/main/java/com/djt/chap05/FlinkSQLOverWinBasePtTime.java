package com.djt.chap05;

import com.djt.entity.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLOverWinBasePtTime {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //3、读入数据
        DataStream<TempSensorData> tempSensorData = env.socketTextStream("localhost", 8888)
                .map(event -> {
                    String[] arr = event.split(",");
                    return TempSensorData
                            .builder()
                            .sensorID(arr[0])
                            .tp(Long.parseLong(arr[1]))
                            .temp(Integer.parseInt(arr[2]))
                            .build();
                });

        //4、流转换为动态表
        Table table = tEnv.fromDataStream(tempSensorData,
                $("sensorID"),
                $("tp"),
                $("temp"),
                $("ptTime").proctime()//新增ptTime字段为proctime
        );

        //5、定义无界Over窗口(往前无界)
        //Table API写法
//        Table result = table.window(Over
//                .partitionBy($("sensorID"))
//                .orderBy($("ptTime"))
//                .preceding(UNBOUNDED_RANGE)//可以省略不写
//                .as("w")
//        ).select(
//                $("sensorID"),
//                $("temp").max().over($("w"))
//        );

        //一次可以执行多个聚合，但是目前只支持在相同的over窗口上
        Table result = tEnv.sqlQuery("select sensorID," +
                "max(temp) OVER(PARTITION BY sensorID ORDER BY ptTime) as max_temp, " +
                "sum(temp) OVER(PARTITION BY sensorID ORDER BY ptTime) as sum_temp " +
                "from "+table);

        //6、指定并打印
        result.execute().print();
    }
}
