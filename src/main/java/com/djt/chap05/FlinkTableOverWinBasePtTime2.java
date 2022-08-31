package com.djt.chap05;

import com.djt.entity.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 基于处理时间开窗2：指定行到当前行开窗
 */
public class FlinkTableOverWinBasePtTime2 {
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

        //5、定义无界窗口(往前无界)
        Table result = table.window(Over
                .partitionBy($("sensorID"))
                .orderBy($("ptTime"))
                .preceding(rowInterval(2L))
                .as("w")
        ).select(
                $("sensorID"),
                $("temp").max().over($("w"))
        );

        //6、转换为流并打印
        tEnv.toDataStream(result, Row.class).print();

        //7、执行
        env.execute("FlinkTableOverWinBasePtTime1");
    }
}
