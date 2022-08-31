package com.djt.chap05;

import com.djt.entity.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 基于时间的滑动窗口
 */
public class FlinkTableSlideWinBasePtTime {
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

        //5、自定义窗口并计算
        Table result = table.window(Slide
                .over(lit(8).second())
                .every(lit(2).second())
                .on($("ptTime"))
                .as("w")
        )
                .groupBy($("sensorID"), $("w"))
                .select($("sensorID"), $("sensorID").count());

        //6、转换为流并打印
        tEnv.toDataStream(result, Row.class).print();

        //7、执行
        env.execute("FlinkTableTumbleWinBaseEvTime");
    }
}
