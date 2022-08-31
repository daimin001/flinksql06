package com.djt.chap05;

import com.djt.entity.TempSensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 基于事件时间的滚动窗口
 */
public class FlinkTableTumbleWinBaseEventTime {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //3、读取数据并提取时间戳指定水印生成策略
        WatermarkStrategy<TempSensorData> watermarkStrategy = WatermarkStrategy
                .<TempSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TempSensorData>() {
                    @Override
                    public long extractTimestamp(TempSensorData element, long recordTimestamp) {
                        return element.getTp()*1000;
                    }
                });
        DataStream<TempSensorData> tempSensorData = env.socketTextStream("localhost", 8888)
                .map(event -> {
                    String[] arr = event.split(",");
                    return TempSensorData
                            .builder()
                            .sensorID(arr[0])
                            .tp(Long.parseLong(arr[1]))
                            .temp(Integer.parseInt(arr[2]))
                            .build();
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        //4、流转换为动态表
        Table table = tEnv.fromDataStream(tempSensorData,
                $("sensorID"),
                $("tp"),
                $("temp"),
                $("evTime").rowtime()//新增evTime字段为rowtime
        );

        //5、自定义窗口并计算
        Table result = table.window(Tumble
                .over(lit(5).second())
                .on($("evTime"))
                .as("w")
        )
                .groupBy($("sensorID"), $("w"))
                .select($("sensorID"), $("sensorID").count());

        //6、打印
        result.execute().print();
    }
}
