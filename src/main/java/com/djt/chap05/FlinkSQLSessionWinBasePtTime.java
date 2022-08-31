package com.djt.chap05;

import com.djt.entity.TempSensorData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * FlinkSQL基于处理时间的会话窗口
 */
public class FlinkSQLSessionWinBasePtTime {
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

        //5、基于SQL的滑动窗口(滑动步长在前)
        Table result = tEnv.sqlQuery("select " +
                "sensorID," +
                "count(sensorID)," +
                "SESSION_START(ptTime, INTERVAL '5' second) as winstart " +
                "from "+table+" GROUP BY sensorID,SESSION(ptTime, INTERVAL '5' second)");

        //6、指定并打印
        result.execute().print();
    }
}
