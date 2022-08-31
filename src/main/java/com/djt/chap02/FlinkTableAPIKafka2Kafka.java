package com.djt.chap02;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 从kafka消费点击日志（JSON），转化为CSV格式之后输出到Kafka
 */
public class FlinkTableAPIKafka2Kafka {
    public static final String input_topic = "clicklog_input";
    public static final String output_topic = "clicklog_output";

    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建kafka source table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("cTime", DataTypes.STRING())
                .build();
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("kafka")
                .schema(schema)
                .format("json")
                .option("topic",input_topic)
                .option("properties.bootstrap.servers","node02:6667")
                .option("properties.group.id","testGroup")//每次都从最早的offsets开始
                .option("scan.startup.mode","latest-offset")
                .build());

        //3、创建kafka sink table
        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("kafka")
                .schema(schema)
                .format("csv")
                .option("topic",output_topic)
                .option("properties.bootstrap.servers","node02:6667")
                .build());

//        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
//                .schema(schema)
//                .build());

        //4、Table API从sourceTable读取数据并以csv格式写入sinkTable
        tEnv.from("sourceTable")
                .select($("user"), $("url"),$("cTime"))
                .executeInsert("sinkTable");
    }
}
