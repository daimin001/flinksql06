package com.djt.chap02;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
/**
 * 从csv文件读取数据，然后以json格式输出到另外一个文件
 */
public class FlinkTableAPIFile2File {
    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("cTime", DataTypes.STRING())
                .build();
        tEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .format("csv")
                .option("path","data/clicklog/input/")
                .option("csv.field-delimiter",",")
                .build());

        //3、创建sink table
        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .format("json")
                .option("path","data/clicklog/output/")
                .build());

        //4、Table API从sourceTable读取数据并以json格式写入sinkTable
        tEnv.from("sourceTable")
                .select($("user"), $("url"),$("cTime"))
                .executeInsert("sinkTable");
    }
}
