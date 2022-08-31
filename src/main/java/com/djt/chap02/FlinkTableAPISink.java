package com.djt.chap02;

import org.apache.flink.table.api.*;
/**
 * FlinkTable API输出的例子：这里输出到Fink内置的专门用于测试用的blackhole Connector
 */
public class FlinkTableAPISink {
    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table：这里演示一下Flink内置的datagen Connector
        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .build())
                .option("rows-per-second", "5")
                .option("fields.name.length","10")
                .option("fields.age.kind","random")
                .option("fields.age.min","1")
                .option("fields.age.max","100")
                .build();

        //注册表到catalog(可选的)
        tEnv.createTemporaryTable("sourceTable", sourceDescriptor);

        //3、创建sink table
        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();

        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //4、查询
        Table resultTable = tEnv.from("sourceTable");

        //5、输出
        resultTable.executeInsert("sinkTable");
    }
}
