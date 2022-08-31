package com.djt.chap09;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 基于Group Windows实现Window Top N
 */
public class FlinkWinTopNBaseGW {
    public static void main(String[] args) throws Exception {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("productId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("orderTp", DataTypes.TIMESTAMP(0))
                .watermark("orderTp", "orderTp - INTERVAL '1' SECOND")
                .build();

        tEnv.createTemporaryTable("orders", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .format("csv")
                .option("path","data/orders/orders")
                .option("csv.field-delimiter",",")
                .build());
        tEnv.from("orders").execute().print();

        //
    }
}
