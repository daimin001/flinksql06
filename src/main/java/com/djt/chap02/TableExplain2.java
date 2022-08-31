package com.djt.chap02;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class TableExplain2 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = Schema.newBuilder()
                .column("count", DataTypes.INT())
                .column("word", DataTypes.STRING())
                .build();

        tEnv.createTemporaryTable("MySource1", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/source/path1")
                .format("csv")
                .build());
        tEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/source/path2")
                .format("csv")
                .build());
        tEnv.createTemporaryTable("MySink1", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/sink/path1")
                .format("csv")
                .build());
        tEnv.createTemporaryTable("MySink2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/sink/path2")
                .format("csv")
                .build());

        StatementSet stmtSet = tEnv.createStatementSet();

        Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
        stmtSet.addInsert("MySink1", table1);

        Table table2 = table1.unionAll(tEnv.from("MySource2"));
        stmtSet.addInsert("MySink2", table2);

        String explanation = stmtSet.explain();
        System.out.println(explanation);

    }
}
