package com.djt.chap02;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class MixTableAPIAndSQL {
    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table
        Table projTable = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("user", DataTypes.STRING()),
                        DataTypes.FIELD("url", DataTypes.STRING()),
                        DataTypes.FIELD("cTime", DataTypes.STRING())
                ),
                row("Mary", "./home","2022-02-02 12:00:00"),
                row("Bob", "./cart","2022-02-02 12:00:00"),
                row("Mary", "./prod?id=1","2022-02-02 12:00:05"),
                row("Liz", "./home","2022-02-02 12:01:00"),
                row("Bob", "./prod?id=3","2022-02-02 12:01:30"),
                row("Mary", "./prod?id=7","2022-02-02 12:01:45")
        ).select($("user"), $("url"),$("cTime"));

        //注册表到catalog(可选的)
        tEnv.createTemporaryView("sourceTable", projTable);

        //3、创建sink table
        final Schema schema = Schema.newBuilder()
                .column("user", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .build();

        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("print")
                .schema(schema)
                .build());

        //4、Table API和SQL混用
        Table resultTable = tEnv.from("sourceTable").select($("user"), $("url"));
        tEnv.createTemporaryView("resultTableView", resultTable);

        Table result = tEnv.sqlQuery("select * from resultTableView where user = 'Mary'");

        //5、输出(包括执行,不需要单独在调用tEnv.execute("job"))
        result.executeInsert("sinkTable");
    }
}
