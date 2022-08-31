package com.djt.chap06.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Table Functions
 */
public class FlinkTableFunction {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table scores = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("scores", DataTypes.STRING())
                ),
                row(1, "zhangsan","Chinese:90,Math:74,English:100"),
                row(2L, "lisi","Chinese:86,Math:99,English:92")
        ).select($("id"), $("name"),$("scores"));

        tEnv.createTemporaryView("scoresTable",scores);

        //注册函数
        tEnv.createTemporarySystemFunction("ScoresSplitFunction", new ScoresSplitFunction());

        // Table API使用：使用call函数调用已注册的UDF
        //这样直接使用是不可以的
//        tEnv.from("scoresTable")
//                .select($("id"), $("name"),call("ScoresSplitFunction", $("scores")))
//                .execute()
//                .print();
        //必须跟joinLateral(内连接)或者leftOuterJoinLateral(左外连接)搭配使用
        tEnv.from("scoresTable")
                .joinLateral(call(ScoresSplitFunction.class, $("scores"))
                        .as("subject","score")
                )
                .select($("id"), $("name"),$("subject"),$("score"))
                .execute()
                .print();

        tEnv.from("scoresTable")
                .leftOuterJoinLateral(call(ScoresSplitFunction.class, $("scores"))
                        .as("subject","score")
                )
                .select($("id"), $("name"),$("subject"),$("score"))
                .execute()
                .print();

        // SQL使用
        tEnv.sqlQuery("SELECT id, name, subject,score " +//诈裂出的字段得跟UDF中定义的字段名一样
                "FROM scoresTable," +//注意有个逗号
                "LATERAL TABLE(ScoresSplitFunction(scores))")
                .execute()
                .print();

        tEnv.sqlQuery("SELECT id, name, subject1,score1 " +
                        "FROM scoresTable " +
                        "LEFT JOIN LATERAL TABLE(ScoresSplitFunction(scores)) AS sc(subject1, score1) ON TRUE")
                .execute()
                .print();
    }

    /**
     * 学生成绩在一个字段里：
     *      1 zhangsan    Chinese:90,Math:74,English:100
     * 需要按照成绩拆分为多行记录:
     *      1 zhangsan Chinese 90
     *      1 zhangsan Math    74
     *      1 zhangsan English 100
     */
    @FunctionHint(output = @DataTypeHint("ROW<subject STRING, score INT>"))
    public static class ScoresSplitFunction extends TableFunction {
        public void eval(String str) {
            for (String s : str.split(",")) {
                String[] arr = s.split(":");
                //使用collect方法发出新的行
                collect(Row.of(arr[0], Integer.parseInt(arr[1])));
            }
        }
    }

}
