package com.djt.chap06.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.row;

/**
 * 标量函数使用
 */
public class FlinkBaseScalarFunction {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table userinfo = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("introduction", DataTypes.STRING())
                ),
                row(1, "zhangsan","In my spare time, I like to do anything relating to English such as listening to English songs, watching English movies or TV programs, or even attending the activities held by some English clubs or institutes. I used to go abroad for a short- term English study. During that time, I learned a lot of daily life English and saw a lot of different things. I think language is very interesting. I could express one substance by using different sounds. So I wish I could study and read more English literatures and enlarge my knowledge"),
                row(2L, "lisi","In my spare time, I like to do anything relating to English such as listening to English songs, watching English movies or TV programs, or even attending the activities held by some English clubs or institutes. I used to go abroad for a short- term English study. During that time, I learned a lot of daily life English and saw a lot of different things. I think language is very interesting. I could express one substance by using different sounds. So I wish I could study and read more English literatures and enlarge my knowledge")
        ).select($("id"), $("name"),$("introduction"));
        tEnv.createTemporaryView("users",userinfo);

        // 调用方式1：以call函数内联方式调用（不需要注册）
        tEnv.from("users").select($("id"), $("name"),call(SubstringFunction.class, $("introduction"), 5, 13))
                .execute()
                .print();

        //调用方式2：先注册在通过注册的名字调用
        //注册函数
        tEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

        // Table API使用：使用call函数调用已注册的UDF
        tEnv.from("users").select($("id"), $("name"),call(
                "SubstringFunction",
                $("introduction"), 5, 13
            )
        )
                .execute()
                .print();

        // SQL使用
        Table result = tEnv.sqlQuery("SELECT id,name,SubstringFunction(introduction, 5, 13) FROM users");
        result.printSchema();
        result.execute().print();
    }

    /**
     * 最简单的标量函数:
     */
    public static class SubstringFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }

}
