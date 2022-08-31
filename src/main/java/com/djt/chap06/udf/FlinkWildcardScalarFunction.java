package com.djt.chap06.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 标量函数使用
 */
public class FlinkWildcardScalarFunction {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table userinfo = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT())
                ),
                row(1, "zhangsan",23),
                row(2L, "lisi",18)
        ).select($("id"), $("name"),$("age"));

        tEnv.createTemporaryView("users",userinfo);

        // 调用方式1：以call函数内联方式调用（不需要注册）
        tEnv.from("users").select(call(MyConcatFunction.class, $("*")))
                .execute()
                .print();

        //调用方式2：先注册在通过注册的名字调用
        //注册函数
        //tEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);
        tEnv.createTemporarySystemFunction("MyConcatFunction", new MyConcatFunction());

        // Table API使用：使用call函数调用已注册的UDF
        tEnv.from("users").select(call("MyConcatFunction", $("id"), $("name")))
                .execute()
                .print();

        // SQL使用(注意：通配符参数类型UDF函数，Flink SQL中不能使用，下面两段会报错)
        tEnv.sqlQuery("SELECT SubstringFunction(*) FROM users")
                .execute()
                .print();

        tEnv.sqlQuery("SELECT SubstringFunction(id,name) FROM users")
                .execute()
                .print();
    }

    /**
     * 支持通配符的标量函数
     */
    public static class MyConcatFunction extends ScalarFunction {

        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... fields) {
            return Arrays.stream(fields)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
        }
    }

}
