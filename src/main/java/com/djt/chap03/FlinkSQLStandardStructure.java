package com.djt.chap03;

import org.apache.flink.table.api.*;
/**
 * Flink SQL标准结构
 */
public class FlinkSQLStandardStructure {
    public static void main(String[] args) {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table(DDL语句)-会自动注册表的
        tEnv.executeSql("CREATE TABLE emp_info (" +
                "    emp_id INT," +
                "    name VARCHAR," +
                "    dept_id INT" +
                ") WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = 'data/emp/input/'," +
                "    'format' = 'csv'" +
                ")");//最后不要有分号,注意空格

        //3、创建sink table(DDL)
        //executeSql执行
        tEnv.executeSql("CREATE TABLE emp_info_copy (" +
                "    emp_id INT," +
                "    name VARCHAR," +
                "    dept_id INT" +
                ") WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = 'data/emp/output/'," +
                "    'format' = 'csv'" +
                ")");

        //4、执行SQL查询并输出结果
        Table resultTable = tEnv.sqlQuery("select * from emp_info where dept_id=10");
        tEnv.createTemporaryView("result",resultTable);

        tEnv.executeSql("INSERT INTO emp_info_copy " +
                "SELECT" +
                "   emp_id," +
                "   name," +
                "   dept_id " +
                "FROM result");
    }
}
