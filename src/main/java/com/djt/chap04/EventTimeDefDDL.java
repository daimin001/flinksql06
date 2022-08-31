package com.djt.chap04;

import org.apache.flink.table.api.*;

/**
 * DDL语句中指定EventTime属性
 */
public class EventTimeDefDDL {
    public static void main(String[] args) throws Exception {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table(DDL语句)-会自动注册表的
        tEnv.executeSql("CREATE TABLE user_behavior_log (" +
                "    user_id BIGINT," +
                "    item_id BIGINT," +
                "    category_id INT," +
                "    behavior VARCHAR," +
                //必须是TIMESTAMP
                "    ts TIMESTAMP(3)," +
                //声明 ts 为EventTime属性并使用5秒延迟水印策略
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
        ") WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = 'data/user_behavior/input/'," +
                "    'format' = 'csv'" +
                ")");//最后不要有分号,注意空格

        Table user_behavior_log = tEnv.from("user_behavior_log");
        user_behavior_log.printSchema();
        user_behavior_log.execute().print();
    }
}
