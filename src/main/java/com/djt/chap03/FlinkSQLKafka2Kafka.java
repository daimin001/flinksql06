package com.djt.chap03;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 以Flink SQL方式从kafka消费点击日志（JSON），转化为CSV格式之后输出到Kafka
 */
public class FlinkSQLKafka2Kafka {
    public static final String input_topic = "clicklog_input";
    public static final String output_topic = "clicklog_output";

    public static void main(String[] args) throws Exception {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、创建source table(DDL语句)-会自动注册表的
        tEnv.executeSql("CREATE TABLE sourceTable (" +
                "  `user` STRING," +
                "  `url` STRING," +
                "  `cTime` STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+input_topic+"'," +
                "  'properties.bootstrap.servers' = 'node02:6667'," +
                "  'properties.group.id' = 'test1'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")");

        //3、创建sink table(DDL)
        tEnv.executeSql("CREATE TABLE sinkTable (" +
                "  `user` STRING," +
                "  `url` STRING," +
                "  `cTime` STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+output_topic+"'," +
                "  'properties.bootstrap.servers' = 'node02:6667'," +
                "  'format' = 'csv'" +
                ")");

        tEnv.executeSql("CREATE TABLE sinkTable (" +
                "  `user` STRING," +
                "  `url` STRING," +
                "  `cTime` STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+output_topic+"'," +
                "  'properties.bootstrap.servers' = 'node02:6667'," +
                "  'format' = 'csv'" +
                ")");

        //4、执行SQL查询并输出结果
        tEnv.executeSql("INSERT INTO sinkTable " +
                "SELECT" +
                "   user," +
                "   url," +
                "   cTime " +
                "FROM sourceTable");

    }
}
