package com.djt.chap07;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HiveUtils {
    /**
     * 初始化hive的相关资源
     * @param tableEnv
     */
    public static void initHive(TableEnvironment tableEnv) {
        String name            = "myhive";
        String version         = "3.1.0";
        String defaultDatabase = "default";
        String hiveConfDir     = "/etc/hive/conf";

        //加载HiveModule(可以使用hive的UDF)
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/modules/
        tableEnv.loadModule(name, new HiveModule(version));
        //使用hive方言(hivesql特有的语法)
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hiveCatalog);

        //设置当前sesion使用的catalog和databse
        tableEnv.useCatalog(name);
        tableEnv.useDatabase("myrs");
    }

    public static void initHiveWithKerberos(TableEnvironment tableEnv) throws IOException, InterruptedException {
        String name            = "myhive";
        String version         = "3.1.0";
        String defaultDatabase = "default";
        String hiveConfDir     = "data/etc/";

        //加载HiveModule(可以使用hive的UDF)
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/modules/
        tableEnv.loadModule(name, new HiveModule(version));
        //使用hive方言(hivesql特有的语法)
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //创建HiveCatalog
        HiveCatalog hiveCatalog = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<HiveCatalog>() {
            @Override
            public HiveCatalog run() throws Exception {
                return new HiveCatalog(name, defaultDatabase, hiveConfDir);
            }
        });

        tableEnv.registerCatalog(name, hiveCatalog);

        //设置当前sesion使用的catalog和databse
        tableEnv.useCatalog(name);
        tableEnv.useDatabase("myrs");
    }

}
