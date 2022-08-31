package com.djt.chap07;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 把元数据保存到Hive的catalog
 */
public class FlinkUseHiveCatalog {
    public static void main(String[] args) throws Exception {
        //1、创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //2、初始化hive catalog(为避免代码重复，我这里封装了一下)
        //访问未启用Kerberos的Hive
//        HiveUtils.initHive(tEnv);

        //访问启动Kerberos的Hive
        //写了个工具类来完成kerberos认证
        new KerberosAuth().kerberosAuth();
        HiveUtils.initHiveWithKerberos(tEnv);

        //3、查询Hive中的表
        tEnv.executeSql("select * from myrs.user_op_log")
                .print();

    }
}
