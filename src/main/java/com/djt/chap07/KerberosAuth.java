package com.djt.chap07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosAuth {
    public void kerberosAuth() {
        try {
            /**
             * 代码在哪台服务器上执行，哪台服务器得安装Kerberos client,下面代码的意思是会自动通过如下命令生成认证票据，后续的操作就合法了
             * kinit -kt /etc/security/keytabs/hive.service.keytab hive/node01@DJT.COM
             *
             * 注意：如果要在本地运行，本地也得安装Kerberos client，能连接集群使用的KDC
             *
             * 访问Kerberos环境下的hive时，需要使用Hadoop API提供的UserGroupInformation类实现Kerberos账号登录认证，该API在登录Kerberos认证后，会启动一个线程定时的刷新认证
             */
            Configuration conf = new Configuration();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hive/node01@DJT.COM", "/etc/security/keytabs/hive.service.keytab");
            System.out.println(UserGroupInformation.getCurrentUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}