package com.djt.chap09;

import com.djt.entity.Orders;
import com.djt.entity.Product;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 这里演示下Flink的join
 */
public class FlinkJoin {
    public static void main(String[] args) throws Exception {
        //1、获取Stream执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //设置空闲状态清理时间,默认0标识永不清理
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //也可以用这种方式设置
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "5s");

        //3、读入订单数据
        DataStream<Orders> ordersStream = env.socketTextStream("localhost", 8888)
                .map(event -> {
                    String[] arr = event.split(",");
                    return Orders
                            .builder()
                            .user(arr[0])
                            .productId(Long.parseLong(arr[1]))
                            .amount(Integer.parseInt(arr[2]))
                            .orderTp(Long.parseLong(arr[3]))
                            .build();
                });

        //4、读取产品数据
        DataStream<Product> productStream = env.socketTextStream("localhost", 9999)
                .map(event -> {
                    String[] arr = event.split(",");
                    return Product
                            .builder()
                            .id(Long.parseLong(arr[0]))
                            .name(arr[1])
                            .build();
                });

        tEnv.createTemporaryView("orders",ordersStream);
        tEnv.createTemporaryView("product",productStream);

        //5、双流Join
        tEnv.sqlQuery("select * from orders o join product p on o.productId=p.id")
                .execute()
                .print();

    }
}
